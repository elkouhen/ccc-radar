import fnmatch
import hashlib
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from archlens.config import Config
from archlens.inventory_freshness import current_endpoint_inventory_signature
from archlens.models import MessageEndpoint
from archlens.modules import (
    discover_module_dependencies,
    discover_modules,
    discover_excluded_module_paths,
)
from archlens.relations import build_architecture_relations
from archlens.scanner import (
    clear_analysis_caches,
    infer_framework_endpoints,
    infer_kafka_endpoints,
    infer_kafka_topic_strategy1_endpoints,
    infer_json_kafka_flow_graph_endpoints,
    infer_markdown_topic_manifest_endpoints,
    apply_kafka_topic_strategy1,
)
from archlens.store import Store


ProgressCallback = Callable[[str], None]


@dataclass
class IndexReport:
    scanned: int
    skipped: int
    findings_added: int
    findings_removed: int
    deleted_files: int
    endpoints_added: int = 0
    endpoints_removed: int = 0


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _matches_any(rel_path: str, patterns: list[str]) -> bool:
    return any(pattern == "**/*" or fnmatch.fnmatch(rel_path, pattern) for pattern in patterns)


def _is_maven_or_gradle_test_source_set(source_set: str) -> bool:
    """`main` est le seul nom de source set universel en Maven/Gradle ;
    ses variants de test suivent tous la convention `test` ou
    `<prefixe>Test` (`componentTest`, `contractTest`, `endToEndTest`, ...).
    BACKLOG-16 P1 : restreint `_is_test_source` à cette convention, plutôt
    qu'à « tout ce qui suit `src/` et n'est pas `main` » — cette dernière
    règle confondait n'importe quel layout `src/<package>` (Python, JS,
    Rust, y compris ce projet lui-même) avec un jeu de sources de test."""
    return source_set == "test" or source_set.endswith("Test")


def _is_test_source(rel_path: str) -> bool:
    """BACKLOG-15 H2 (ADR-34) : tout fichier sous un dossier `src/<jeu-de-
    sources>` où `<jeu-de-sources>` suit la convention Maven/Gradle de
    nommage des source sets de test (voir
    `_is_maven_or_gradle_test_source_set`) est exclu du scan, findings et
    endpoints confondus. Décision explicite qui revient sur BACKLOG-2 R2/
    ADR-14 (« ne jamais exclure silencieusement les tests ») — voir ADR-34.
    Basé sur les segments du chemin, pas un pattern glob : un `fnmatch`
    avec `*` ne respecte pas les frontières de répertoire et confondrait un
    paquet nommé `testutils` sous `src/main/...` avec un vrai jeu de
    sources de test."""
    segments = rel_path.split("/")
    return any(
        segment == "src"
        and i + 1 < len(segments)
        and _is_maven_or_gradle_test_source_set(segments[i + 1])
        for i, segment in enumerate(segments)
    )


def _is_strategy1_openapi_declaration(rel_path: str) -> bool:
    path = Path(rel_path)
    parts = path.parts
    return path.suffix.casefold() == ".rest" and any(
        parts[index:index + 4] == ("src", "main", "resources", "openapi")
        for index in range(max(0, len(parts) - 3))
    )


def _strategy1_requires_full_reindex(
    changed_or_deleted: set[str], repo_root: Path, modules: list
) -> bool:
    """Whether a change can alter service ↔ model OpenAPI attribution."""
    model_roots = {
        module.path.resolve().relative_to(repo_root.resolve()).as_posix()
        for module in modules
        if module.name.casefold().startswith("model-")
        and module.path.resolve() != repo_root.resolve()
    }
    for rel_path in changed_or_deleted:
        if rel_path.endswith("pom.xml") or _is_strategy1_openapi_declaration(rel_path):
            return True
        if any(rel_path == root or rel_path.startswith(f"{root}/") for root in model_roots):
            return True
    return False


def _is_git_metadata(rel_path: str) -> bool:
    """Git metadata is never source input, even when config omits `.git/**`.

    This is intentionally a segment check rather than a glob so a source file
    merely containing the characters ``.git`` in its name remains indexable.
    """
    return ".git" in rel_path.split("/")


def _nested_build_roots(repo_root: Path) -> tuple[Path, ...]:
    """Return the outermost Maven/Gradle modules below a container root.

    A directory used only as a workspace (for example ``~/examples``) must
    not be mistaken for one source module.  When it has no build descriptor of
    its own, scanning is restricted to its child Maven/Gradle projects; nested
    modules remain included and are later attributed to their nearest build
    descriptor.  A normal repository root keeps the historical whole-tree
    behaviour.
    """
    descriptors = ("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts")
    if any((repo_root / descriptor).is_file() for descriptor in descriptors):
        return ()
    candidates = {
        path.parent
        for descriptor in descriptors
        for path in repo_root.rglob(descriptor)
        if not _is_git_metadata(path.relative_to(repo_root).as_posix())
        if len(path.parent.relative_to(repo_root).parts) <= 5
    }
    return tuple(
        candidate
        for candidate in sorted(candidates)
        if not any(parent != candidate and parent in candidates for parent in candidate.parents)
    )


def _is_in_excluded_module(path: Path, excluded_module_paths: tuple[Path, ...]) -> bool:
    return any(module_path == path.parent or module_path in path.parents for module_path in excluded_module_paths)


def _list_repo_files(
    repo_root: Path,
    config: Config,
    *,
    excluded_module_paths: tuple[Path, ...] = (),
) -> dict[str, str]:
    repo_root = repo_root.resolve()
    hashes: dict[str, str] = {}
    nested_roots = _nested_build_roots(repo_root)
    for path in sorted(repo_root.rglob("*")):
        if not path.is_file():
            continue
        if _is_in_excluded_module(path, excluded_module_paths):
            continue
        if nested_roots and not any(root == path.parent or root in path.parents for root in nested_roots):
            continue
        rel_path = path.relative_to(repo_root).as_posix()
        if _is_git_metadata(rel_path):
            continue
        if _is_test_source(rel_path):
            continue
        if config.exclude and _matches_any(rel_path, config.exclude):
            continue
        if config.include and not _matches_any(rel_path, config.include):
            continue
        hashes[rel_path] = _sha256_file(path)
    return hashes


def _analysis_inputs_signature(repo_root: Path, config: Config) -> str:
    """Fingerprint local configuration that changes AST analysis facts."""
    digest = hashlib.sha256()
    config_file = repo_root / ".archlens" / "config.yml"
    if config_file.is_file():
        digest.update(_sha256_file(config_file).encode())
    return digest.hexdigest()


def _report_progress(progress: ProgressCallback | None, message: str) -> None:
    if progress is not None:
        progress(message)


def _trace(stage: str, **fields: object) -> None:
    """Emit an opt-in, flush-on-write checkpoint for native crash diagnosis."""
    if os.environ.get("ARCHLENS_TRACE") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(f"ARCHLENS_TRACE ts={time.monotonic():.6f} stage={stage} {details}".rstrip(), file=sys.stderr, flush=True)


def index_repo(
    repo_root: Path,
    config: Config,
    store: Store,
    full: bool = False,
    disabled: frozenset[str] = frozenset(),
    extra_files: list[str] | None = None,
    topic_strategy: str = "default",
    progress: ProgressCallback | None = None,
) -> IndexReport:
    # BACKLOG-16 P2 : purge les lru_cache d'analyse best-effort (package
    # Java, propriétés Spring, module Maven/Gradle) avant de relire le
    # repo — nécessaire dans un process long-vivant (serveur MCP) où
    # `reindex_findings` doit voir les fichiers tels qu'ils sont maintenant,
    # pas tels qu'un `archlens index` précédent les avait mémorisés.
    clear_analysis_caches()
    _trace(
        "index_repo.begin", root=repo_root, full=full, disabled=",".join(sorted(disabled)),
        topic_strategy=topic_strategy,
    )
    discovered_modules = []
    if "properties" not in disabled:
        _report_progress(progress, "→ Indexation : découverte des modules Maven/Gradle...")
        _trace("modules.begin")
        discovered_modules = discover_modules(
            repo_root,
            enrich_architecture="module-architecture" not in disabled,
            use_tree_sitter="module-tree-sitter" not in disabled,
        )
        _trace("modules.end", count=len(discovered_modules))
        if discovered_modules:
            for module in discovered_modules:
                _report_progress(
                    progress,
                    f"  • [{module.build_system}/{module.kind}] {module.name}  {module.path}",
                )
        else:
            _report_progress(progress, "  • aucun module Maven/Gradle détecté ; scan de la racine.")
    # Les signatures d'inventaire d'endpoints pilotent aussi l'analyse locale
    # (REST/Kafka/manifests) : une évolution du code
    # d'inférence ou de stratégie Kafka doit forcer un rescan complet.
    endpoint_signature = current_endpoint_inventory_signature()
    if store.get_meta("endpoint_inventory_signature") != endpoint_signature:
        full = True
    if store.get_meta("topic_strategy") != topic_strategy:
        full = True
    analysis_inputs_signature = _analysis_inputs_signature(repo_root, config)
    if store.get_meta("analysis_inputs_signature") != analysis_inputs_signature:
        full = True

    _report_progress(progress, "→ Indexation : inventaire des fichiers du dépôt...")
    _trace("files.begin")
    excluded_module_paths = discover_excluded_module_paths(repo_root)
    current_hashes = _list_repo_files(
        repo_root,
        config,
        excluded_module_paths=excluded_module_paths,
    )
    for rel_path in extra_files or []:
        candidate = repo_root / rel_path
        if candidate.is_file() and not _is_in_excluded_module(candidate, excluded_module_paths):
            current_hashes[rel_path] = _sha256_file(candidate)
    previous_hashes = store.get_file_hashes()
    _trace("files.end", current=len(current_hashes), previous=len(previous_hashes))

    # The module inventory is intentionally materialized with the index rather
    # than reconstructed by `archlens modules`: its configuration examples describe
    # the exact repository state that was audited.
    current_paths = set(current_hashes)
    previous_paths = set(previous_hashes)

    deleted = sorted(previous_paths - current_paths)

    if full:
        changed = sorted(current_paths)
    else:
        added = current_paths - previous_paths
        modified = {
            p
            for p in current_paths & previous_paths
            if current_hashes[p] != previous_hashes[p]
        }
        changed = sorted(added | modified)
    unchanged = current_paths - set(changed)
    # Strategy1 resolves service declarations against contracts in model-*
    # modules. Only a change to that cross-module join requires a full pass;
    # ordinary Java/configuration changes keep the incremental fast path.
    if (
        topic_strategy == "strategy1"
        and not full
        and _strategy1_requires_full_reindex(
            set(changed) | set(deleted), repo_root, discovered_modules
        )
    ):
        full = True
        changed = sorted(current_paths)
        unchanged = set()
    _report_progress(
        progress,
        "→ Indexation : delta calculé "
        f"({len(changed)} fichier(s) à scanner, {len(unchanged)} inchangé(s), "
        f"{len(deleted)} supprimé(s)).",
    )

    # Les fichiers supprimés quittent toujours l'inventaire.
    endpoints_removed = store.count_endpoints_for_paths(deleted)
    store.remove_files(deleted)  # purge aussi les endpoints (K1)

    # Clear retired external-analyzer results once, including for files which
    # did not otherwise need a rescan.
    legacy_findings_removed = store.clear_findings_once("ast_only_analysis_v1")
    endpoints_added = 0
    endpoints: list[MessageEndpoint] = []
    if changed:
        endpoints_removed += store.count_endpoints_for_paths(changed)
        _report_progress(progress, f"→ Indexation : analyse AST sur {len(changed)} fichier(s)...")
        _trace("endpoint_inference.begin")
        endpoints.extend(
            infer_framework_endpoints(
                repo_root,
                changed,
                configured_api_client_strategy1=topic_strategy == "strategy1",
            )
        )
        endpoints.extend(infer_kafka_endpoints(repo_root, changed))
        endpoints.extend(infer_markdown_topic_manifest_endpoints(repo_root, changed))
        endpoints.extend(infer_json_kafka_flow_graph_endpoints(repo_root, changed))
        if topic_strategy == "strategy1":
            endpoints = apply_kafka_topic_strategy1(
                endpoints, infer_kafka_topic_strategy1_endpoints(repo_root, changed)
            )
        _trace("endpoint_inference.end", endpoints=len(endpoints))

        _report_progress(
            progress,
            "→ Indexation : écriture des résultats "
            f"({len(endpoints)} endpoint(s)).",
        )
        store.replace_endpoints_for_files(changed, endpoints)
        _trace("store.endpoints_written", endpoints=len(endpoints))
        endpoints_added = len(endpoints)

    # Les empreintes de fichiers sont l'état de l'inventaire, indépendant du
    # Les empreintes de fichiers sont persistées afin de garder l'indexation
    # incrémentale.
    for path in changed:
        store.set_file_hash(path, current_hashes[path])

    store.set_meta("endpoint_inventory_signature", endpoint_signature)
    store.set_meta("endpoint_inventory_indexed", "1")
    store.set_meta("topic_strategy", topic_strategy)
    store.set_meta("analysis_inputs_signature", analysis_inputs_signature)

    # Persist only after the scan path has completed.  The inventory remains
    # transactional with the rest of the index and represents the audited
    # repository state, not a partially failed scan.
    if "properties" not in disabled:
        _report_progress(progress, "→ Indexation : inventaire des modules et propriétés...")
        _trace("store.modules.begin", count=len(discovered_modules))
        store.replace_modules(discovered_modules)
        module_dependencies = discover_module_dependencies(repo_root, discovered_modules)
        store.replace_module_dependencies(module_dependencies)
        _trace("store.modules.end")
    else:
        _report_progress(progress, "→ Indexation : propriétés et inventaire des modules désactivés, snapshot conservé.")

    relation_modules = discovered_modules if "properties" not in disabled else store.all_modules()
    relation_dependencies = (
        module_dependencies if "properties" not in disabled else store.all_module_dependencies()
    )
    relations = build_architecture_relations(
        relation_modules,
        store.all_endpoints(),
        relation_dependencies,
        kafka_reply_strategy1=topic_strategy == "strategy1",
    )
    store.replace_architecture_relations(relations)
    _report_progress(progress, f"→ Indexation : {len(relations)} relation(s) d'architecture matérialisée(s).")

    _trace("index_repo.end", scanned=len(changed), skipped=len(unchanged))
    return IndexReport(
        scanned=len(changed),
        skipped=len(unchanged),
        findings_added=0,
        findings_removed=legacy_findings_removed,
        deleted_files=len(deleted),
        endpoints_added=endpoints_added,
        endpoints_removed=endpoints_removed,
    )
