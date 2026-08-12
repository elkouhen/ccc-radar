"""Lecture minimale de `pom.xml` (artifactId, détection Spring Boot) partagée
entre `workspace.py` (fédération multi-services, BACKLOG-11 A2) et `scanner.py`
(attribution d'un module à chaque finding/endpoint indexé, BACKLOG-13 M1)."""

import xml.etree.ElementTree as ET
from functools import lru_cache
from pathlib import Path
import re

from systemlens import java_parser

_MAVEN_NS = "{http://maven.apache.org/POM/4.0.0}"
_MAVEN_PROPERTY_RE = re.compile(r"\$\{([^}]+)\}")


def _pom_child_text(root: ET.Element, tag: str) -> str | None:
    element = root.find(f"{_MAVEN_NS}{tag}")
    if element is None:
        element = root.find(tag)  # pom sans espace de noms déclaré (rare)
    return element.text.strip() if element is not None and element.text else None


def _pom_findall(root: ET.Element, path: str) -> list[ET.Element]:
    namespaced = root.findall(path)
    plain = root.findall(path.replace(_MAVEN_NS, ""))
    seen: set[int] = set()
    merged: list[ET.Element] = []
    for element in [*namespaced, *plain]:
        marker = id(element)
        if marker in seen:
            continue
        seen.add(marker)
        merged.append(element)
    return merged


def _parse_pom_root(pom_path: Path) -> ET.Element | None:
    try:
        return ET.fromstring(pom_path.read_text(encoding="utf-8", errors="replace"))
    except (ET.ParseError, OSError):
        return None


def _resolve_maven_value(value: str, properties: dict[str, str]) -> str:
    resolved = value.strip()
    for _ in range(8):
        updated = _MAVEN_PROPERTY_RE.sub(
            lambda match: properties.get(match.group(1), match.group(0)),
            resolved,
        )
        if updated == resolved:
            break
        resolved = updated
    return resolved


def _pom_properties(root: ET.Element, pom_path: Path) -> dict[str, str]:
    module_dir = pom_path.parent.resolve()
    properties = {
        "basedir": str(module_dir),
        "pom.basedir": str(module_dir),
        "project.basedir": str(module_dir),
        "project.parent.basedir": str(module_dir.parent),
        "project.build.directory": str(module_dir / "target"),
    }
    artifact_id = _pom_child_text(root, "artifactId")
    version = _pom_child_text(root, "version")
    if artifact_id:
        properties.update({
            "artifactId": artifact_id,
            "project.artifactId": artifact_id,
            "pom.artifactId": artifact_id,
        })
    if version:
        properties.update({
            "version": version,
            "project.version": version,
            "pom.version": version,
        })
    for props in _pom_findall(root, f"{_MAVEN_NS}properties"):
        for child in list(props):
            tag = child.tag.split("}", 1)[-1]
            if child.text and tag:
                properties[tag] = child.text.strip()
    return properties


def _plugin_config_value(plugin: ET.Element, tag: str) -> str | None:
    config = plugin.find(f"{_MAVEN_NS}configuration")
    if config is None:
        config = plugin.find("configuration")
    return _pom_child_text(config, tag) if config is not None else None


def _execution_configurations(plugin: ET.Element) -> list[ET.Element]:
    executions = plugin.find(f"{_MAVEN_NS}executions")
    if executions is None:
        executions = plugin.find("executions")
    if executions is None:
        return []
    configurations: list[ET.Element] = []
    for execution in list(executions):
        config = execution.find(f"{_MAVEN_NS}configuration")
        if config is None:
            config = execution.find("configuration")
        if config is not None:
            configurations.append(config)
    return configurations


def _iter_openapi_generator_plugins(root: ET.Element) -> list[ET.Element]:
    plugins: list[ET.Element] = []
    for plugin in _pom_findall(root, f".//{_MAVEN_NS}plugin"):
        artifact_id = _pom_child_text(plugin, "artifactId")
        if artifact_id and artifact_id.strip().casefold() == "openapi-generator-maven-plugin":
            plugins.append(plugin)
    return plugins


def _resolve_openapi_spec_path(
    pom_path: Path, raw_value: str, properties: dict[str, str]
) -> str | None:
    resolved = _resolve_maven_value(raw_value, properties)
    if "${" in resolved or "://" in resolved and not resolved.startswith("file://"):
        return None
    if resolved.startswith("file://"):
        resolved = resolved[7:]
    candidate = Path(resolved)
    if not candidate.is_absolute():
        candidate = pom_path.parent / candidate
    try:
        module_relative = candidate.resolve(strict=False).relative_to(pom_path.parent.resolve())
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return module_relative.as_posix()


def _resolve_openapi_spec_root_directory(
    pom_path: Path, raw_value: str, properties: dict[str, str]
) -> tuple[str, ...]:
    """Return every local file configured through ``inputSpecRootDirectory``."""
    resolved = _resolve_maven_value(raw_value, properties)
    if "${" in resolved or "://" in resolved and not resolved.startswith("file://"):
        return ()
    if resolved.startswith("file://"):
        resolved = resolved[7:]
    candidate = Path(resolved)
    if not candidate.is_absolute():
        candidate = pom_path.parent / candidate
    if not candidate.is_dir():
        return ()
    try:
        root = pom_path.parent.resolve()
        return tuple(
            path.relative_to(root).as_posix()
            for path in sorted(candidate.rglob("*"))
            if path.is_file() and root in path.resolve().parents
        )
    except (OSError, ValueError):
        return ()


def _is_spring_boot_main_class(source: bytes) -> bool:
    return java_parser.has_spring_boot_main_class(source)


@lru_cache(maxsize=256)
def _module_has_spring_boot_main_class(module_dir_str: str) -> bool:
    module_dir = Path(module_dir_str)
    source_root = module_dir / "src" / "main" / "java"
    if not source_root.is_dir():
        return False
    for java_file in source_root.rglob("*.java"):
        try:
            source = java_file.read_bytes()
        except OSError:
            continue
        if _is_spring_boot_main_class(source):
            return True
    return False


def parse_pom(pom_path: Path) -> tuple[str | None, bool, str | None]:
    """Renvoie (artifactId, is_spring_boot_app, packaging). Un pom.xml illisible ou mal
    formé renvoie (None, False, None) plutôt que de faire échouer toute la
    découverte — un seul module cassé ne doit pas bloquer les autres."""
    try:
        text = pom_path.read_text(encoding="utf-8", errors="replace")
        root = ET.fromstring(text)
    except (ET.ParseError, OSError):
        return None, False, None
    artifact_id = _pom_child_text(root, "artifactId")
    packaging = _pom_child_text(root, "packaging")
    # A Spring Boot plugin can be inherited by a shared library. A deployable
    # microservice must have an actual Spring Boot entry point; otherwise a
    # `buildingblocks`-style module pollutes the architecture graph.
    is_spring_boot_app = _module_has_spring_boot_main_class(str(pom_path.parent))
    return artifact_id, is_spring_boot_app, packaging


def pom_version(pom_path: Path) -> str | None:
    """Return the locally declared Maven version or its parent declaration."""
    try:
        root = ET.fromstring(pom_path.read_text(encoding="utf-8", errors="replace"))
    except (ET.ParseError, OSError):
        return None
    version = _pom_child_text(root, "version")
    if version is not None:
        return version
    parent = root.find(f"{_MAVEN_NS}parent")
    if parent is None:
        parent = root.find("parent")
    return _pom_child_text(parent, "version") if parent is not None else None


def is_runtime_service(packaging: str | None, is_spring_boot_app: bool) -> bool:
    """Un parent/agrégateur Maven `packaging=pom` n'est jamais un service
    runtime, même s'il déclare le plugin Spring Boot pour ses enfants."""
    return packaging != "pom" and is_spring_boot_app


def _has_openapi_generator_plugin(pom_path: Path) -> bool:
    """Détecte la présence du plugin openapi-generator-maven-plugin dans le pom.xml."""
    root = _parse_pom_root(pom_path)
    if root is None:
        return False
    return bool(_iter_openapi_generator_plugins(root))


def detect_openapi_generator_input_specs(pom_path: Path) -> tuple[str, ...]:
    """Liste les contrats OpenAPI/Swagger locaux référencés par le plugin Maven.

    Les implémentations serveur générées par ``openapi-generator-maven-plugin``
    publient l'API décrite par ``inputSpec`` ou ``inputSpecRootDirectory`` même quand les classes
    ``@RestController`` n'exposent aucune annotation de méthode locale.
    """
    root = _parse_pom_root(pom_path)
    if root is None:
        return ()
    properties = _pom_properties(root, pom_path)
    specs: set[str] = set()
    for plugin in _iter_openapi_generator_plugins(root):
        plugin_level = _plugin_config_value(plugin, "inputSpec")
        if plugin_level:
            resolved = _resolve_openapi_spec_path(pom_path, plugin_level, properties)
            if resolved is not None:
                specs.add(resolved)
        plugin_root_directory = _plugin_config_value(plugin, "inputSpecRootDirectory")
        if plugin_root_directory:
            specs.update(
                _resolve_openapi_spec_root_directory(pom_path, plugin_root_directory, properties)
            )
        for configuration in _execution_configurations(plugin):
            execution_level = _pom_child_text(configuration, "inputSpec")
            if execution_level:
                resolved = _resolve_openapi_spec_path(pom_path, execution_level, properties)
                if resolved is not None:
                    specs.add(resolved)
            execution_root_directory = _pom_child_text(configuration, "inputSpecRootDirectory")
            if execution_root_directory:
                specs.update(
                    _resolve_openapi_spec_root_directory(
                        pom_path, execution_root_directory, properties
                    )
                )
    return tuple(sorted(specs))


def detect_openapi_generated_clients(pom_path: Path) -> tuple[str, ...]:
    """Détecte les clients OpenAPI générés par openapi-generator-maven-plugin.

    Retourne un tuple des chemins relatifs des fichiers Java générés.
    """
    if not _has_openapi_generator_plugin(pom_path):
        return ()

    module_dir = pom_path.parent

    # Chemins typiques pour les sources générées par openapi-generator
    possible_paths = [
        module_dir / "target" / "generated-sources" / "openapi",
        module_dir / "target" / "generated-sources" / "openapi-mapstruct",
        module_dir / "target" / "generated-sources" / "openapi-nullable",
    ]

    generated_sources = None
    for path in possible_paths:
        if path.exists() and path.is_dir():
            generated_sources = path
            break

    if not generated_sources:
        return ()

    client_files = []
    for java_file in generated_sources.rglob("*.java"):
        # Calculer le chemin relatif par rapport au module
        try:
            rel_path = java_file.relative_to(module_dir)
            client_files.append(str(rel_path))
        except ValueError:
            # Si le fichier n'est pas relatif au module, on l'ignore
            continue

    return tuple(sorted(set(client_files)))


@lru_cache(maxsize=512)
def _cached_module_name(pom_path_str: str) -> str:
    pom_path = Path(pom_path_str)
    artifact_id, _, _ = parse_pom(pom_path)
    return artifact_id or pom_path.parent.name


def clear_caches() -> None:
    """BACKLOG-16 P2 : à appeler en tête de chaque indexation dans un
    process long-vivant (serveur MCP) — `_cached_module_name` est caché par
    chemin de pom.xml pour toute la durée du process, un artifactId modifié
    entre deux `systemlens index` resterait sinon périmé."""
    _cached_module_name.cache_clear()
    _module_has_spring_boot_main_class.cache_clear()


def module_name_for_path(repo_root: Path, rel_path: str) -> str | None:
    """Nom de module Maven (artifactId, repli sur le nom du répertoire) du
    plus proche `pom.xml` en remontant depuis le répertoire de `rel_path`
    jusqu'à `repo_root` inclus (BACKLOG-13 M1). `None` si aucun `pom.xml`
    n'est trouvé sur ce chemin — repo non-Maven, ou fichier hors
    arborescence Maven. Résultat caché par `pom.xml` (mêmes garanties que
    `resolve_spring_property`/`_load_flat_spring_properties` : un pom lu une
    seule fois par process). Même bornage que `_candidate_spring_roots`
    (`scanner.py`) : jamais de remontée au-delà de `repo_root`."""
    source_abs = (repo_root / rel_path).resolve()
    repo_root_resolved = repo_root.resolve()
    candidates = [source_abs.parent, *source_abs.parent.parents]
    for candidate in candidates:
        if candidate == repo_root_resolved or repo_root_resolved in candidate.parents:
            pom_path = candidate / "pom.xml"
            if pom_path.is_file():
                return _cached_module_name(str(pom_path))
        if candidate == repo_root_resolved:
            break
    return None
