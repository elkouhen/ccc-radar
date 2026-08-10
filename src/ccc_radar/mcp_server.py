from pathlib import Path
from typing import cast

from mcp.server.fastmcp import FastMCP

from ccc_radar.coco_indexer import ENGINE_META_VALUE, index_repo_with_cocoindex
from ccc_radar.config import ConfigError, load_config
from ccc_radar.architecture_inventory import load_architecture_inventory
from ccc_radar.architecture import (
    analyze as analyze_architecture,
    build_catalog,
    find_microservice_paths,
    inventory_coverage,
    list_objects,
    neighbors,
    normalize_kind,
    request_reply_patterns,
    show_object,
    trace_topic_flows,
)
from ccc_radar.audit import assess_architecture, render_audit_json
from ccc_radar.dependency_analysis import (
    DependencyAuditResult,
    DependencyGraphResult,
    audit_dependency_graph as run_dependency_audit,
    build_dependency_graph,
)
from ccc_radar.embedder import EmbeddingError, make_embedder
from ccc_radar.flow import (
    FlowError,
    group_endpoints_by_module_for_flow,
    group_findings_by_module_for_flow,
    resolve_topic_by_similarity,
    trace_flow,
)
from ccc_radar.graph import (
    build_graph,
    find_outbound_calls_in_consumers,
)
from ccc_radar.indexer import IndexReport, index_repo
from ccc_radar.inventory_freshness import endpoint_inventory_warning
from ccc_radar.modules import DiscoveredModule
from ccc_radar.paths import db_path
from ccc_radar.render import (
    EndpointHit,
    FindingHit,
    FindingsSummary,
    FlowResultInfo,
    GraphResult,
    ModuleSummary,
    WorkspaceResult,
    render_endpoints_json,
    render_flow_json,
    render_graph_json,
    render_modules_list_json,
    render_search_json,
    render_summary_json,
    render_workspace_json,
)
from ccc_radar.search import search_findings as run_search_findings
from ccc_radar.search import summary as compute_summary
from ccc_radar.store import Store
from ccc_radar.workspace import (
    discover_maven_services,
    load_federation,
)

mcp = FastMCP("cccr")


def _repo_root() -> Path:
    return Path.cwd()


def _require_index(repo_root: Path) -> None:
    if not db_path(repo_root).is_file():
        raise RuntimeError("Index absent. Lancez d'abord: cccr index")


def _current_repo_endpoint_warning(store: Store) -> str | None:
    return endpoint_inventory_warning(
        store.get_meta("endpoint_inventory_signature"),
        scope="ce projet",
        inventory_indexed=store.get_meta("endpoint_inventory_indexed") == "1",
    )


def _dependency_inventory(
    workspace_root: str | None,
) -> tuple[dict[str, list], dict[str, DiscoveredModule], list[str]]:
    """Load runtime services and module metadata for graph-oriented MCP tools."""
    inventory = load_architecture_inventory(
        _repo_root(), Path(workspace_root) if workspace_root else None
    )
    return inventory.endpoints_by_service, inventory.modules_by_service, inventory.warnings


def _architecture_catalog(workspace_root: str | None):
    inventory = load_architecture_inventory(
        _repo_root(), Path(workspace_root) if workspace_root else None
    )
    return build_catalog(inventory.modules, inventory.endpoints), inventory


@mcp.tool()
def architecture_catalog(
    kind: str,
    action: str = "list",
    name: str | None = None,
    target: str | None = None,
    workspace_root: str | None = None,
    max_depth: int = 12,
    limit: int = 50,
) -> object:
    """Navigation MCP équivalente aux commandes CLI `microservices`, `topics`,
    `dtos`, `apis` et `mongodb`.

    `kind` accepte microservice, module, topic, dto, api, collection ou endpoint.
    `action` accepte list, show, neighbors; topic accepte aussi producers,
    consumers, trace; dto producers/consumers; api providers/consumers;
    collection services; microservice path, calls, dependencies, impact,
    external-apis et orphan-integrations. `name` est l'objet concerné et
    `target` est la destination d'un path.
    """
    normalized_kind = normalize_kind(kind)
    if normalized_kind is None:
        raise ValueError(f"Type d'architecture inconnu : {kind}")
    catalog, _inventory = _architecture_catalog(workspace_root)
    action = action.casefold()
    if action == "list":
        return {"kind": normalized_kind, "items": list_objects(catalog, normalized_kind)}
    result: object
    if action == "show":
        if name is None:
            raise ValueError("`name` est requis pour l'action show.")
        result = show_object(catalog, normalized_kind, name)
    elif action == "neighbors":
        if name is None:
            raise ValueError("`name` est requis pour l'action neighbors.")
        result = neighbors(catalog, normalized_kind, name)
    elif normalized_kind == "topic" and action in {"producers", "consumers"}:
        result = analyze_architecture(catalog, action, name)
    elif normalized_kind == "topic" and action == "trace" and name is not None:
        result = trace_topic_flows(catalog, name, max_depth=max_depth, limit=limit)
    elif normalized_kind == "dto" and action in {"producers", "consumers"} and name is not None:
        summary = show_object(catalog, "dto", name)
        key = "producer_microservices" if action == "producers" else "consumer_microservices"
        result = {"query": action, "dto": name, "microservices": summary[key]} if summary else None
    elif normalized_kind == "api" and action in {"providers", "consumers"} and name is not None:
        summary = show_object(catalog, "api", name)
        result = {"query": action, "api": name, "microservices": summary[action]} if summary else None
    elif normalized_kind == "collection" and action == "services" and name is not None:
        summary = show_object(catalog, "collection", name)
        result = {"query": action, "collection": name, "microservices": summary["modules"]} if summary else None
    elif normalized_kind == "microservice" and action == "path" and name and target:
        result = find_microservice_paths(catalog, name, target, max_depth=max_depth, limit=limit)
    elif normalized_kind == "microservice" and action in {"calls", "dependencies", "impact", "external-apis", "orphan-integrations"}:
        result = analyze_architecture(catalog, action, name)
    else:
        raise ValueError(f"Action {action!r} incompatible avec {normalized_kind!r}.")
    if result is None:
        raise ValueError(f"Objet d'architecture introuvable : {name or target or kind}")
    return result


@mcp.tool()
def architecture_audit(workspace_root: str | None = None) -> list[dict[str, object]]:
    """Équivalent structuré de `cccr analyze audit [--workspace ROOT]`."""
    inventory = load_architecture_inventory(
        _repo_root(), Path(workspace_root) if workspace_root else None
    )
    risks = assess_architecture(
        inventory.endpoints_by_service,
        build_graph(inventory.endpoints_by_service),
        modules=inventory.modules,
        endpoints_by_module=inventory.endpoints_by_module,
    )
    return render_audit_json(risks)


@mcp.tool()
def architecture_coverage() -> dict[str, object]:
    """Équivalent structuré de `cccr analyze coverage --json` du projet courant."""
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        catalog = build_catalog(store.all_modules(), store.all_endpoints())
        return inventory_coverage(catalog, store.all_architecture_relations())


@mcp.tool()
def list_request_reply_patterns() -> dict[str, object]:
    """List Kafka request/reply candidates from the Strategy1 topic convention.

    Equivalent to `cccr analyze request-reply --json`. It matches
    `retour_<request-topic>` only when the request topic is indexed too.
    """
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        catalog = build_catalog(store.all_modules(), store.all_endpoints())
        return request_reply_patterns(catalog)


@mcp.tool()
def search_findings(
    query: str,
    severity: str | None = None,
    rule: str | None = None,
    path_glob: str | None = None,
    limit: int = 5,
    include_context: bool = False,
) -> list[FindingHit]:
    """Recherche en langage naturel dans les findings Semgrep indexés du repo.
    Utiliser AVANT de modifier du code pour connaître les problèmes connus,
    et pour localiser des vulnérabilités par description.
    """
    repo_root = _repo_root()
    _require_index(repo_root)
    config = load_config(repo_root)
    embedder = make_embedder(config.embedding_model)

    with Store(repo_root) as store:
        hits = run_search_findings(
            store,
            embedder,
            query,
            severity=severity,
            rule=rule,
            path_glob=path_glob,
            limit=limit,
        )
        return render_search_json(hits, repo_root, include_context)


@mcp.tool()
def findings_summary() -> FindingsSummary:
    """Vue agrégée des findings (sévérités, top règles).
    Utiliser pour une vue d'ensemble à faible coût.
    """
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root) as store:
        result = compute_summary(store)
    return render_summary_json(result)


@mcp.tool()
def reindex_findings() -> IndexReport:
    """Met à jour l'index des findings après modification de fichiers.
    Appeler après un patch pour vérifier la disparition d'un finding.
    """
    repo_root = _repo_root()
    config = load_config(repo_root)
    embedder = make_embedder(config.embedding_model)
    with Store(repo_root) as store:
        # BACKLOG-16 P3 : même dispatch que `cccr index` (cli.py) — un repo
        # indexé avec `--engine cocoindex` doit continuer de rafraîchir ses
        # chunks de code ici, sinon `search` (MCP) sert des chunks périmés
        # après un `reindex_findings` qui les a silencieusement ignorés.
        if store.get_meta("index_engine") == ENGINE_META_VALUE:
            return index_repo_with_cocoindex(repo_root, config, store, embedder)
        report = index_repo(repo_root, config, store, embedder)
        store.set_meta("index_engine", "manual")
        return report


@mcp.tool()


@mcp.tool()
def list_endpoints(
    system: str | None = None,
    role: str | None = None,
    topic: str | None = None,
    path_glob: str | None = None,
) -> list[EndpointHit]:
    """Liste les endpoints REST/Kafka indexés (BACKLOG-10 K1, BACKLOG-11 A1),
    filtrable par système (rest/kafka), rôle (serve/call/produce/consume),
    topic exact ou motif de chemin. Utiliser pour explorer l'inventaire des
    échanges entre services avant d'appeler `graph`.
    """
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root) as store:
        endpoints = store.all_endpoints(
            system=system, role=role, topic=topic, path_glob=path_glob
        )
    return render_endpoints_json(endpoints)


@mcp.tool()
def graph(workspace_root: str | None = None) -> GraphResult:
    """Graphe dérivé des endpoints indexés : nœuds = microservices + topics
    Kafka ; arêtes = appel HTTP, production Kafka, consommation Kafka, plus
    points de blocage probables (BACKLOG-10 K12) : appels REST synchrones
    dans un handler de consommation Kafka du projet courant. Utiliser pour
    visualiser la topologie distribuée ET localiser les endroits
    susceptibles de causer un verrouillage intermittent. Sans
    `workspace_root`, si l'index couvre un répertoire multi-modules Maven ou
    Gradle (`cccr index` lancé au parent, BACKLOG-13/15), les endpoints attribués à
    un module sont automatiquement groupés pour rapporter de vraies arêtes
    inter-modules. Avec `workspace_root`, fédère en plus les autres
    microservices indexés séparément (BACKLOG-11 A2, lecture seule) —
    sinon `services`/`nodes`/`edges` restent vides, voir `note`.
    """
    inventory = load_architecture_inventory(
        _repo_root(), Path(workspace_root) if workspace_root else None
    )
    outbound_calls = find_outbound_calls_in_consumers(inventory.endpoints)
    edges = build_graph(inventory.endpoints_by_service)
    return render_graph_json(
        list(inventory.endpoints_by_service),
        edges,
        outbound_calls,
        warnings=inventory.warnings,
        cross_module_data_available=workspace_root is not None or bool(inventory.endpoints_by_service),
    )


@mcp.tool()
def dependency_graph(workspace_root: str | None = None) -> DependencyGraphResult:
    """Retourne la topologie de dépendances statique exploitable par un agent.

    Les nœuds sont les microservices, topics Kafka, collections MongoDB
    (scopées par microservice) et APIs HTTP externes. Les relations indiquent
    appels HTTP internes/externes, publications et consommations Kafka avec les
    types Java connus, ainsi que lectures/écritures MongoDB. Utiliser avant un
    audit ou pour reconstruire une dépendance service -> topic -> stockage.
    Avec `workspace_root`, fédère les services déjà indexés séparément.
    """
    endpoints_by_service, modules_by_service, warnings = _dependency_inventory(workspace_root)
    return build_dependency_graph(endpoints_by_service, modules_by_service, warnings=warnings)


@mcp.tool()
def audit_dependency_graph(workspace_root: str | None = None) -> DependencyAuditResult:
    """Audite le graphe de dépendances statique pour les mauvaises pratiques.

    Retourne la topologie analysée, les risques déjà détectés (orphans Kafka,
    cibles dynamiques, incompatibilités de DTO, cycles HTTP synchrones et
    activités runtime de bibliothèques), les cycles événementiels entre
    microservices et les appels HTTP synchrones depuis un consumer Kafka.
    Les résultats sont des signaux statiques avec un niveau de confiance, pas
    des traces d'exécution. Avec `workspace_root`, fédère les services indexés.
    """
    endpoints_by_service, modules_by_service, warnings = _dependency_inventory(workspace_root)
    return run_dependency_audit(endpoints_by_service, modules_by_service, warnings=warnings)


@mcp.tool()
def list_workspace_services(root: str) -> WorkspaceResult:
    """Découvre les services fédérables sous `root` (BACKLOG-11 A2) :
    modules Maven runtime/shared et microservices Gradle Spring Boot.
    Lit en lecture seule les projets déjà indexés (`cccr index`) pour
    compter endpoints/findings par service — n'écrit jamais dans leurs
    bases. Utiliser avant `graph` pour vérifier quels services d'un
    répertoire multi-services sont prêts à être fédérés.
    """
    services = discover_maven_services(Path(root))
    federation = load_federation(services)
    return render_workspace_json(services, federation)


@mcp.tool()
def list_modules() -> list[ModuleSummary]:
    """Liste tous les modules indexés avec leurs collections/opérations Mongo
    et leurs contrats OpenAPI déclarés. Utiliser pour établir le périmètre
    applicatif avant un audit de données ou d'API.
    """
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root) as store:
        return render_modules_list_json(store.all_modules())


@mcp.tool()
def trace_message_flow(query: str, workspace_root: str | None = None) -> FlowResultInfo:
    """Résout `query` en topic Kafka ou route REST (nom exact, sinon
    sous-chaîne non ambiguë parmi les endpoints indexés, BACKLOG-10 K5) et
    liste tous ses sites (producteurs/consommateurs Kafka, ou
    serveurs/appelants REST) avec les findings Semgrep qui les recouvrent.
    Utiliser pour comprendre qui produit/consomme un topic donné, ou qui
    appelle une route donnée, avant de plonger dans le code. Sans
    `workspace_root`, ne cherche que dans le projet courant — chaque site
    est attribué à son module Maven ou service Gradle si l'index couvre un
    répertoire multi-modules (BACKLOG-13/15) ; avec, fédère en plus les autres
    microservices indexés séparément (BACKLOG-11 A2, lecture seule) pour un
    flux qui traverse plusieurs services. Requête sans correspondance, ou
    ambiguë, lève une erreur explicite plutôt que de deviner un topic au
    hasard.
    """
    repo_root = _repo_root()

    if workspace_root is None:
        _require_index(repo_root)
        with Store(repo_root) as store:
            endpoints = store.all_endpoints()
            endpoints_by_service: dict[str | None, list] = group_endpoints_by_module_for_flow(
                endpoints
            )
            findings_by_service: dict[str | None, list] = group_findings_by_module_for_flow(
                store.all_findings()
            )
            warnings = []
            repo_warning = _current_repo_endpoint_warning(store)
            if repo_warning is not None:
                warnings.append(repo_warning)
            try:
                result = trace_flow(query, endpoints_by_service, findings_by_service, warnings)
            except FlowError as exc:
                fallback_topic = None
                try:
                    config = load_config(repo_root)
                    embedder = make_embedder(config.embedding_model)
                    fallback_topic = resolve_topic_by_similarity(
                        store, embedder, query, endpoints
                    )
                except (ConfigError, EmbeddingError):
                    pass
                if fallback_topic is None:
                    raise exc
                result = trace_flow(
                    fallback_topic, endpoints_by_service, findings_by_service, warnings
                )
        return render_flow_json(result)

    services = discover_maven_services(Path(workspace_root))
    federation = load_federation(services)
    endpoints_by_service = cast(
        "dict[str | None, list]", dict(federation.endpoints_by_service)
    )
    findings_by_service = cast(
        "dict[str | None, list]", dict(federation.findings_by_service)
    )
    result = trace_flow(query, endpoints_by_service, findings_by_service, federation.warnings)
    return render_flow_json(result)
