from pathlib import Path
from typing import cast

from mcp.server.fastmcp import FastMCP

from systemlens.config import load_config
from systemlens.architecture_inventory import load_architecture_inventory
from systemlens.architecture import (
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
from systemlens.audit import assess_architecture, render_audit_json
from systemlens.dependency_analysis import (
    DependencyAuditResult,
    DependencyGraphResult,
    audit_dependency_graph as run_dependency_audit,
    build_dependency_graph,
)
from systemlens.flow import group_endpoints_by_module_for_flow, trace_flow
from systemlens.graph import (
    build_graph,
    find_outbound_calls_in_consumers,
)
from systemlens.indexer import IndexReport, index_repo
from systemlens.inventory_freshness import endpoint_inventory_warning
from systemlens.modules import DiscoveredModule
from systemlens.paths import db_path
from systemlens.render import (
    EndpointHit,
    FlowResultInfo,
    GraphResult,
    ModuleSummary,
    WorkspaceResult,
    render_endpoints_json,
    render_flow_json,
    render_graph_json,
    render_modules_list_json,
    render_workspace_json,
)
from systemlens.store import Store
from systemlens.workspace import (
    discover_maven_services,
    load_federation,
)

mcp = FastMCP("systemlens")


def _repo_root() -> Path:
    return Path.cwd()


def _require_index(repo_root: Path) -> None:
    if not db_path(repo_root).is_file():
        raise RuntimeError("Index absent. Lancez d'abord: systemlens index")


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
    return build_catalog(
        inventory.modules, inventory.endpoints, strategy1=inventory.strategy1
    ), inventory


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
    """Équivalent structuré de `systemlens analyze audit [--workspace ROOT]`."""
    inventory = load_architecture_inventory(
        _repo_root(), Path(workspace_root) if workspace_root else None
    )
    risks = assess_architecture(
        inventory.endpoints_by_service,
        build_graph(inventory.endpoints_by_service, strategy1=inventory.strategy1),
        modules=inventory.modules,
        endpoints_by_module=inventory.endpoints_by_module,
    )
    return render_audit_json(risks)


@mcp.tool()
def architecture_coverage() -> dict[str, object]:
    """Équivalent structuré de `systemlens analyze coverage --json` du projet courant."""
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        catalog = build_catalog(store.all_modules(), store.all_endpoints())
        return inventory_coverage(catalog, store.all_architecture_relations())


@mcp.tool()
def list_request_reply_patterns() -> dict[str, object]:
    """List Kafka request/reply candidates from the Strategy1 topic convention.

    Equivalent to `systemlens analyze request-reply --json`. It matches
    `retour_<request-topic>` only when the request topic is indexed too.
    """
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        catalog = build_catalog(store.all_modules(), store.all_endpoints())
        return request_reply_patterns(catalog)


@mcp.tool()
def reindex_architecture() -> IndexReport:
    """Met à jour l'index AST après modification de fichiers."""
    repo_root = _repo_root()
    config = load_config(repo_root)
    with Store(repo_root) as store:
        report = index_repo(repo_root, config, store)
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
    Gradle (`systemlens index` lancé au parent, BACKLOG-13/15), les endpoints attribués à
    un module sont automatiquement groupés pour rapporter de vraies arêtes
    inter-modules. Avec `workspace_root`, fédère en plus les autres
    microservices indexés séparément (BACKLOG-11 A2, lecture seule) —
    sinon `services`/`nodes`/`edges` restent vides, voir `note`.
    """
    inventory = load_architecture_inventory(
        _repo_root(), Path(workspace_root) if workspace_root else None
    )
    outbound_calls = find_outbound_calls_in_consumers(inventory.endpoints)
    edges = build_graph(inventory.endpoints_by_service, strategy1=inventory.strategy1)
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
    Lit en lecture seule les projets déjà indexés (`systemlens index`) pour
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
    serveurs/appelants REST) avec leurs preuves de source.
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
            warnings = []
            repo_warning = _current_repo_endpoint_warning(store)
            if repo_warning is not None:
                warnings.append(repo_warning)
            result = trace_flow(query, endpoints_by_service, warnings)
        return render_flow_json(result)

    services = discover_maven_services(Path(workspace_root))
    federation = load_federation(services)
    endpoints_by_service = cast(
        "dict[str | None, list]", dict(federation.endpoints_by_service)
    )
    result = trace_flow(query, endpoints_by_service, federation.warnings)
    return render_flow_json(result)
