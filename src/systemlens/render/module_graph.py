"""Endpoint/module/workspace/flow rendering, plus the module-dependency HTML export.

The dependency-graph HTML payload lives in ``assets/module_graph.html``; this
module builds the JSON data model injected into that static template.
"""

import json
from pathlib import Path
from typing import TypedDict

from systemlens.flow import FlowResult
from systemlens.models import MessageEndpoint
from systemlens.modules import DiscoveredModule, ModuleDependency
from systemlens.workspace import DiscoveredService, FederationResult

_MODULE_GRAPH_HTML_TEMPLATE = (
    Path(__file__).parent / "assets" / "module_graph.html"
).read_text(encoding="utf-8")


class EndpointHit(TypedDict):
    """Shape returned by the `list_endpoints` MCP tool and module inventory views."""

    id: str
    role: str
    system: str
    topic: str
    topic_dynamic: bool
    source: str
    framework: str | None
    message_type: str | None
    path: str
    start_line: int
    end_line: int
    module: str | None
    qualified_name: str | None


def render_endpoints_json(endpoints: list[MessageEndpoint]) -> list[EndpointHit]:
    return [
        EndpointHit(
            id=e.id,
            role=e.role,
            system=e.system,
            topic=e.topic,
            topic_dynamic=e.topic_dynamic,
            source=e.source,
            framework=e.framework,
            message_type=e.message_type,
            path=e.path,
            start_line=e.start_line,
            end_line=e.end_line,
            module=e.module,
            qualified_name=e.qualified_name,
        )
        for e in endpoints
    ]


def render_endpoints_text(endpoints: list[MessageEndpoint], warnings: list[str] | None = None) -> str:
    if not endpoints:
        lines = ["Aucune intégration détectée."]
        for warning in warnings or []:
            lines.append(f"⚠ {warning}")
        return "\n".join(lines)
    lines = []
    for e in endpoints:
        dynamic_marker = " (dynamique)" if e.topic_dynamic else ""
        module_marker = f" [{e.module}]" if e.module else ""
        type_marker = f" <{e.message_type}>" if e.message_type else ""
        lines.append(
            f"[{e.system}/{e.role}] {e.topic}{type_marker}{dynamic_marker}{module_marker}  "
            f"{e.path}:{e.start_line}-{e.end_line}"
        )
    for warning in warnings or []:
        lines.append(f"⚠ {warning}")
    return "\n".join(lines)


class WorkspaceServiceInfo(TypedDict):
    name: str
    kind: str
    starts_application: bool
    indexed: bool
    integration_count: int
    finding_count: int
    exposes_http_api: bool
    http_apis_exposed: list[str]
    http_apis_consumed: list[str]
    kafka_topics_published: list[str]
    kafka_topics_consumed: list[str]
    kafka_message_types_published: dict[str, list[str]]
    kafka_message_types_consumed: dict[str, list[str]]
    mongo_collections: list[str]
    openapi_files: list[str]


class ModuleSummary(TypedDict):
    name: str
    path: str
    build_system: str
    version: str | None
    kind: str
    starts_application: bool
    mongo_collections: list[str]
    mongo_method_count: int
    kafka_method_count: int
    blocking_point_count: int
    openapi_files: list[str]
    rest_controllers: list[str]
    openapi_generated_clients: list[str]


class ModuleDetail(ModuleSummary):
    application_entrypoint: dict[str, object] | None
    configuration_example: str
    mongo_methods: list[dict[str, object]]
    kafka_methods: list[dict[str, object]]
    blocking_points: list[dict[str, object]]


class WorkspaceResult(TypedDict):
    """Shape returned by `systemlens microservices [--root ROOT] --json` and the
    `list_workspace_services` MCP tool (BACKLOG-11 A2)."""

    services: list[WorkspaceServiceInfo]
    warnings: list[str]


def render_workspace_json(
    services: list[DiscoveredService], federation: FederationResult
) -> WorkspaceResult:
    return WorkspaceResult(
        services=[_workspace_service_info(service, federation) for service in services],
        warnings=federation.warnings,
    )


def _workspace_service_info(
    service: DiscoveredService, federation: FederationResult
) -> WorkspaceServiceInfo:
    endpoints = federation.endpoints_by_service.get(service.name, [])
    module = federation.modules_by_service.get(service.name)
    http_apis_exposed = sorted({
        endpoint.topic for endpoint in endpoints
        if endpoint.system == "rest" and endpoint.role == "serve"
    })
    kafka_message_types_published = _workspace_kafka_message_types(endpoints, "produce")
    kafka_message_types_consumed = _workspace_kafka_message_types(endpoints, "consume")
    return WorkspaceServiceInfo(
        name=service.name,
        kind=service.kind,
        starts_application=True,
        indexed=service.indexed,
        integration_count=len(endpoints),
        finding_count=len(federation.findings_by_service.get(service.name, [])),
        exposes_http_api=bool(http_apis_exposed),
        http_apis_exposed=http_apis_exposed,
        http_apis_consumed=sorted({
            endpoint.topic for endpoint in endpoints
            if endpoint.system == "rest" and endpoint.role == "call"
        }),
        kafka_topics_published=sorted({
            endpoint.topic for endpoint in endpoints
            if endpoint.system == "kafka" and endpoint.role == "produce"
        }),
        kafka_topics_consumed=sorted({
            endpoint.topic for endpoint in endpoints
            if endpoint.system == "kafka" and endpoint.role == "consume"
        }),
        kafka_message_types_published=kafka_message_types_published,
        kafka_message_types_consumed=kafka_message_types_consumed,
        mongo_collections=list(module.mongo_collections) if module else [],
        openapi_files=list(module.openapi_files) if module else [],
    )


def _workspace_kafka_message_types(
    endpoints: list[MessageEndpoint], role: str
) -> dict[str, list[str]]:
    message_types: dict[str, set[str]] = {}
    for endpoint in endpoints:
        if endpoint.system != "kafka" or endpoint.role != role or not endpoint.message_type:
            continue
        message_types.setdefault(endpoint.topic, set()).add(endpoint.message_type)
    return {topic: sorted(values) for topic, values in sorted(message_types.items())}


def render_workspace_text(result: WorkspaceResult) -> str:
    if not result["services"]:
        return "Aucun service workspace découvert (ni module Maven runtime, ni microservice Gradle Spring Boot)."
    lines = []
    for info in result["services"]:
        status = "indexé" if info["indexed"] else "non indexé"
        lines.append(
            f"[{info['kind']}] {info['name']} ({status})  "
            f"integrations={info['integration_count']} findings={info['finding_count']}"
        )
        lines.append(
            f"  HTTP exposées: {', '.join(info['http_apis_exposed']) or '-'} | "
            f"HTTP consommées: {', '.join(info['http_apis_consumed']) or '-'}"
        )
        lines.append(
            f"  Kafka publiés: {', '.join(info['kafka_topics_published']) or '-'} | "
            f"Kafka consommés: {', '.join(info['kafka_topics_consumed']) or '-'} | "
            f"Mongo: {', '.join(info['mongo_collections']) or '-'}"
        )
        if info["openapi_files"]:
            lines.append(f"  OpenAPI: {', '.join(info['openapi_files'])}")
        if info["kafka_message_types_published"] or info["kafka_message_types_consumed"]:
            lines.append(
                f"  Types Kafka publiés: {info['kafka_message_types_published'] or '-'} | "
                f"Types Kafka consommés: {info['kafka_message_types_consumed'] or '-'}"
            )
    for warning in result["warnings"]:
        lines.append(f"⚠ {warning}")
    return "\n".join(lines)


def render_modules_list_json(modules: list[DiscoveredModule]) -> list[ModuleSummary]:
    return [
        ModuleSummary(
            name=module.name,
            path=str(module.path),
            build_system=module.build_system,
            version=module.version,
            kind=module.kind,
            starts_application=module.starts_application,
            mongo_collections=list(module.mongo_collections),
            mongo_method_count=len(module.mongo_methods),
            kafka_method_count=len(module.kafka_methods),
            blocking_point_count=len(module.blocking_points),
            openapi_files=list(module.openapi_files),
            rest_controllers=list(module.rest_controllers),
            openapi_generated_clients=list(module.openapi_generated_clients),
        )
        for module in modules
    ]


def render_modules_list_text(modules: list[ModuleSummary]) -> str:
    if not modules:
        return "Aucun module Maven ou Gradle découvert."
    lines: list[str] = []
    for module in modules:
        version = module["version"] or "inconnue"
        lines.append(
            f"[{module['build_system']}/{module['kind']}] {module['name']} "
            f"version={version} mongo={len(module['mongo_collections'])} "
            f"mongo_ops={module['mongo_method_count']} kafka_ops={module['kafka_method_count']} "
            f"blocking={module['blocking_point_count']} app={module['starts_application']} "
            f"openapi={len(module['openapi_files'])} "
            f"rest_controllers={len(module['rest_controllers'])} "
            f"generated_clients={len(module['openapi_generated_clients'])}  {module['path']}"
        )
    return "\n".join(lines)


class ModuleGraphDependency(TypedDict):
    source: str
    target: str


class ModuleGraphResult(TypedDict):
    modules: list[str]
    dependencies: list[ModuleGraphDependency]


def render_module_graph_json(
    modules: list[DiscoveredModule], dependencies: list[ModuleDependency]
) -> ModuleGraphResult:
    return ModuleGraphResult(
        modules=[module.name for module in modules],
        dependencies=[
            ModuleGraphDependency(source=dependency.source, target=dependency.target)
            for dependency in dependencies
        ],
    )


def render_module_graph_text(result: ModuleGraphResult) -> str:
    if not result["modules"]:
        return "Aucun module indexé."
    lines = [f"Modules ({len(result['modules'])}) : {', '.join(result['modules'])}"]
    if not result["dependencies"]:
        lines.append("Aucune dépendance interne déclarée.")
    else:
        lines.extend(
            f"{dependency['source']} --> {dependency['target']}"
            for dependency in result["dependencies"]
        )
    return "\n".join(lines)


def _module_dependency_layout(
    modules: list[DiscoveredModule], dependencies: list[ModuleDependency]
) -> dict[str, tuple[float, float]]:
    """Positionne les dépendances locales en niveaux, de l'appelant vers sa cible.

    Les graphes de dépendances sont le plus souvent des DAG. Les rares cycles
    sont conservés dans une dernière couche plutôt que de bloquer le rendu.
    """
    names = sorted(module.name for module in modules)
    known = set(names)
    outgoing: dict[str, list[str]] = {name: [] for name in names}
    incoming: dict[str, list[str]] = {name: [] for name in names}
    for dependency in dependencies:
        if dependency.source not in known or dependency.target not in known:
            continue
        outgoing[dependency.source].append(dependency.target)
        incoming[dependency.target].append(dependency.source)
    indegree = {name: len(incoming[name]) for name in names}
    levels = {name: 0 for name in names if indegree[name] == 0}
    pending = sorted(levels)
    cursor = 0
    while cursor < len(pending):
        source = pending[cursor]
        cursor += 1
        for target in sorted(outgoing[source]):
            levels[target] = max(levels.get(target, 0), levels[source] + 1)
            indegree[target] -= 1
            if indegree[target] == 0:
                pending.append(target)
    unresolved = [name for name in names if name not in levels]
    if unresolved:
        cycle_level = max(levels.values(), default=-1) + 1
        levels.update({name: cycle_level for name in unresolved})

    layers: dict[int, list[str]] = {}
    for name, level in levels.items():
        layers.setdefault(level, []).append(name)
    order = {name: index for index, name in enumerate(names)}
    for level in sorted(layers):
        layers[level].sort(
            key=lambda name: (
                sum(order[parent] for parent in incoming[name] if parent in order)
                / max(1, sum(parent in order for parent in incoming[name])),
                name,
            )
        )
        order.update({name: index for index, name in enumerate(layers[level])})

    widest = max((len(layer) for layer in layers.values()), default=1)
    positions: dict[str, tuple[float, float]] = {}
    for level, layer in layers.items():
        offset = (widest - len(layer)) * 0.5
        for index, name in enumerate(layer):
            positions[name] = (offset + index, level)
    return positions




def render_module_graph_html(
    modules: list[DiscoveredModule],
    dependencies: list[ModuleDependency],
    endpoints: list[MessageEndpoint],
) -> str:
    """Rend les dépendances de build dans une vue Sigma.js hiérarchique."""
    positions = _module_dependency_layout(modules, dependencies)
    endpoints_by_module = {
        module.name: [endpoint for endpoint in endpoints if endpoint.module == module.name]
        for module in modules
    }
    nodes = [
        {
            "id": module.name,
            "name": module.name,
            "kind": "microservice" if module.starts_application else module.kind,
            "x": positions.get(module.name, (0, 0))[0],
            "y": -positions.get(module.name, (0, 0))[1],
            "httpApisExposed": sorted({
                endpoint.topic
                for endpoint in endpoints_by_module[module.name]
                if endpoint.system == "rest" and endpoint.role == "serve"
            }),
            "kafkaTopicsPublished": sorted({
                endpoint.topic
                for endpoint in endpoints_by_module[module.name]
                if endpoint.system == "kafka" and endpoint.role == "produce"
            }),
            "kafkaTopicsConsumed": sorted({
                endpoint.topic
                for endpoint in endpoints_by_module[module.name]
                if endpoint.system == "kafka" and endpoint.role == "consume"
            }),
        }
        for module in sorted(modules, key=lambda item: item.name)
    ]
    links = [
        {"source": dependency.source, "target": dependency.target}
        for dependency in dependencies
        if dependency.source in positions and dependency.target in positions
    ]
    graph_data = json.dumps({"nodes": nodes, "links": links}, ensure_ascii=False).replace("</", "<\\/")
    return _MODULE_GRAPH_HTML_TEMPLATE.replace("__MODULE_GRAPH_DATA__", graph_data)


def render_module_detail_json(module: DiscoveredModule) -> ModuleDetail:
    return ModuleDetail(
        name=module.name,
        path=str(module.path),
        build_system=module.build_system,
        version=module.version,
        kind=module.kind,
        starts_application=module.starts_application,
        application_entrypoint=(
            module.application_entrypoint.__dict__ if module.application_entrypoint else None
        ),
        configuration_example=module.configuration_example,
        mongo_collections=list(module.mongo_collections),
        mongo_method_count=len(module.mongo_methods),
        kafka_method_count=len(module.kafka_methods),
        blocking_point_count=len(module.blocking_points),
        openapi_files=list(module.openapi_files),
        rest_controllers=list(module.rest_controllers),
        openapi_generated_clients=list(module.openapi_generated_clients),
        mongo_methods=[
            {
                "operation": method.operation,
                "receiver": method.receiver,
                "path": method.path,
                "line": method.line,
                "collection": method.collection,
                "evidence": method.evidence.__dict__ if method.evidence else None,
            }
            for method in module.mongo_methods
        ],
        kafka_methods=[
            {
                "role": method.role,
                "mechanism": method.mechanism,
                "method": method.method,
                "path": method.path,
                "line": method.line,
                "topic": method.topic,
                "evidence": method.evidence.__dict__ if method.evidence else None,
            }
            for method in module.kafka_methods
        ],
        blocking_points=[
            {
                "mechanism": point.mechanism,
                "method": point.method,
                "path": point.path,
                "line": point.line,
                "detail": point.detail,
                "evidence": point.evidence.__dict__ if point.evidence else None,
            }
            for point in module.blocking_points
        ],
    )


def render_module_detail_text(module: ModuleDetail) -> str:
    version = module["version"] or "inconnue"
    return (
        f"[{module['build_system']}/{module['kind']}] {module['name']}\n"
        f"version={version}\nchemin={module['path']}\n"
        f"démarre l'application={module['starts_application']}\n"
        f"collections Mongo={', '.join(module['mongo_collections']) or 'aucune'}\n"
        f"opérations Mongo={module['mongo_method_count']}\n"
        f"opérations Kafka={module['kafka_method_count']}\n"
        f"points bloquants={module['blocking_point_count']}\n"
        f"OpenAPI={', '.join(module['openapi_files']) or 'aucun'}\n"
        f"Contrôleurs REST ({len(module['rest_controllers'])})={', '.join(module['rest_controllers']) or 'aucun'}\n"
        f"Clients OpenAPI générés ({len(module['openapi_generated_clients'])})={', '.join(module['openapi_generated_clients']) or 'aucun'}"
    )


class FlowSiteInfo(TypedDict):
    service: str | None  # None hors fédération (projet courant seul)
    role: str
    system: str
    framework: str | None
    path: str
    start_line: int
    end_line: int
    topic_dynamic: bool


class FlowResultInfo(TypedDict):
    """Shape returned by the `trace_message_flow` MCP tool (BACKLOG-10 K5/K6)."""

    query: str
    resolved_topic: str
    sites: list[FlowSiteInfo]
    warnings: list[str]


def render_flow_json(result: FlowResult) -> FlowResultInfo:
    return FlowResultInfo(
        query=result.query,
        resolved_topic=result.resolved_topic,
        sites=[
            FlowSiteInfo(
                service=site.service,
                role=site.endpoint.role,
                system=site.endpoint.system,
                framework=site.endpoint.framework,
                path=site.endpoint.path,
                start_line=site.endpoint.start_line,
                end_line=site.endpoint.end_line,
                topic_dynamic=site.endpoint.topic_dynamic,
            )
            for site in result.sites
        ],
        warnings=result.warnings,
    )


def render_flow_text(result: FlowResultInfo) -> str:
    lines = [f"Topic/route résolu : {result['resolved_topic']}"]
    if not result["sites"]:
        lines.append("Aucun site (producteur/consommateur/serveur/appelant) trouvé.")
        return "\n".join(lines)
    for site in result["sites"]:
        service_marker = f"[{site['service']}] " if site["service"] else ""
        framework_marker = f" ({site['framework']})" if site["framework"] else ""
        dynamic_marker = " (dynamique)" if site["topic_dynamic"] else ""
        lines.append(
            f"  {service_marker}{site['role']}/{site['system']}{framework_marker}"
            f"{dynamic_marker}  {site['path']}:{site['start_line']}-{site['end_line']}"
        )
    for warning in result["warnings"]:
        lines.append(f"⚠ {warning}")
    return "\n".join(lines)
