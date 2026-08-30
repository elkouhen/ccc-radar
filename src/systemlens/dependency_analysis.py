"""Dependency graph construction and static architecture audit helpers."""

from __future__ import annotations

from typing import NotRequired, TypedDict

from systemlens.audit import assess_architecture, render_audit_json
from systemlens.graph import (
    build_graph,
    configured_api_client_domain,
    external_microservice_name,
    find_outbound_calls_in_consumers,
    graph_edge_rest_resource,
    graph_edges_from_relations,
    qualified_rest_resource,
)
from systemlens.models import ArchitectureRelation, MessageEndpoint
from systemlens.modules import DiscoveredModule


_MONGO_WRITE_OPERATIONS = frozenset({
    "bulkOps", "findAndModify", "findAndReplace", "insert", "remove", "save",
    "updateFirst", "updateMulti", "upsert",
})


class DependencyNode(TypedDict):
    id: str
    kind: str
    name: str
    service: str | None
    external: bool
    technology: NotRequired[str]
    metadata: NotRequired[dict[str, object]]
    status: NotRequired[str]


class DependencyEdge(TypedDict):
    source: str
    target: str
    kind: str
    label: str
    confidence: str
    technology: NotRequired[str]
    metadata: NotRequired[dict[str, object]]
    status: NotRequired[str]


class DependencyGraphResult(TypedDict):
    nodes: list[DependencyNode]
    edges: list[DependencyEdge]
    summary: dict[str, int]
    warnings: list[str]


class DependencyCycle(TypedDict):
    nodes: list[DependencyNode]
    services: list[str]


class DependencyAuditResult(TypedDict):
    graph: DependencyGraphResult
    issues: list[dict[str, object]]
    cycles: list[DependencyCycle]


def _node_id(kind: str, name: str, service: str | None = None) -> str:
    return f"{kind}:{service}:{name}" if service else f"{kind}:{name}"


def _effective_kafka_endpoints(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
) -> list[tuple[str, MessageEndpoint]]:
    """Apply the same per-service manifest precedence as ``build_graph``."""
    manifest_services = {
        service
        for service, endpoints in endpoints_by_service.items()
        for endpoint in endpoints
        if endpoint.system == "kafka" and endpoint.source == "manifest"
    }
    return [
        (service, endpoint)
        for service, endpoints in endpoints_by_service.items()
        for endpoint in endpoints
        if endpoint.system == "kafka"
        and (service not in manifest_services or endpoint.source == "manifest")
    ]


def _known_microservices(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    modules_by_service: dict[str, DiscoveredModule],
) -> set[str]:
    """Return services discovered by the workspace, even without endpoints.

    A configured API client establishes a service-level dependency.  Its host
    therefore only needs to be indexed as a runtime service; it does *not*
    need a detected REST ``serve`` endpoint.  Federation preserves an empty
    endpoint list for such a service, hence the dictionary keys are facts too.
    """
    return set(endpoints_by_service) | set(modules_by_service)


def build_dependency_graph(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    modules_by_service: dict[str, DiscoveredModule],
    *,
    warnings: list[str] | None = None,
    relations: list[ArchitectureRelation] | None = None,
    strategy1: bool = False,
) -> DependencyGraphResult:
    """Build an evidenced service/topic/data-store topology for agent use."""
    nodes: dict[str, DependencyNode] = {}
    edges: dict[tuple[str, str, str, str], DependencyEdge] = {}
    result_warnings = list(warnings or [])

    def add_node(
        kind: str, name: str, service: str | None = None, *, external: bool = False
    ) -> str:
        identifier = _node_id(kind, name, service)
        node = nodes.setdefault(
            identifier,
            {"id": identifier, "kind": kind, "name": name, "service": service, "external": external},
        )
        if external:
            node["external"] = True
        return identifier

    def add_edge(source: str, target: str, kind: str, label: str, confidence: str = "high") -> None:
        edges.setdefault(
            (source, target, kind, label),
            {"source": source, "target": target, "kind": kind, "label": label, "confidence": confidence},
        )

    for service in sorted(endpoints_by_service):
        add_node("microservice", service)

    internal_edges = (
        graph_edges_from_relations(relations, endpoints_by_service)
        if relations is not None
        else build_graph(endpoints_by_service, strategy1=strategy1)
    )
    matched_calls = {edge.from_endpoint.id for edge in internal_edges if edge.kind == "rest"}

    # Clients d'API configurés (createInternalClientApi) : la route HTTP n'est
    # pas prouvée au site d'appel. On ignore donc le fan-out par route fabriqué
    # par build_graph ; une relation service→service unique est émise plus bas,
    # résolue contre le registre des microservices (prérequis : POM analysé).
    configured_call_ids = {
        edge.from_endpoint.id
        for edge in internal_edges
        if edge.kind == "rest" and configured_api_client_domain(edge.from_endpoint) is not None
    }
    for edge in internal_edges:
        if edge.kind == "rest" and edge.from_endpoint.id in configured_call_ids:
            continue
        source = add_node("microservice", edge.from_service)
        target = add_node("microservice", edge.to_service)
        if edge.kind == "rest":
            add_edge(
                source,
                target,
                "http",
                graph_edge_rest_resource(edge),
            )

    for service, endpoint in _effective_kafka_endpoints(endpoints_by_service):
        service_id = add_node("microservice", service)
        # Dynamic topic expressions are useful unresolved evidence but do not
        # identify a shared Kafka topic. Scope their node to the declaring
        # service so the dependency graph cannot accidentally connect two
        # independent `<dynamic>` expressions.
        topic_id = add_node("topic", endpoint.topic, service if endpoint.topic_dynamic else None)
        confidence = "medium" if endpoint.topic_dynamic else "high"
        if endpoint.role == "produce":
            label = f"publishes {endpoint.message_type}" if endpoint.message_type else "publishes"
            add_edge(service_id, topic_id, "publishes", label, confidence)
        else:
            label = f"consumes {endpoint.message_type}" if endpoint.message_type else "consumes"
            add_edge(topic_id, service_id, "consumes", label, confidence)

    # Paires (appelant, domaine) tirées du tampon systemlens-api-domain: présent sur
    # les sites d'appel — y compris ceux non matchés à un serve (pour diagnostic).
    configured_clients: dict[str, set[str]] = {}
    known_services = _known_microservices(endpoints_by_service, modules_by_service)

    for service, endpoints in endpoints_by_service.items():
        service_id = add_node("microservice", service)
        for endpoint in endpoints:
            if endpoint.system != "rest" or endpoint.role != "call":
                continue
            configured_domain = configured_api_client_domain(endpoint)
            if configured_domain is not None:
                configured_clients.setdefault(service, set()).add(configured_domain)
                continue
            external_microservice = external_microservice_name(endpoint)
            if external_microservice is not None:
                target_id = add_node("microservice", external_microservice, external=True)
                add_edge(service_id, target_id, "http", qualified_rest_resource(external_microservice, "API"))
                continue
            if endpoint.id in matched_calls:
                continue
            external_id = add_node("external_api", endpoint.topic)
            add_edge(service_id, external_id, "calls_external", endpoint.topic, "medium" if endpoint.topic_dynamic else "high")

    # Relation service→service unique par client configuré, résolue contre le
    # registre des microservices (convention : domaine == artifactId de l'hôte).
    for caller, domains in configured_clients.items():
        caller_id = add_node("microservice", caller)
        for domain in sorted(domains):
            if domain in known_services:
                host_id = add_node("microservice", domain)
                add_edge(caller_id, host_id, "http", qualified_rest_resource(domain, "API"))
            else:
                result_warnings.append(
                    f"{caller} : client configuré du domaine « {domain} » — "
                    "aucun microservice hôte indexé (POM cible à analyser)."
                )

    for service, module in modules_by_service.items():
        if service not in endpoints_by_service:
            continue
        service_id = add_node("microservice", service)
        operations_by_collection: dict[str, set[str]] = {}
        for method in module.mongo_methods:
            if method.collection:
                kind = "writes" if method.operation in _MONGO_WRITE_OPERATIONS else "reads"
                operations_by_collection.setdefault(method.collection, set()).add(kind)
        for collection in sorted(module.mongo_collections):
            collection_id = add_node("mongodb_collection", collection, service)
            operations = operations_by_collection.get(collection, {"uses"})
            for operation in sorted(operations):
                add_edge(service_id, collection_id, operation, operation)

    ordered_nodes = sorted(nodes.values(), key=lambda item: (item["kind"], item["service"] or "", item["name"]))
    ordered_edges = sorted(edges.values(), key=lambda item: (item["source"], item["target"], item["kind"], item["label"]))
    return {
        "nodes": ordered_nodes,
        "edges": ordered_edges,
        "summary": {
            "microservices": sum(node["kind"] == "microservice" for node in ordered_nodes),
            "topics": sum(node["kind"] == "topic" for node in ordered_nodes),
            "mongodb_collections": sum(node["kind"] == "mongodb_collection" for node in ordered_nodes),
            "external_apis": sum(node["kind"] == "external_api" for node in ordered_nodes),
            "relations": len(ordered_edges),
            "configured_client_relations": sum(
                1
                for edge in ordered_edges
                if edge["kind"] == "http" and edge["label"].endswith(": API")
            ),
        },
        "warnings": result_warnings,
    }


def _event_cycles(graph: DependencyGraphResult) -> list[DependencyCycle]:
    """Return SCCs with several services linked by at least one Kafka topic."""
    relevant_kinds = {"publishes", "consumes", "http"}
    adjacency: dict[str, set[str]] = {node["id"]: set() for node in graph["nodes"]}
    for edge in graph["edges"]:
        if edge["kind"] in relevant_kinds:
            adjacency[edge["source"]].add(edge["target"])

    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    components: list[set[str]] = []

    def visit(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for target in sorted(adjacency[node]):
            if target not in indices:
                visit(target)
                lowlinks[node] = min(lowlinks[node], lowlinks[target])
            elif target in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[target])
        if lowlinks[node] != indices[node]:
            return
        component: set[str] = set()
        while stack:
            target = stack.pop()
            on_stack.remove(target)
            component.add(target)
            if target == node:
                break
        components.append(component)

    for node in sorted(adjacency):
        if node not in indices:
            visit(node)

    nodes_by_id = {node["id"]: node for node in graph["nodes"]}
    cycles: list[DependencyCycle] = []
    for component in components:
        members = [nodes_by_id[node] for node in sorted(component)]
        services = sorted({node["name"] for node in members if node["kind"] == "microservice"})
        if len(component) < 3 or len(services) < 2 or not any(node["kind"] == "topic" for node in members):
            continue
        cycles.append({"nodes": members, "services": services})
    return cycles


def audit_dependency_graph(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    modules_by_service: dict[str, DiscoveredModule],
    *,
    warnings: list[str] | None = None,
    relations: list[ArchitectureRelation] | None = None,
    strategy1: bool = False,
) -> DependencyAuditResult:
    """Combine inventory risks with graph-specific cycles and blocking patterns."""
    graph = build_dependency_graph(
        endpoints_by_service,
        modules_by_service,
        warnings=warnings,
        relations=relations,
        strategy1=strategy1,
    )
    risks = render_audit_json(
        assess_architecture(
            endpoints_by_service,
            (
                graph_edges_from_relations(relations, endpoints_by_service)
                if relations is not None
                else build_graph(endpoints_by_service, strategy1=strategy1)
            ),
            modules=list(modules_by_service.values()),
            endpoints_by_module=endpoints_by_service,
        )
    )
    cycles = _event_cycles(graph)
    issues: list[dict[str, object]] = [*risks]
    for cycle in cycles:
        issues.append({
            "id": "event-dependency-cycle",
            "severity": "WARNING",
            "title": "Cycle de dépendance événementielle",
            "evidence": " -> ".join(node["name"] for node in cycle["nodes"]),
            "services": cycle["services"],
            "confidence": "medium",
        })
    for service, endpoints in endpoints_by_service.items():
        for outbound_call in find_outbound_calls_in_consumers(endpoints):
            issues.append({
                "id": "synchronous-http-in-kafka-consumer",
                "severity": "WARNING",
                "title": "Appel HTTP synchrone dans un consumer Kafka",
                "evidence": (
                    f"{service} consomme `{outbound_call.consumer.topic}` puis appelle "
                    f"`{outbound_call.call.topic}` dans {outbound_call.call.path}:"
                    f"{outbound_call.call.start_line}."
                ),
                "services": [service],
                "confidence": "high",
            })
    return {"graph": graph, "issues": sorted(issues, key=lambda item: (str(item["severity"]), str(item["id"]), str(item["evidence"]))), "cycles": cycles}
