"""Catalogue métier et navigation orientée architecture.

Les fonctions de ce module transforment les inventaires indexés en objets
stables (module, API, topic et collection), sans exposer les fichiers source.
L'accès à une implantation reste une opération explicite de la CLI.
"""

from collections import deque
from dataclasses import dataclass
from typing import cast

from archlens.graph import GraphEdge, build_graph, graph_edge_rest_resource
from archlens.models import ArchitectureRelation, MessageEndpoint
from archlens.modules import DiscoveredModule


_KINDS = {
    "module": "module",
    "modules": "module",
    "microservice": "microservice",
    "microservices": "microservice",
    "topic": "topic",
    "topics": "topic",
    "api": "api",
    "apis": "api",
    "collection": "collection",
    "collections": "collection",
    "dto": "dto",
    "dtos": "dto",
    "integration": "endpoint",
    "integrations": "endpoint",
    "endpoint": "endpoint",
    "endpoints": "endpoint",
}


@dataclass(frozen=True)
class ArchitectureCatalog:
    modules: tuple[DiscoveredModule, ...]
    endpoints: tuple[MessageEndpoint, ...]
    edges: tuple[GraphEdge, ...]

    @property
    def modules_by_name(self) -> dict[str, DiscoveredModule]:
        return {module.name: module for module in self.modules}


def normalize_kind(kind: str) -> str | None:
    return _KINDS.get(kind.casefold())


def build_catalog(
    modules: list[DiscoveredModule], endpoints: list[MessageEndpoint], *, strategy1: bool = False
) -> ArchitectureCatalog:
    endpoints_by_module: dict[str, list[MessageEndpoint]] = {}
    for endpoint in endpoints:
        if endpoint.module is not None:
            endpoints_by_module.setdefault(endpoint.module, []).append(endpoint)
    return ArchitectureCatalog(
        modules=tuple(sorted(modules, key=lambda module: module.name)),
        endpoints=tuple(endpoints),
        edges=tuple(build_graph(endpoints_by_module, strategy1=strategy1)),
    )


def module_summary(catalog: ArchitectureCatalog, name: str) -> dict[str, object] | None:
    module = catalog.modules_by_name.get(name)
    if module is None:
        return None
    endpoints = [endpoint for endpoint in catalog.endpoints if endpoint.module == name]
    served = sorted({endpoint.topic for endpoint in endpoints if endpoint.system == "rest" and endpoint.role == "serve"})
    called = sorted({endpoint.topic for endpoint in endpoints if endpoint.system == "rest" and endpoint.role == "call"})
    produced = sorted({endpoint.topic for endpoint in endpoints if endpoint.system == "kafka" and endpoint.role == "produce"})
    consumed = sorted({endpoint.topic for endpoint in endpoints if endpoint.system == "kafka" and endpoint.role == "consume"})
    produced_types = _kafka_message_types(endpoints, "produce")
    consumed_types = _kafka_message_types(endpoints, "consume")
    outgoing = sorted({edge.to_service for edge in catalog.edges if edge.from_service == name})
    incoming = sorted({edge.from_service for edge in catalog.edges if edge.to_service == name})
    matched_call_ids = {edge.from_endpoint.id for edge in catalog.edges if edge.kind == "rest" and edge.from_service == name}
    external_apis = sorted({
        endpoint.topic
        for endpoint in endpoints
        if endpoint.system == "rest" and endpoint.role == "call" and endpoint.id not in matched_call_ids
    })
    technologies = ["Java"]
    if module.starts_application:
        technologies.append("Spring Boot")
    if produced or consumed or module.kafka_methods:
        technologies.append("Kafka")
    if module.mongo_collections or module.mongo_methods:
        technologies.append("MongoDB")
    if module.openapi_files:
        technologies.append("OpenAPI")
    return {
        "kind": "microservice" if module.starts_application else "module",
        "name": module.name,
        "language": "Java",
        "build_tool": module.build_system,
        "version": module.version,
        "exposes_http_api": bool(served),
        "http_apis_exposed": served,
        "http_apis_consumed": called,
        "kafka_topics_published": produced,
        "kafka_topics_consumed": consumed,
        "kafka_message_types_published": produced_types,
        "kafka_message_types_consumed": consumed_types,
        "databases": {"mongodb_collections": list(module.mongo_collections)},
        "technologies": technologies,
        "openapi": bool(module.openapi_files),
        "openapi_files": list(module.openapi_files),
        "scheduled_tasks": [],
        "scheduled_tasks_detection": "not_available",
        "dependencies": {"outgoing_modules": outgoing, "incoming_modules": incoming},
        "external_apis": external_apis,
    }


def list_objects(catalog: ArchitectureCatalog, kind: str) -> list[dict[str, object]]:
    if kind == "microservice":
        return [
            summary for module in catalog.modules
            if module.starts_application
            if (summary := module_summary(catalog, module.name)) is not None
        ]
    if kind == "module":
        return [
            summary for module in catalog.modules
            if (summary := module_summary(catalog, module.name)) is not None
        ]
    if kind == "topic":
        topics = sorted({endpoint.topic for endpoint in catalog.endpoints if endpoint.system == "kafka"})
        return [topic_summary(catalog, topic) for topic in topics]
    if kind == "api":
        apis = sorted({endpoint.topic for endpoint in catalog.endpoints if endpoint.system == "rest"})
        return [api_summary(catalog, api) for api in apis]
    if kind == "collection":
        collections = sorted({collection for module in catalog.modules for collection in module.mongo_collections})
        return [collection_summary(catalog, collection) for collection in collections]
    if kind == "dto":
        dto_names = sorted({
            endpoint.message_type
            for endpoint in catalog.endpoints
            if endpoint.system == "kafka" and endpoint.message_type
        })
        return [dto_summary(catalog, dto) for dto in dto_names]
    if kind == "endpoint":
        return [endpoint_summary(endpoint) for endpoint in catalog.endpoints]
    return []


def topic_summary(catalog: ArchitectureCatalog, topic: str) -> dict[str, object]:
    endpoints = [endpoint for endpoint in catalog.endpoints if endpoint.system == "kafka" and endpoint.topic == topic]
    return {
        "kind": "topic",
        "name": topic,
        "producers": sorted({endpoint.module for endpoint in endpoints if endpoint.role == "produce" and endpoint.module}),
        "consumers": sorted({endpoint.module for endpoint in endpoints if endpoint.role == "consume" and endpoint.module}),
        "message_types_published": _kafka_message_types(endpoints, "produce").get(topic, []),
        "message_types_consumed": _kafka_message_types(endpoints, "consume").get(topic, []),
    }


def request_reply_patterns(catalog: ArchitectureCatalog) -> dict[str, object]:
    """Find Kafka request/reply candidates using the Strategy1 naming convention.

    A reply topic named ``retour_<request-topic>`` is paired only when the
    matching request topic is also indexed. This is a convention-based view,
    not evidence of a runtime request/reply exchange.
    """
    topics = sorted({
        endpoint.topic
        for endpoint in catalog.endpoints
        if endpoint.system == "kafka" and not endpoint.topic_dynamic
    })
    topics_by_folded_name = {topic.casefold(): topic for topic in topics}
    pairs = {
        (topics_by_folded_name[reply_topic[len("retour_"):].casefold()], reply_topic)
        for reply_topic in topics
        if reply_topic.casefold().startswith("retour_")
        and reply_topic[len("retour_"):].casefold() in topics_by_folded_name
    }
    patterns = []
    for request_topic, reply_topic in sorted(pairs, key=lambda pair: (pair[0], pair[1])):
        request = topic_summary(catalog, request_topic)
        reply = topic_summary(catalog, reply_topic)
        patterns.append(
            {
                "request_topic": request_topic,
                "reply_topic": reply_topic,
                "request_producers": request["producers"],
                "request_consumers": request["consumers"],
                "reply_producers": reply["producers"],
                "reply_consumers": reply["consumers"],
            }
        )
    return {
        "kind": "kafka_request_reply_patterns",
        "strategy": "strategy1",
        "detection": "topic naming convention: retour_<request-topic>",
        "confidence": "conventional",
        "count": len(patterns),
        "patterns": patterns,
    }


def api_summary(catalog: ArchitectureCatalog, api: str) -> dict[str, object]:
    endpoints = [endpoint for endpoint in catalog.endpoints if endpoint.system == "rest" and endpoint.topic == api]
    return {
        "kind": "api",
        "name": api,
        "providers": sorted({endpoint.module for endpoint in endpoints if endpoint.role == "serve" and endpoint.module}),
        "consumers": sorted({endpoint.module for endpoint in endpoints if endpoint.role == "call" and endpoint.module}),
    }


def collection_summary(catalog: ArchitectureCatalog, collection: str) -> dict[str, object]:
    modules = [module for module in catalog.modules if collection in module.mongo_collections]
    return {
        "kind": "collection",
        "name": collection,
        "modules": [module.name for module in modules],
        "operations": sum(
            1 for module in modules for method in module.mongo_methods if method.collection == collection
        ),
    }


def dto_summary(catalog: ArchitectureCatalog, dto: str) -> dict[str, object]:
    """Summarize a statically inferred Kafka Java message type.

    Only runtime modules are reported as microservices. A shared library can
    contain a Kafka helper but must not be presented as a deployable consumer
    or producer.
    """
    endpoints = [
        endpoint
        for endpoint in catalog.endpoints
        if endpoint.system == "kafka" and endpoint.message_type == dto
    ]
    microservices = {module.name for module in catalog.modules if module.starts_application}
    return {
        "kind": "dto",
        "name": dto,
        "topics": sorted({endpoint.topic for endpoint in endpoints}),
        "producer_microservices": sorted({
            endpoint.module
            for endpoint in endpoints
            if endpoint.role == "produce" and endpoint.module in microservices
        }),
        "consumer_microservices": sorted({
            endpoint.module
            for endpoint in endpoints
            if endpoint.role == "consume" and endpoint.module in microservices
        }),
    }


def endpoint_summary(endpoint: MessageEndpoint) -> dict[str, object]:
    return {
        "kind": "integration",
        "id": endpoint.id,
        "name": endpoint.topic,
        "topic": endpoint.topic,
        "role": endpoint.role,
        "system": endpoint.system,
        "module": endpoint.module,
        "framework": endpoint.framework,
        "message_type": endpoint.message_type,
        "dynamic": endpoint.topic_dynamic,
    }


def show_object(catalog: ArchitectureCatalog, kind: str, name: str) -> dict[str, object] | None:
    if kind in {"module", "microservice"}:
        summary = module_summary(catalog, name)
        if summary is None or (kind == "microservice" and summary["kind"] != "microservice"):
            return None
        return summary
    if kind == "topic" and any(endpoint.system == "kafka" and endpoint.topic == name for endpoint in catalog.endpoints):
        return topic_summary(catalog, name)
    if kind == "api" and any(endpoint.system == "rest" and endpoint.topic == name for endpoint in catalog.endpoints):
        return api_summary(catalog, name)
    if kind == "collection" and any(name in module.mongo_collections for module in catalog.modules):
        return collection_summary(catalog, name)
    if kind == "dto" and any(
        endpoint.system == "kafka" and endpoint.message_type == name
        for endpoint in catalog.endpoints
    ):
        return dto_summary(catalog, name)
    if kind == "endpoint":
        endpoint = next((item for item in catalog.endpoints if item.id == name), None)
        return endpoint_summary(endpoint) if endpoint else None
    return None


def _kafka_message_types(
    endpoints: list[MessageEndpoint], role: str
) -> dict[str, list[str]]:
    """Aggregate only statically inferred Java payload types by topic."""
    types: dict[str, set[str]] = {}
    for endpoint in endpoints:
        if endpoint.system != "kafka" or endpoint.role != role or not endpoint.message_type:
            continue
        types.setdefault(endpoint.topic, set()).add(endpoint.message_type)
    return {topic: sorted(values) for topic, values in sorted(types.items())}


def neighbors(catalog: ArchitectureCatalog, kind: str, name: str) -> list[dict[str, str]] | None:
    if show_object(catalog, kind, name) is None:
        return None
    related: set[tuple[str, str, str]] = set()
    if kind in {"module", "microservice"}:
        for endpoint in (endpoint for endpoint in catalog.endpoints if endpoint.module == name):
            object_kind = "topic" if endpoint.system == "kafka" else "api"
            relation = {
                "produce": "publishes",
                "consume": "consumes",
                "serve": "provides",
                "call": "calls",
            }[endpoint.role]
            related.add((object_kind, endpoint.topic, relation))
        module = catalog.modules_by_name[name]
        for collection in module.mongo_collections:
            related.add(("collection", collection, "uses"))
        for edge in catalog.edges:
            if edge.from_service == name:
                related.add(("module", edge.to_service, "depends_on"))
            if edge.to_service == name:
                related.add(("module", edge.from_service, "used_by"))
    elif kind == "topic":
        for endpoint in (endpoint for endpoint in catalog.endpoints if endpoint.system == "kafka" and endpoint.topic == name):
            if endpoint.module:
                related.add(("module", endpoint.module, "producer" if endpoint.role == "produce" else "consumer"))
    elif kind == "api":
        for endpoint in (endpoint for endpoint in catalog.endpoints if endpoint.system == "rest" and endpoint.topic == name):
            if endpoint.module:
                related.add(("module", endpoint.module, "provider" if endpoint.role == "serve" else "consumer"))
    elif kind == "dto":
        for endpoint in (
            item
            for item in catalog.endpoints
            if item.system == "kafka" and item.message_type == name
        ):
            related.add(("topic", endpoint.topic, "published_as" if endpoint.role == "produce" else "consumed_as"))
            if endpoint.module:
                related.add(("module", endpoint.module, "producer" if endpoint.role == "produce" else "consumer"))
    elif kind == "endpoint":
        endpoint = next(item for item in catalog.endpoints if item.id == name)
        if endpoint.module:
            related.add(("module", endpoint.module, "belongs_to"))
        related.add(("topic" if endpoint.system == "kafka" else "api", endpoint.topic, "implements"))
    else:
        for module in catalog.modules:
            if name in module.mongo_collections:
                related.add(("module", module.name, "uses"))
    return [
        {"kind": item_kind, "name": item_name, "relation": relation}
        for item_kind, item_name, relation in sorted(related)
    ]


def find_microservice_paths(
    catalog: ArchitectureCatalog,
    source: str,
    target: str,
    *,
    max_depth: int = 12,
    limit: int = 20,
) -> dict[str, object] | None:
    """Return bounded shortest directed paths between two microservices.

    Kafka is represented by an explicit topic node, preserving the same
    topology as the interactive graph exports. REST stays a direct
    service-to-service relation labelled with the matched API.
    """
    if (
        show_object(catalog, "microservice", source) is None
        or show_object(catalog, "microservice", target) is None
    ):
        return None
    source_node = ("microservice", source)
    target_node = ("microservice", target)
    adjacency: dict[tuple[str, str], list[tuple[tuple[str, str], dict[str, str]]]] = {}

    def add_edge(
        origin: tuple[str, str], destination: tuple[str, str], relation: dict[str, str]
    ) -> None:
        adjacency.setdefault(origin, []).append((destination, relation))

    for edge in catalog.edges:
        origin = ("microservice", edge.from_service)
        destination = ("microservice", edge.to_service)
        if edge.kind == "rest":
            add_edge(
                origin,
                destination,
                {
                    "kind": "http",
                    "label": graph_edge_rest_resource(edge),
                },
            )
            continue
        topic = ("topic", edge.from_endpoint.topic)
        add_edge(origin, topic, {"kind": "publishes", "label": edge.from_endpoint.topic})
        add_edge(topic, destination, {"kind": "consumes", "label": edge.from_endpoint.topic})
    for entries in adjacency.values():
        entries.sort(key=lambda item: (item[0], item[1]["kind"], item[1]["label"]))

    queue: deque[tuple[tuple[str, str], list[tuple[str, str]], list[dict[str, str]]]] = deque(
        [(source_node, [source_node], [])]
    )
    paths: list[dict[str, object]] = []
    shortest_depth: int | None = None
    truncated = False
    while queue:
        node, nodes, relations = queue.popleft()
        depth = len(relations)
        if node == target_node:
            if shortest_depth is None:
                shortest_depth = depth
            if depth != shortest_depth:
                continue
            if len(paths) >= limit:
                truncated = True
                continue
            paths.append(
                {
                    "nodes": [{"kind": kind, "name": name} for kind, name in nodes],
                    "relations": relations,
                }
            )
            continue
        if depth >= max_depth or (shortest_depth is not None and depth >= shortest_depth):
            continue
        for next_node, relation in adjacency.get(node, []):
            if next_node in nodes:
                continue
            queue.append((next_node, [*nodes, next_node], [*relations, relation]))
    return {
        "kind": "microservice_paths",
        "source": source,
        "target": target,
        "paths": paths,
        "max_depth": max_depth,
        "truncated": truncated,
    }


def analyze(catalog: ArchitectureCatalog, query: str, target: str | None) -> dict[str, object] | None:
    normalized = query.casefold()
    if normalized in {"consumers", "consumer"} and target:
        result = show_object(catalog, "topic", target)
        return {"query": "consumers", "topic": target, "microservices": result["consumers"]} if result else None
    if normalized in {"producers", "producer"} and target:
        result = show_object(catalog, "topic", target)
        return {"query": "producers", "topic": target, "microservices": result["producers"]} if result else None
    if normalized in {"calls", "dependencies"} and target:
        result = show_object(catalog, "module", target)
        return {"query": "calls", "module": target, "dependencies": result["dependencies"]} if result else None
    if normalized in {"external-apis", "external_api"}:
        items = [
            {"module": module.name, "apis": summary["external_apis"]}
            for module in catalog.modules
            if (summary := module_summary(catalog, module.name)) is not None and summary["external_apis"]
        ]
        return {"query": "external-apis", "items": items}
    if normalized in {"orphan-integrations", "orphan-endpoints", "orphans"}:
        results: list[dict[str, object]] = []
        for topic in list_objects(catalog, "topic"):
            if not topic["producers"] or not topic["consumers"]:
                results.append(topic)
        for api in list_objects(catalog, "api"):
            if not api["providers"] or not api["consumers"]:
                results.append(api)
        return {"query": "orphan-integrations", "items": results}
    if normalized == "impact" and target:
        for kind in ("module", "topic", "api", "collection"):
            impact_neighbors = neighbors(catalog, kind, target)
            if impact_neighbors is not None:
                return {
                    "query": "impact",
                    "object": {"kind": kind, "name": target},
                    "neighbors": impact_neighbors,
                }
    return None


def trace_topic_flows(
    catalog: ArchitectureCatalog, topic: str, *, max_depth: int = 6, limit: int = 50
) -> dict[str, object] | None:
    """Explore des enchaînements Kafka plausibles au niveau microservice.

    Un inventaire d'endpoints ne permet pas d'établir la causalité entre une
    consommation et une production au sein d'un même service. Les chemins
    renvoyés sont donc des pistes de compréhension, jamais une trace runtime.
    """
    kafka_endpoints = [
        endpoint
        for endpoint in catalog.endpoints
        if endpoint.system == "kafka" and not endpoint.topic_dynamic and endpoint.module
    ]
    known_topics = {endpoint.topic for endpoint in kafka_endpoints}
    if topic not in known_topics:
        return None
    consumers_by_topic: dict[str, set[str]] = {}
    published_by_module: dict[str, set[str]] = {}
    for endpoint in kafka_endpoints:
        assert endpoint.module is not None
        if endpoint.role == "consume":
            consumers_by_topic.setdefault(endpoint.topic, set()).add(endpoint.module)
        elif endpoint.role == "produce":
            published_by_module.setdefault(endpoint.module, set()).add(endpoint.topic)

    flows: list[dict[str, object]] = []
    truncated = False

    def add_flow(nodes: list[dict[str, str]], cycle_detected: bool = False) -> bool:
        nonlocal truncated
        if len(flows) >= limit:
            truncated = True
            return False
        flow: dict[str, object] = {"nodes": nodes}
        if cycle_detected:
            flow["cycle_detected"] = True
        flows.append(flow)
        return True

    def visit(current_topic: str, nodes: list[dict[str, str]], seen_topics: set[str], depth: int) -> None:
        nonlocal truncated
        for consumer in sorted(consumers_by_topic.get(current_topic, ())):
            if truncated:
                return
            service_node = {"kind": "microservice", "name": consumer}
            branch = [*nodes, service_node]
            next_topics = sorted(published_by_module.get(consumer, ()))
            if depth >= max_depth or not next_topics:
                add_flow(branch)
                continue
            for next_topic in next_topics:
                topic_node = {"kind": "topic", "name": next_topic}
                if next_topic in seen_topics:
                    add_flow([*branch, topic_node], cycle_detected=True)
                else:
                    visit(next_topic, [*branch, topic_node], seen_topics | {next_topic}, depth + 1)

    visit(topic, [{"kind": "topic", "name": topic}], {topic}, 1)
    return {
        "kind": "potential_topic_flows",
        "topic": topic,
        "max_depth": max_depth,
        "flows": flows,
        "truncated": truncated,
        "caveat": (
            "Les transitions d'un microservice consommateur vers ses topics publiés "
            "sont des hypothèses d'exploration, pas des traces d'exécution."
        ),
    }


def endpoint_implementation(catalog: ArchitectureCatalog, endpoint_id: str) -> dict[str, object] | None:
    endpoint = next((item for item in catalog.endpoints if item.id == endpoint_id), None)
    if endpoint is None:
        return None
    return {
        "kind": "integration",
        "id": endpoint.id,
        "name": endpoint.topic,
        "role": endpoint.role,
        "system": endpoint.system,
        "module": endpoint.module,
        "message_type": endpoint.message_type,
        "implementation": {
            "path": endpoint.path,
            "start_line": endpoint.start_line,
            "end_line": endpoint.end_line,
            "qualified_name": endpoint.qualified_name,
            "snippet": endpoint.snippet,
        },
    }


def inventory_coverage(
    catalog: ArchitectureCatalog, relations: list[ArchitectureRelation]
) -> dict[str, object]:
    """Summarize known and unresolved architecture facts without source code."""
    kafka_endpoints = [endpoint for endpoint in catalog.endpoints if endpoint.system == "kafka"]
    rest_calls = [
        endpoint
        for endpoint in catalog.endpoints
        if endpoint.system == "rest" and endpoint.role == "call"
    ]
    matched_call_ids = {edge.from_endpoint.id for edge in catalog.edges if edge.kind == "rest"}
    dynamic_topics = [endpoint for endpoint in kafka_endpoints if endpoint.topic_dynamic]
    unknown_types = [endpoint for endpoint in kafka_endpoints if not endpoint.message_type]
    unmatched_calls = [endpoint for endpoint in rest_calls if endpoint.id not in matched_call_ids]
    by_relation: dict[str, int] = {}
    by_confidence: dict[str, int] = {}
    for relation in relations:
        by_relation[relation.relation] = by_relation.get(relation.relation, 0) + 1
        by_confidence[relation.confidence] = by_confidence.get(relation.confidence, 0) + 1

    def unresolved(endpoint: MessageEndpoint) -> dict[str, object]:
        return {
            "module": endpoint.module,
            "topic_or_api": endpoint.topic,
            "role": endpoint.role,
            "path": endpoint.path,
            "line": endpoint.start_line,
        }

    return {
        "kind": "inventory_coverage",
        "integrations": {
            "total": len(catalog.endpoints),
            "kafka": len(kafka_endpoints),
            "http_calls": len(rest_calls),
        },
        "relations": {
            "total": len(relations),
            "by_type": dict(sorted(by_relation.items())),
            "by_confidence": dict(sorted(by_confidence.items())),
        },
        "unresolved": {
            "dynamic_kafka_topics": [unresolved(endpoint) for endpoint in dynamic_topics[:20]],
            "unknown_kafka_message_types": [unresolved(endpoint) for endpoint in unknown_types[:20]],
            "unmatched_http_calls": [unresolved(endpoint) for endpoint in unmatched_calls[:20]],
            "truncated": any(len(items) > 20 for items in (dynamic_topics, unknown_types, unmatched_calls)),
        },
    }


def indexing_issues(
    catalog: ArchitectureCatalog, warnings: list[str] | tuple[str, ...] = ()
) -> dict[str, object]:
    """Return unresolved facts with source evidence for remediation analysis.

    Unlike :func:`inventory_coverage`, this view is intentionally itemized and
    includes the original extracted expression so an external reviewer or AI
    can assess a proposed heuristic without guessing from a summary.
    """
    issues: list[dict[str, object]] = []

    def endpoint_issue(
        code: str, severity: str, message: str, endpoint: MessageEndpoint
    ) -> None:
        issues.append(
            {
                "code": code,
                "severity": severity,
                "message": message,
                "service": endpoint.module,
                "system": endpoint.system,
                "role": endpoint.role,
                "topic_or_api": endpoint.topic,
                "topic_dynamic": endpoint.topic_dynamic,
                "message_type": endpoint.message_type,
                "framework": endpoint.framework,
                "source": {
                    "path": endpoint.path,
                    "start_line": endpoint.start_line,
                    "end_line": endpoint.end_line,
                    "snippet": endpoint.snippet,
                },
            }
        )

    for warning in dict.fromkeys(warnings):
        issues.append(
            {
                "code": "inventory_warning",
                "severity": "warning",
                "message": warning,
                "service": None,
                "source": None,
            }
        )

    matched_http_call_ids = {edge.from_endpoint.id for edge in catalog.edges if edge.kind == "rest"}
    for endpoint in sorted(catalog.endpoints, key=lambda item: (item.path, item.start_line, item.id)):
        service = endpoint.module or "service inconnu"
        if endpoint.system == "kafka" and endpoint.topic_dynamic:
            endpoint_issue(
                "dynamic_kafka_topic",
                "warning",
                f"{service} : le topic {endpoint.topic!r} ne peut pas etre resolu statiquement.",
                endpoint,
            )
        if endpoint.system == "kafka" and not endpoint.message_type:
            endpoint_issue(
                "unknown_kafka_message_type",
                "info",
                f"{service} : le type Java du message sur {endpoint.topic!r} n'a pas ete deduit.",
                endpoint,
            )
        if endpoint.system == "rest" and endpoint.role == "call" and endpoint.id not in matched_http_call_ids:
            endpoint_issue(
                "unmatched_http_call",
                "warning" if endpoint.topic_dynamic else "info",
                f"{service} : aucun microservice fournisseur n'a ete identifie pour {endpoint.topic!r}.",
                endpoint,
            )

    severity_rank = {"warning": 0, "info": 1}
    issues.sort(key=lambda issue: (severity_rank[str(issue["severity"])], str(issue["code"]), str(issue["message"])))
    by_code: dict[str, int] = {}
    for issue in issues:
        code = str(issue["code"])
        by_code[code] = by_code.get(code, 0) + 1
    return {"kind": "indexing_issues", "count": len(issues), "by_code": by_code, "issues": issues}


def render_text(result: object) -> str:
    if isinstance(result, list):
        if not result:
            return "Aucun objet d'architecture trouvé."
        return "\n".join(_render_item(item) for item in result)
    if isinstance(result, dict):
        return _render_item(result)
    return str(result)


def _render_item(item: dict[str, object]) -> str:
    kind = item.get("kind") or item.get("query", "result")
    if kind == "inventory_coverage":
        integrations = cast("dict[str, object]", item["integrations"])
        relations = cast("dict[str, object]", item["relations"])
        unresolved = cast("dict[str, object]", item["unresolved"])
        lines = [
            "[couverture de l'inventaire]",
            f"  integrations={integrations['total']} kafka={integrations['kafka']} appels_http={integrations['http_calls']}",
            f"  relations={relations['total']} confiance={relations['by_confidence']}",
            f"  topics_kafka_dynamiques={len(cast('list', unresolved['dynamic_kafka_topics']))}",
            f"  types_kafka_inconnus={len(cast('list', unresolved['unknown_kafka_message_types']))}",
            f"  appels_http_non_rapproches={len(cast('list', unresolved['unmatched_http_calls']))}",
        ]
        if unresolved["truncated"]:
            lines.append("  Details non resolus tronques a 20 elements par categorie.")
        return "\n".join(lines)
    if kind == "potential_topic_flows":
        lines = [f"[flux potentiels] {item['topic']}", str(item["caveat"])]
        for flow in cast("list[dict[str, object]]", item["flows"]):
            nodes = cast("list[dict[str, object]]", flow["nodes"])
            path = " -> ".join(str(node["name"]) for node in nodes)
            cycle = " (cycle)" if flow.get("cycle_detected") else ""
            lines.append(f"  {path}{cycle}")
        if item["truncated"]:
            lines.append("  Résultats tronqués par la limite demandée.")
        return "\n".join(lines)
    if kind == "microservice_paths":
        lines = [f"[chemins] {item['source']} -> {item['target']}"]
        if not item["paths"]:
            lines.append("  Aucun chemin dirigé trouvé.")
        for route in cast("list[dict[str, object]]", item["paths"]):
            nodes = cast("list[dict[str, object]]", route["nodes"])
            lines.append("  " + " -> ".join(str(node["name"]) for node in nodes))
        if item["truncated"]:
            lines.append("  Résultats tronqués par la limite demandée.")
        return "\n".join(lines)
    if kind == "kafka_request_reply_patterns":
        lines = [f"[patterns Kafka request/reply] {item['count']} détecté(s)"]
        for pattern in cast("list[dict[str, object]]", item["patterns"]):
            lines.append(f"  {pattern['request_topic']} -> {pattern['reply_topic']}")
            request_producers = cast("list[str]", pattern["request_producers"])
            request_consumers = cast("list[str]", pattern["request_consumers"])
            reply_producers = cast("list[str]", pattern["reply_producers"])
            reply_consumers = cast("list[str]", pattern["reply_consumers"])
            lines.append(
                "    requête: producteurs="
                f"{', '.join(request_producers) or '-'} "
                "consommateurs="
                f"{', '.join(request_consumers) or '-'}"
            )
            lines.append(
                "    réponse: producteurs="
                f"{', '.join(reply_producers) or '-'} "
                "consommateurs="
                f"{', '.join(reply_consumers) or '-'}"
            )
        if not item["patterns"]:
            lines.append("  Aucun couple correspondant à la convention retour_<request-topic>.")
        return "\n".join(lines)
    name = item.get("name") or item.get("topic") or item.get("module") or ""
    details = []
    for key, raw_value in item.items():
        if key in {"kind", "name", "topic", "module", "query"} or raw_value in (None, [], {}, False):
            continue
        value: object = raw_value
        if isinstance(raw_value, dict):
            value = ", ".join(f"{entry}={content}" for entry, content in raw_value.items() if content)
        elif isinstance(raw_value, list):
            value = ", ".join(str(entry) for entry in raw_value)
        details.append(f"{key}={value}")
    return f"[{kind}] {name}" + ("\n  " + "\n  ".join(details) if details else "")
