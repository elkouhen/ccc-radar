"""LikeC4 project export of the inferred architecture.

Renders a text-based LikeC4 model (services, topics, collections, external
APIs, build modules) with dedicated runtime/contracts/build/quality views.
"""

import re
from html import escape
from typing import Literal, TypedDict

from systemlens.graph import GraphEdge, external_microservice_names
from systemlens.models import Finding, MessageEndpoint
from systemlens.modules import DiscoveredModule, ModuleDependency
from systemlens.render._graph_view_helpers import _mongodb_collection_nodes, _rest_resources_served


class ComplexityRanking(TypedDict):
    level: Literal["low", "medium", "high"]
    rank: int
    population: int
    tier_start: int
    tier_end: int


def _likec4_identifier_map(prefix: str, names: list[str]) -> dict[str, str]:
    """Create deterministic LikeC4 identifiers while keeping source names as titles."""
    identifiers: dict[str, str] = {}
    used: set[str] = set()
    for name in sorted(names):
        normalized = re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_")
        base = f"{prefix}_{normalized or 'item'}"
        if base[0].isdigit():
            base = f"{prefix}_{base}"
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        identifiers[name] = candidate
    return identifiers


def _likec4_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", " ")


_MONGO_WRITE_OPERATIONS = frozenset({
    "bulkOps", "findAndModify", "findAndReplace", "insert", "remove", "save",
    "updateFirst", "updateMulti", "upsert",
})


def _complexity_levels(relation_counts: dict[str, int]) -> dict[str, str]:
    """Répartit un type de ressource en trois tiers de complexité équilibrés.

    Le score est le degré du nœud dans le graphe de dépendances. Les niveaux
    sont calculés séparément pour les microservices, topics Kafka et collections
    MongoDB afin qu'une catégorie peu nombreuse reste lisible. Les égalités de
    score sont départagées par l'identifiant pour conserver un export déterministe.
    """
    ranked_nodes = sorted(relation_counts, key=lambda node_id: (relation_counts[node_id], node_id))
    size, remainder = divmod(len(ranked_nodes), 3)
    group_sizes = [size, size, size]
    # Les éventuels services restants complètent d'abord les groupes les moins
    # complexes ; les tailles des tiers ne diffèrent jamais de plus d'un.
    for index in range(remainder):
        group_sizes[index] += 1

    return {
        node_id: ranking["level"]
        for node_id, ranking in _complexity_ranking(relation_counts).items()
    }


def _complexity_ranking(relation_counts: dict[str, int]) -> dict[str, ComplexityRanking]:
    """Return the soft tercile, rank, and inclusive bounds for each resource."""
    ranked_nodes = sorted(relation_counts, key=lambda node_id: (relation_counts[node_id], node_id))
    size, remainder = divmod(len(ranked_nodes), 3)
    group_sizes = [size, size, size]
    for index in range(remainder):
        group_sizes[index] += 1

    rankings: dict[str, ComplexityRanking] = {}
    offset = 0
    for level, group_size in zip(("low", "medium", "high"), group_sizes):
        typed_level: Literal["low", "medium", "high"] = level  # type: ignore[assignment]
        for rank, node_id in enumerate(ranked_nodes[offset:offset + group_size], start=offset + 1):
            rankings[node_id] = {
                "level": typed_level,
                "rank": rank,
                "population": len(ranked_nodes),
                "tier_start": offset + 1,
                "tier_end": offset + group_size,
            }
        offset += group_size
    return rankings


def render_request_reply_html(result: dict[str, object]) -> str:
    """Render a compact, standalone view of Strategy1 request/reply candidates."""
    patterns = result["patterns"]
    assert isinstance(patterns, list)
    rows = []
    for pattern in patterns:
        assert isinstance(pattern, dict)
        request_producers = ", ".join(pattern["request_producers"]) or "—"
        request_consumers = ", ".join(pattern["request_consumers"]) or "—"
        reply_producers = ", ".join(pattern["reply_producers"]) or "—"
        reply_consumers = ", ".join(pattern["reply_consumers"]) or "—"
        rows.append(
            "<article class=\"pattern\">"
            f"<div class=\"topic request\">{escape(str(pattern['request_topic']))}</div>"
            "<div class=\"arrow\">→<small>retour_</small></div>"
            f"<div class=\"topic reply\">{escape(str(pattern['reply_topic']))}</div>"
            "<dl>"
            f"<dt>Request producers</dt><dd>{escape(request_producers)}</dd>"
            f"<dt>Request consumers</dt><dd>{escape(request_consumers)}</dd>"
            f"<dt>Reply producers</dt><dd>{escape(reply_producers)}</dd>"
            f"<dt>Reply consumers</dt><dd>{escape(reply_consumers)}</dd>"
            "</dl></article>"
        )
    body = "\n".join(rows) or (
        "<p class=\"empty\">No indexed topic pair matches the "
        "<code>retour_&lt;request-topic&gt;</code> convention.</p>"
    )
    count = int(str(result["count"]))
    return f"""<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>Kafka request/reply patterns</title><style>
:root{{color-scheme:dark;font-family:Inter,ui-sans-serif,system-ui,sans-serif;background:#101827;color:#e6edf7}}body{{max-width:1120px;margin:0 auto;padding:42px 24px}}h1{{margin:0 0 8px}}.subtitle{{color:#9aabc4;margin:0 0 30px}}.badge{{display:inline-block;background:#5b21b6;color:#f5f3ff;border-radius:99px;padding:4px 10px;font-size:.85rem}}.pattern{{display:grid;grid-template-columns:minmax(180px,1fr) 72px minmax(180px,1fr);gap:16px;align-items:center;background:#172235;border:1px solid #263854;border-radius:14px;padding:20px;margin:14px 0}}.topic{{border-radius:9px;padding:13px;font-weight:650;overflow-wrap:anywhere}}.request{{background:#12375c;border:1px solid #2586d7}}.reply{{background:#402064;border:1px solid #9666e9}}.arrow{{text-align:center;font-size:2rem;color:#b794f6}}.arrow small{{display:block;font-size:.72rem;color:#9aabc4}}dl{{grid-column:1 / -1;display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:4px 0 0}}dt{{font-size:.76rem;color:#9aabc4}}dd{{margin:4px 0 0;overflow-wrap:anywhere}}.empty{{padding:24px;background:#172235;border-radius:12px}}@media(max-width:700px){{.pattern{{grid-template-columns:1fr}}.arrow{{transform:rotate(90deg)}}dl{{grid-template-columns:1fr 1fr}}}}
</style></head><body><span class=\"badge\">Strategy1 convention</span><h1>Kafka request/reply</h1><p class=\"subtitle\">{count} candidate pair(s) detected from <code>retour_&lt;request-topic&gt;</code>. This view is convention-based, not a runtime trace.</p>{body}</body></html>"""


def _likec4_complexity(
    service_ids: dict[str, str],
    topic_ids: dict[str, str],
    collection_ids: dict[str, str],
    external_api_ids: dict[str, str],
    relations: set[tuple[str, str, str, str]],
    findings_by_service: dict[str, list[Finding]],
) -> dict[str, tuple[str | None, str]]:
    """Build a microservice-only complexity signal while retaining finding details."""
    relation_counts = {
        node_id: 0
        for node_id in (*service_ids.values(), *topic_ids.values(), *collection_ids.values(), *external_api_ids.values())
    }
    for _, source, target, _ in relations:
        relation_counts[source] += 1
        relation_counts[target] += 1
    complexity_levels = _complexity_levels(
        {service_id: relation_counts[service_id] for service_id in service_ids.values()}
    )

    severities = ("ERROR", "WARNING", "INFO")
    details: dict[str, tuple[str | None, str]] = {}
    for service, service_id in service_ids.items():
        findings = findings_by_service.get(service, [])
        severity_counts = {
            severity: sum(1 for finding in findings if finding.severity == severity)
            for severity in severities
        }
        score = relation_counts[service_id]
        color = f"complexity_{complexity_levels[service_id]}"
        finding_summary = ", ".join(
            f"{severity}={count}" for severity, count in severity_counts.items() if count
        ) or "none"
        details[service_id] = (
            color,
            f"Complexity score {score}: {relation_counts[service_id]} relations; "
            f"{len(findings)} findings ({finding_summary})",
        )
    for node_id in (*topic_ids.values(), *collection_ids.values(), *external_api_ids.values()):
        score = relation_counts[node_id]
        details[node_id] = (None, f"{score} direct relations")
    return details


def render_graph_likec4(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    edges: list[GraphEdge],
    collections_by_service: dict[str, list[str]] | None = None,
    findings_by_service: dict[str, list[Finding]] | None = None,
    modules_by_service: dict[str, DiscoveredModule] | None = None,
    indexing_warnings: list[str] | None = None,
    build_modules: list[DiscoveredModule] | None = None,
    module_dependencies: list[ModuleDependency] | None = None,
) -> str:
    """Render the inferred architecture as a standalone LikeC4 model.

    The project exposes dedicated runtime, contracts, build and quality views.
    The source inventory is static, so relations carry protocol semantics but
    never claim to be runtime traces.
    """
    external_services = external_microservice_names(edges)
    services = sorted(set(endpoints_by_service) | external_services)
    topics = sorted({edge.from_endpoint.topic for edge in edges if edge.kind == "kafka"})
    collection_nodes = _mongodb_collection_nodes(collections_by_service)
    collection_names = {identity: collection for _service, collection, identity in collection_nodes}
    collection_services = {identity: service for service, _collection, identity in collection_nodes}
    matched_internal_call_ids = {edge.from_endpoint.id for edge in edges if edge.kind == "rest"}
    external_calls = sorted(
        [
            (service, endpoint)
            for service, endpoints in endpoints_by_service.items()
            for endpoint in endpoints
            if endpoint.system == "rest"
            and endpoint.role == "call"
            and endpoint.id not in matched_internal_call_ids
        ],
        key=lambda item: (item[0], item[1].topic, item[1].path, item[1].start_line),
    )
    external_apis = sorted({endpoint.topic for _service, endpoint in external_calls})
    module_details = modules_by_service or {}
    service_ids = _likec4_identifier_map("service", services)
    topic_ids = _likec4_identifier_map("topic", topics)
    collection_ids = _likec4_identifier_map("collection", sorted(collection_names))
    external_api_ids = _likec4_identifier_map("external_api", external_apis)
    build_module_details = {module.name: module for module in build_modules or []}
    build_module_names = sorted(
        set(build_module_details)
        | {name for dependency in module_dependencies or [] for name in (dependency.source, dependency.target)}
    )
    build_module_ids = _likec4_identifier_map("build_module", build_module_names)
    quality_warnings = list(dict.fromkeys(indexing_warnings or []))
    warning_ids = _likec4_identifier_map(
        "indexing_warning", [f"warning-{index}" for index in range(len(quality_warnings))]
    )
    topic_types: dict[str, dict[str, set[str]]] = {
        topic: {"published": set(), "consumed": set()} for topic in topics
    }
    for endpoints in endpoints_by_service.values():
        for endpoint in endpoints:
            if endpoint.system != "kafka" or endpoint.topic not in topic_types or not endpoint.message_type:
                continue
            direction = "published" if endpoint.role == "produce" else "consumed"
            topic_types[endpoint.topic][direction].add(endpoint.message_type)

    relations: set[tuple[str, str, str, str]] = set()
    for edge in edges:
        if edge.kind == "rest":
            relations.add((
                "http",
                service_ids[edge.from_service],
                service_ids[edge.to_service],
                "HTTP",
            ))
            continue
        topic_id = topic_ids[edge.from_endpoint.topic]
        produced_type = edge.from_endpoint.message_type
        assert edge.to_endpoint is not None, "a matched kafka edge always has a consumer endpoint"
        consumed_type = edge.to_endpoint.message_type
        relations.add((
            "publishes", service_ids[edge.from_service], topic_id,
            f"publishes {produced_type}" if produced_type else "publishes",
        ))
        relations.add((
            "consumes", topic_id, service_ids[edge.to_service],
            f"consumes {consumed_type}" if consumed_type else "consumes",
        ))
    for service, endpoint in external_calls:
        relations.add(("calls_external", service_ids[service], external_api_ids[endpoint.topic], endpoint.topic))
    for service, collection, identity in collection_nodes:
        if service not in service_ids:
            continue
        module = module_details.get(service)
        operations = {
            "writes_data" if method.operation in _MONGO_WRITE_OPERATIONS else "reads_data"
            for method in (module.mongo_methods if module else ())
            if method.collection == collection
        }
        if not operations:
            operations = {"uses_data"}
        for operation in operations:
            label = {"reads_data": "reads", "writes_data": "writes", "uses_data": "uses"}[operation]
            relations.add((operation, service_ids[service], collection_ids[identity], label))
    complexities = _likec4_complexity(
        service_ids, topic_ids, collection_ids, external_api_ids, relations, findings_by_service or {}
    )

    lines = [
        "// Generated by systemlens export microservices --c4. Do not edit generated identifiers.",
        "specification {",
        "  color complexity_low #2563EB",
        "  color complexity_medium #D97706",
        "  color complexity_high #DC2626",
        "  color outgoing #0F766E",
        "  color incoming #D97706",
        "  color data_access #2563EB",
        "  element system",
        "  element microservice {",
        "    notation 'Microservice'",
        "    style { shape component }",
        "  }",
        "  element external_microservice {",
        "    notation 'External microservice'",
        "    style { shape triangle }",
        "  }",
        "  element kafka_topic {",
        "    notation 'Kafka topic'",
        "    style { shape queue }",
        "  }",
        "  element mongodb_collection {",
        "    notation 'MongoDB collection'",
        "    style { shape rectangle }",
        "  }",
        "  element external_api {",
        "    notation 'External HTTP API'",
        "    style { shape browser }",
        "  }",
        "  element build_module {",
        "    notation 'Build module'",
        "    style { shape component }",
        "  }",
        "  element indexing_warning {",
        "    notation 'Indexing warning'",
        "    style { shape rectangle }",
        "  }",
        "  relationship http {",
        "    color outgoing",
        "    line solid",
        "    head vee",
        "    multiple true",
        "  }",
        "  relationship publishes {",
        "    color outgoing",
        "    line solid",
        "    head vee",
        "    multiple true",
        "  }",
        "  relationship consumes {",
        "    color incoming",
        "    line dotted",
        "    head vee",
        "    multiple true",
        "  }",
        "  relationship uses_data {",
        "    color data_access",
        "    line solid",
        "    head diamond",
        "    multiple true",
        "  }",
        "  relationship reads_data {",
        "    color data_access",
        "    line dotted",
        "    head vee",
        "    multiple true",
        "  }",
        "  relationship writes_data {",
        "    color outgoing",
        "    line solid",
        "    head vee",
        "    multiple true",
        "  }",
        "  relationship calls_external {",
        "    color outgoing",
        "    line dashed",
        "    head vee",
        "    multiple true",
        "  }",
        "  relationship build_dependency {",
        "    color incoming",
        "    line solid",
        "    head vee",
        "    multiple true",
        "  }",
        "}",
        "",
        "model {",
        "  radar = system 'Indexed microservice architecture' {",
    ]
    for service in services:
        color, description = complexities[service_ids[service]]
        service_endpoints = endpoints_by_service.get(service, [])
        openapi_files = module_details[service].openapi_files if service in module_details else ()
        published_resources = _rest_resources_served(service_endpoints)
        produced_messages = sorted({
            f"{endpoint.topic}{f' <{endpoint.message_type}>' if endpoint.message_type else ''}"
            for endpoint in service_endpoints
            if endpoint.system == "kafka" and endpoint.role == "produce"
        })
        consumed_messages = sorted({
            f"{endpoint.topic}{f' <{endpoint.message_type}>' if endpoint.message_type else ''}"
            for endpoint in service_endpoints
            if endpoint.system == "kafka" and endpoint.role == "consume"
        })
        if openapi_files:
            description = f"{description}; OpenAPI contracts: {', '.join(openapi_files)}"
        if published_resources:
            description = f"{description}; HTTP published: {', '.join(published_resources)}"
        if produced_messages:
            description = f"{description}; Kafka published: {', '.join(produced_messages)}"
        if consumed_messages:
            description = f"{description}; Kafka consumed: {', '.join(consumed_messages)}"
        if service in external_services:
            description = f"{description}; External microservice"
        lines.extend(
            [
                f"    {service_ids[service]} = {'external_microservice' if service in external_services else 'microservice'} '{_likec4_string(service)}' {{",
                "      technology 'Spring Boot'",
                f"      description '{_likec4_string(description)}'",
                f"      style {{ color {color} }}",
                "    }",
            ]
        )
    for topic in topics:
        color, description = complexities[topic_ids[topic]]
        published_types = sorted(topic_types[topic]["published"])
        consumed_types = sorted(topic_types[topic]["consumed"])
        if published_types:
            description = f"{description}; Published Java types: {', '.join(published_types)}"
        if consumed_types:
            description = f"{description}; Consumed Java types: {', '.join(consumed_types)}"
        lines.extend([
            f"    {topic_ids[topic]} = kafka_topic '{_likec4_string(topic)}' {{",
            "      technology 'Kafka'",
            f"      description '{_likec4_string(description)}'",
            *([f"      style {{ color {color} }}"] if color else []),
            "    }",
        ])
    for identity in sorted(collection_names):
        color, description = complexities[collection_ids[identity]]
        collection = collection_names[identity]
        service = collection_services[identity]
        lines.extend(
            [
                f"    {collection_ids[identity]} = mongodb_collection '{_likec4_string(collection)}' {{",
                "      technology 'MongoDB'",
                f"      description '{_likec4_string(description)}'",
                *([f"      style {{ color {color} }}"] if color else []),
                "    }",
            ]
        )
    for external_api in external_apis:
        color, description = complexities[external_api_ids[external_api]]
        lines.extend(
            [
                f"    {external_api_ids[external_api]} = external_api '{_likec4_string(external_api)}' {{",
                "      technology 'HTTP'",
                f"      description '{_likec4_string(description)}'",
                *([f"      style {{ color {color} }}"] if color else []),
                "    }",
            ]
        )
    for kind, source, target, label in sorted(relations):
        lines.append(f"    {source} -[{kind}]-> {target} '{_likec4_string(label)}'")
    lines.extend(
        [
            "  }",
            "  build = system 'Maven and Gradle dependencies' {",
        ]
    )
    for name in build_module_names:
        module = build_module_details.get(name)
        technology = module.build_system if module else "unknown"
        description = "Starts an application" if module and module.starts_application else "Shared build module"
        lines.extend(
            [
                f"    {build_module_ids[name]} = build_module '{_likec4_string(name)}' {{",
                f"      technology '{_likec4_string(technology)}'",
                f"      description '{_likec4_string(description)}'",
                "    }",
            ]
        )
    for dependency in sorted(module_dependencies or []):
        lines.append(
            f"    {build_module_ids[dependency.source]} -[build_dependency]-> "
            f"{build_module_ids[dependency.target]} 'depends on'"
        )
    lines.extend(
        [
            "  }",
            "  quality = system 'Indexing quality' {",
        ]
    )
    for index, warning in enumerate(quality_warnings):
        warning_name = f"warning-{index}"
        lines.extend(
            [
                f"    {warning_ids[warning_name]} = indexing_warning 'Warning {index + 1}' {{",
                f"      description '{_likec4_string(warning)}'",
                "    }",
            ]
        )
    lines.extend(
        [
            "  }",
            "}",
            "",
            "views {",
            "  view runtime {",
            "    title 'Runtime interactions: HTTP, Kafka and MongoDB'",
            "    include radar.**",
            "  }",
            "  view contracts {",
            "    title 'Published HTTP contracts and Kafka message types'",
            "    include radar.**",
            "  }",
        ]
    )
    if build_module_names:
        lines.extend(
            [
                "  view build {",
                "    title 'Maven and Gradle module dependencies'",
                "    include build.**",
                "  }",
            ]
        )
    lines.extend(
        [
            "  view quality {",
            "    title 'Connectivity complexity, findings and indexing warnings'",
            "    include radar.**",
            *(["    include quality.**"] if quality_warnings else []),
            "  }",
            "}",
            "",
        ]
    )
    return "\n".join(lines)
