"""Build normalized architecture relations from the indexed inventories."""

import re
from typing import TypedDict

from systemlens.models import ArchitectureRelation, MessageEndpoint, compute_architecture_relation_id
from systemlens.graph import build_graph, group_endpoints_by_module
from systemlens.modules import DiscoveredModule, ModuleDependency, module_identity
from systemlens.scanner import _local_spring_application_names


class _RelationEvidence(TypedDict):
    origin: str
    confidence: str
    module: str | None
    path: str | None
    start_line: int | None
    end_line: int | None
    qualified_name: str | None


_MONGO_WRITE_OPERATIONS = frozenset({
    "bulkOps", "findAndModify", "findAndReplace", "insert", "remove", "save",
    "updateFirst", "updateMulti", "upsert",
})
_SPRING_PROPERTY_RE = re.compile(r"\$\{\s*([^}:\s]+)")


def _relation(
    source_kind: str,
    source_name: str,
    relation: str,
    target_kind: str,
    target_name: str,
    *,
    origin: str,
    confidence: str,
    module: str | None = None,
    path: str | None = None,
    start_line: int | None = None,
    end_line: int | None = None,
    qualified_name: str | None = None,
) -> ArchitectureRelation:
    return ArchitectureRelation(
        id=compute_architecture_relation_id(
            source_kind, source_name, relation, target_kind, target_name, path, start_line
        ),
        source_kind=source_kind,
        source_name=source_name,
        relation=relation,
        target_kind=target_kind,
        target_name=target_name,
        origin=origin,
        confidence=confidence,
        module=module,
        path=path,
        start_line=start_line,
        end_line=end_line,
        qualified_name=qualified_name,
    )


def build_architecture_relations(
    modules: list[DiscoveredModule],
    endpoints: list[MessageEndpoint],
    dependencies: list[ModuleDependency],
    *,
    kafka_reply_strategy1: bool = False,
) -> list[ArchitectureRelation]:
    """Materialize relations only when an indexed fact provides evidence."""
    module_kinds = {
        module_identity(module): "microservice" if module.starts_application else "module"
        for module in modules
    }
    relations: dict[str, ArchitectureRelation] = {}

    def add(relation: ArchitectureRelation) -> None:
        relations[relation.id] = relation

    for dependency in dependencies:
        add(_relation(
            module_kinds.get(dependency.source, "module"), dependency.source, "depends_on",
            module_kinds.get(dependency.target, "module"), dependency.target,
            origin="derived", confidence="high", module=dependency.source,
        ))

    for endpoint in endpoints:
        if endpoint.module is None:
            continue
        source_kind = module_kinds.get(endpoint.module, "module")
        target_kind = "topic" if endpoint.system == "kafka" else "api"
        relation = {
            "produce": "publishes",
            "consume": "consumes",
            "serve": "provides",
            "call": "calls",
        }[endpoint.role]
        confidence = "medium" if endpoint.topic_dynamic else "high"
        evidence: _RelationEvidence = {
            "origin": endpoint.source,
            "confidence": confidence,
            "module": endpoint.module,
            "path": endpoint.path,
            "start_line": endpoint.start_line,
            "end_line": endpoint.end_line,
            "qualified_name": endpoint.qualified_name,
        }
        add(_relation(
            source_kind, endpoint.module, relation, target_kind, endpoint.topic, **evidence
        ))
        if endpoint.qualified_name:
            add(_relation(
                "class", endpoint.qualified_name, "implements", target_kind, endpoint.topic, **evidence
            ))
        property_source_kind = "class" if endpoint.qualified_name else source_kind
        property_source_name = endpoint.qualified_name or endpoint.module
        for property_key in sorted(set(_SPRING_PROPERTY_RE.findall(endpoint.snippet))):
            add(_relation(
                property_source_kind,
                property_source_name,
                "uses_configuration",
                "property",
                property_key,
                **evidence,
            ))
        if endpoint.system == "kafka" and endpoint.message_type:
            dto_relation = "publishes_type" if endpoint.role == "produce" else "consumes_type"
            add(_relation(
                "topic", endpoint.topic, dto_relation, "dto", endpoint.message_type, **evidence
            ))

    if kafka_reply_strategy1:
        kafka_topics = {
            endpoint.topic
            for endpoint in endpoints
            if endpoint.system == "kafka" and not endpoint.topic_dynamic
        }
        for reply_topic in sorted(kafka_topics):
            if not reply_topic.casefold().startswith("retour_"):
                continue
            request_topic = reply_topic[len("retour_"):]
            if request_topic and request_topic in kafka_topics:
                add(_relation(
                    "topic", request_topic, "request_reply", "topic", reply_topic,
                    origin="derived", confidence="high",
                ))

    for module in modules:
        identity = module_identity(module)
        source_kind = module_kinds[identity]
        for method in module.mongo_methods:
            if not method.collection:
                continue
            add(_relation(
                source_kind,
                identity,
                "writes" if method.operation in _MONGO_WRITE_OPERATIONS else "reads",
                "collection",
                method.collection,
                origin="code",
                confidence="high",
                module=identity,
                path=method.path,
                start_line=method.line,
                end_line=method.line,
            ))
            if method.owner_method:
                add(_relation(
                    "method",
                    f"{identity}:{method.owner_method}",
                    "writes" if method.operation in _MONGO_WRITE_OPERATIONS else "reads",
                    "collection",
                    method.collection,
                    origin="code",
                    confidence="high",
                    module=identity,
                    path=method.path,
                    start_line=method.line,
                    end_line=method.line,
                ))

    # Inter-service links are materialized from the same conservative graph
    # resolver used by all delivery adapters.  They are snapshot facts, not a
    # renderer-specific route coincidence.
    service_aliases = {
        module_identity(module): _local_spring_application_names(module.path, None)
        for module in modules
    }
    for edge in build_graph(
        group_endpoints_by_module(endpoints), strategy1=kafka_reply_strategy1,
        service_aliases=service_aliases,
    ):
        relation_name = "calls_service" if edge.kind == "rest" else "publishes_to"
        add(_relation(
            "microservice",
            edge.from_service,
            relation_name,
            "microservice",
            edge.to_service,
            origin=edge.from_endpoint.source,
            confidence="medium" if edge.from_endpoint.topic_dynamic else "high",
            module=edge.from_service,
            path=edge.from_endpoint.path,
            start_line=edge.from_endpoint.start_line,
            end_line=edge.from_endpoint.end_line,
            qualified_name=edge.from_endpoint.qualified_name,
        ))
    return sorted(
        relations.values(),
        key=lambda item: (
            item.source_kind, item.source_name, item.relation, item.target_kind,
            item.target_name, item.path or "", item.start_line or 0,
        ),
    )
