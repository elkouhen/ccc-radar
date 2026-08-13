import json
import os
import re
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Literal, NotRequired, TypedDict
from urllib.parse import quote

import yaml

from systemlens.flow import FlowResult
from systemlens.graph import (
    GraphEdge,
    OutboundCallInConsumer,
    external_microservice_names,
    resolve_rest_target_service,
    graph_edge_rest_resource,
)
from systemlens import java_parser
from systemlens.models import ExtractionDiagnostic, Finding, MessageEndpoint
from systemlens.modules import DiscoveredModule, ModuleDependency
from systemlens.search import SearchHit, Summary, get_context
from systemlens.render_snapshot import kafka_dto_views
from systemlens.workspace import DiscoveredService, FederationResult


class FindingHit(TypedDict):
    """Shape returned by `systemlens search --json` and the `search_findings` MCP tool."""

    id: str
    rule_id: str
    severity: str
    message: str
    path: str
    start_line: int
    end_line: int
    score: float
    fix: str | None
    cwe: list[str]
    owasp: list[str]
    context: str | None
    context_error: str | None


class ComplexityRanking(TypedDict):
    level: Literal["low", "medium", "high"]
    rank: int
    population: int
    tier_start: int
    tier_end: int


class RuleCount(TypedDict):
    rule_id: str
    count: int


class FindingsSummary(TypedDict):
    """Shape returned by `systemlens summary --json` and the `findings_summary` MCP tool."""

    by_severity: dict[str, int]
    top_rules: list[RuleCount]
    by_top_level_dir: dict[str, int]


def render_search_text(hits: list[SearchHit], repo_root: Path, include_context: bool) -> str:
    lines = []
    for i, hit in enumerate(hits, start=1):
        finding = hit.finding
        lines.append(
            f"{i}. [{finding.severity}] {finding.rule_id}  "
            f"{finding.path}:{finding.start_line}-{finding.end_line}  ({hit.score:.2f})"
        )
        lines.append(f"   {finding.message}")
        if include_context:
            try:
                context = get_context(repo_root, finding)
            except OSError as exc:
                lines.append(f"   contexte indisponible : {exc}")
            else:
                for context_line in context.splitlines():
                    lines.append(f"   {context_line}")
    return "\n".join(lines)


def render_search_json(
    hits: list[SearchHit], repo_root: Path, include_context: bool
) -> list[FindingHit]:
    results: list[FindingHit] = []
    for hit in hits:
        finding = hit.finding
        context: str | None = None
        context_error: str | None = None
        if include_context:
            try:
                context = get_context(repo_root, finding)
            except OSError as exc:
                context_error = str(exc)
        results.append(
            FindingHit(
                id=finding.id,
                rule_id=finding.rule_id,
                severity=finding.severity,
                message=finding.message,
                path=finding.path,
                start_line=finding.start_line,
                end_line=finding.end_line,
                score=hit.score,
                fix=finding.fix,
                cwe=finding.cwe,
                owasp=finding.owasp,
                context=context,
                context_error=context_error,
            )
        )
    return results


def render_summary_text(result: Summary) -> str:
    severities = " | ".join(f"{sev} {count}" for sev, count in result.by_severity.items())
    top_rules = ", ".join(f"{rule} ({count})" for rule, count in result.top_rules)
    top_dirs = ", ".join(f"{d} ({count})" for d, count in result.by_top_level_dir.items())
    return "\n".join(
        [
            severities,
            f"top règles : {top_rules}",
            f"top répertoires : {top_dirs}",
        ]
    )


def render_summary_json(result: Summary) -> FindingsSummary:
    return FindingsSummary(
        by_severity=result.by_severity,
        top_rules=[RuleCount(rule_id=r, count=c) for r, c in result.top_rules],
        by_top_level_dir=result.by_top_level_dir,
    )


class GraphSite(TypedDict):
    path: str
    start_line: int
    end_line: int
    topic: str


class GraphNodeInfo(TypedDict):
    name: str
    kind: str  # "microservice" | "kafka_topic"
    external: NotRequired[bool]
    shape: NotRequired[str]


class OutboundCallHit(TypedDict):
    """Un appel REST détecté à l'intérieur d'un handler de consommation
    Kafka (BACKLOG-10 K12)."""

    consumer: GraphSite
    call: GraphSite


class GraphEdgeInfo(TypedDict):
    kind: str  # "rest" | "kafka_produce" | "kafka_consume"
    from_node: str
    from_kind: str  # "microservice" | "kafka_topic"
    to_node: str
    to_kind: str  # "microservice" | "kafka_topic"
    label: str
    from_site: GraphSite | None
    to_site: GraphSite | None


class GraphResult(TypedDict):
    """Shape returned by `systemlens export microservices --json` and the MCP `graph` tool.

    `services`/`nodes`/`edges` restent vides tant qu'aucune donnée
    inter-module n'est disponible : ni fédération explicite
    (`--workspace`/`workspace_root`, BACKLOG-11 A2), ni endpoints attribués à
    un module Maven par l'indexation d'un répertoire parent multi-modules
    (BACKLOG-13 M1/M2/M3) — voir `note`.
    """

    services: list[str]
    nodes: list[GraphNodeInfo]
    edges: list[GraphEdgeInfo]
    outbound_calls_in_consumers: list[OutboundCallHit]
    note: str


_NO_CROSS_MODULE_DATA_NOTE = (
    "La topologie inter-services nécessite soit un répertoire multi-services "
    "fédéré (--workspace/workspace_root, BACKLOG-11 A2), soit des endpoints "
    "attribués à un module Maven par une indexation multi-modules (BACKLOG-13) — "
    "seuls les appels REST détectés dans un handler Kafka de ce projet sont "
    "remontés pour l'instant."
)


def _endpoint_to_site(endpoint: MessageEndpoint) -> GraphSite:
    return GraphSite(
        path=endpoint.path,
        start_line=endpoint.start_line,
        end_line=endpoint.end_line,
        topic=endpoint.topic,
    )


def _graph_nodes(services: list[str], edges: list[GraphEdge]) -> list[GraphNodeInfo]:
    external_services = external_microservice_names(edges)
    all_services = sorted(set(services) | external_services)
    kafka_topics = sorted({edge.from_endpoint.topic for edge in edges if edge.kind == "kafka"})
    nodes: list[GraphNodeInfo] = []
    for service in all_services:
        node: GraphNodeInfo = {"name": service, "kind": "microservice"}
        if service in external_services:
            node["external"] = True
            node["shape"] = "triangle"
        nodes.append(node)
    nodes.extend(GraphNodeInfo(name=topic, kind="kafka_topic") for topic in kafka_topics)
    return nodes


def _graph_edges(edges: list[GraphEdge]) -> list[GraphEdgeInfo]:
    rendered_edges: list[GraphEdgeInfo] = []
    for edge in edges:
        if edge.kind == "rest":
            rendered_edges.append(
                GraphEdgeInfo(
                    kind="rest",
                    from_node=edge.from_service,
                    from_kind="microservice",
                    to_node=edge.to_service,
                    to_kind="microservice",
                    label=graph_edge_rest_resource(edge),
                    from_site=_endpoint_to_site(edge.from_endpoint),
                    to_site=(
                        _endpoint_to_site(edge.to_endpoint)
                        if edge.to_endpoint is not None
                        else None
                    ),
                )
            )
            continue

        topic = edge.from_endpoint.topic
        rendered_edges.append(
            GraphEdgeInfo(
                kind="kafka_produce",
                from_node=edge.from_service,
                from_kind="microservice",
                to_node=topic,
                to_kind="kafka_topic",
                label=topic,
                from_site=_endpoint_to_site(edge.from_endpoint),
                to_site=None,
            )
        )
        assert edge.to_endpoint is not None, "a matched kafka edge always has a consumer endpoint"
        rendered_edges.append(
            GraphEdgeInfo(
                kind="kafka_consume",
                from_node=topic,
                from_kind="kafka_topic",
                to_node=edge.to_service,
                to_kind="microservice",
                label=topic,
                from_site=None,
                to_site=_endpoint_to_site(edge.to_endpoint),
            )
        )
    return rendered_edges


def render_graph_json(
    services: list[str],
    edges: list[GraphEdge],
    outbound_calls: list[OutboundCallInConsumer],
    warnings: list[str] | None = None,
    cross_module_data_available: bool = False,
) -> GraphResult:
    rendered_services = sorted(set(services) | external_microservice_names(edges))
    warning_note = " ".join(f"⚠ {w}" for w in (warnings or []))
    if cross_module_data_available:
        note = warning_note
    elif warning_note:
        note = f"{_NO_CROSS_MODULE_DATA_NOTE} {warning_note}"
    else:
        note = _NO_CROSS_MODULE_DATA_NOTE
    return GraphResult(
        services=rendered_services,
        nodes=_graph_nodes(rendered_services, edges),
        edges=_graph_edges(edges),
        outbound_calls_in_consumers=[
            OutboundCallHit(
                consumer=_endpoint_to_site(hit.consumer), call=_endpoint_to_site(hit.call)
            )
            for hit in outbound_calls
        ],
        note=note,
    )


def render_graph_text(result: GraphResult) -> str:
    lines: list[str] = []
    services = result["services"]
    nodes = result["nodes"]
    edges = result["edges"]
    if services:
        lines.append(f"Services ({len(services)}) : {', '.join(services)}")
    else:
        lines.append("Aucun service inter-module disponible pour construire le graphe.")

    kafka_topics = [node["name"] for node in nodes if node["kind"] == "kafka_topic"]
    if kafka_topics:
        lines.append(f"Topics Kafka ({len(kafka_topics)}) : {', '.join(kafka_topics)}")
    else:
        lines.append("Aucun topic Kafka inter-service détecté.")

    if edges:
        lines.append(f"Arêtes du graphe ({len(edges)}) :")
        for edge in edges:
            from_site = (
                f" ({edge['from_site']['path']}:{edge['from_site']['start_line']})"
                if edge["from_site"] is not None
                else ""
            )
            to_site = (
                f" ({edge['to_site']['path']}:{edge['to_site']['start_line']})"
                if edge["to_site"] is not None
                else ""
            )
            lines.append(
                f"  [{edge['kind']}] {edge['from_node']}{from_site} --{edge['label']}--> "
                f"{edge['to_node']}{to_site}"
            )
    else:
        lines.append("Aucune arête inter-service détectée.")

    calls = result["outbound_calls_in_consumers"]
    if calls:
        lines.append(f"Appels REST dans un handler Kafka ({len(calls)}) :")
        for hit in calls:
            call, consumer = hit["call"], hit["consumer"]
            lines.append(
                f"  {call['path']}:{call['start_line']} {call['topic']}  "
                f"(dans le handler {consumer['topic']}, "
                f"{consumer['path']}:{consumer['start_line']}-{consumer['end_line']})"
            )
    else:
        lines.append("Aucun appel REST détecté dans un handler Kafka.")

    if result["note"]:
        lines.append(result["note"])
    return "\n".join(lines)




def _indexing_issues(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    edges: list[GraphEdge],
    warnings: list[str] | None,
    modules: list[DiscoveredModule],
    source_roots: list[Path] | None,
    vscode_wsl_distro: str | None,
    diagnostics: list[ExtractionDiagnostic] | None = None,
) -> list[dict[str, str]]:
    """Return every unresolved inventory fact suitable for the HTML export."""
    issues: list[dict[str, str]] = []

    def add(
        severity: str, category: str, message: str, endpoint: MessageEndpoint | None = None
    ) -> None:
        issue = {"severity": severity, "category": category, "message": message}
        if endpoint is not None:
            issue["location"] = f"{endpoint.path}:{endpoint.start_line}"
            issue["vscode_uri"] = _endpoint_vscode_uri(
                endpoint, modules, source_roots, vscode_wsl_distro
            )
        issues.append(issue)

    for warning in dict.fromkeys(warnings or []):
        add("warning", "Avertissement d'inventaire", warning)
    for diagnostic in diagnostics or []:
        issues.append({
            "severity": diagnostic.severity,
            "category": "Diagnostic d'extraction",
            "message": f"{diagnostic.extractor}: {diagnostic.detail}",
            "location": diagnostic.path,
        })

    matched_http_call_ids = {edge.from_endpoint.id for edge in edges if edge.kind == "rest"}
    service_names = sorted(endpoints_by_service)
    for service, endpoints in sorted(endpoints_by_service.items()):
        for endpoint in sorted(endpoints, key=lambda item: (item.path, item.start_line, item.id)):
            if endpoint.system == "kafka" and endpoint.topic_dynamic:
                add(
                    "warning",
                    "Topic Kafka dynamique",
                    f"{service} : le topic {endpoint.topic!r} ne peut pas etre resolu statiquement.",
                    endpoint,
                )
            if endpoint.system == "kafka" and not endpoint.message_type:
                add(
                    "info",
                    "Type Kafka inconnu",
                    f"{service} : le type Java du message sur {endpoint.topic!r} n'a pas ete deduit.",
                    endpoint,
                )
            if endpoint.system == "rest" and endpoint.role == "call" and endpoint.id not in matched_http_call_ids:
                resolution = resolve_rest_target_service(endpoint, service_names)
                if resolution.status == "ambiguous":
                    add(
                        "warning",
                        "Cible HTTP ambiguë",
                        f"{service} : la cible explicite {resolution.hint!r} correspond à plusieurs microservices.",
                        endpoint,
                    )
                    continue
                add(
                    "warning" if endpoint.topic_dynamic else "info",
                    "Appel HTTP non rapproche",
                    f"{service} : aucun microservice fournisseur n'a ete identifie pour {endpoint.topic!r}.",
                    endpoint,
                )

    severity_rank = {"warning": 0, "info": 1}
    return sorted(
        issues,
        key=lambda item: (severity_rank[item["severity"]], item["category"], item["message"]),
    )


def _module_dependency_view(
    modules: list[DiscoveredModule] | None,
    dependencies: list[ModuleDependency] | None,
) -> dict[str, list[dict[str, object]]]:
    """Serialize the Maven/Gradle dependency tree used by the HTML sub-view."""
    dependencies = dependencies or []
    connected = {name for dependency in dependencies for name in (dependency.source, dependency.target)}
    modules_by_name = {module.name: module for module in modules or []}
    module_names = set(modules_by_name) | connected
    return {
        "nodes": [
            {
                "id": f"module:{name}",
                "name": name,
                "kind": "build_module",
                "build_system": modules_by_name[name].build_system if name in modules_by_name else "unknown",
                "color": "#2563eb" if modules_by_name.get(name, None) and modules_by_name[name].starts_application else "#64748b",
                "size": 17 if modules_by_name.get(name, None) and modules_by_name[name].starts_application else 14,
            }
            for name in sorted(module_names)
        ],
        "links": [
            {
                "source": f"module:{dependency.source}",
                "target": f"module:{dependency.target}",
                "kind": "build",
                "label": "dépend de",
            }
            for dependency in dependencies
        ],
    }


def _openapi_contract_spec(
    contract_path: str,
    modules: list[DiscoveredModule],
    source_roots: list[Path] | None = None,
) -> dict[str, object] | None:
    """Read a local OpenAPI document so Swagger UI can render it offline.

    Strategy1 contracts may live in a sibling ``model-*`` module while the
    publishing service only carries a declaration marker.  Try the module,
    then the common workspace root; absence is normal for federated indexes.
    """
    source_path = Path(contract_path)
    candidates = [source_path] if source_path.is_absolute() else []
    module_paths = [module.path.resolve() for module in modules]
    candidates.extend(root.resolve() / source_path for root in source_roots or [])
    candidates.extend(path / source_path for path in module_paths)
    if module_paths:
        common_root = Path(os.path.commonpath(module_paths))
        candidates.append(common_root / source_path)
    for candidate in dict.fromkeys(candidates):
        try:
            content = candidate.read_text(encoding="utf-8", errors="replace")
            parsed = yaml.safe_load(content)
        except (OSError, yaml.YAMLError):
            continue
        if isinstance(parsed, dict) and ("openapi" in parsed or "swagger" in parsed):
            # PyYAML resolves unquoted ISO dates into ``date`` objects, while
            # the HTML payload must be strict JSON. Round-trip through the
            # JSON encoder to preserve the document shape and normalize such
            # scalar values to strings before the final graph serialization.
            return json.loads(json.dumps(parsed, default=str))
    return None


def _java_type_references(source_bytes: bytes, type_node) -> list[str]:
    """Return declared Java type names, retaining their package when present."""
    return re.findall(
        r"(?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*",
        java_parser.node_text(source_bytes, type_node),
    )


def _java_dto_fields(source: str, dto_name: str) -> tuple[list[dict[str, object]], list[list[str]]]:
    """Extract the readable fields of a Java class or record DTO.

    This intentionally stays conservative: it exposes declared data members,
    never guesses inherited or serializer-generated properties.
    """
    source_bytes = source.encode("utf-8")
    root = java_parser.java_parser("dto_fields").parse(source_bytes).root_node
    if root.has_error:
        return [], []
    declaration = next(
        (
            node
            for node in java_parser.type_declarations(root)
            if java_parser.declaration_name(node, source_bytes) == dto_name
        ),
        None,
    )
    if declaration is None:
        return [], []

    def field(node) -> tuple[dict[str, object], list[str]] | None:
        type_node = node.child_by_field_name("type")
        name_node = node.child_by_field_name("name")
        if type_node is None or name_node is None:
            return None
        references = _java_type_references(source_bytes, type_node)
        return (
            {
                "type": java_parser.node_text(source_bytes, type_node).strip(),
                "name": java_parser.node_text(source_bytes, name_node),
            },
            references,
        )

    if declaration.type == "record_declaration":
        parameters = java_parser.child_by_type(declaration, "formal_parameters")
        if parameters is None:
            return [], []
        values = [
            value
            for parameter in parameters.named_children
            if parameter.type == "formal_parameter"
            if (value := field(parameter)) is not None
        ]
        return [field for field, _references in values], [references for _field, references in values]

    fields: list[dict[str, object]] = []
    references_by_field: list[list[str]] = []
    for node in java_parser.walk(declaration):
        if node.type != "field_declaration" or java_parser.enclosing(
            node, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration"
        ) != declaration:
            continue
        type_node = node.child_by_field_name("type")
        if type_node is None:
            continue
        field_type = java_parser.node_text(source_bytes, type_node).strip()
        for declarator in node.children:
            if declarator.type != "variable_declarator":
                continue
            name_node = declarator.child_by_field_name("name")
            if name_node is not None:
                fields.append({"type": field_type, "name": java_parser.node_text(source_bytes, name_node)})
                references_by_field.append(_java_type_references(source_bytes, type_node))
    return fields, references_by_field


def _java_enum_values(source: str, enum_name: str) -> list[str]:
    """Return declared enum constants without inferring enum behaviour."""
    source_bytes = source.encode("utf-8")
    root = java_parser.java_parser("enum_values").parse(source_bytes).root_node
    if root.has_error:
        return []
    declaration = next(
        (
            node
            for node in java_parser.type_declarations(root)
            if node.type == "enum_declaration"
            if java_parser.declaration_name(node, source_bytes) == enum_name
        ),
        None,
    )
    if declaration is None:
        return []
    body = java_parser.child_by_type(declaration, "enum_body")
    if body is None:
        return []
    values: list[str] = []
    for node in body.named_children:
        if node.type != "enum_constant":
            continue
        name = node.child_by_field_name("name")
        if name is not None:
            values.append(java_parser.node_text(source_bytes, name))
    return values


def _java_project_dto_names(source: str) -> set[str]:
    source_bytes = source.encode("utf-8")
    root = java_parser.java_parser("dto_names").parse(source_bytes).root_node
    if root.has_error:
        return set()
    return {
        name
        for declaration in java_parser.type_declarations(root)
        if declaration.type in {"class_declaration", "record_declaration", "enum_declaration"}
        if (name := java_parser.declaration_name(declaration, source_bytes)) is not None
    }


@dataclass(frozen=True)
class _JavaDtoCandidate:
    qualified_name: str
    name: str
    source_path: str
    source: str
    source_file: Path
    package: str
    imports: frozenset[str]


def _java_package_and_imports(source: str) -> tuple[str, frozenset[str]]:
    """Extract a Java source context used to disambiguate simple type names."""
    source_bytes = source.encode("utf-8")
    root = java_parser.java_parser("dto_context").parse(source_bytes).root_node
    if root.has_error:
        return "", frozenset()
    package = ""
    imports: set[str] = set()
    for node in root.named_children:
        text = java_parser.node_text(source_bytes, node).strip().rstrip(";")
        if node.type == "package_declaration":
            package = text.removeprefix("package").strip()
        elif node.type == "import_declaration" and not text.startswith("import static "):
            imported = text.removeprefix("import").strip()
            if imported:
                imports.add(imported)
    return package, frozenset(imports)


def _live_kafka_dto_views(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    modules: list[DiscoveredModule],
    vscode_wsl_distro: str | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Deprecated live DTO enrichment retained only for migration tooling.

    Exports deliberately do not call this function: source parsing belongs to
    indexing so a rendered file cannot mix two repository revisions.
    """
    candidates: dict[str, _JavaDtoCandidate] = {}
    source_contexts: dict[str, tuple[str, frozenset[str]]] = {}
    for module in modules:
        source_root = module.path / "src" / "main" / "java"
        if not source_root.is_dir():
            continue
        for java_path in source_root.glob("**/*.java"):
            try:
                source = java_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            relative_path = str(java_path.relative_to(module.path))
            package, imports = _java_package_and_imports(source)
            for dto_name in _java_project_dto_names(source):
                qualified_name = f"{package}.{dto_name}" if package else dto_name
                candidates.setdefault(
                    qualified_name,
                    _JavaDtoCandidate(
                        qualified_name=qualified_name,
                        name=dto_name,
                        source_path=relative_path,
                        source=source,
                        source_file=java_path,
                        package=package,
                        imports=imports,
                    ),
                )
                source_contexts.setdefault(qualified_name, (package, imports))

    candidates_by_name: dict[str, list[_JavaDtoCandidate]] = {}
    for dto_candidate in candidates.values():
        candidates_by_name.setdefault(dto_candidate.name, []).append(dto_candidate)

    def resolve_type(
        type_name: str,
        context: tuple[str, frozenset[str]] | None = None,
    ) -> str | None:
        """Resolve a Java type without guessing between distinct packages."""
        normalized = type_name.strip().replace("$", ".")
        if normalized in candidates:
            return normalized
        simple_name = normalized.rsplit(".", 1)[-1]
        package, imports = context or ("", frozenset())
        for imported in imports:
            if imported.endswith(".*"):
                qualified_name = f"{imported[:-2]}.{simple_name}"
                if qualified_name in candidates:
                    return qualified_name
            elif imported.rsplit(".", 1)[-1] == simple_name and imported in candidates:
                return imported
        if package:
            qualified_name = f"{package}.{simple_name}"
            if qualified_name in candidates:
                return qualified_name
        matches = candidates_by_name.get(simple_name, [])
        return matches[0].qualified_name if len(matches) == 1 else None

    root_ids: set[str] = set()
    endpoint_root_ids: dict[str, str] = {}
    for endpoints in endpoints_by_service.values():
        for endpoint in endpoints:
            if endpoint.system != "kafka" or not endpoint.message_type:
                continue
            root_id = resolve_type(
                endpoint.message_type,
                source_contexts.get(endpoint.qualified_name or ""),
            )
            if root_id is None:
                root_id = f"unresolved:{endpoint.message_type}"
            root_ids.add(root_id)
            endpoint_root_ids[endpoint.id] = root_id

    definitions: dict[str, dict[str, object]] = {}
    pending = list(sorted(root_ids))
    while pending:
        dto_id = pending.pop(0)
        if dto_id in definitions:
            continue
        candidate = candidates.get(dto_id)
        dto_name = candidate.name if candidate else dto_id.removeprefix("unresolved:").rsplit(".", 1)[-1]
        definition: dict[str, object] = {
            "id": dto_id,
            "name": dto_name,
            "qualified_name": candidate.qualified_name if candidate else None,
            "fields": [],
            "source": None,
        }
        if candidate:
            fields, references_by_field = _java_dto_fields(candidate.source, candidate.name)
            for field, references in zip(fields, references_by_field, strict=True):
                nested = sorted(
                    {
                        resolved
                        for reference in references
                        if (resolved := resolve_type(reference, (candidate.package, candidate.imports)))
                    }
                )
                if nested:
                    field["dto_references"] = nested
                    pending.extend(reference for reference in nested if reference != dto_id)
            definition["fields"] = fields
            if enum_values := _java_enum_values(candidate.source, candidate.name):
                definition["enum_values"] = enum_values
            definition["source"] = candidate.source_path
            definition["vscode_uri"] = _vscode_file_uri(candidate.source_file, vscode_wsl_distro)
        definitions[dto_id] = definition

    root_definitions: list[dict[str, object]] = []
    nested_definitions: list[dict[str, object]] = []
    for dto_id, definition in sorted(definitions.items()):
        matches = [
            (service, endpoint)
            for service, endpoints in endpoints_by_service.items()
            for endpoint in endpoints
            if endpoint_root_ids.get(endpoint.id) == dto_id
        ]
        definition["producers"] = sorted({service for service, endpoint in matches if endpoint.role == "produce"})
        definition["consumers"] = sorted({service for service, endpoint in matches if endpoint.role == "consume"})
        definition["topics"] = sorted({endpoint.topic for _service, endpoint in matches})
        (root_definitions if dto_id in root_ids else nested_definitions).append(definition)
    return root_definitions, nested_definitions




def render_graph_html(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    edges: list[GraphEdge],
    collections_by_service: dict[str, list[str]] | None = None,
    modules_by_service: dict[str, DiscoveredModule] | None = None,
    indexing_warnings: list[str] | None = None,
    build_modules: list[DiscoveredModule] | None = None,
    module_dependencies: list[ModuleDependency] | None = None,
    source_roots: list[Path] | None = None,
    findings_by_service: dict[str, list[Finding]] | None = None,
    vscode_wsl_distro: str | None = None,
    request_reply_strategy1: bool = False,
    diagnostics: list[ExtractionDiagnostic] | None = None,
) -> str:
    """Render an interactive Sigma.js graph as a self-contained HTML document.

    Sigma.js and Graphology are loaded from their CDNs at viewing time; graph
    data is embedded locally and safely serialized so the generated file
    contains no application data in executable JavaScript.
    """
    external_services = external_microservice_names(edges)
    ordered_services = sorted(set(endpoints_by_service) | external_services)
    kafka_topics = sorted({edge.from_endpoint.topic for edge in edges if edge.kind == "kafka"})
    topic_message_types: dict[str, dict[str, set[str]]] = {
        topic: {"produce": set(), "consume": set()} for topic in kafka_topics
    }
    published_message_types_by_relation: dict[tuple[str, str], set[str]] = {}
    consumed_message_types_by_relation: dict[tuple[str, str], set[str]] = {}
    for service, endpoints in endpoints_by_service.items():
        for endpoint in endpoints:
            if (
                endpoint.system == "kafka"
                and endpoint.topic in topic_message_types
                and endpoint.message_type
            ):
                topic_message_types[endpoint.topic][endpoint.role].add(endpoint.message_type)
                if endpoint.role == "produce":
                    published_message_types_by_relation.setdefault((service, endpoint.topic), set()).add(
                        endpoint.message_type
                    )
                if endpoint.role == "consume":
                    consumed_message_types_by_relation.setdefault((service, endpoint.topic), set()).add(
                        endpoint.message_type
                    )
    module_details = modules_by_service or {}
    all_modules = list({
        module.path.resolve(): module
        for module in [*(build_modules or []), *module_details.values()]
    }.values())
    nodes: list[dict[str, object]] = []
    for name in ordered_services:
        endpoints = endpoints_by_service.get(name, [])
        resources = _rest_resources_served(endpoints)
        contract_resources: dict[str, set[str]] = {}
        for endpoint in endpoints:
            if (
                endpoint.system == "rest"
                and endpoint.role == "serve"
                and endpoint.framework == "openapi"
            ):
                contract_resources.setdefault(_openapi_contract_evidence_path(endpoint), set()).add(endpoint.topic)
        module = module_details.get(name)
        openapi_files = sorted(
            set(module.openapi_files if module else ()) | set(contract_resources)
        )
        nodes.append(
            {
                "id": f"microservice:{name}",
                "kind": "microservice",
                "name": name,
                "kafka_endpoints": [
                    {
                        "role": endpoint.role,
                        "topic": endpoint.topic,
                        "message_type": endpoint.message_type,
                        "location": f"{endpoint.path}:{endpoint.start_line}",
                        "vscode_uri": _endpoint_vscode_uri(endpoint, all_modules, source_roots, vscode_wsl_distro),
                    }
                    for endpoint in endpoints
                    if endpoint.system == "kafka"
                ],
                **({"vscode_uri": _vscode_file_uri(module.path, vscode_wsl_distro)} if module else {}),
                "resources": resources,
                "openapi_files": openapi_files,
                "openapi_contracts": [
                    {
                        "path": path,
                        "resources": sorted(contract_resources.get(path, set())),
                        **({"vscode_uri": _vscode_file_uri(module.path / path, vscode_wsl_distro)} if module else {}),
                    }
                    for path in openapi_files
                ],
                **(
                    {
                        "findings": [
                            {
                                "severity": finding.severity,
                                "rule_id": finding.rule_id,
                                "message": finding.message,
                                "path": finding.path,
                                "start_line": finding.start_line,
                                "vscode_uri": _vscode_uri(finding, module, source_roots, vscode_wsl_distro),
                            }
                            for finding in findings_by_service.get(name, [])
                        ]
                    }
                    if findings_by_service is not None
                    else {}
                ),
                "label": name,
                "width": 190,
                "height": 42,
                **(
                    {"external": True, "shape": "triangle"}
                    if name in external_services
                    else {}
                ),
            }
        )
    nodes += [
        {
            "id": f"kafka_topic:{name}",
            "kind": "kafka_topic",
            "name": name,
            "label": name,
            "published_message_types": sorted(topic_message_types[name]["produce"]),
            "consumed_message_types": sorted(topic_message_types[name]["consume"]),
            "width": 190,
            "height": 42,
        }
        for name in kafka_topics
    ]
    nodes += [
        {
            "id": f"mongodb_collection:{identity}",
            "kind": "mongodb_collection",
            "name": collection,
            "owner": service,
            "label": collection,
            "width": 190,
            "height": 42,
        }
        for service, collection, identity in _mongodb_collection_nodes(collections_by_service)
    ]
    links: list[dict[str, object]] = []
    for source_kind, source_name, target_kind, target_name, label, kind in _visual_graph_edges(edges):
        confidence, provenance = _visual_link_evidence(
            kind, source_kind, source_name, target_kind, target_name, edges
        )
        link: dict[str, object] = {
            "source": f"{source_kind}:{source_name}",
            "target": f"{target_kind}:{target_name}",
            "kind": kind,
            "direction": "outgoing" if kind == "rest" or source_kind == "microservice" else "incoming",
            "label": label.replace("<br/>", "\\n"),
            "confidence": confidence,
            "provenance": provenance,
        }
        if kind == "kafka" and source_kind == "microservice" and target_kind == "kafka_topic":
            link["published_message_types"] = sorted(
                published_message_types_by_relation.get((source_name, target_name), set())
            )
        if kind == "kafka" and source_kind == "kafka_topic" and target_kind == "microservice":
            link["consumed_message_types"] = sorted(
                consumed_message_types_by_relation.get((target_name, source_name), set())
            )
        links.append(link)
    links += [
        {
            "source": f"{source_kind}:{source_name}",
            "target": f"{target_kind}:{target_name}",
            "kind": kind,
            "direction": "data_access",
            "label": label,
            "confidence": "inferred",
            "provenance": "module inventory",
        }
        for source_kind, source_name, target_kind, target_name, label, kind in _mongodb_visual_graph_edges(
            collections_by_service
        )
    ]
    if request_reply_strategy1:
        known_topics = set(kafka_topics)
        links += [
            {
                "source": f"kafka_topic:{request_topic}",
                "target": f"kafka_topic:{reply_topic}",
                "kind": "request_reply",
                "direction": "reply",
                "label": "request/reply",
                "confidence": "conventional",
                "provenance": "Strategy1 · retour_",
            }
            for reply_topic in kafka_topics
            if reply_topic.casefold().startswith("retour_")
            if (request_topic := reply_topic[len("retour_"):]) in known_topics
        ]
    # Complexity is derived from the links exported to the browser, rather
    # than from a parallel graph projection. A microservice score is exactly:
    # inbound HTTP clients + outbound HTTP targets + Kafka relations + MongoDB
    # relations. Routes between the same directed pair are deduplicated.
    complexity_relations = {
        (str(link["source"]), str(link["target"]), str(link["kind"]))
        for link in links
        if link["kind"] in {"rest", "kafka", "mongodb"}
    }
    relation_counts: dict[str, int] = {str(node["id"]): 0 for node in nodes}
    relation_breakdowns: dict[str, dict[str, int]] = {
        str(node["id"]): {"http": 0, "kafka": 0, "mongodb": 0}
        for node in nodes
    }
    for source, target, kind in complexity_relations:
        relation_counts[source] += 1
        relation_counts[target] += 1
        bucket = "http" if kind == "rest" else kind
        relation_breakdowns[source][bucket] += 1
        relation_breakdowns[target][bucket] += 1
    complexity_rankings_by_kind = {
        kind: _complexity_ranking({
            str(node["id"]): relation_counts[str(node["id"])]
            for node in nodes
            if node["kind"] == kind
        })
        for kind in ("microservice", "kafka_topic", "mongodb_collection")
    }
    for node in nodes:
        base_size = 17 if node["kind"] == "microservice" else 14 if node["kind"] == "mongodb_collection" else 13
        if node["kind"] not in complexity_rankings_by_kind:
            node["color"] = "#64748b"
            node["size"] = base_size
            continue
        node_id = str(node["id"])
        score = relation_counts[node_id]
        ranking = complexity_rankings_by_kind[str(node["kind"])][node_id]
        level = ranking["level"]
        node["complexity"] = {
            "score": score,
            "level": level,
            "relations": relation_counts[node_id],
            "breakdown": relation_breakdowns[node_id],
            "rank": ranking["rank"],
            "population": ranking["population"],
            "tier_start": ranking["tier_start"],
            "tier_end": ranking["tier_end"],
        }
        # Keep labels to the resource name. Connectivity remains encoded by
        # the outline and available in the detail panel, without making the
        # node label carry a dependency count.
        node["label"] = str(node["name"])
        node["color"] = {"low": "#2563eb", "medium": "#d97706", "high": "#dc2626"}[level]
        node["size"] = base_size + {"low": 0, "medium": 2, "high": 4}[level]
    kafka_dtos, project_dto_definitions = kafka_dto_views(endpoints_by_service)
    graph_data = json.dumps(
        {
            "nodes": nodes,
            "links": links,
            "build_dependencies": _module_dependency_view(build_modules, module_dependencies),
            "kafka_dtos": kafka_dtos,
            "project_dto_definitions": project_dto_definitions,
            "indexing_issues": _indexing_issues(
                endpoints_by_service,
                edges,
                indexing_warnings,
                all_modules,
                source_roots,
                vscode_wsl_distro,
                diagnostics,
            ),
        },
        ensure_ascii=False,
    ).replace("</", "<\\/")
    return _SIGMA_GRAPH_HTML_TEMPLATE.replace("__GRAPH_DATA__", graph_data)


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


_SIGMA_GRAPH_HTML_TEMPLATE = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SystemLens graph</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/graphology/0.25.4/graphology.umd.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/sigma.js/2.4.0/sigma.min.js"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js"></script>
  <style>
    :root { color: #172033; background: #f5f7fb; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
    * { box-sizing: border-box; }
    body { margin: 0; overflow: hidden; }
    #graph, #dependency-graph { width: 100vw; height: 100vh; background: #f8fafc; touch-action: none; }
    #dependency-graph[hidden] { display: none; }
    .toolbar { position: fixed; z-index: 2; top: 16px; left: 16px; display: grid; gap: 10px; width: min(390px, calc(100vw - 32px)); max-height: calc(100vh - 32px); padding: 12px; overflow-y: auto; border: 1px solid #d7dee9; border-radius: 10px; background: rgba(255, 255, 255, .96); box-shadow: 0 4px 20px rgba(15, 23, 42, .12); }
    .toolbar-header { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .toolbar strong { color: #172033; font-size: 15px; white-space: nowrap; }
    .toolbar input:not([type="checkbox"]) { height: 34px; padding: 0 10px; border: 1px solid #b9c5d6; border-radius: 6px; color: #172033; background: #fff; font: inherit; font-size: 13px; }
    .toolbar-tabs { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 4px; padding: 3px; border-radius: 8px; background: #edf2f7; }
    .toolbar-tab { min-width: 0; height: 30px !important; width: auto !important; padding: 0 7px; overflow: hidden; border: 0 !important; border-radius: 6px !important; color: #52616b !important; background: transparent !important; font-size: 11px !important; font-weight: 700; text-overflow: ellipsis; white-space: nowrap; }
    .toolbar-tab:hover { background: rgba(255, 255, 255, .65) !important; }
    .toolbar-tab.is-active { color: #1d4f91 !important; background: #fff !important; box-shadow: 0 1px 3px rgba(15, 23, 42, .12); }
    .toolbar-panel { display: grid; gap: 10px; }
    .toolbar-panel[hidden] { display: none; }
    #search, #path-query { width: 100%; }
    .graph-actions { display: flex; gap: 4px; }
    .toolbar button { width: 34px; height: 34px; border: 1px solid #b9c5d6; border-radius: 6px; color: #315f9b; background: #fff; font-size: 19px; line-height: 1; cursor: pointer; }
    .toolbar button:hover { background: #eaf2ff; }
    .graph-actions .graph-action-label { width: auto; padding: 0 8px; font-size: 11px; font-weight: 700; }
    .exploration-start { display: grid; gap: 7px; padding: 10px; border: 1px solid #dbeafe; border-radius: 8px; background: #f8fbff; }
    .exploration-start h2 { margin: 0; color: #1d4f91; font-size: 12px; }
    .exploration-start p { margin: 0; color: #52616b; font-size: 11px; line-height: 1.4; }
    .question-actions { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; }
    .question-action { width: auto !important; height: auto !important; min-height: 38px; padding: 7px !important; color: #1d4f91 !important; border-color: #bfdbfe !important; background: #fff !important; font-size: 11px !important; font-weight: 700; line-height: 1.2; text-align: left; }
    .advanced-controls, .advanced-tools { border: 0; border-top: 1px solid #e2e8f0; padding-top: 8px; }
    .advanced-controls > summary, .advanced-tools > summary { color: #315f9b; font-size: 12px; font-weight: 700; cursor: pointer; }
    .advanced-controls[open] > summary, .advanced-tools[open] > summary { margin-bottom: 8px; }
    .advanced-tools .toolbar-tabs { margin-top: 8px; }
    .relation-filters { display: flex; flex-wrap: wrap; gap: 6px; margin: 0; padding: 0; border: 0; }
    .relation-filters legend { width: 100%; margin-bottom: 2px; color: #59708d; font-size: 11px; font-weight: 700; text-transform: uppercase; }
    .relation-filter { display: inline-flex; align-items: center; gap: 5px; height: 30px; padding: 0 8px; border: 1px solid #cdd7e5; border-radius: 999px; color: #315f9b; background: #fff; font-size: 12px; white-space: nowrap; cursor: pointer; }
    .relation-filter input, .path-lock input { width: 14px; height: 14px; margin: 0; padding: 0; border: 0; accent-color: #315f9b; }
    .filter-presets { display: flex; flex-wrap: wrap; gap: 5px; padding-top: 7px; border-top: 1px solid #e2e8f0; }
    .filter-preset { width: auto !important; height: 27px !important; padding: 0 8px !important; color: #52616b !important; border-color: #d7dee9 !important; background: #fff !important; font-size: 11px !important; font-weight: 700; }
    .filter-preset:hover, .filter-preset.is-active { color: #1d4f91 !important; border-color: #93c5fd !important; background: #eff6ff !important; }
    .layout-controls { display: grid; gap: 6px; margin: 0; padding: 8px 0 0; border: 0; border-top: 1px solid #e2e8f0; }
    .layout-controls legend { padding: 0; color: #59708d; font-size: 11px; font-weight: 700; text-transform: uppercase; }
    .layout-options { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 5px; }
    .layout-option { width: auto !important; height: auto !important; min-height: 38px; padding: 6px !important; color: #52616b !important; border-color: #cdd7e5 !important; background: #fff !important; font-size: 11px !important; font-weight: 700; line-height: 1.15; }
    .layout-option:hover { background: #eff6ff !important; }
    .layout-option.is-active { color: #1d4f91 !important; border-color: #93c5fd !important; background: #dbeafe !important; box-shadow: inset 0 0 0 1px #bfdbfe; }
    .layout-status { margin: 0; color: #64748b; font-size: 11px; line-height: 1.35; }
    .path-controls { border-top: 1px solid #e2e8f0; padding-top: 8px; }
    .path-controls summary, .legend summary { color: #315f9b; font-size: 12px; font-weight: 600; cursor: pointer; }
    .path-controls[open] summary { margin-bottom: 8px; }
    .path-row { display: grid; grid-template-columns: 1fr auto; gap: 6px; align-items: center; }
    .path-actions { display: flex; align-items: center; gap: 6px; grid-column: 1 / -1; }
    .path-lock { display: inline-flex; align-items: center; gap: 5px; height: 30px; padding: 0 8px; border: 1px solid #cdd7e5; border-radius: 6px; color: #315f9b; background: #fff; font-size: 12px; white-space: nowrap; cursor: pointer; }
    #show-path, #show-simple-paths { width: auto; padding: 0 10px; font-size: 12px; font-weight: 600; }
    .explore-search-label { color: #315f9b; font-size: 12px; font-weight: 700; }
    .explore-search-help, .explore-search-status { margin: -5px 0 0; color: #64748b; font-size: 11px; line-height: 1.35; }
    .explore-search-status { min-height: 15px; color: #a53f3f; font-weight: 600; }
    #show-simple-paths { height: 30px; color: #1d4f91; border-color: #c7d8f3; background: #f8fbff; }
    .simple-paths { display: grid; gap: 7px; }
    .simple-paths-summary { margin: 0; color: #52616b; font-size: 12px; line-height: 1.4; }
    .simple-paths-list { display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; }
    .simple-path-choice { width: 100% !important; height: auto !important; min-height: 38px; padding: 8px 10px !important; color: #1d4f91 !important; border-color: #dbeafe !important; background: #f8fbff !important; font-size: 12px !important; font-weight: 600; text-align: left; overflow-wrap: anywhere; }
    .simple-path-choice:hover, .simple-path-choice:focus-visible { border-color: #93c5fd !important; background: #eff6ff !important; outline: none; }
    .path-history { gap: 10px; }
    .path-history-header { padding: 2px 2px 6px; }
    .path-history-kicker { margin: 0 0 2px; color: #64748b; font-size: 10px; font-weight: 800; letter-spacing: .09em; text-transform: uppercase; }
    .path-history-title { margin: 0; color: #172033; font-size: 15px; line-height: 1.2; }
    .path-history-description, .path-history-empty { margin: 5px 0 0; color: #64748b; font-size: 12px; line-height: 1.4; }
    .path-history-list { display: grid; gap: 7px; max-height: 360px; margin: 0; padding: 0; overflow: auto; list-style: none; }
    .path-history-item { display: grid; grid-template-columns: minmax(0, 1fr) 30px; gap: 6px; }
    .path-history-replay { width: auto !important; min-width: 0; height: auto !important; min-height: 42px; padding: 8px 10px; border-color: #dbeafe !important; color: #1e429f !important; background: linear-gradient(135deg, #f8fbff, #eff6ff) !important; font-size: 12px !important; font-weight: 600; text-align: left; overflow-wrap: anywhere; }
    .path-history-replay:hover { border-color: #93c5fd !important; background: #dbeafe !important; }
    .path-history-delete { align-self: center; width: 30px !important; height: 30px !important; color: #a53f3f !important; font-size: 16px !important; }
    .indexing-issues { gap: 10px; }
    .indexing-issues-header { padding: 2px 2px 6px; }
    .indexing-issues-kicker { margin: 0 0 2px; color: #64748b; font-size: 10px; font-weight: 800; letter-spacing: .09em; text-transform: uppercase; }
    .indexing-issues-title { margin: 0; color: #172033; font-size: 15px; line-height: 1.2; }
    .indexing-issues-description, .indexing-issues-empty { margin: 5px 0 0; color: #64748b; font-size: 12px; line-height: 1.4; }
    .indexing-issues-list { display: grid; gap: 7px; max-height: 360px; margin: 0; padding: 0; overflow: auto; list-style: none; }
    .indexing-issue { padding: 9px 10px; border: 1px solid #e2e8f0; border-left: 3px solid #94a3b8; border-radius: 7px; background: #f8fafc; }
    .indexing-issue.warning { border-left-color: #d97706; background: #fffbeb; }
    .indexing-issue.info { border-left-color: #2563eb; background: #eff6ff; }
    .indexing-issue-header { display: flex; align-items: center; gap: 6px; }
    .indexing-issue-category { color: #334155; font-size: 12px; font-weight: 700; }
    .indexing-issue-severity { padding: 2px 5px; border-radius: 999px; color: #475569; background: #e2e8f0; font-size: 9px; font-weight: 800; letter-spacing: .06em; text-transform: uppercase; }
    .indexing-issue.warning .indexing-issue-severity { color: #92400e; background: #fef3c7; }
    .indexing-issue.info .indexing-issue-severity { color: #1d4f91; background: #dbeafe; }
    .indexing-issue-message { margin: 5px 0 0; color: #475569; font-size: 12px; line-height: 1.4; overflow-wrap: anywhere; }
    .indexing-issue-location { display: inline-flex; margin-top: 7px; color: #1d4f91; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 10px; overflow-wrap: anywhere; text-decoration: underline; }
    .references-view, .request-reply-view { gap: 12px; }
    .references-header { padding: 2px 2px 5px; }
    .references-kicker { margin: 0 0 2px; color: #64748b; font-size: 10px; font-weight: 800; letter-spacing: .09em; text-transform: uppercase; }
    .references-title { margin: 0; color: #172033; font-size: 15px; }
    .references-description, .references-empty { margin: 5px 0 0; color: #64748b; font-size: 12px; line-height: 1.4; }
    .references-section { display: grid; gap: 7px; }
    .references-section h3 { margin: 0; color: #59708d; font-size: 10px; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }
    .references-list { display: grid; gap: 7px; max-height: min(50vh, 460px); margin: 0; padding: 0 3px 8px 0; overflow: auto; scroll-padding-bottom: 8px; list-style: none; }
    .reference-filter-input { width: 100%; }
    .resource-analyses { border-top: 1px solid #e2e8f0; padding-top: 10px; }
    .resource-analyses summary { color: #315f9b; font-size: 12px; font-weight: 700; cursor: pointer; }
    .resource-analysis-actions { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; margin-top: 8px; }
    .resource-analysis-actions button { width: auto; height: auto; min-height: 38px; padding: 7px; font-size: 11px; font-weight: 700; line-height: 1.2; }
    .reference-item { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px; align-items: center; padding: 8px; border: 1px solid #e2e8f0; border-radius: 7px; background: #f8fafc; }
    .reference-title { color: #334155; font-size: 12px; font-weight: 700; overflow-wrap: anywhere; }
    .reference-meta { margin-top: 2px; color: #64748b; font-size: 10px; overflow-wrap: anywhere; }
    .reference-action { width: auto !important; height: 29px !important; padding: 0 8px !important; color: #1d4f91 !important; border-color: #bfdbfe !important; background: #eff6ff !important; font-size: 11px !important; font-weight: 700; white-space: nowrap; }
    .reference-action:disabled { color: #94a3b8 !important; border-color: #e2e8f0 !important; background: #f8fafc !important; cursor: not-allowed; }
    #details { position: fixed; z-index: 2; right: 16px; bottom: 16px; width: min(400px, calc(100vw - 32px)); max-height: min(68vh, 560px); overflow: auto; border: 1px solid #d7dee9; border-radius: 14px; background: rgba(255, 255, 255, .97); color: #475569; font-size: 13px; line-height: 1.45; box-shadow: 0 12px 32px rgba(15, 23, 42, .16); }
    .details-header { padding: 16px; border-bottom: 1px solid #e2e8f0; background: linear-gradient(135deg, #f8fafc, #eef5ff); }
    .details-header.is-low { border-left: 4px solid #2563eb; }
    .details-header.is-medium { border-left: 4px solid #d97706; }
    .details-header.is-high { border-left: 4px solid #dc2626; }
    .details-kicker { margin: 0 0 3px; color: #64748b; font-size: 11px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; }
    .details-title { margin: 0; overflow-wrap: anywhere; color: #172033; font-size: 18px; line-height: 1.2; }
    .details-meta { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 12px; }
    .detail-badge { display: inline-flex; align-items: center; min-height: 24px; padding: 3px 8px; border: 1px solid #cbd5e1; border-radius: 999px; color: #475569; background: #fff; font-size: 11px; font-weight: 600; }
    .detail-badge.complexity { border-color: currentColor; }
    .detail-badge.complexity.low { color: #2563eb; background: #eff6ff; }
    .detail-badge.complexity.medium { color: #b45309; background: #fffbeb; }
    .detail-badge.complexity.high { color: #dc2626; background: #fef2f2; }
    .details-section { padding: 12px 16px; border-bottom: 1px solid #edf2f7; }
    .details-section:last-child { border-bottom: 0; }
    .details-section h2 { margin: 0 0 7px; color: #64748b; font-size: 10px; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }
    .details-section ul { display: grid; gap: 5px; margin: 0; padding: 0; list-style: none; }
    .details-section li { padding: 6px 8px; border-radius: 6px; color: #334155; background: #f8fafc; overflow-wrap: anywhere; }
    .details-section li.relation-item { padding: 0; background: transparent; }
    .details-group { border-bottom: 1px solid #dfe7f0; }
    .details-group:last-child { border-bottom: 0; }
    .details-group > summary { display: flex; align-items: center; min-height: 38px; padding: 0 16px; color: #315f9b; font-size: 12px; font-weight: 800; letter-spacing: .05em; text-transform: uppercase; cursor: pointer; }
    .details-group[open] > summary { border-bottom: 1px solid #edf2f7; background: #f8fafc; }
    .details-group > .details-section { padding-left: 16px; padding-right: 16px; }
    .relation-link { display: block; width: 100%; padding: 7px 8px; border: 1px solid #e2e8f0; border-radius: 6px; color: #1d4f91; background: #f8fafc; font: inherit; text-align: left; cursor: pointer; overflow-wrap: anywhere; }
    .relation-link:hover, .relation-link:focus-visible { border-color: #93c5fd; background: #eff6ff; outline: none; }
    .service-kafka-list { display: grid; gap: 7px; margin: 0; padding: 0; list-style: none; }
    .service-kafka-item { display: grid; gap: 6px; padding: 8px; border: 1px solid #dbeafe; border-radius: 7px; background: #f8fbff; }
    .service-kafka-topic { width: 100%; padding: 0; border: 0; color: #1d4f91; background: transparent; font: inherit; font-weight: 700; text-align: left; cursor: pointer; overflow-wrap: anywhere; }
    .service-kafka-topic:hover, .service-kafka-topic:focus-visible { color: #1e429f; text-decoration: underline; outline: none; }
    .service-kafka-meta { display: flex; flex-wrap: wrap; gap: 5px; }
    .service-kafka-meta button, .service-kafka-meta a, .service-kafka-meta span { padding: 3px 6px; border: 1px solid #cbd5e1; border-radius: 999px; color: #475569; background: #fff; font: inherit; font-size: 11px; line-height: 1.25; text-decoration: none; }
    .service-kafka-meta button { color: #1d4f91; border-color: #bfdbfe; cursor: pointer; }
    .service-kafka-meta button:hover, .service-kafka-meta button:focus-visible { background: #eff6ff; outline: none; }
    .details-empty { padding: 18px; color: #64748b; text-align: center; }
    .path-details-header { padding: 16px; border-bottom: 1px solid #dbeafe; background: linear-gradient(135deg, #eff6ff, #f8fafc 60%, #f0fdf4); }
    .path-details-kicker { margin: 0 0 3px; color: #1d4f91; font-size: 10px; font-weight: 800; letter-spacing: .09em; text-transform: uppercase; }
    .path-details-title { margin: 0; color: #172033; font-size: 18px; line-height: 1.25; }
    .path-details-summary { margin: 8px 0 0; color: #52616b; font-size: 12px; }
    .path-overview { display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; }
    .path-overview-item { position: relative; padding: 7px 9px 7px 31px !important; border: 1px solid #e2e8f0; background: #fff !important; }
    .path-overview-item::before { position: absolute; top: 8px; left: 9px; color: #94a3b8; content: "→"; }
    .path-overview-item:first-child::before { color: #2563eb; content: "●"; font-size: 9px; }
    .path-overview-item:last-child::before { color: #16a34a; content: "●"; font-size: 9px; }
    .path-overview-item.is-topic { border-style: dashed; color: #475569; background: #f8fafc !important; }
    .path-overview-item.is-service { border-left: 3px solid #2563eb; }
    .path-overview-item.is-external { border-left: 3px solid #9333ea; }
    .path-overview-item.is-collection { border-left: 3px solid #16a34a; }
    .path-overview-stop { width: 100%; padding: 0; border: 0; color: inherit; background: transparent; font: inherit; text-align: left; cursor: pointer; }
    .path-overview-stop:hover, .path-overview-stop:focus-visible { color: #1d4f91; text-decoration: underline; outline: none; }
    .dependency-view { display: grid; gap: 8px; padding: 2px 0; }
    .dependency-view-kicker { margin: 0; color: #1d4f91; font-size: 10px; font-weight: 800; letter-spacing: .09em; text-transform: uppercase; }
    .dependency-view h2 { margin: 0; color: #172033; font-size: 16px; }
    .dependency-view p:last-child { margin: 0; color: #52616b; font-size: 12px; line-height: 1.45; }
    .legend { position: fixed; z-index: 2; left: 16px; bottom: 16px; width: 210px; padding: 9px 11px; border: 1px solid #d7dee9; border-radius: 8px; background: rgba(255, 255, 255, .95); color: #475569; font-size: 11px; box-shadow: 0 2px 12px rgba(15, 23, 42, .10); }
    .legend[open] summary { margin-bottom: 8px; }
    .legend-content { display: grid; gap: 5px; }
    .legend-row { display: flex; align-items: center; gap: 6px; }
    .legend-mark { display: inline-block; width: 10px; height: 10px; border-radius: 50%; }
    .legend-resource-mark { box-sizing: border-box; background: #f8fafc; border: 2px solid #64748b; }
    .legend-resource-mark.microservice { clip-path: polygon(25% 7%, 75% 7%, 100% 50%, 75% 93%, 25% 93%, 0 50%); }
    .legend-resource-mark.collection { border-radius: 4px; }
    .legend-line { width: 18px; height: 2px; }
    .graph-summary { display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 16px; border-bottom: 1px solid #e2e8f0; background: #f8fafc; }
    .graph-summary-item { display: inline-flex; align-items: center; min-height: 22px; padding: 2px 8px; border: 1px solid #dbe4ee; border-radius: 999px; color: #475569; background: #fff; font-size: 11px; font-weight: 700; }
    .graph-summary-item.is-warning { border-color: #fcd34d; color: #92400e; background: #fffbeb; }
    .inventory-status { width: auto !important; height: auto !important; min-height: 30px; padding: 6px 8px !important; color: #166534 !important; border-color: #bbf7d0 !important; background: #f0fdf4 !important; font-size: 11px !important; font-weight: 700; text-align: left; }
    .inventory-status:hover, .inventory-status:focus-visible { background: #dcfce7 !important; outline: none; }
    .inventory-status.is-warning { color: #92400e !important; border-color: #fcd34d !important; background: #fffbeb !important; }
    .inventory-status.is-warning:hover, .inventory-status.is-warning:focus-visible { background: #fef3c7 !important; }
    .inspector-modal[hidden] { display: none; }
    .inspector-modal { position: fixed; z-index: 10; inset: 0; display: grid; place-items: center; padding: 24px; background: rgba(15, 23, 42, .52); }
    .inspector-dialog { display: grid; grid-template-rows: auto minmax(0, 1fr); width: min(1120px, 100%); height: min(820px, 100%); overflow: hidden; border: 1px solid #cbd5e1; border-radius: 14px; background: #fff; box-shadow: 0 24px 80px rgba(15, 23, 42, .34); }
    .inspector-header { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 16px; border-bottom: 1px solid #e2e8f0; background: #f8fafc; }
    .inspector-title { margin: 0; color: #172033; font-size: 16px; overflow-wrap: anywhere; }
    .inspector-close { flex: 0 0 auto; width: 32px; height: 32px; border: 1px solid #cbd5e1; border-radius: 6px; color: #475569; background: #fff; font-size: 20px; cursor: pointer; }
    .inspector-body { min-height: 0; overflow: auto; padding: 18px; }
    .inspector-body.swagger-ui { padding: 0; }
    .dto-inspector { display: grid; gap: 16px; max-width: 720px; }
    .dto-navigation { display: flex; align-items: center; gap: 8px; }
    .dto-back { width: fit-content; padding: 6px 9px; border: 1px solid #bfdbfe; border-radius: 6px; color: #1d4f91; background: #eff6ff; font-size: 12px; font-weight: 700; cursor: pointer; }
    .dto-summary { margin: 0; color: #64748b; font-size: 13px; }
    .dto-section { padding: 14px; border: 1px solid #e2e8f0; border-radius: 9px; background: #f8fafc; }
    .dto-section h2 { margin: 0 0 9px; color: #475569; font-size: 11px; letter-spacing: .08em; text-transform: uppercase; }
    .dto-fields, .dto-tags { display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; }
    .dto-field { display: grid; grid-template-columns: minmax(120px, 1fr) minmax(0, 1.4fr); gap: 12px; padding: 8px 10px; border-radius: 6px; background: #fff; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
    .dto-field-type { color: #1d4f91; overflow-wrap: anywhere; }
    button.dto-field-type { border: 0; padding: 0; background: transparent; color: #1d4f91; text-align: left; text-decoration: underline; cursor: pointer; font: inherit; }
    .dto-field-name { color: #334155; font-weight: 700; overflow-wrap: anywhere; }
    .dto-tag { display: inline-flex; width: fit-content; padding: 4px 7px; border-radius: 999px; color: #315f9b; background: #dbeafe; font-size: 12px; }
  </style>
</head>
<body>
  <div class="toolbar">
    <div class="toolbar-header">
      <strong>SystemLens</strong>
      <div class="graph-actions" aria-label="Navigation du graphe">
        <button id="zoom-out" type="button" aria-label="Dézoomer" title="Dézoomer">−</button>
        <button id="zoom-in" type="button" aria-label="Zoomer" title="Zoomer">+</button>
        <button id="fit-view" class="graph-action-label" type="button" aria-label="Ajuster le graphe à l'écran" title="Ajuster le graphe à l'écran">Ajuster</button>
        <button id="reset" class="graph-action-label" type="button" aria-label="Effacer la sélection" title="Effacer la sélection">Effacer</button>
      </div>
    </div>
    <div id="graph-summary" class="graph-summary" aria-label="Synthese de l'architecture"></div>
    <button id="inventory-status" class="inventory-status" type="button" hidden></button>
    <div class="toolbar-tabs" role="tablist" aria-label="Vues de l'architecture">
      <button id="graph-tab" class="toolbar-tab is-active" type="button" role="tab" aria-selected="true" aria-controls="graph-panel">Explorer</button>
      <button id="paths-tab" class="toolbar-tab" type="button" role="tab" aria-selected="false" aria-controls="paths-panel">Parcours</button>
      <button id="resources-tab" class="toolbar-tab" type="button" role="tab" aria-selected="false" aria-controls="resources-panel">Ressources</button>
      <button id="issues-tab" class="toolbar-tab" type="button" role="tab" aria-selected="false" aria-controls="issues-panel" title="Problemes d'indexation">Qualité</button>
    </div>
    <div id="graph-panel" class="toolbar-panel" role="tabpanel" aria-labelledby="graph-tab">
      <section class="exploration-start" aria-labelledby="exploration-start-title">
        <h2 id="exploration-start-title">Que voulez-vous comprendre ?</h2>
        <p>Choisissez un point de départ, puis sélectionnez un nœud du graphe pour voir son contexte.</p>
        <div class="question-actions">
          <button id="question-topic" class="question-action" type="button">Qui produit ou consomme un topic Kafka ?</button>
          <button id="question-service" class="question-action" type="button">Quelles dépendances a ce service ?</button>
          <button id="question-path" class="question-action" type="button">Quel chemin relie deux services ?</button>
          <button id="question-messages" class="question-action" type="button">Quel DTO circule via Kafka ?</button>
        </div>
      </section>
      <label class="explore-search-label" for="search">Rechercher une ressource ou un itinéraire</label>
      <input id="search" type="search" list="node-suggestions" placeholder="orders ou orders -> payments" autocomplete="off" aria-describedby="search-help search-status" aria-label="Rechercher une ressource ou un itinéraire">
      <datalist id="node-suggestions"></datalist>
      <p id="search-help" class="explore-search-help">Choisissez une suggestion ou saisissez le nom exact d'un noeud. Utilisez <code>-></code>, puis Entrée, pour afficher l'itinéraire le plus court.</p>
      <p id="search-status" class="explore-search-status" role="status"></p>
      <div class="filter-presets" role="group" aria-label="Vues de relations">
        <button class="filter-preset is-active" type="button" data-preset="all">Toutes</button>
        <button class="filter-preset" type="button" data-preset="http">REST</button>
        <button class="filter-preset" type="button" data-preset="kafka">Kafka</button>
        <button class="filter-preset" type="button" data-preset="mongodb">MongoDB</button>
        <button class="filter-preset" type="button" data-preset="selection" title="Isoler les relations du noeud selectionne">Sélection</button>
      </div>
      <details id="advanced-controls" class="advanced-controls">
        <summary>Filtres et disposition avancés</summary>
      <fieldset class="relation-filters">
        <legend>Relations affichees</legend>
        <label class="relation-filter" title="Afficher les appels HTTP"><input id="relation-http" type="checkbox" checked aria-label="Afficher les relations HTTP">HTTP</label>
        <label class="relation-filter" title="Afficher les publications et consommations Kafka"><input id="relation-kafka" type="checkbox" checked aria-label="Afficher les relations Kafka">Kafka</label>
        <label class="relation-filter" title="Afficher les acces aux collections MongoDB"><input id="relation-mongodb" type="checkbox" checked aria-label="Afficher les relations MongoDB">MongoDB</label>
      </fieldset>
      <fieldset class="relation-filters">
        <legend>Ressources affichees</legend>
        <label class="relation-filter" title="Afficher les microservices indexes"><input id="node-microservice" type="checkbox" checked aria-label="Afficher les microservices">Microservices</label>
        <label class="relation-filter" title="Afficher les microservices externes"><input id="node-external-microservice" type="checkbox" checked aria-label="Afficher les microservices externes">Externes</label>
        <label class="relation-filter" title="Afficher les topics Kafka"><input id="node-kafka-topic" type="checkbox" checked aria-label="Afficher les topics Kafka">Topics Kafka</label>
        <label class="relation-filter" title="Afficher les collections MongoDB"><input id="node-mongodb-collection" type="checkbox" checked aria-label="Afficher les collections MongoDB">MongoDB</label>
      </fieldset>
      <fieldset class="layout-controls">
        <legend>Disposition</legend>
        <div class="layout-options" role="group" aria-label="Choix de la disposition du graphe">
          <button id="layout-forceatlas2" class="layout-option" type="button" aria-pressed="false" title="ForceAtlas2 : rapprocher les noeuds lies">Regroupée</button>
          <button id="layout-noverlap" class="layout-option" type="button" aria-pressed="false" title="Noverlap : écarter les noeuds qui se chevauchent">Aérée</button>
          <button id="layout-forceatlas2-noverlap" class="layout-option is-active" type="button" aria-pressed="true" title="ForceAtlas2 + Noverlap : rapprocher puis écarter">Équilibrée</button>
        </div>
        <p id="layout-status" class="layout-status" role="status">Chargement de la vue équilibrée…</p>
      </fieldset>
      <details class="path-controls">
        <summary>Outils d'itinéraire avancés</summary>
        <div class="path-row">
          <input id="path-query" type="text" placeholder="service-a -> topic-1 -> service-b" autocomplete="off" aria-label="Chemin avec des noms de services ou topics">
          <button id="show-path" type="button" aria-label="Afficher le plus court chemin" title="Afficher le plus court chemin">Afficher</button>
          <div class="path-actions">
            <button id="show-simple-paths" type="button" aria-label="Lister les chemins simples entre les deux microservices" title="Lister les chemins simples entre les deux microservices">Chemins simples</button>
            <label class="path-lock" title="Conserver le chemin lors de la selection d'un noeud"><input id="path-lock" type="checkbox" aria-label="Verrouiller le chemin">Verrouiller</label>
          </div>
        </div>
      </details>
      </details>
    </div>
    <div id="dependencies-panel" class="toolbar-panel dependency-view" role="tabpanel" aria-label="Dépendances de build" hidden>
      <p class="dependency-view-kicker">Structure de build</p>
      <h2>Arbre des dépendances</h2>
      <p>Disposition Sugiyama : les modules sont rangés par niveaux de dépendance. Un lien part du module dépendant, à gauche, vers le module requis, à droite. Les interactions HTTP, Kafka et MongoDB restent dans la vue Interactions.</p>
    </div>
    <div id="issues-panel" class="toolbar-panel indexing-issues" role="tabpanel" aria-labelledby="issues-tab" hidden>
      <div class="indexing-issues-header">
        <p class="indexing-issues-kicker">Qualite de l'inventaire</p>
        <h2 id="indexing-issues-title" class="indexing-issues-title">Problemes d'indexation</h2>
        <p class="indexing-issues-description">Corrigez ces points pour rendre le graphe plus complet et plus fiable.</p>
      </div>
      <ul id="indexing-issues" class="indexing-issues-list" aria-label="Problemes d'indexation"></ul>
      <p id="indexing-issues-empty" class="indexing-issues-empty">Aucun probleme d'indexation detecte.</p>
    </div>
    <div id="paths-panel" class="toolbar-panel path-history" role="tabpanel" aria-labelledby="paths-tab" hidden>
      <div class="path-history-header">
        <p class="path-history-kicker">Navigation architecture</p>
        <h2 id="path-history-title" class="path-history-title">Chemins analyses</h2>
        <p class="path-history-description">Rejouez un parcours ou retirez-le de cette liste locale.</p>
      </div>
      <ul id="analyzed-paths" class="path-history-list" aria-label="Chemins analyses"></ul>
      <p id="analyzed-paths-empty" class="path-history-empty">Aucun chemin analyse pour le moment.</p>
    </div>
    <div id="resources-panel" class="toolbar-panel references-view" role="tabpanel" aria-labelledby="resources-tab" hidden>
      <div class="references-header">
        <p class="references-kicker">Documentation d'API</p>
        <h2 id="openapi-references-title" class="references-title">Contrats OpenAPI</h2>
        <p class="references-description">Ouvrez une spécification locale dans Swagger UI.</p>
      </div>
      <section class="references-section"><input id="openapi-reference-filter" class="reference-filter-input" type="search" placeholder="Filtrer les contrats par chemin ou service" autocomplete="off" aria-label="Filtrer les contrats OpenAPI"><ul id="openapi-references" class="references-list"></ul><p id="openapi-references-empty" class="references-empty">Aucun contrat OpenAPI détecté.</p></section>
      <div class="references-header">
        <p class="references-kicker">Événements Kafka</p>
        <h2 id="dto-references-title" class="references-title">DTO Kafka</h2>
        <p class="references-description">Inspectez les classes Java échangées via Kafka.</p>
      </div>
      <section class="references-section"><input id="dto-reference-filter" class="reference-filter-input" type="search" placeholder="Filtrer les DTO par nom ou package" autocomplete="off" aria-label="Filtrer les DTO Kafka"><ul id="dto-references" class="references-list"></ul><p id="dto-references-empty" class="references-empty">Aucun DTO Kafka détecté.</p></section>
      <details class="resource-analyses">
        <summary>Analyses complémentaires</summary>
        <div class="resource-analysis-actions">
          <button id="show-request-reply" type="button">Request/reply Kafka</button>
          <button id="show-dependencies" type="button">Dépendances build</button>
        </div>
      </details>
    </div>
    <div id="request-reply-panel" class="toolbar-panel request-reply-view" role="tabpanel" aria-label="Patterns request/reply Kafka" hidden>
      <div class="references-header">
        <p class="references-kicker">Convention Strategy1</p>
        <h2 id="request-reply-title" class="references-title">Patterns request/reply Kafka</h2>
        <p class="references-description">Couples détectés par la convention <code>retour_&lt;topic-de-requête&gt;</code>. Cette vue est conventionnelle, pas une trace d’exécution.</p>
      </div>
      <ul id="request-reply-patterns" class="references-list" aria-label="Patterns Kafka request reply"></ul>
      <p id="request-reply-empty" class="references-empty">Aucun couple request/reply détecté.</p>
    </div>
  </div>
  <details id="graph-legend" class="legend" aria-label="Legende du graphe">
    <summary>Legende</summary>
    <div class="legend-content">
      <div class="legend-row"><span class="legend-mark" style="background:#2563eb"></span>Connectivité relative basse (premier tiers)</div>
      <div class="legend-row"><span class="legend-mark" style="background:#d97706"></span>Connectivité relative médiane (tiers central)</div>
      <div class="legend-row"><span class="legend-mark" style="background:#dc2626"></span>Connectivité relative élevée (dernier tiers)</div>
      <div class="legend-row">La connectivité est relative à chaque type de ressource, pas un niveau de risque.</div>
      <div class="legend-row"><span class="legend-mark legend-resource-mark microservice"></span>Microservice</div>
      <div class="legend-row"><span class="legend-mark legend-resource-mark topic"></span>Topic Kafka</div>
      <div class="legend-row"><span class="legend-mark legend-resource-mark collection"></span>Collection MongoDB</div>
      <div class="legend-row"><span class="legend-line" style="background:#D55E00"></span>Appel HTTP</div>
      <div class="legend-row"><span class="legend-line" style="background:#009E73"></span>Publication Kafka</div>
      <div class="legend-row"><span class="legend-line" style="background:#0072B2"></span>Consommation Kafka</div>
      <div class="legend-row"><span class="legend-line" style="background:#7C3AED"></span>Pattern request/reply Kafka</div>
      <div class="legend-row"><span class="legend-line" style="background:#CC79A7"></span>Acces MongoDB</div>
      <div class="legend-row"><span class="legend-mark" style="background:#16a34a"></span>Relation prouvee : code ou manifeste</div>
      <div class="legend-row"><span class="legend-mark" style="background:#d97706"></span>Relation inferee : OpenAPI ou inventaire</div>
      <div class="legend-row"><span class="legend-mark" style="background:#7c3aed"></span>Relation conventionnelle : Strategy1</div>
    </div>
  </details>
  <div id="details"><div class="details-empty">Selectionnez un noeud pour isoler ses relations et afficher ses APIs.</div></div>
  <div id="graph" aria-label="Graphe des interactions"></div>
  <div id="dependency-graph" aria-label="Arbre des dependances entre microservices" hidden></div>
  <div id="inspector-modal" class="inspector-modal" role="dialog" aria-modal="true" aria-labelledby="inspector-title" hidden>
    <div class="inspector-dialog">
      <header class="inspector-header"><h1 id="inspector-title" class="inspector-title"></h1><button id="inspector-close" class="inspector-close" type="button" aria-label="Fermer">×</button></header>
      <div id="inspector-body" class="inspector-body"></div>
    </div>
  </div>
  <script id="graph-data" type="application/json">__GRAPH_DATA__</script>
  <script type="module">
    const graphData = JSON.parse(document.getElementById("graph-data").textContent);
    const nodeDataById = new Map(graphData.nodes.map(node => [node.id, node]));
    const linkedNodeIds = new Set(graphData.links.flatMap(link => [link.source, link.target]));
    const isolatedNodeIds = new Set(
      graphData.nodes.filter(node => !linkedNodeIds.has(node.id)).map(node => node.id)
    );
    const nodeSuggestions = document.getElementById("node-suggestions");
    const nodeKindSuggestion = node => (
      node.kind === "kafka_topic" ? "Topic Kafka"
        : node.kind === "mongodb_collection" ? "Collection MongoDB"
          : node.external ? "Service externe" : "Microservice"
    );
    graphData.nodes
      .slice()
      .sort((left, right) => left.name.localeCompare(right.name))
      .forEach(node => {
        const option = document.createElement("option");
        option.value = node.name;
        option.label = nodeKindSuggestion(node);
        nodeSuggestions.append(option);
      });
    const graphSummary = document.getElementById("graph-summary");
    const summaryCounts = {
      microservices: graphData.nodes.filter(node => node.kind === "microservice").length,
      topics: graphData.nodes.filter(node => node.kind === "kafka_topic").length,
      collections: graphData.nodes.filter(node => node.kind === "mongodb_collection").length,
      requestReplies: graphData.links.filter(link => link.kind === "request_reply").length,
    };
    const summaryItems = [
      `${summaryCounts.microservices} microservice${summaryCounts.microservices > 1 ? "s" : ""}`,
      `${summaryCounts.topics} topic${summaryCounts.topics > 1 ? "s" : ""} Kafka`,
      `${summaryCounts.collections} collection${summaryCounts.collections > 1 ? "s" : ""} MongoDB`,
      `${graphData.links.length} relation${graphData.links.length > 1 ? "s" : ""}`,
      ...(isolatedNodeIds.size
        ? [`${isolatedNodeIds.size} ressource${isolatedNodeIds.size > 1 ? "s" : ""} isolée${isolatedNodeIds.size > 1 ? "s" : ""}`]
        : []),
    ];
    summaryItems.forEach(text => {
      const item = document.createElement("span");
      item.className = "graph-summary-item";
      item.textContent = text;
      graphSummary.append(item);
    });
    if (summaryCounts.requestReplies) {
      const item = document.createElement("span");
      item.className = "graph-summary-item is-warning";
      item.textContent = `${summaryCounts.requestReplies} pattern${summaryCounts.requestReplies > 1 ? "s" : ""} request/reply`;
      graphSummary.append(item);
    }
    const RELATION_COLORS = Object.freeze({
      http: "#D55E00",
      kafkaPublish: "#009E73",
      kafkaConsume: "#0072B2",
      requestReply: "#7C3AED",
      mongodb: "#CC79A7",
      build: "#475569",
    });
    function relationColor(link) {
      if (link.kind === "rest") return RELATION_COLORS.http;
      if (link.kind === "build") return RELATION_COLORS.build;
      if (link.kind === "request_reply") return RELATION_COLORS.requestReply;
      if (link.direction === "incoming") return RELATION_COLORS.kafkaConsume;
      if (link.direction === "data_access") return RELATION_COLORS.mongodb;
      return RELATION_COLORS.kafkaPublish;
    }
    function dependencyGraphData() {
      return graphData.build_dependencies || { nodes: [], links: [] };
    }
    function buildHierarchyPositions(nodes, links) {
      // Sugiyama starts by condensing cycles. The resulting component graph is
      // acyclic and can therefore be assigned stable dependency layers.
      const adjacency = new Map(nodes.map(node => [node.id, []]));
      links.forEach(link => adjacency.get(link.source)?.push(link.target));
      const indexes = new Map(), lowlinks = new Map(), stack = [], onStack = new Set(), components = [];
      let nextIndex = 0;
      function visit(nodeId) {
        indexes.set(nodeId, nextIndex); lowlinks.set(nodeId, nextIndex); nextIndex += 1;
        stack.push(nodeId); onStack.add(nodeId);
        for (const targetId of adjacency.get(nodeId) || []) {
          if (!indexes.has(targetId)) {
            visit(targetId);
            lowlinks.set(nodeId, Math.min(lowlinks.get(nodeId), lowlinks.get(targetId)));
          } else if (onStack.has(targetId)) {
            lowlinks.set(nodeId, Math.min(lowlinks.get(nodeId), indexes.get(targetId)));
          }
        }
        if (lowlinks.get(nodeId) !== indexes.get(nodeId)) return;
        const component = [];
        for (;;) {
          const member = stack.pop(); onStack.delete(member); component.push(member);
          if (member === nodeId) break;
        }
        components.push(component.sort());
      }
      nodes.map(node => node.id).sort().forEach(nodeId => { if (!indexes.has(nodeId)) visit(nodeId); });
      const componentByNode = new Map();
      components.forEach((component, index) => component.forEach(nodeId => componentByNode.set(nodeId, index)));
      const successors = components.map(() => new Set());
      const indegrees = components.map(() => 0);
      links.forEach(link => {
        const source = componentByNode.get(link.source), target = componentByNode.get(link.target);
        if (source === target || successors[source].has(target)) return;
        successors[source].add(target); indegrees[target] += 1;
      });
      const levels = components.map(() => 0);
      const queue = components.map((_component, index) => index).filter(index => indegrees[index] === 0).sort((a, b) => a - b);
      for (let cursor = 0; cursor < queue.length; cursor += 1) {
        const component = queue[cursor];
        [...successors[component]].sort((a, b) => a - b).forEach(target => {
          levels[target] = Math.max(levels[target], levels[component] + 1);
          indegrees[target] -= 1;
          if (indegrees[target] === 0) queue.push(target);
        });
      }
      const layers = new Map();
      components.forEach((component, index) => {
        const level = levels[index];
        layers.set(level, [...(layers.get(level) || []), ...component]);
      });
      const positions = new Map();
      [...layers.entries()].sort(([left], [right]) => left - right).forEach(([level, nodeIds]) => {
        nodeIds.sort();
        const center = (nodeIds.length - 1) / 2;
        nodeIds.forEach((nodeId, row) => positions.set(nodeId, { x: level * 2.8, y: row - center }));
      });
      return positions;
    }
    let network;
    let renderer;
    let initialNodePositions = new Map();
    const layoutLibraries = Promise.all([
      import("https://esm.sh/graphology-layout-forceatlas2@0.10.1"),
      import("https://esm.sh/graphology-layout-noverlap@0.4.2"),
    ]).then(([forceAtlas2Module, noverlapModule]) => ({
      forceAtlas2: forceAtlas2Module.default,
      noverlap: noverlapModule.default,
    })).catch(error => {
      console.warn("Impossible de charger les dispositions Graphology.", error);
      return null;
    });

    let selectedId = null;
    let relatedNodes = null;
    let relatedEdges = null;
    let pathMicroserviceOrder = new Map();
    // Sigma invokes reducers while it is constructed, so these controls must
    // exist before creating the renderer.
    const relationHttp = document.getElementById("relation-http");
    const relationKafka = document.getElementById("relation-kafka");
    const relationMongodb = document.getElementById("relation-mongodb");
    const nodeMicroservice = document.getElementById("node-microservice");
    const nodeExternalMicroservice = document.getElementById("node-external-microservice");
    const nodeKafkaTopic = document.getElementById("node-kafka-topic");
    const nodeMongodbCollection = document.getElementById("node-mongodb-collection");
    function isVisibleRelation(kind) {
      return (kind !== "rest" || relationHttp.checked)
        && (!["kafka", "request_reply"].includes(kind) || relationKafka.checked)
        && (kind !== "mongodb" || relationMongodb.checked);
    }
    function isVisibleNode(node) {
      if (!node) return false;
      if (node.kind === "microservice") {
        return node.external ? nodeExternalMicroservice.checked : nodeMicroservice.checked;
      }
      if (node.kind === "kafka_topic") return nodeKafkaTopic.checked;
      if (node.kind === "mongodb_collection") return nodeMongodbCollection.checked;
      return true;
    }
    function isVisibleNodeId(id) { return isVisibleNode(nodeDataById.get(id)); }
    const NODE_VERTEX_SHADER = `
      attribute vec2 a_position;
      attribute float a_size;
      attribute vec4 a_color;
      uniform float u_ratio;
      uniform float u_scale;
      uniform mat3 u_matrix;
      varying vec4 v_color;
      void main() {
        gl_Position = vec4((u_matrix * vec3(a_position, 1.0)).xy, 0.0, 1.0);
        gl_PointSize = a_size * u_ratio * u_scale * 2.0;
        v_color = a_color;
      }
    `;
    const MICROSERVICE_FRAGMENT_SHADER = `
      precision mediump float;
      varying vec4 v_color;
      void main() {
        vec2 point = gl_PointCoord - vec2(.5);
        float shape = max(abs(point.x) * .866025 + abs(point.y) * .5, abs(point.y));
        float distance = shape - .43;
        float alpha = 1.0 - smoothstep(-.014, .014, distance);
        if (alpha < .01) discard;
        float border = smoothstep(.33, .42, shape);
        vec3 fill = vec3(.98, .99, 1.0);
        gl_FragColor = vec4(mix(fill, v_color.rgb, border), v_color.a * alpha);
      }
    `;
    const EXTERNAL_MICROSERVICE_FRAGMENT_SHADER = `
      precision mediump float;
      varying vec4 v_color;
      void main() {
        vec2 point = gl_PointCoord - vec2(.5);
        float shape = max(abs(point.x) * .866025 + point.y * .5, -point.y);
        float distance = shape - .36;
        float alpha = 1.0 - smoothstep(-.014, .014, distance);
        if (alpha < .01) discard;
        float border = smoothstep(.27, .35, shape);
        vec3 fill = vec3(.98, .99, 1.0);
        gl_FragColor = vec4(mix(fill, v_color.rgb, border), v_color.a * alpha);
      }
    `;
    const KAFKA_TOPIC_FRAGMENT_SHADER = `
      precision mediump float;
      varying vec4 v_color;
      void main() {
        vec2 point = gl_PointCoord - vec2(.5);
        float shape = length(point);
        float distance = shape - .43;
        float alpha = 1.0 - smoothstep(-.014, .014, distance);
        if (alpha < .01) discard;
        // Same visual contract as microservices: white interior and a thick
        // complexity-coloured border. The band starts well inside the circle
        // so it remains visible at normal zoom.
        float border = smoothstep(.27, .40, shape);
        vec3 fill = vec3(.98, .99, 1.0);
        gl_FragColor = vec4(mix(fill, v_color.rgb, border), v_color.a * alpha);
      }
    `;
    const MONGODB_COLLECTION_FRAGMENT_SHADER = `
      precision mediump float;
      varying vec4 v_color;
      void main() {
        vec2 point = gl_PointCoord - vec2(.5);
        vec2 bounds = vec2(.38);
        float radius = .09;
        vec2 corner = abs(point) - (bounds - radius);
        float distance = length(max(corner, 0.0)) + min(max(corner.x, corner.y), 0.0) - radius;
        float alpha = 1.0 - smoothstep(-.014, .014, distance);
        if (alpha < .01) discard;
        // Collections use the same visual grammar as microservices and
        // topics: shape communicates the resource type, while the coloured
        // outline communicates connectivity complexity.
        float border = smoothstep(-.095, -.020, distance);
        vec3 fill = vec3(.98, .99, 1.0);
        gl_FragColor = vec4(mix(fill, v_color.rgb, border), v_color.a * alpha);
      }
    `;
    const packedColorBuffer = new ArrayBuffer(4);
    const packedColorBytes = new Uint8Array(packedColorBuffer);
    const packedColorFloat = new Float32Array(packedColorBuffer);
    function packColor(color) {
      const value = color.startsWith("#") ? color.slice(1) : color;
      packedColorBytes[0] = parseInt(value.slice(0, 2), 16) || 0;
      packedColorBytes[1] = parseInt(value.slice(2, 4), 16) || 0;
      packedColorBytes[2] = parseInt(value.slice(4, 6), 16) || 0;
      packedColorBytes[3] = 254;
      return packedColorFloat[0];
    }
    function compileShader(gl, type, source) {
      const shader = gl.createShader(type);
      gl.shaderSource(shader, source);
      gl.compileShader(shader);
      if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
        throw new Error(`Impossible de compiler le shader WebGL: ${gl.getShaderInfoLog(shader)}`);
      }
      return shader;
    }
    function createNodeProgram(fragmentShader) {
      return class ShapeNodeProgram {
        constructor(gl) {
          this.gl = gl;
          this.array = new Float32Array();
          this.buffer = gl.createBuffer();
          const vertexShader = compileShader(gl, gl.VERTEX_SHADER, NODE_VERTEX_SHADER);
          const pixelShader = compileShader(gl, gl.FRAGMENT_SHADER, fragmentShader);
          this.program = gl.createProgram();
          gl.attachShader(this.program, vertexShader);
          gl.attachShader(this.program, pixelShader);
          gl.linkProgram(this.program);
          if (!gl.getProgramParameter(this.program, gl.LINK_STATUS)) {
            throw new Error(`Impossible d'associer le shader WebGL: ${gl.getProgramInfoLog(this.program)}`);
          }
          this.positionLocation = gl.getAttribLocation(this.program, "a_position");
          this.sizeLocation = gl.getAttribLocation(this.program, "a_size");
          this.colorLocation = gl.getAttribLocation(this.program, "a_color");
          this.matrixLocation = gl.getUniformLocation(this.program, "u_matrix");
          this.ratioLocation = gl.getUniformLocation(this.program, "u_ratio");
          this.scaleLocation = gl.getUniformLocation(this.program, "u_scale");
          this.bind();
        }
        allocate(capacity) { this.array = new Float32Array(capacity * 4); }
        process(data, hidden, offset) {
          const index = offset * 4;
          if (hidden) {
            this.array.fill(0, index, index + 4);
            return;
          }
          this.array[index] = data.x;
          this.array[index + 1] = data.y;
          this.array[index + 2] = data.size;
          this.array[index + 3] = packColor(data.color);
        }
        bind() {
          const gl = this.gl;
          gl.bindBuffer(gl.ARRAY_BUFFER, this.buffer);
          gl.enableVertexAttribArray(this.positionLocation);
          gl.enableVertexAttribArray(this.sizeLocation);
          gl.enableVertexAttribArray(this.colorLocation);
          gl.vertexAttribPointer(this.positionLocation, 2, gl.FLOAT, false, 16, 0);
          gl.vertexAttribPointer(this.sizeLocation, 1, gl.FLOAT, false, 16, 8);
          gl.vertexAttribPointer(this.colorLocation, 4, gl.UNSIGNED_BYTE, true, 16, 12);
        }
        bufferData() { this.gl.bufferData(this.gl.ARRAY_BUFFER, this.array, this.gl.DYNAMIC_DRAW); }
        render(params) {
          if (!this.array.length) return;
          const gl = this.gl;
          gl.useProgram(this.program);
          gl.uniform1f(this.ratioLocation, 1 / Math.sqrt(params.ratio));
          gl.uniform1f(this.scaleLocation, params.scalingRatio);
          gl.uniformMatrix3fv(this.matrixLocation, false, params.matrix);
          gl.drawArrays(gl.POINTS, 0, this.array.length / 4);
        }
      };
    }
    function layoutGraphNodes(nodes, links) {
      const layoutNodes = nodes.map((node, index) => {
        const angle = (Math.PI * 2 * index) / Math.max(1, nodes.length);
        return { ...node, x: Math.cos(angle), y: Math.sin(angle), vx: 0, vy: 0 };
      });
      const layoutById = new Map(layoutNodes.map(node => [node.id, node]));
      // The layout is deliberately recomputed from the visible dependencies.
      // This prevents hidden relation types from influencing node positions.
      for (let iteration = 0; iteration < 720; iteration += 1) {
        const cooling = .14 * (1 - iteration / 720) + .015;
        for (let i = 0; i < layoutNodes.length; i += 1) {
          for (let j = i + 1; j < layoutNodes.length; j += 1) {
            const a = layoutNodes[i], b = layoutNodes[j];
            const dx = b.x - a.x || (i < j ? .001 : -.001);
            const dy = b.y - a.y || .001;
            const distance2 = dx * dx + dy * dy + .012;
            const strength = 1.25 / distance2;
            a.vx -= dx * strength; a.vy -= dy * strength;
            b.vx += dx * strength; b.vy += dy * strength;
          }
        }
        links.forEach(link => {
          const source = layoutById.get(link.source), target = layoutById.get(link.target);
          if (!source || !target) return;
          const dx = target.x - source.x, dy = target.y - source.y;
          const distance = Math.hypot(dx, dy) || .001;
          const desired = ["kafka", "request_reply"].includes(link.kind) ? 1.05 : link.kind === "mongodb" ? .68 : .82;
          const pull = (distance - desired) * .035;
          const ux = dx / distance, uy = dy / distance;
          source.vx += ux * pull; source.vy += uy * pull;
          target.vx -= ux * pull; target.vy -= uy * pull;
        });
        layoutNodes.forEach(node => {
          node.vx += -node.x * .008; node.vy += -node.y * .008;
          node.x += node.vx * cooling; node.y += node.vy * cooling;
          node.vx *= .72; node.vy *= .72;
        });
      }
      return layoutNodes;
    }
    function layoutIsolatedNodes(nodes, connectedNodes) {
      if (!nodes.length) return [];
      const startX = connectedNodes.length
        ? Math.max(...connectedNodes.map(node => node.x)) + 1.4
        : 0;
      const columns = Math.max(1, Math.ceil(Math.sqrt(nodes.length)));
      return nodes.map((node, index) => ({
        ...node,
        x: startX + (index % columns) * .55,
        y: (Math.floor(index / columns) - (Math.ceil(nodes.length / columns) - 1) / 2) * .55,
        isolated: true,
      }));
    }
    function rebuildGraph() {
      const visibleLinks = graphData.links.filter(link => isVisibleRelation(link.kind));
      const visibleNodeIds = new Set(visibleLinks.flatMap(link => [link.source, link.target]));
      const connectedNodes = graphData.nodes.filter(node => visibleNodeIds.has(node.id));
      const isolatedNodes = graphData.nodes.filter(node => isolatedNodeIds.has(node.id));
      const positionedConnectedNodes = layoutGraphNodes(connectedNodes, visibleLinks);
      const layoutNodes = [
        ...positionedConnectedNodes,
        ...layoutIsolatedNodes(isolatedNodes, positionedConnectedNodes),
      ];
      renderer?.kill();
      network = new graphology.MultiDirectedGraph();
      layoutNodes.forEach(node => network.addNode(node.id, {
        label: node.label, x: node.x, y: node.y, size: node.size, color: node.color,
        type: node.external ? "external_microservice" : node.kind,
      }));
      visibleLinks.forEach((link, index) => network.addEdgeWithKey(`edge-${index}`, link.source, link.target, {
        label: link.label, size: 1.2, color: relationColor(link), kind: link.kind, type: "arrow",
      }));
      initialNodePositions = new Map();
      network.forEachNode((node, attributes) => initialNodePositions.set(node, { x: attributes.x, y: attributes.y }));
      renderer = new Sigma(network, document.getElementById("graph"), {
        nodeProgramClasses: {
          microservice: createNodeProgram(MICROSERVICE_FRAGMENT_SHADER),
          external_microservice: createNodeProgram(EXTERNAL_MICROSERVICE_FRAGMENT_SHADER),
          kafka_topic: createNodeProgram(KAFKA_TOPIC_FRAGMENT_SHADER),
          mongodb_collection: createNodeProgram(MONGODB_COLLECTION_FRAGMENT_SHADER),
        },
        renderEdgeLabels: false, labelDensity: .08, labelGridCellSize: 110, labelRenderedSizeThreshold: 8,
        nodeReducer: (node, data) => {
          if (!isVisibleNodeId(node)) return { ...data, hidden: true, label: "" };
          if (!selectedId || relatedNodes.has(node)) {
            const order = pathMicroserviceOrder.get(node);
            return order ? { ...data, label: `${order}. ${data.label}` } : data;
          }
          return { ...data, color: "#d8e0ea", label: "" };
        },
        edgeReducer: (edge, data) => {
          if (!isVisibleNodeId(network.source(edge)) || !isVisibleNodeId(network.target(edge))) return { ...data, hidden: true };
          if (!selectedId || relatedEdges.has(edge)) return data;
          return { ...data, color: "#e5eaf0", size: .35 };
        },
      });
      renderer.on("clickNode", ({ node }) => selectNode(node));
      renderer.on("clickStage", reset);
      const graphCanvas = document.getElementById("graph");
      graphCanvas.dataset.relationCount = String(visibleLinks.length);
      graphCanvas.setAttribute("aria-label", `Graphe des interactions : ${visibleLinks.length} relations`);
    }
    rebuildGraph();
    let dependencyRenderer = null;
    const details = document.getElementById("details");
    const search = document.getElementById("search");
    const searchStatus = document.getElementById("search-status");
    const pathQuery = document.getElementById("path-query");
    const pathLock = document.getElementById("path-lock");
    const graphTab = document.getElementById("graph-tab");
    const resourcesTab = document.getElementById("resources-tab");
    const issuesTab = document.getElementById("issues-tab");
    const pathsTab = document.getElementById("paths-tab");
    const graphLegend = document.getElementById("graph-legend");
    const graphPanel = document.getElementById("graph-panel");
    const dependenciesPanel = document.getElementById("dependencies-panel");
    const issuesPanel = document.getElementById("issues-panel");
    const pathsPanel = document.getElementById("paths-panel");
    const resourcesPanel = document.getElementById("resources-panel");
    const requestReplyPanel = document.getElementById("request-reply-panel");
    const advancedControls = document.getElementById("advanced-controls");
    const graphCanvas = document.getElementById("graph");
    const dependencyCanvas = document.getElementById("dependency-graph");
    function ensureDependencyRenderer() {
      if (dependencyRenderer !== null) return dependencyRenderer;
      const dependencyData = dependencyGraphData();
      const dependencyPositions = buildHierarchyPositions(dependencyData.nodes, dependencyData.links);
      const dependencyNetwork = new graphology.MultiDirectedGraph();
      dependencyData.nodes.forEach(node => {
        const position = dependencyPositions.get(node.id) || { x: 0, y: 0 };
        dependencyNetwork.addNode(node.id, {
          label: node.name,
          x: position.x,
          y: position.y,
          size: node.size,
          color: node.color,
          type: "build_module",
        });
      });
      dependencyData.links.forEach((link, index) => dependencyNetwork.addEdgeWithKey(
        `dependency-edge-${index}`, link.source, link.target, {
          label: link.label,
          size: 1.5,
          color: relationColor(link),
          kind: link.kind,
          type: "arrow",
        }
      ));
      dependencyRenderer = new Sigma(dependencyNetwork, dependencyCanvas, {
        nodeProgramClasses: { build_module: createNodeProgram(MICROSERVICE_FRAGMENT_SHADER) },
        renderEdgeLabels: false,
        labelDensity: .12,
        labelGridCellSize: 110,
        labelRenderedSizeThreshold: 8,
      });
      dependencyRenderer.on("clickNode", ({ node }) => selectDependencyModule(node));
      dependencyRenderer.on("clickStage", reset);
      return dependencyRenderer;
    }
    const indexingIssuesList = document.getElementById("indexing-issues");
    const indexingIssuesEmpty = document.getElementById("indexing-issues-empty");
    const indexingIssuesTitle = document.getElementById("indexing-issues-title");
    const indexingIssues = graphData.indexing_issues || [];
    const inventoryStatus = document.getElementById("inventory-status");
    const openApiReferencesList = document.getElementById("openapi-references");
    const openApiReferencesEmpty = document.getElementById("openapi-references-empty");
    const openApiReferencesFilter = document.getElementById("openapi-reference-filter");
    const dtoReferencesList = document.getElementById("dto-references");
    const dtoReferencesEmpty = document.getElementById("dto-references-empty");
    const dtoReferencesFilter = document.getElementById("dto-reference-filter");
    const openapiReferencesTitle = document.getElementById("openapi-references-title");
    const dtoReferencesTitle = document.getElementById("dto-references-title");
    const requestReplyPatternsList = document.getElementById("request-reply-patterns");
    const requestReplyEmpty = document.getElementById("request-reply-empty");
    const requestReplyTitle = document.getElementById("request-reply-title");
    const analyzedPathsList = document.getElementById("analyzed-paths");
    const analyzedPathsEmpty = document.getElementById("analyzed-paths-empty");
    const pathHistoryTitle = document.getElementById("path-history-title");
    const layoutStatus = document.getElementById("layout-status");
    const layoutButtons = new Map([
      ["forceatlas2", document.getElementById("layout-forceatlas2")],
      ["noverlap", document.getElementById("layout-noverlap")],
      ["forceatlas2-noverlap", document.getElementById("layout-forceatlas2-noverlap")],
    ]);
    const layoutLabels = new Map([
      ["forceatlas2", "vue regroupée"],
      ["noverlap", "vue aérée"],
      ["forceatlas2-noverlap", "vue équilibrée"],
    ]);
    let layoutRequest = 0;
    const pathStops = [];
    const analyzedPaths = [];
    const MAX_SIMPLE_PATH_DEPTH = 8;
    const MAX_SIMPLE_PATHS = 8;
    const MAX_SIMPLE_PATH_EXPLORATIONS = 2000;
    function restoreInitialNodePositions() {
      network.forEachNode(node => {
        const position = initialNodePositions.get(node);
        network.setNodeAttribute(node, "x", position.x);
        network.setNodeAttribute(node, "y", position.y);
      });
    }
    function setActiveLayout(layout) {
      layoutButtons.forEach((button, key) => {
        const active = key === layout;
        button.classList.toggle("is-active", active);
        button.setAttribute("aria-pressed", String(active));
      });
    }
    async function applyLayout(layout) {
      const request = ++layoutRequest;
      const label = layoutLabels.get(layout);
      setActiveLayout(layout);
      layoutStatus.textContent = `Calcul de la disposition ${label}…`;
      const libraries = await layoutLibraries;
      if (request !== layoutRequest) return;
      if (libraries === null) {
        layoutStatus.textContent = "Les dispositions Graphology sont indisponibles ; la disposition initiale est conservee.";
        return;
      }
      try {
        restoreInitialNodePositions();
        if (layout === "forceatlas2" || layout === "forceatlas2-noverlap") {
          libraries.forceAtlas2.assign(network, {
            iterations: Math.min(220, Math.max(80, network.order * 3)),
            settings: {
              adjustSizes: true,
              barnesHutOptimize: network.order >= 30,
              barnesHutTheta: .7,
              gravity: 1.5,
              scalingRatio: 12,
              slowDown: 2,
            },
          });
        }
        if (layout === "noverlap" || layout === "forceatlas2-noverlap") {
          libraries.noverlap.assign(network, {
            maxIterations: 160,
            settings: { expansion: 1.1, gridSize: 20, margin: 4, ratio: 1.3, speed: 3 },
          });
        }
      } catch (error) {
        console.error(`Impossible de calculer la disposition ${label}.`, error);
        restoreInitialNodePositions();
        renderer.refresh();
        layoutStatus.textContent = `La disposition ${label} a echoue ; la disposition initiale est restauree.`;
        return;
      }
      if (request !== layoutRequest) return;
      renderer.refresh();
      renderer.getCamera().animatedReset({ duration: 260 });
      layoutStatus.textContent = `${label} actif.`;
    }
    const nodesByNormalizedName = new Map();
    function normalizeNodeName(name) {
      return name.trim().replace(/\\s+/g, " ").toLocaleLowerCase();
    }
    graphData.nodes.forEach(node => {
      const key = normalizeNodeName(node.name);
      nodesByNormalizedName.set(key, [...(nodesByNormalizedName.get(key) || []), node]);
    });
    const pathHistoryStorageKey = (() => {
      const signature = [
        ...graphData.nodes.map(node => node.id),
        ...graphData.links.map(link => `${link.source}->${link.target}:${link.kind}`),
      ].sort().join("|");
      let hash = 2166136261;
      for (let index = 0; index < signature.length; index += 1) {
        hash = Math.imul(hash ^ signature.charCodeAt(index), 16777619);
      }
      return `systemlens:analyzed-paths:${hash >>> 0}`;
    })();

    function isValidPathStops(stops) {
      if (!Array.isArray(stops) || stops.length < 2 || new Set(stops).size !== stops.length) return false;
      return stops.every(id => nodeDataById.has(id) && ["microservice", "kafka_topic"].includes(nodeDataById.get(id).kind))
        && nodeDataById.get(stops[0]).kind === "microservice"
        && nodeDataById.get(stops.at(-1)).kind === "microservice";
    }
    function loadAnalyzedPaths() {
      try {
        const stored = JSON.parse(localStorage.getItem(pathHistoryStorageKey) || "[]");
        if (!Array.isArray(stored)) return;
        stored.filter(isValidPathStops).forEach(stops => analyzedPaths.push(stops));
      } catch (_error) {
        // The export remains usable when browser storage is unavailable or stale.
      }
    }
    function persistAnalyzedPaths() {
      try {
        localStorage.setItem(pathHistoryStorageKey, JSON.stringify(analyzedPaths));
      } catch (_error) {
        // Saving the optional history must never prevent graph exploration.
      }
    }
    function setToolbarTab(tab) {
      const showingGraph = tab === "graph";
      const showingDependencies = tab === "dependencies";
      const showingIssues = tab === "issues";
      const showingPaths = tab === "paths";
      const showingRequestReply = tab === "request-reply";
      const showingResources = ["resources", "dependencies", "request-reply"].includes(tab);
      const showingResourceContent = tab === "resources";
      graphTab.classList.toggle("is-active", showingGraph);
      graphTab.setAttribute("aria-selected", String(showingGraph));
      resourcesTab.classList.toggle("is-active", showingResources);
      resourcesTab.setAttribute("aria-selected", String(showingResources));
      issuesTab.classList.toggle("is-active", showingIssues);
      issuesTab.setAttribute("aria-selected", String(showingIssues));
      pathsTab.classList.toggle("is-active", showingPaths);
      pathsTab.setAttribute("aria-selected", String(showingPaths));
      graphPanel.hidden = !showingGraph;
      dependenciesPanel.hidden = !showingDependencies;
      issuesPanel.hidden = !showingIssues;
      pathsPanel.hidden = !showingPaths;
      resourcesPanel.hidden = !showingResourceContent;
      requestReplyPanel.hidden = !showingRequestReply;
      graphLegend.hidden = !showingGraph;
      graphCanvas.hidden = showingDependencies;
      dependencyCanvas.hidden = !showingDependencies;
      if (showingDependencies) {
        const activeDependencyRenderer = ensureDependencyRenderer();
        requestAnimationFrame(() => {
          activeDependencyRenderer.refresh();
          activeDependencyRenderer.getCamera().animatedReset({ duration: 220 });
        });
      }
    }
    const filterPresetButtons = [...document.querySelectorAll(".filter-preset")];
    function setActiveRelationPreset(preset) {
      filterPresetButtons.forEach(button => button.classList.toggle("is-active", button.dataset.preset === preset));
    }
    function setRelationFilters(http, kafka, mongodb) {
      relationHttp.checked = http;
      relationKafka.checked = kafka;
      relationMongodb.checked = mongodb;
    }
    function applyRelationPreset(preset) {
      if (preset === "selection") {
        setRelationFilters(true, true, true);
        rebuildGraph();
        if (!selectedId) {
          layoutStatus.textContent = "Selectionnez d'abord un noeud pour isoler ses relations.";
          setActiveRelationPreset("all");
          reset();
          return;
        }
        relatedNodes = new Set([selectedId]);
        relatedEdges = new Set();
        network.forEachEdge((edge, _attributes, source, target) => {
          if (source === selectedId || target === selectedId) {
            relatedEdges.add(edge); relatedNodes.add(source); relatedNodes.add(target);
          }
        });
        setActiveRelationPreset(preset);
        renderer.refresh();
        return;
      }
      const filters = {
        all: [true, true, true],
        http: [true, false, false],
        kafka: [false, true, false],
        mongodb: [false, false, true],
      };
      const selected = filters[preset];
      if (!selected) return;
      setRelationFilters(...selected);
      setActiveRelationPreset(preset);
      rebuildGraph();
      reset();
    }
    function renderIndexingIssues() {
      inventoryStatus.hidden = false;
      inventoryStatus.classList.toggle("is-warning", indexingIssues.length > 0);
      inventoryStatus.textContent = indexingIssues.length
        ? `Inventaire : ${indexingIssues.length} fait${indexingIssues.length > 1 ? "s" : ""} à vérifier`
        : "Inventaire : aucun fait non résolu";
      inventoryStatus.title = indexingIssues.length
        ? "Ouvrir les problèmes d'indexation"
        : "Aucun fait non résolu dans cet inventaire";
      indexingIssuesTitle.textContent = `Problemes d'indexation (${indexingIssues.length})`;
      indexingIssuesList.replaceChildren();
      indexingIssuesEmpty.hidden = indexingIssues.length > 0;
      indexingIssues.forEach(issue => {
        const item = document.createElement("li");
        item.className = `indexing-issue ${issue.severity}`;
        const header = document.createElement("div");
        header.className = "indexing-issue-header";
        const severity = document.createElement("span");
        severity.className = "indexing-issue-severity";
        severity.textContent = issue.severity === "warning" ? "A corriger" : "A verifier";
        const category = document.createElement("span");
        category.className = "indexing-issue-category";
        category.textContent = issue.category;
        const message = document.createElement("p");
        message.className = "indexing-issue-message";
        message.textContent = issue.message;
        header.append(severity, category);
        item.append(header, message);
        if (issue.location) {
          const location = document.createElement(issue.vscode_uri ? "a" : "code");
          location.className = "indexing-issue-location";
          location.textContent = issue.location;
          if (issue.vscode_uri) {
            location.href = issue.vscode_uri;
            location.title = "Ouvrir le fichier concerné dans VS Code";
          }
          item.append(location);
        }
        indexingIssuesList.append(item);
      });
    }
    function referenceItem(title, meta, actionLabel, action, disabled = false) {
      const item = document.createElement("li");
      item.className = "reference-item";
      const text = document.createElement("div");
      const name = document.createElement("div");
      name.className = "reference-title";
      name.textContent = title;
      const details = document.createElement("div");
      details.className = "reference-meta";
      details.textContent = meta;
      text.append(name, details);
      const button = document.createElement("button");
      button.className = "reference-action";
      button.type = "button";
      button.textContent = actionLabel;
      button.disabled = disabled;
      if (!disabled) button.addEventListener("click", action);
      item.append(text, button);
      return item;
    }
    function renderReferences() {
      openApiReferencesList.replaceChildren();
      const contracts = graphData.nodes.flatMap(node => (
        node.kind === "microservice"
          ? (node.openapi_contracts || []).map(contract => ({ service: node.name, contract }))
          : []
      ));
      const openApiQuery = openApiReferencesFilter.value.trim().toLocaleLowerCase();
      const visibleContracts = contracts.filter(({ service, contract }) => (
        !openApiQuery
        || service.toLocaleLowerCase().includes(openApiQuery)
        || contract.path.toLocaleLowerCase().includes(openApiQuery)
      ));
      openApiReferencesEmpty.hidden = visibleContracts.length > 0;
      openApiReferencesEmpty.textContent = openApiQuery && !visibleContracts.length
        ? "Aucun contrat ne correspond à ce filtre."
        : "Aucun contrat OpenAPI détecté.";
      visibleContracts.forEach(({ service, contract }) => {
        openApiReferencesList.append(referenceItem(
          contract.path,
          `${service} · ${contract.resources?.length || 0} ressource(s)`,
          contract.spec ? "Swagger UI" : "Indisponible",
          () => openOpenApiContract(contract),
          !contract.spec,
        ));
      });
      dtoReferencesList.replaceChildren();
      const dtos = graphData.kafka_dtos || [];
      const query = dtoReferencesFilter.value.trim().toLocaleLowerCase();
      const visibleDtos = dtos.filter(dto => (
        !query || dtoLabel(dto).toLocaleLowerCase().includes(query)
      ));
      dtoReferencesEmpty.hidden = visibleDtos.length > 0;
      dtoReferencesEmpty.textContent = query && !visibleDtos.length
        ? "Aucun DTO ne correspond à ce filtre."
        : "Aucun DTO Kafka détecté.";
      visibleDtos.forEach(dto => {
        const exchangeCount = (dto.producers?.length || 0) + (dto.consumers?.length || 0);
        dtoReferencesList.append(referenceItem(
          dtoLabel(dto),
          `${dto.fields?.length || 0} champ(s) · ${dto.topics?.length || 0} topic(s) · ${exchangeCount} liaison(s)`,
          "Inspecter",
          () => openDtoInspector(dto.id),
        ));
      });
      openapiReferencesTitle.textContent = `Contrats OpenAPI (${visibleContracts.length}/${contracts.length})`;
      dtoReferencesTitle.textContent = `DTO Kafka (${visibleDtos.length}/${dtos.length})`;
    }
    function renderRequestReplyPatterns() {
      const patterns = graphData.links.filter(link => link.kind === "request_reply");
      requestReplyPatternsList.replaceChildren();
      requestReplyEmpty.hidden = patterns.length > 0;
      patterns.forEach(pattern => {
        const request = nodeDataById.get(pattern.source);
        const reply = nodeDataById.get(pattern.target);
        const requestProducers = graphData.links
          .filter(link => link.target === pattern.source && link.kind === "kafka")
          .map(link => link.source);
        const requestConsumers = graphData.links
          .filter(link => link.source === pattern.source && link.kind === "kafka")
          .map(link => link.target);
        const replyProducers = graphData.links
          .filter(link => link.target === pattern.target && link.kind === "kafka")
          .map(link => link.source);
        const replyConsumers = graphData.links
          .filter(link => link.source === pattern.target && link.kind === "kafka")
          .map(link => link.target);
        const sources = requestProducers.filter(service => replyConsumers.includes(service));
        const destinations = requestConsumers.filter(service => replyProducers.includes(service));
        const servicePairs = [...new Set(sources)].flatMap(source =>
          [...new Set(destinations)].filter(target => target !== source).map(target => ({ source, target }))
        );
        if (!servicePairs.length) {
          requestReplyPatternsList.append(referenceItem(
            `${request?.name || pattern.source} → ${reply?.name || pattern.target}`,
            "Couple de topics détecté ; les services qui réalisent l’aller-retour ne sont pas tous indexés.",
            "Voir dans le graphe",
            () => { setToolbarTab("graph"); selectNode(pattern.source); },
          ));
          return;
        }
        servicePairs.forEach(({ source, target }) => {
          const sourceName = nodeDataById.get(source)?.name || source;
          const targetName = nodeDataById.get(target)?.name || target;
          requestReplyPatternsList.append(referenceItem(
            `${sourceName} ⇄ ${targetName}`,
            `${request?.name || pattern.source} → ${reply?.name || pattern.target} · chemin le plus court entre services`,
            "Voir le chemin",
            () => {
              setToolbarTab("graph");
              const path = shortestPath(source, target);
              if (path) showPath(path, [source, target]);
              else setDetailsEmpty(`Aucun chemin orienté entre ${sourceName} et ${targetName}.`);
            },
          ));
        });
      });
      requestReplyTitle.textContent = `Patterns request/reply Kafka (${patterns.length})`;
    }
    function renderAnalyzedPaths() {
      pathHistoryTitle.textContent = `Chemins analyses (${analyzedPaths.length})`;
      analyzedPathsList.replaceChildren();
      analyzedPathsEmpty.hidden = analyzedPaths.length > 0;
      analyzedPaths.forEach((stops, index) => {
        const item = document.createElement("li");
        item.className = "path-history-item";
        const replay = document.createElement("button");
        replay.className = "path-history-replay";
        replay.type = "button";
        replay.textContent = stops.map(id => nodeDataById.get(id).name).join(" -> ");
        replay.title = "Reanalyser ce chemin";
        replay.addEventListener("click", () => replayAnalyzedPath(stops));
        const remove = document.createElement("button");
        remove.className = "path-history-delete";
        remove.type = "button";
        remove.textContent = "×";
        remove.title = "Supprimer ce chemin analyse";
        remove.setAttribute("aria-label", `Supprimer le chemin ${replay.textContent}`);
        remove.addEventListener("click", () => {
          analyzedPaths.splice(index, 1);
          persistAnalyzedPaths();
          renderAnalyzedPaths();
        });
        item.append(replay, remove);
        analyzedPathsList.append(item);
      });
    }
    function rememberAnalyzedPath(stops) {
      const path = [...stops];
      const key = path.join("|");
      const existingIndex = analyzedPaths.findIndex(item => item.join("|") === key);
      if (existingIndex >= 0) analyzedPaths.splice(existingIndex, 1);
      analyzedPaths.unshift(path);
      persistAnalyzedPaths();
      renderAnalyzedPaths();
    }
    function replayAnalyzedPath(stops) {
      pathStops.splice(0, pathStops.length, ...stops);
      renderPathQuery();
      setToolbarTab("graph");
      showShortestPath();
    }
    loadAnalyzedPaths();

    function createDetailsGroup(title, open = true) {
      const group = document.createElement("details");
      group.className = "details-group";
      group.open = open;
      const summary = document.createElement("summary");
      summary.textContent = title;
      group.append(summary);
      details.append(group);
      return group;
    }
    function discardEmptyDetailsGroup(group) {
      if (!group.querySelector(".details-section")) group.remove();
    }
    function appendList(title, values, container = details) {
      if (!values.length) return;
      const section = document.createElement("section");
      section.className = "details-section";
      const heading = document.createElement("h2");
      heading.textContent = title;
      const list = document.createElement("ul");
      values.forEach(value => { const item = document.createElement("li"); item.textContent = value; list.append(item); });
      section.append(heading, list);
      container.append(section);
    }
    function appendActionList(title, entries, container = details) {
      if (!entries.length) return;
      const section = document.createElement("section");
      section.className = "details-section";
      const heading = document.createElement("h2");
      heading.textContent = title;
      const list = document.createElement("ul");
      entries.forEach(({ label, title: actionTitle, action }) => {
        const item = document.createElement("li");
        item.className = "relation-item";
        const button = document.createElement("button");
        button.className = "relation-link";
        button.type = "button";
        button.textContent = label;
        button.title = actionTitle || "Explorer cet element dans le graphe";
        button.addEventListener("click", action);
        item.append(button);
        list.append(item);
      });
      section.append(heading, list);
      container.append(section);
    }
    function appendFindings(findings, container = details) {
      if (!findings.length) return;
      const section = document.createElement("section");
      section.className = "details-section";
      const heading = document.createElement("h2");
      heading.textContent = `Findings (${findings.length})`;
      const list = document.createElement("ul");
      findings.forEach(finding => {
        const item = document.createElement("li");
        const link = document.createElement("a");
        link.href = finding.vscode_uri;
        link.textContent = `[${finding.severity}] ${finding.rule_id} · Ouvrir le fichier`;
        link.title = `${finding.path}:${finding.start_line} — Ouvrir ce finding dans VS Code`;
        const message = document.createElement("div");
        message.textContent = finding.message;
        item.append(link, message);
        list.append(item);
      });
      section.append(heading, list);
      container.append(section);
    }
    function appendRelationList(title, links, currentId, labelForLink, container = details) {
      const seen = new Set();
      const entries = links.flatMap(link => {
        const targetId = link.source === currentId ? link.target : link.source;
        const label = labelForLink(link);
        const key = `${targetId}::${label}`;
        if (seen.has(key)) return [];
        seen.add(key);
        return [{ targetId, label }];
      });
      if (!entries.length) return;
      const section = document.createElement("section");
      section.className = "details-section";
      const heading = document.createElement("h2");
      heading.textContent = title;
      const list = document.createElement("ul");
      entries.forEach(({ targetId, label }) => {
        const item = document.createElement("li");
        item.className = "relation-item";
        const button = document.createElement("button");
        button.className = "relation-link";
        button.type = "button";
        button.textContent = label;
        button.title = "Selectionner ce noeud dans le graphe";
        button.addEventListener("click", () => selectNode(targetId));
        item.append(button);
        list.append(item);
      });
      section.append(heading, list);
      container.append(section);
    }
    const inspectorModal = document.getElementById("inspector-modal");
    const inspectorTitle = document.getElementById("inspector-title");
    const inspectorBody = document.getElementById("inspector-body");
    const dtoNavigation = [];
    function closeInspector() {
      inspectorModal.hidden = true;
      inspectorBody.replaceChildren();
      inspectorBody.className = "inspector-body";
      dtoNavigation.splice(0);
    }
    function openInspector(title) {
      inspectorTitle.textContent = title;
      inspectorBody.replaceChildren();
      inspectorBody.className = "inspector-body";
      inspectorModal.hidden = false;
    }
    function openOpenApiContract(contract) {
      openInspector(`OpenAPI · ${contract.path}`);
      if (contract.vscode_uri) {
        const link = document.createElement("a");
        link.href = contract.vscode_uri;
        link.textContent = "Ouvrir le fichier dans VS Code";
        link.className = "dto-summary";
        inspectorBody.append(link);
      }
      if (!contract.spec || !window.SwaggerUIBundle) {
        const message = document.createElement("p");
        message.className = "dto-summary";
        message.textContent = "La specification locale ou Swagger UI n'est pas disponible dans cet export.";
        inspectorBody.append(message);
        return;
      }
      inspectorBody.classList.add("swagger-ui");
      window.SwaggerUIBundle({
        spec: contract.spec,
        domNode: inspectorBody,
        deepLinking: false,
        docExpansion: "list",
        supportedSubmitMethods: [],
      });
    }
    function appendDtoInspectorSection(title, entries, itemClass = "dto-tag") {
      if (!entries.length) return;
      const section = document.createElement("section");
      section.className = "dto-section";
      const heading = document.createElement("h2");
      heading.textContent = title;
      const list = document.createElement("ul");
      list.className = "dto-tags";
      entries.forEach(entry => {
        const item = document.createElement("li");
        item.className = itemClass;
        item.textContent = entry;
        list.append(item);
      });
      section.append(heading, list);
      inspectorBody.append(section);
    }
    function dtoDefinition(dtoName) {
      return [...(graphData.kafka_dtos || []), ...(graphData.project_dto_definitions || [])]
        .find(item => item.id === dtoName);
    }
    function dtoLabel(dto) {
      const definitions = [...(graphData.kafka_dtos || []), ...(graphData.project_dto_definitions || [])];
      const duplicate = definitions.filter(item => item.name === dto.name).length > 1;
      return duplicate && dto.qualified_name ? `${dto.name} · ${dto.qualified_name}` : dto.name;
    }
    function openDtoInspector(dtoName) {
      dtoNavigation.splice(0);
      renderDtoInspector(dtoName);
    }
    function openNestedDtoInspector(dtoName, parentDtoName) {
      dtoNavigation.push(parentDtoName);
      renderDtoInspector(dtoName);
    }
    function returnToContainingDto() {
      const parentDtoName = dtoNavigation.pop();
      if (parentDtoName) renderDtoInspector(parentDtoName);
    }
    function renderDtoInspector(dtoName) {
      const dto = dtoDefinition(dtoName);
      if (!dto) return;
      openInspector(`DTO Kafka · ${dto.name}`);
      inspectorBody.classList.add("dto-inspector");
      if (dtoNavigation.length) {
        const navigation = document.createElement("div");
        navigation.className = "dto-navigation";
        const back = document.createElement("button");
        back.className = "dto-back";
        back.type = "button";
        back.textContent = "← Retour";
        back.title = `Retour vers ${dtoNavigation.at(-1)}`;
        back.addEventListener("click", returnToContainingDto);
        navigation.append(back);
        inspectorBody.append(navigation);
      }
      const summary = document.createElement("p");
      summary.className = "dto-summary";
      summary.textContent = dto.source
        ? `Classe source : ${dto.source}`
        : "Classe Java non retrouvee dans les sources indexees ; les relations Kafka restent disponibles.";
      inspectorBody.append(summary);
      if (dto.vscode_uri) {
        const sourceLink = document.createElement("a");
        sourceLink.href = dto.vscode_uri;
        sourceLink.className = "dto-summary";
        sourceLink.textContent = "Ouvrir la classe dans VS Code";
        inspectorBody.append(sourceLink);
      }
      const fields = dto.fields || [];
      if (fields.length) {
        const section = document.createElement("section");
        section.className = "dto-section";
        const heading = document.createElement("h2");
        heading.textContent = "Champs declares";
        const list = document.createElement("ul");
        list.className = "dto-fields";
        fields.forEach(field => {
          const item = document.createElement("li");
          item.className = "dto-field";
          const references = field.dto_references || [];
          const type = document.createElement(references.length ? "button" : "span");
          type.className = "dto-field-type";
          type.textContent = field.type;
          if (references.length) {
            type.type = "button";
            const referencedDto = dtoDefinition(references[0]);
            type.title = `Ouvrir le type projet ${referencedDto ? dtoLabel(referencedDto) : references[0]}`;
            type.addEventListener("click", () => openNestedDtoInspector(references[0], dto.id));
          }
          const name = document.createElement("span");
          name.className = "dto-field-name";
          name.textContent = field.name;
          item.append(type, name);
          list.append(item);
        });
        section.append(heading, list);
        inspectorBody.append(section);
      }
      appendDtoInspectorSection("Topics", dto.topics || []);
      appendDtoInspectorSection("Valeurs enum", dto.enum_values || []);
      appendDtoInspectorSection("Producteurs", dto.producers || []);
      appendDtoInspectorSection("Consommateurs", dto.consumers || []);
    }
    function selectDependencyModule(id) {
      const node = (graphData.build_dependencies?.nodes || []).find(item => item.id === id);
      if (!node) return;
      const links = graphData.build_dependencies?.links || [];
      const dependencies = links
        .filter(link => link.source === id)
        .map(link => (graphData.build_dependencies.nodes.find(item => item.id === link.target) || {}).name)
        .filter(Boolean);
      const dependents = links
        .filter(link => link.target === id)
        .map(link => (graphData.build_dependencies.nodes.find(item => item.id === link.source) || {}).name)
        .filter(Boolean);
      details.replaceChildren();
      const header = document.createElement("header");
      header.className = "details-header";
      const kicker = document.createElement("p");
      kicker.className = "details-kicker";
      kicker.textContent = `Module ${node.build_system === "unknown" ? "Maven / Gradle" : node.build_system}`;
      const title = document.createElement("h1");
      title.className = "details-title";
      title.textContent = node.name;
      header.append(kicker, title);
      details.append(header);
      appendList("Depend de", [...new Set(dependencies)].sort());
      appendList("Utilise par", [...new Set(dependents)].sort());
    }
    function setDetailsEmpty(message) {
      details.replaceChildren();
      const empty = document.createElement("div");
      empty.className = "details-empty";
      empty.textContent = message;
      details.append(empty);
    }
    function persistState() {
      const params = new URLSearchParams();
      if (pathStops.length) params.set("from", pathStops[0]);
      if (pathStops.length > 1) params.set("to", pathStops[pathStops.length - 1]);
      pathStops.slice(1, -1).forEach(id => params.append("via", id));
      if (pathLock.checked) params.set("lock", "1");
      if (!pathStops.length && selectedId) {
        params.set("selected", selectedId);
      }
      const fragment = params.toString();
      try {
        history.replaceState(null, "", fragment ? `#${fragment}` : location.pathname);
      } catch (_error) {
        location.hash = fragment;
      }
    }
    function clearPathControls() {
      pathQuery.value = "";
      pathStops.splice(0, pathStops.length);
    }
    function restResourceLabel(link, target) {
      const servicePrefix = `${target.name}: `;
      if (link.label === `${target.name}: API`) return "";
      return link.label.startsWith(servicePrefix) ? link.label.slice(servicePrefix.length) : link.label;
    }
    function contractsForPublishedRestResource(node, resource) {
      const contracts = node.openapi_contracts || [];
      const matchingContracts = contracts.filter(contract => (
        (contract.resources || []).includes(resource)
      ));
      return matchingContracts.length || contracts.length === 1
        ? (matchingContracts.length ? matchingContracts : contracts)
        : [];
    }
    function relationText(link) {
      const source = nodeDataById.get(link.source);
      const target = nodeDataById.get(link.target);
      if (link.kind === "rest") {
        const resource = restResourceLabel(link, target);
        return resource
          ? `HTTP · ${source.name} appelle ${target.name} (${resource})`
          : `HTTP · ${source.name} appelle ${target.name} (contrat non indexe)`;
      }
      if (link.kind === "mongodb") return `MongoDB · ${source.name} stocke dans ${target.name}`;
      if (link.kind === "request_reply") return `Kafka request/reply · ${source.name} → ${target.name}`;
      if (source.kind === "microservice") {
        const types = link.published_message_types || [];
        return `Kafka · ${source.name} publie${types.length ? ` <${types.join(", ")}>` : ""} sur ${target.name}`;
      }
      return `Kafka · ${target.name} consomme ${source.name}`;
    }
    function shortestPath(sourceId, targetId, matchesLink = () => true) {
      const outgoing = new Map();
      graphData.links.forEach((link, index) => {
        if (!matchesLink(link)) return;
        if (!outgoing.has(link.source)) outgoing.set(link.source, []);
        outgoing.get(link.source).push({ target: link.target, edge: `edge-${index}`, link });
      });
      const queue = [sourceId];
      const previous = new Map([[sourceId, null]]);
      for (let cursor = 0; cursor < queue.length; cursor += 1) {
        const current = queue[cursor];
        if (current === targetId) break;
        for (const step of outgoing.get(current) || []) {
          if (previous.has(step.target)) continue;
          previous.set(step.target, { node: current, edge: step.edge, link: step.link });
          queue.push(step.target);
        }
      }
      if (!previous.has(targetId)) return null;
      const nodes = [];
      const edges = [];
      for (let current = targetId; current !== null;) {
        nodes.unshift(current);
        const step = previous.get(current);
        if (step === null) break;
        edges.unshift(step);
        current = step.node;
      }
      return { nodes, edges };
    }
    function shortestPathThrough(stops) {
      const path = { nodes: [], edges: [] };
      for (let index = 0; index < stops.length - 1; index += 1) {
        const segment = shortestPath(stops[index], stops[index + 1], link => link.kind === "kafka");
        if (segment === null) return null;
        path.nodes.push(...(index === 0 ? segment.nodes : segment.nodes.slice(1)));
        path.edges.push(...segment.edges);
      }
      return path;
    }
    function allSimplePaths(sourceId, targetId, maxDepth = MAX_SIMPLE_PATH_DEPTH, maxPaths = MAX_SIMPLE_PATHS, maxExplorations = MAX_SIMPLE_PATH_EXPLORATIONS) {
      const outgoing = new Map();
      graphData.links.forEach((link, index) => {
        if (link.kind !== "kafka") return;
        if (!outgoing.has(link.source)) outgoing.set(link.source, []);
        outgoing.get(link.source).push({ target: link.target, edge: `edge-${index}`, link });
      });
      outgoing.forEach((steps, source) => {
        const firstStepByTarget = new Map();
        steps.sort((left, right) => (
          nodeDataById.get(left.target).name.localeCompare(nodeDataById.get(right.target).name)
        )).forEach(step => {
          if (!firstStepByTarget.has(step.target)) firstStepByTarget.set(step.target, step);
        });
        outgoing.set(source, [...firstStepByTarget.values()]);
      });
      const paths = [];
      const queue = [{ nodes: [sourceId], edges: [] }];
      let explorations = 0;
      for (let cursor = 0; cursor < queue.length && paths.length < maxPaths; cursor += 1) {
        const candidate = queue[cursor];
        if (candidate.edges.length >= maxDepth) continue;
        const current = candidate.nodes[candidate.nodes.length - 1];
        for (const step of outgoing.get(current) || []) {
          if (candidate.nodes.includes(step.target)) continue;
          explorations += 1;
          if (explorations > maxExplorations) return { paths, limited: true };
          const nextNodes = [...candidate.nodes, step.target];
          const nextEdges = [...candidate.edges, step];
          if (step.target === targetId) {
            paths.push({ nodes: nextNodes, edges: nextEdges });
            continue;
          }
          queue.push({ nodes: nextNodes, edges: nextEdges });
        }
      }
      return {
        paths: paths.sort((left, right) => left.nodes.length - right.nodes.length || (
          left.nodes.map(id => nodeDataById.get(id).name).join("\\u0000").localeCompare(
            right.nodes.map(id => nodeDataById.get(id).name).join("\\u0000")
          )
        )),
        limited: paths.length >= maxPaths,
      };
    }
    function resolveExactNodeName(name) {
      const candidates = nodesByNormalizedName.get(normalizeNodeName(name)) || [];
      if (!candidates.length) return { error: `Noeud introuvable : ${name}. Saisissez son nom exact.` };
      if (candidates.length > 1) return { error: `Nom ambigu : ${name}. Precisez un nom de noeud unique.` };
      return { id: candidates[0].id };
    }
    function parsePathQuery(query = pathQuery.value) {
      const names = query.split("->").map(name => name.trim());
      if (names.length < 2) return { error: "Saisissez au moins deux noeuds separes par ->." };
      if (names.some(name => !name)) return { error: "Chaque etape de l'itineraire doit avoir un nom : retirez le -> en trop ou renseignez le noeud manquant." };
      const stops = [];
      for (const name of names) {
        const resolved = resolveExactNodeName(name);
        if (resolved.error) return resolved;
        stops.push(resolved.id);
      }
      if (stops.some(id => !["microservice", "kafka_topic"].includes(nodeDataById.get(id).kind))) {
        return { error: "Un itineraire Kafka ne peut contenir que des microservices et des topics Kafka." };
      }
      if (nodeDataById.get(stops[0]).kind !== "microservice" || nodeDataById.get(stops.at(-1)).kind !== "microservice") {
        return { error: "Un itineraire Kafka doit commencer et se terminer par un microservice." };
      }
      if (new Set(stops).size !== stops.length) {
        return { error: "Un itineraire ne peut pas repeter le meme noeud." };
      }
      return { stops };
    }
    function renderPathQuery() {
      const query = pathStops.map(id => nodeDataById.get(id).name).join(" -> ");
      pathQuery.value = query;
      search.value = query;
      searchStatus.textContent = "";
    }
    function setPathMicroserviceOrder(path) {
      pathMicroserviceOrder = new Map();
      let order = 1;
      path.nodes.forEach(id => {
        if (nodeDataById.get(id).kind !== "microservice") return;
        pathMicroserviceOrder.set(id, order);
        order += 1;
      });
    }
    function renderPathDetails(path) {
      details.replaceChildren();
      const nodeKindLabel = node => {
        if (node.kind === "kafka_topic") return "Topic Kafka";
        if (node.kind === "mongodb_collection") return "Collection MongoDB";
        return node.external ? "Service externe" : "Microservice";
      };
      const pathNodeLabel = (id, index) => {
        const node = nodeDataById.get(id);
        const topicDtos = node.kind === "kafka_topic"
          ? (graphData.kafka_dtos || [])
            .filter(dto => (dto.topics || []).includes(node.name))
            .sort((left, right) => dtoLabel(left).localeCompare(dtoLabel(right)))
          : [];
        const dtoSuffix = topicDtos.length ? ` (${topicDtos.map(dto => dtoLabel(dto)).join(", ")})` : "";
        return `${index + 1}. ${node.name} : ${nodeKindLabel(node)}${dtoSuffix}`;
      };
      const header = document.createElement("header");
      header.className = "path-details-header";
      const kicker = document.createElement("p");
      kicker.className = "path-details-kicker";
      kicker.textContent = "Analyse de flux";
      const title = document.createElement("h1");
      title.className = "path-details-title";
      title.textContent = pathStops.length > 2 ? "Chemin avec noeuds intermediaires" : "Chemin le plus court";
      const summary = document.createElement("p");
      summary.className = "path-details-summary";
      const serviceCount = path.nodes.filter(id => nodeDataById.get(id).kind === "microservice").length;
      const topicCount = path.nodes.filter(id => nodeDataById.get(id).kind === "kafka_topic").length;
      summary.textContent = `${serviceCount} microservice${serviceCount > 1 ? "s" : ""} · ${topicCount} topic${topicCount > 1 ? "s" : ""} Kafka`;
      header.append(kicker, title, summary);
      details.append(header);
      const overview = document.createElement("section");
      overview.className = "details-section";
      const overviewTitle = document.createElement("h2");
      overviewTitle.textContent = "Parcours";
      const overviewList = document.createElement("ol");
      overviewList.className = "path-overview";
      path.nodes.forEach((id, index) => {
        const item = document.createElement("li");
        item.className = "path-overview-item";
        const node = nodeDataById.get(id);
        item.classList.add(node.kind === "kafka_topic" ? "is-topic" : node.kind === "mongodb_collection" ? "is-collection" : node.external ? "is-external" : "is-service");
        const stop = document.createElement("button");
        stop.type = "button";
        stop.className = "path-overview-stop";
        stop.textContent = pathNodeLabel(id, index);
        stop.title = `Afficher les details et les preuves de ${node.name}`;
        stop.addEventListener("click", () => selectNode(id, true));
        item.append(stop);
        overviewList.append(item);
      });
      overview.append(overviewTitle, overviewList);
      details.append(overview);
    }
    function showPath(path, stops = path.nodes) {
      pathStops.splice(0, pathStops.length, ...stops);
      renderPathQuery();
      selectedId = path.nodes[0];
      relatedNodes = new Set(path.nodes);
      relatedEdges = new Set(path.edges.map(step => step.edge));
      setPathMicroserviceOrder(path);
      rememberAnalyzedPath(pathStops);
      renderer.refresh();
      renderPathDetails(path);
      renderer.getCamera().animatedReset({ duration: 220 });
      persistState();
    }
    function renderSimplePathChoices(paths, limited) {
      details.replaceChildren();
      const section = document.createElement("section");
      section.className = "details-section simple-paths";
      const title = document.createElement("h2");
      title.textContent = "Chemins simples disponibles";
      const summary = document.createElement("p");
      summary.className = "simple-paths-summary";
      summary.textContent = `${paths.length} chemin${paths.length > 1 ? "s" : ""} propose${paths.length > 1 ? "s" : ""}, sans repeter de noeud, sur au plus ${MAX_SIMPLE_PATH_DEPTH} relations.${limited ? ` Recherche limitee a ${MAX_SIMPLE_PATHS} chemins et ${MAX_SIMPLE_PATH_EXPLORATIONS} explorations.` : ""}`;
      const list = document.createElement("ol");
      list.className = "simple-paths-list";
      paths.forEach((path, index) => {
        const item = document.createElement("li");
        const choice = document.createElement("button");
        choice.type = "button";
        choice.className = "simple-path-choice";
        choice.textContent = `${index + 1}. ${path.nodes.map(id => nodeDataById.get(id).name).join(" → ")}`;
        choice.addEventListener("click", () => showPath(path));
        item.append(choice);
        list.append(item);
      });
      section.append(title, summary, list);
      details.append(section);
    }
    function showShortestPath(query = pathQuery.value, preserveGraphOnError = false) {
      const parsed = parsePathQuery(query);
      if (parsed.error) {
        if (preserveGraphOnError) { searchStatus.textContent = parsed.error; return false; }
        selectedId = null; relatedNodes = null; relatedEdges = null; pathMicroserviceOrder = new Map();
        renderer.refresh();
        setDetailsEmpty(parsed.error);
        pathStops.splice(0, pathStops.length);
        persistState();
        return;
      }
      const stops = parsed.stops;
      const path = shortestPathThrough(stops);
      if (path === null) {
        const message = "Aucun itineraire Kafka oriente ne passe par les noeuds demandes dans cet ordre.";
        if (preserveGraphOnError) { searchStatus.textContent = message; return false; }
        selectedId = null; relatedNodes = null; relatedEdges = null; pathMicroserviceOrder = new Map();
        renderer.refresh();
        setDetailsEmpty(message);
        persistState();
        return false;
      }
      showPath(path, stops);
      return true;
    }
    function showSimplePaths() {
      const parsed = parsePathQuery();
      if (parsed.error) {
        selectedId = null; relatedNodes = null; relatedEdges = null; pathMicroserviceOrder = new Map();
        renderer.refresh();
        setDetailsEmpty(parsed.error);
        pathStops.splice(0, pathStops.length);
        persistState();
        return;
      }
      if (parsed.stops.length !== 2) {
        setDetailsEmpty("Les chemins simples se recherchent entre un microservice source et un microservice cible, sans noeud intermediaire impose.");
        return;
      }
      const simplePaths = allSimplePaths(parsed.stops[0], parsed.stops[1]);
      selectedId = null; relatedNodes = null; relatedEdges = null; pathMicroserviceOrder = new Map();
      pathStops.splice(0, pathStops.length);
      renderer.refresh();
      if (!simplePaths.paths.length) {
        setDetailsEmpty(`Aucun chemin simple oriente, de ${nodeDataById.get(parsed.stops[0]).name} vers ${nodeDataById.get(parsed.stops[1]).name}, dans les limites de recherche.`);
        persistState();
        return;
      }
      renderSimplePathChoices(simplePaths.paths, simplePaths.limited);
      persistState();
    }
    function appendServiceKafkaActivities(node, role, title, links, container) {
      if (!links.length) return;
      const section = document.createElement("section");
      section.className = "details-section";
      const heading = document.createElement("h2");
      heading.textContent = title;
      const list = document.createElement("ul");
      list.className = "service-kafka-list";
      const topicIds = [...new Set(links.map(link => role === "produce" ? link.target : link.source))];
      topicIds.sort((left, right) => nodeDataById.get(left).name.localeCompare(nodeDataById.get(right).name));
      topicIds.forEach(topicId => {
        const topic = nodeDataById.get(topicId);
        const item = document.createElement("li");
        item.className = "service-kafka-item";
        const topicButton = document.createElement("button");
        topicButton.type = "button";
        topicButton.className = "service-kafka-topic";
        topicButton.textContent = topic.name;
        topicButton.title = `Afficher le detail du topic ${topic.name}`;
        topicButton.addEventListener("click", () => selectNode(topicId));
        item.append(topicButton);
        const meta = document.createElement("div");
        meta.className = "service-kafka-meta";
        const dtos = (graphData.kafka_dtos || []).filter(dto => {
          const matchesRole = (
            (role === "produce" && (dto.producers || []).includes(node.name))
            || (role === "consume" && (dto.consumers || []).includes(node.name))
          );
          return (dto.topics || []).includes(topic.name) && matchesRole;
        }).sort((left, right) => dtoLabel(left).localeCompare(dtoLabel(right)));
        if (dtos.length) {
          dtos.forEach(dto => {
            const dtoButton = document.createElement("button");
            dtoButton.type = "button";
            dtoButton.textContent = `DTO · ${dtoLabel(dto)}`;
            dtoButton.title = `Afficher la structure de ${dtoLabel(dto)}`;
            dtoButton.addEventListener("click", () => openDtoInspector(dto.id));
            meta.append(dtoButton);
          });
        } else {
          const unknown = document.createElement("span");
          unknown.textContent = "DTO non indexe";
          meta.append(unknown);
        }
        item.append(meta);
        list.append(item);
      });
      section.append(heading, list);
      container.append(section);
    }
    function renderDetails(id) {
      const node = nodeDataById.get(id);
      const indexedEdges = graphData.links.filter(link => link.source === id || link.target === id);
      const edges = indexedEdges.filter(
        link => isVisibleRelation(link.kind) && (link.source === id || link.target === id)
      );
      const isMicroservice = node.kind === "microservice";
      const publishedApiCount = isMicroservice ? (node.resources || []).length : 0;
      const publishedTopicCount = isMicroservice ? new Set(
        indexedEdges.filter(link => link.kind === "kafka" && link.source === id).map(link => link.target)
      ).size : 0;
      const collectionCount = isMicroservice ? new Set(
        indexedEdges.filter(link => link.kind === "mongodb" && link.source === id).map(link => link.target)
      ).size : 0;
      details.replaceChildren();
      const kindLabel = node.kind === "kafka_topic" ? "Topic Kafka" : node.kind === "mongodb_collection" ? "Collection MongoDB" : "Microservice";
      const complexity = node.complexity;
      const header = document.createElement("header");
      header.className = "details-header";
      if (complexity) header.classList.add(`is-${complexity.level}`);
      const kicker = document.createElement("p");
      kicker.className = "details-kicker";
      kicker.textContent = kindLabel;
      const title = document.createElement("h1");
      title.className = "details-title";
      title.textContent = node.name;
      const meta = document.createElement("div");
      meta.className = "details-meta";
      const relationBadge = document.createElement("span");
      relationBadge.className = "detail-badge";
      relationBadge.textContent = `Relations indexees : ${indexedEdges.length}`;
      const visibleBadge = document.createElement("span");
      visibleBadge.className = "detail-badge";
      visibleBadge.textContent = `Affichees : ${edges.length}`;
      meta.append(relationBadge, visibleBadge);
      if (isMicroservice) {
        [
          `${publishedApiCount} API${publishedApiCount > 1 ? "s" : ""} exposee${publishedApiCount > 1 ? "s" : ""}`,
          `${publishedTopicCount} topic${publishedTopicCount > 1 ? "s" : ""} publie${publishedTopicCount > 1 ? "s" : ""}`,
          `${collectionCount} collection${collectionCount > 1 ? "s" : ""} utilisee${collectionCount > 1 ? "s" : ""}`,
        ].forEach(label => { const badge = document.createElement("span"); badge.className = "detail-badge"; badge.textContent = label; meta.append(badge); });
      }
      const confidenceLabels = { proved: "prouvee", inferred: "inferee", conventional: "conventionnelle" };
      ["proved", "inferred", "conventional"].forEach(confidence => {
        const count = edges.filter(link => link.confidence === confidence).length;
        if (!count) return;
        const badge = document.createElement("span");
        badge.className = "detail-badge";
        badge.textContent = `${count} ${confidenceLabels[confidence]}`;
        badge.title = `Relation ${confidenceLabels[confidence]} : ${[...new Set(edges.filter(link => link.confidence === confidence).map(link => link.provenance))].join(", ")}`;
        meta.append(badge);
      });
      if (complexity) {
        const scoreBadge = document.createElement("span");
        scoreBadge.className = `detail-badge complexity ${complexity.level}`;
        const connectivityLabels = { low: "basse", medium: "médiane", high: "élevée" };
        scoreBadge.textContent = `Connectivité relative : ${connectivityLabels[complexity.level]} (${complexity.score})`;
        const breakdown = complexity.breakdown || {};
        scoreBadge.title = `HTTP : ${breakdown.http || 0} · Kafka : ${breakdown.kafka || 0} · MongoDB : ${breakdown.mongodb || 0} · Rang relatif ${complexity.rank}/${complexity.population} · Tiers : ${complexity.tier_start}-${complexity.tier_end}`;
        meta.append(scoreBadge);
      }
      header.append(kicker, title, meta);
      details.append(header);
      if (node.kind === "microservice") {
        const httpCalls = edges.filter(link => link.kind === "rest" && link.source === id);
        const kafkaPublications = edges.filter(link => link.kind === "kafka" && link.source === id);
        const kafkaConsumptions = edges.filter(link => link.kind === "kafka" && link.target === id);
        const mongoCollections = edges.filter(link => link.kind === "mongodb" && link.source === id);
        const openApiContracts = node.openapi_contracts || [];
        appendFindings(node.findings || []);
        const publishedApis = [
          ...openApiContracts.map(contract => ({
            label: `${contract.spec ? "Contrat OpenAPI" : "Contrat OpenAPI indisponible"} · ${contract.path}`,
            title: `Ouvrir le contrat OpenAPI ${contract.path}`,
            action: () => openOpenApiContract(contract),
          })),
          ...node.resources
            .filter(resource => !contractsForPublishedRestResource(node, resource).length)
            .map(resource => ({
              label: `REST · ${resource}`,
              title: "Mettre en evidence les consommateurs de cette API REST",
              action: () => focusPublishedRestResource(id, resource),
            })),
        ];
        const relationsGroup = createDetailsGroup("Relations");
        appendRelationList("APIs consommees", httpCalls, id, link => (
          `API de ${nodeDataById.get(link.target).name}`
        ), relationsGroup);
        appendActionList("APIs publiees", publishedApis, relationsGroup);
        appendServiceKafkaActivities(node, "consume", "Topics consommes", kafkaConsumptions, relationsGroup);
        appendServiceKafkaActivities(node, "produce", "Topics publies", kafkaPublications, relationsGroup);
        appendRelationList("Collections MongoDB", mongoCollections, id, link => (
          nodeDataById.get(link.target).name
        ), relationsGroup);
        discardEmptyDetailsGroup(relationsGroup);
        const sourceEntries = [
          ...openApiContracts.map(contract => ({
            label: `OpenAPI · ${contract.path}`,
            title: `Ouvrir ${contract.path} dans VS Code`,
            action: () => { if (contract.vscode_uri) window.location.href = contract.vscode_uri; },
          })),
          ...(node.kafka_endpoints || []).map(endpoint => ({
            label: `Kafka · ${endpoint.location}`,
            title: `Ouvrir ${endpoint.location} dans VS Code`,
            action: () => { if (endpoint.vscode_uri) window.location.href = endpoint.vscode_uri; },
          })),
        ];
        const sourcesGroup = createDetailsGroup("Sources", false);
        appendActionList("Fichiers de preuve", sourceEntries, sourcesGroup);
        discardEmptyDetailsGroup(sourcesGroup);
      }
      if (node.kind === "kafka_topic") {
        const relationsGroup = createDetailsGroup("Relations");
        appendRelationList("Services producteurs", edges.filter(link => link.kind === "kafka" && link.target === id), id,
          link => nodeDataById.get(link.source).name, relationsGroup);
        appendRelationList("Services consommateurs", edges.filter(link => link.kind === "kafka" && link.source === id), id,
          link => nodeDataById.get(link.source).name, relationsGroup);
        appendRelationList("Pattern request/reply", edges.filter(link => link.kind === "request_reply" && (link.source === id || link.target === id)), id,
          link => nodeDataById.get(link.source === id ? link.target : link.source).name, relationsGroup);
        const dtos = (graphData.kafka_dtos || [])
          .filter(dto => (dto.topics || []).includes(node.name))
          .sort((left, right) => dtoLabel(left).localeCompare(dtoLabel(right)));
        appendActionList("DTO Kafka", dtos.map(dto => ({
          label: dtoLabel(dto),
          title: "Afficher les champs et les relations Kafka de ce DTO",
          action: () => openDtoInspector(dto.id),
        })), relationsGroup);
        const indexedDtoTypes = new Set(dtos.flatMap(dto => [dto.id, dto.name, dto.qualified_name].filter(Boolean)));
        const unresolvedTypes = [...new Set([
          ...(node.published_message_types || []),
          ...(node.consumed_message_types || []),
        ])].filter(type => !indexedDtoTypes.has(type) && !indexedDtoTypes.has(type.split(".").at(-1)));
        appendList("Types de message non resolus", unresolvedTypes, relationsGroup);
        const endpointSources = graphData.nodes
          .filter(candidate => candidate.kind === "microservice")
          .flatMap(candidate => (candidate.kafka_endpoints || []).map(endpoint => ({ service: candidate.name, ...endpoint })))
          .filter(endpoint => endpoint.topic === node.name);
        appendActionList("Sources producteurs et consommateurs", endpointSources.map(endpoint => ({
          label: `${endpoint.service} · ${endpoint.role === "produce" ? "publication" : "consommation"} · ${endpoint.location}`,
          title: `Ouvrir ${endpoint.location} dans VS Code`,
          action: () => { if (endpoint.vscode_uri) window.location.href = endpoint.vscode_uri; },
        })), relationsGroup);
        discardEmptyDetailsGroup(relationsGroup);
      }
      if (node.kind === "mongodb_collection") {
        const relationsGroup = createDetailsGroup("Relations");
        appendList("Stockee par", [node.owner], relationsGroup);
        appendRelationList("Services utilisant cette collection", edges.filter(link => link.kind === "mongodb" && link.target === id), id,
          link => nodeDataById.get(link.source).name, relationsGroup);
        discardEmptyDetailsGroup(relationsGroup);
      }
    }
    function focusNodeRelations(id, matches) {
      if (!pathLock.checked) clearPathControls();
      pathMicroserviceOrder = new Map();
      selectedId = id;
      relatedNodes = new Set([id]);
      relatedEdges = new Set();
      network.forEachEdge((edge, attributes, source, target) => {
        if (!isVisibleRelation(attributes.kind) || !matches(attributes, source, target)) return;
        relatedEdges.add(edge); relatedNodes.add(source); relatedNodes.add(target);
      });
      renderer.refresh();
      renderDetails(id);
      const position = renderer.getNodeDisplayData(id);
      if (position) renderer.getCamera().animate({ x: position.x, y: position.y, ratio: .55 }, { duration: 260 });
      persistState();
    }
    function focusPublishedRestResource(id, resource) {
      const target = nodeDataById.get(id);
      focusNodeRelations(id, (link, _source, targetId) => (
        link.kind === "rest" && targetId === id && restResourceLabel(link, target) === resource
      ));
    }
    function selectNode(id, preservePath = false) {
      if (!preservePath && !pathLock.checked) clearPathControls();
      pathMicroserviceOrder = new Map();
      selectedId = id;
      relatedNodes = new Set([id]);
      relatedEdges = new Set();
      network.forEachEdge((edge, attributes, source, target) => {
        if (!isVisibleRelation(attributes.kind)) return;
        if (source === id || target === id) {
          relatedEdges.add(edge); relatedNodes.add(source); relatedNodes.add(target);
        }
      });
      renderer.refresh();
      renderDetails(id);
      const position = renderer.getNodeDisplayData(id);
      if (position) renderer.getCamera().animate({ x: position.x, y: position.y, ratio: .55 }, { duration: 260 });
      persistState();
    }
    function reset() {
      selectedId = null; relatedNodes = null; relatedEdges = null; pathMicroserviceOrder = new Map();
      if (document.querySelector('.filter-preset[data-preset="selection"]')?.classList.contains("is-active")) {
        setActiveRelationPreset("all");
      }
      renderer.refresh();
      setDetailsEmpty("Selectionnez un noeud pour isoler ses relations et afficher ses APIs.");
      search.value = "";
      clearPathControls();
      persistState();
    }
    function restoreState() {
      const params = new URLSearchParams(location.hash.slice(1));
      const sourceId = params.get("from");
      const targetId = params.get("to");
      pathLock.checked = params.get("lock") === "1";
      const restoredStops = [sourceId, ...params.getAll("via"), targetId];
      if (
        sourceId
        && targetId
        && isValidPathStops(restoredStops)
      ) {
        pathStops.push(...restoredStops);
        renderPathQuery();
        showShortestPath();
        return;
      }
      const selectedIdFromUrl = params.get("selected");
      if (selectedIdFromUrl && nodeDataById.has(selectedIdFromUrl)) selectNode(selectedIdFromUrl);
    }
    function activeRenderer() {
      return dependencyCanvas.hidden ? renderer : ensureDependencyRenderer();
    }
    document.getElementById("zoom-in").addEventListener("click", () => activeRenderer().getCamera().animatedZoom({ duration: 180 }));
    document.getElementById("zoom-out").addEventListener("click", () => activeRenderer().getCamera().animatedUnzoom({ duration: 180 }));
    document.getElementById("fit-view").addEventListener("click", () => activeRenderer().getCamera().animatedReset({ duration: 220 }));
    document.getElementById("reset").addEventListener("click", reset);
    document.getElementById("inspector-close").addEventListener("click", closeInspector);
    inspectorModal.addEventListener("click", event => { if (event.target === inspectorModal) closeInspector(); });
    window.addEventListener("keydown", event => { if (event.key === "Escape" && !inspectorModal.hidden) closeInspector(); });
    document.getElementById("show-path").addEventListener("click", showShortestPath);
    document.getElementById("show-simple-paths").addEventListener("click", showSimplePaths);
    document.getElementById("question-topic").addEventListener("click", () => {
      setToolbarTab("graph");
      applyRelationPreset("kafka");
      search.placeholder = "orders.created ou orders -> orders.created";
      search.focus();
    });
    document.getElementById("question-service").addEventListener("click", () => {
      setToolbarTab("graph");
      applyRelationPreset("all");
      search.placeholder = "orders ou orders -> payments";
      search.focus();
    });
    document.getElementById("question-path").addEventListener("click", () => {
      setToolbarTab("graph");
      search.focus();
    });
    document.getElementById("question-messages").addEventListener("click", () => {
      setToolbarTab("resources");
      dtoReferencesFilter.focus();
    });
    layoutButtons.forEach((button, layout) => button.addEventListener("click", () => applyLayout(layout)));
    graphTab.addEventListener("click", () => setToolbarTab("graph"));
    resourcesTab.addEventListener("click", () => setToolbarTab("resources"));
    issuesTab.addEventListener("click", () => setToolbarTab("issues"));
    pathsTab.addEventListener("click", () => setToolbarTab("paths"));
    inventoryStatus.addEventListener("click", () => setToolbarTab("issues"));
    document.getElementById("show-request-reply").addEventListener("click", () => setToolbarTab("request-reply"));
    document.getElementById("show-dependencies").addEventListener("click", () => setToolbarTab("dependencies"));
    filterPresetButtons.forEach(button => button.addEventListener("click", () => applyRelationPreset(button.dataset.preset)));
    [
      relationHttp,
      relationKafka,
      relationMongodb,
      nodeMicroservice,
      nodeExternalMicroservice,
      nodeKafkaTopic,
      nodeMongodbCollection,
    ].forEach(control => control.addEventListener("change", () => {
      setActiveRelationPreset(null);
      if ([relationHttp, relationKafka, relationMongodb].includes(control)) rebuildGraph();
      reset();
      dependencyRenderer?.refresh();
    }));
    openApiReferencesFilter.addEventListener("input", renderReferences);
    dtoReferencesFilter.addEventListener("input", renderReferences);
    pathLock.addEventListener("change", persistState);
    pathQuery.addEventListener("keydown", event => {
      if (event.key === "Enter") showShortestPath();
    });
    renderIndexingIssues();
    renderAnalyzedPaths();
    renderReferences();
    renderRequestReplyPatterns();
    restoreState();
    applyLayout("forceatlas2-noverlap");
    function runExploreSearch() {
      const query = search.value.trim();
      searchStatus.textContent = "";
      if (!query) { reset(); return; }
      if (query.includes("->")) {
        showShortestPath(query, true);
        return;
      }
      const resolved = resolveExactNodeName(query);
      if (resolved.error) { searchStatus.textContent = resolved.error; return; }
      selectNode(resolved.id);
    }
    search.addEventListener("input", () => {
      const query = search.value.trim();
      searchStatus.textContent = "";
      if (!query) { reset(); return; }
      if (query.includes("->")) return;
      const resolved = resolveExactNodeName(query);
      if (resolved.id) selectNode(resolved.id);
    });
    search.addEventListener("keydown", event => {
      if (event.key === "Enter") { event.preventDefault(); runExploreSearch(); }
    });
    window.addEventListener("resize", () => {
      renderer.refresh();
      dependencyRenderer?.refresh();
    });
  </script>
</body>
</html>
"""


_SIGMA_MODULE_GRAPH_HTML_TEMPLATE = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SystemLens module dependencies</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/graphology/0.25.4/graphology.umd.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/sigma.js/2.4.0/sigma.min.js"></script>
  <style>
    :root { color: #172033; background: #f5f7fb; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
    * { box-sizing: border-box; }
    body { margin: 0; overflow: hidden; }
    #graph { width: 100vw; height: 100vh; background: #f8fafc; touch-action: none; }
    .toolbar { position: fixed; z-index: 2; top: 16px; left: 16px; display: flex; align-items: center; gap: 8px; padding: 8px; border: 1px solid #d7dee9; border-radius: 6px; background: rgba(255, 255, 255, .95); box-shadow: 0 2px 12px rgba(15, 23, 42, .10); }
    .toolbar strong { padding: 0 6px; font-size: 14px; white-space: nowrap; }
    .toolbar input { width: 220px; height: 32px; padding: 0 9px; border: 1px solid #b9c5d6; border-radius: 4px; color: #172033; background: #fff; font: inherit; font-size: 13px; }
    .toolbar button { width: 32px; height: 32px; border: 1px solid #b9c5d6; border-radius: 4px; color: #315f9b; background: #fff; font-size: 19px; line-height: 1; cursor: pointer; }
    .toolbar button:hover { background: #eaf2ff; }
    #details { position: fixed; z-index: 2; right: 16px; bottom: 16px; width: min(340px, calc(100vw - 32px)); max-height: min(56vh, 440px); overflow: auto; padding: 10px 12px; border: 1px solid #d7dee9; border-radius: 6px; background: rgba(255, 255, 255, .95); color: #475569; font-size: 13px; line-height: 1.4; box-shadow: 0 2px 12px rgba(15, 23, 42, .10); }
    #details strong { display: block; color: #172033; font-size: 14px; }
    #details h2 { margin: 10px 0 4px; color: #59708d; font-size: 11px; font-weight: 700; text-transform: uppercase; }
    #details ul { margin: 0; padding-left: 18px; }
  </style>
</head>
<body>
  <div class="toolbar">
    <strong>Modules</strong>
    <input id="search" type="search" placeholder="Rechercher un module" autocomplete="off" aria-label="Rechercher un module">
    <button id="zoom-out" type="button" aria-label="Dezoomer" title="Dezoomer">-</button>
    <button id="zoom-in" type="button" aria-label="Zoomer" title="Zoomer">+</button>
    <button id="fit-view" type="button" aria-label="Ajuster a l'ecran" title="Ajuster a l'ecran">o</button>
    <button id="reset" type="button" aria-label="Reinitialiser la selection" title="Reinitialiser">x</button>
  </div>
  <div id="details">Selectionnez un module pour explorer ses dependances directes.</div>
  <div id="graph" aria-label="Graphe des dependances de modules"></div>
  <script id="module-graph-data" type="application/json">__MODULE_GRAPH_DATA__</script>
  <script>
    const graphData = JSON.parse(document.getElementById("module-graph-data").textContent);
    const nodeById = new Map(graphData.nodes.map(node => [node.id, node]));
    const network = new graphology.MultiDirectedGraph();
    graphData.nodes.forEach(node => network.addNode(node.id, {
      label: node.name, x: node.x, y: node.y, size: node.kind === "microservice" ? 13 : 10,
      color: node.kind === "microservice" ? "#4f79b5" : "#718096",
    }));
    graphData.links.forEach((link, index) => network.addEdgeWithKey(`dependency-${index}`, link.source, link.target, {
      size: 1.4, color: "#52616b",
    }));
    let selectedId = null;
    let relatedNodes = null;
    let relatedEdges = null;
    const renderer = new Sigma(network, document.getElementById("graph"), {
      labelDensity: .1, labelGridCellSize: 120, labelRenderedSizeThreshold: 7,
      nodeReducer: (node, data) => !selectedId || relatedNodes.has(node)
        ? data : { ...data, color: "#d8e0ea", label: "" },
      edgeReducer: (edge, data) => !selectedId || relatedEdges.has(edge)
        ? data : { ...data, color: "#e5eaf0", size: .35 },
    });
    const details = document.getElementById("details");
    const search = document.getElementById("search");
    function appendList(title, values) {
      if (!values.length) return;
      const heading = document.createElement("h2"); heading.textContent = title;
      const list = document.createElement("ul");
      values.forEach(value => { const item = document.createElement("li"); item.textContent = value; list.append(item); });
      details.append(heading, list);
    }
    function selectModule(id) {
      selectedId = id; relatedNodes = new Set([id]); relatedEdges = new Set();
      const dependencies = [];
      const dependents = [];
      network.forEachEdge((edge, attributes, source, target) => {
        if (source === id || target === id) {
          relatedEdges.add(edge); relatedNodes.add(source); relatedNodes.add(target);
          if (source === id) dependencies.push(nodeById.get(target).name);
          else dependents.push(nodeById.get(source).name);
        }
      });
      renderer.refresh();
      const node = nodeById.get(id);
      details.replaceChildren();
      const title = document.createElement("strong"); title.textContent = node.name;
      details.append(title, document.createTextNode(`${node.kind} - ${relatedEdges.size} dependance${relatedEdges.size > 1 ? "s" : ""}`));
      appendList("APIs exposees", node.httpApisExposed);
      appendList("Topics publies", node.kafkaTopicsPublished);
      appendList("Topics consommes", node.kafkaTopicsConsumed);
      appendList("Depend de", dependencies);
      appendList("Utilise par", dependents);
      const position = renderer.getNodeDisplayData(id);
      if (position) renderer.getCamera().animate({ x: position.x, y: position.y, ratio: .55 }, { duration: 260 });
    }
    function reset() {
      selectedId = null; relatedNodes = null; relatedEdges = null; renderer.refresh();
      details.textContent = "Selectionnez un module pour explorer ses dependances directes.";
      search.value = "";
    }
    renderer.on("clickNode", ({ node }) => selectModule(node));
    renderer.on("clickStage", reset);
    document.getElementById("zoom-in").addEventListener("click", () => renderer.getCamera().animatedZoom({ duration: 180 }));
    document.getElementById("zoom-out").addEventListener("click", () => renderer.getCamera().animatedUnzoom({ duration: 180 }));
    document.getElementById("fit-view").addEventListener("click", () => renderer.getCamera().animatedReset({ duration: 220 }));
    document.getElementById("reset").addEventListener("click", reset);
    search.addEventListener("input", event => {
      const query = event.target.value.trim().toLocaleLowerCase();
      const node = graphData.nodes.find(item => item.name.toLocaleLowerCase().includes(query));
      if (node) selectModule(node.id); else if (!query) reset();
    });
    window.addEventListener("resize", () => renderer.refresh());
  </script>
</body>
</html>
"""


def _rest_resources_served(endpoints: list[MessageEndpoint]) -> list[str]:
    return sorted(
        {
            endpoint.topic
            for endpoint in endpoints
            if endpoint.system == "rest" and endpoint.role == "serve"
        }
    )


def _endpoint_vscode_uri(
    endpoint: MessageEndpoint,
    modules: list[DiscoveredModule],
    source_roots: list[Path] | None,
    wsl_distro: str | None = None,
) -> str:
    """Build a deep link to an endpoint source location when reporting an issue."""
    module_roots = [module.path for module in modules if module.name == endpoint.module]
    roots = module_roots + list(source_roots or []) + [module.path for module in modules]
    for root in dict.fromkeys(roots):
        candidate = (root / endpoint.path).resolve()
        if candidate.is_file():
            if wsl_distro:
                return f"vscode://file//wsl.localhost/{quote(wsl_distro, safe='')}{quote(candidate.as_posix(), safe='/')}:{endpoint.start_line}"
            return f"vscode://file/{quote(candidate.as_posix(), safe='/')}:{endpoint.start_line}"
    root = roots[0] if roots else Path.cwd()
    candidate = (root / endpoint.path).resolve()
    if wsl_distro:
        return f"vscode://file//wsl.localhost/{quote(wsl_distro, safe='')}{quote(candidate.as_posix(), safe='/')}:{endpoint.start_line}"
    return f"vscode://file/{quote(candidate.as_posix(), safe='/')}:{endpoint.start_line}"


def _vscode_uri(finding: Finding, module: DiscoveredModule | None, source_roots: list[Path] | None, wsl_distro: str | None = None) -> str:
    """Build a VS Code deep link for an evidenced finding location."""
    candidates = ([module.path] if module is not None else []) + list(source_roots or [])
    for root in candidates:
        candidate = (root / finding.path).resolve()
        if candidate.is_file():
            if wsl_distro:
                return f"vscode://file//wsl.localhost/{quote(wsl_distro, safe='')}{quote(candidate.as_posix(), safe='/')}:{finding.start_line}"
            return f"vscode://file/{quote(candidate.as_posix(), safe='/')}:{finding.start_line}"
    root = candidates[0] if candidates else Path.cwd()
    candidate = (root / finding.path).resolve()
    if wsl_distro:
        return f"vscode://file//wsl.localhost/{quote(wsl_distro, safe='')}{quote(candidate.as_posix(), safe='/')}:{finding.start_line}"
    return f"vscode://file/{quote(candidate.as_posix(), safe='/')}:{finding.start_line}"


def _vscode_file_uri(path: Path, wsl_distro: str | None = None) -> str:
    resolved = path.resolve()
    if wsl_distro:
        return f"vscode://file//wsl.localhost/{quote(wsl_distro, safe='')}{quote(resolved.as_posix(), safe='/')}"
    return f"vscode://file/{quote(resolved.as_posix(), safe='/')}"


def _openapi_contract_evidence_path(endpoint: MessageEndpoint) -> str:
    """Return the physical contract path carried by a Strategy1 declaration."""
    for line in endpoint.snippet.splitlines():
        if line.startswith("systemlens-openapi-contract:"):
            return line.removeprefix("systemlens-openapi-contract:")
    return endpoint.path


def _mongodb_collection_nodes(
    collections_by_service: dict[str, list[str]] | None,
) -> list[tuple[str, str, str]]:
    """Returns a distinct graph identity for each service/collection pair.

    Collection names alone are not globally unique: two microservices can both
    use `orders` in independent Mongo databases. Keeping the service in the
    node identity prevents the visual graph from inventing a shared store.
    """
    return [
        (service, collection, f"{service}:{collection}")
        for service in sorted(collections_by_service or {})
        for collection in sorted(set((collections_by_service or {})[service]))
        if collection
    ]


def _mongodb_visual_graph_edges(
    collections_by_service: dict[str, list[str]] | None,
) -> list[tuple[str, str, str, str, str, str]]:
    return [
        ("microservice", service, "mongodb_collection", identity, "stocke", "mongodb")
        for service, _collection, identity in _mongodb_collection_nodes(collections_by_service)
    ]


def _visual_graph_edges(
    edges: list[GraphEdge],
) -> list[tuple[str, str, str, str, str, str]]:
    """Projette les `GraphEdge` vers les arêtes réellement dessinées, en
    supprimant les doublons ayant la même source, destination et label.

    Retourne `(source_kind, source, target_kind, target, label, kind)`, où les
    types de nœuds évitent toute ambiguïté quand un service porte le même nom
    qu'un topic Kafka."""
    projected: dict[tuple[str, str, str, str, str], str] = {}
    order: list[tuple[str, str, str, str, str]] = []
    for edge in edges:
        visual_edges: list[tuple[str, str, str, str, str]] = []
        if edge.kind == "rest":
            label = graph_edge_rest_resource(edge)
            if edge.from_endpoint.framework == "spring-cloud-gateway":
                match = re.search(r"Path=([^;]+)", edge.from_endpoint.snippet)
                if match is not None:
                    label = f"ANY {match.group(1)}"
            visual_edges.append(
                ("microservice", edge.from_service, "microservice", edge.to_service, label)
            )
        else:
            topic = edge.from_endpoint.topic
            visual_edges.append(("microservice", edge.from_service, "kafka_topic", topic, topic))
            visual_edges.append(("kafka_topic", topic, "microservice", edge.to_service, topic))

        for source_kind, source_name, target_kind, target_name, label in visual_edges:
            key = (source_kind, source_name, target_kind, target_name, label)
            if key not in projected:
                projected[key] = edge.kind
                order.append(key)

    return [
        (*key, projected[key])
        for key in order
    ]


def _visual_link_evidence(
    kind: str, source_kind: str, source_name: str, target_kind: str, target_name: str,
    edges: list[GraphEdge],
) -> tuple[str, str]:
    """Return the confidence level and origin displayed by the HTML explorer."""
    candidates = [
        edge
        for edge in edges
        if edge.kind == kind
        and (
            (kind == "rest" and edge.from_service == source_name and edge.to_service == target_name)
            or (kind == "kafka" and source_kind == "microservice" and edge.from_service == source_name and edge.from_endpoint.topic == target_name)
            or (kind == "kafka" and target_kind == "microservice" and edge.to_service == target_name and edge.from_endpoint.topic == source_name)
        )
    ]
    if not candidates:
        return "inferred", "inference"
    endpoints = [endpoint for edge in candidates for endpoint in (edge.from_endpoint, edge.to_endpoint) if endpoint]
    if any(endpoint.framework == "kafka-topic-strategy1" for endpoint in endpoints):
        return "conventional", "Strategy1"
    if any(endpoint.framework == "openapi" for endpoint in endpoints):
        return "inferred", "OpenAPI"
    if any(endpoint.topic_dynamic for endpoint in endpoints):
        return "inferred", "code (dynamic)"
    if any(endpoint.source == "manifest" for endpoint in endpoints):
        return "proved", "manifest"
    return "proved", "code"


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
    return _SIGMA_MODULE_GRAPH_HTML_TEMPLATE.replace("__MODULE_GRAPH_DATA__", graph_data)


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
