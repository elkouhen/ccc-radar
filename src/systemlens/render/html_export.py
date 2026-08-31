"""Standalone interactive HTML export (Sigma.js) of the microservice graph.

The heavy JS/CSS payload lives in ``assets/graph.html``; this module only
builds the JSON data model injected into that static template.
"""

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from systemlens import java_parser
from systemlens.graph import (
    GraphEdge,
    external_microservice_names,
    resolve_rest_target_service,
)
from systemlens.models import ExtractionDiagnostic, Finding, GraphFact, MessageEndpoint
from systemlens.modules import (
    DiscoveredModule,
    ModuleDependency,
    MongoPersistenceClass,
    module_identity,
)
from systemlens.render.namespaces import project_namespace
from systemlens.render.software_layers import software_layer
from systemlens.render._graph_view_helpers import (
    _endpoint_vscode_uri,
    _mongodb_collection_nodes,
    _mongodb_visual_graph_edges,
    _openapi_contract_evidence_path,
    _resolve_openapi_contract_owner,
    _rest_resources_served,
    _visual_graph_edges,
    _visual_link_evidence,
    _vscode_file_uri,
    _vscode_uri,
)
from systemlens.render.likec4_export import _complexity_ranking
from systemlens.render_snapshot import kafka_dto_views

_GRAPH_HTML_TEMPLATE = (Path(__file__).parent / "assets" / "graph.html").read_text(encoding="utf-8")


def _indexing_issues(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
    edges: list[GraphEdge],
    warnings: list[str] | None,
    modules: list[DiscoveredModule],
    source_roots: list[Path] | None,
    root_path: Path | None,
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
                endpoint, modules, source_roots, root_path
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
    module: str


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
    root_path: Path | None = None,
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
                    module=module_identity(module),
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
            "module": None,
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
            definition["module"] = candidate.module
            definition["vscode_uri"] = _vscode_file_uri(candidate.source_file, root_path)
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
    root_path: Path | None = None,
    request_reply_strategy1: bool = False,
    diagnostics: list[ExtractionDiagnostic] | None = None,
    kafka_dto_definitions: list[dict[str, object]] | None = None,
    openapi_contracts: list[dict[str, object]] | None = None,
    graph_facts: list[GraphFact] | None = None,
    strategy1: bool = False,
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
    module_by_identity = {module_identity(module): module for module in all_modules}
    fact_namespaces_by_service: dict[str, set[str]] = {}
    for fact in graph_facts or []:
        fact_names = {value for value in (fact.name, fact.source_name, fact.target_name) if value}
        for service in ordered_services:
            if service in fact_names:
                fact_namespaces_by_service.setdefault(service, set()).add(fact.namespace)
    openapi_specs = {
        (str(contract["module"]), str(contract["path"])): contract["spec"]
        for contract in openapi_contracts or []
    }

    def module_owns_openapi_file(module: DiscoveredModule, path: str) -> bool:
        """Whether ``module`` directly encloses an OpenAPI source file.

        The inventory normally applies this ownership rule. Reapply it while
        building the export so an older snapshot or federated inventory cannot
        render one physical contract for both an enclosing project and a
        nested module that merely references it.
        """
        contract_path = (module.path / path).resolve()
        enclosing_modules = [
            candidate.path.resolve()
            for candidate in all_modules
            if candidate.path.resolve() == contract_path
            or candidate.path.resolve() in contract_path.parents
        ]
        owner_path = max(enclosing_modules, key=lambda candidate: len(candidate.parts), default=None)
        return owner_path is None or owner_path == module.path.resolve()
    dependencies_by_source: dict[str, set[str]] = {}
    for dependency in module_dependencies or []:
        dependencies_by_source.setdefault(dependency.source, set()).add(dependency.target)

    def reachable_modules(service: str) -> set[str]:
        reachable = {service}
        pending = [service]
        while pending:
            source = pending.pop()
            for target in dependencies_by_source.get(source, set()):
                if target not in reachable:
                    reachable.add(target)
                    pending.append(target)
        return reachable

    persistence_candidates_by_collection: dict[
        str, list[tuple[str, DiscoveredModule, MongoPersistenceClass]]
    ] = {}
    for identity, candidate_module in module_by_identity.items():
        for item in candidate_module.mongo_persistence_classes:
            persistence_candidates_by_collection.setdefault(item.collection, []).append(
                (identity, candidate_module, item)
            )
    mongo_persistence_classes: list[dict[str, object]] = []
    for service, collections in sorted((collections_by_service or {}).items()):
        reachable = reachable_modules(service)
        for collection in sorted(set(collections)):
            candidates = persistence_candidates_by_collection.get(collection, [])
            scoped = [candidate for candidate in candidates if candidate[0] in reachable]
            # A unique workspace-wide candidate is safe when dependency metadata
            # is absent (common in small Gradle builds and federated snapshots).
            selected = scoped or (candidates if len(candidates) == 1 else [])
            for identity, candidate_module, item in selected:
                mongo_persistence_classes.append({
                    "id": f"{service}:{identity}:{item.qualified_name}",
                    "service": service,
                    "module": identity,
                    "collection": item.collection,
                    "name": item.name,
                    "qualified_name": item.qualified_name,
                    "source": item.path,
                    "line": item.line,
                    "root": item.root,
                    "vscode_uri": _vscode_file_uri(
                        candidate_module.path / item.path, root_path, source_roots
                    ),
                    "fields": [
                        {
                            "name": field.name,
                            "type": field.type,
                            "references": [
                                f"{service}:{identity}:{reference}"
                                for reference in field.references
                            ],
                        }
                        for field in item.fields
                    ],
                })
    nodes: list[dict[str, object]] = []
    runtime_namespaces = sorted({
        workload.namespace
        for module in all_modules
        for workload in module.kubernetes_workloads
        if workload.namespace
    } | {
        str(namespace)
        for fact in graph_facts or []
        for namespace in (
            (fact.metadata or {}).get("namespaces", [])
            if isinstance((fact.metadata or {}).get("namespaces", []), list)
            else [(fact.metadata or {}).get("namespace")]
        )
        if namespace
    })
    fact_namespaces = sorted({fact.namespace for fact in graph_facts or [] if fact.namespace})
    for name in ordered_services:
        endpoints = endpoints_by_service.get(name, [])
        resources = _rest_resources_served(endpoints)
        contract_resources: dict[str, set[str]] = {}
        contract_owner_identity: dict[str, str] = {}
        module = module_details.get(name)
        module_layer = software_layer(module, strategy1=strategy1, root_path=root_path) if module else "external"
        module_namespaces = sorted({
            workload.namespace
            for workload in (module.kubernetes_workloads if module else ())
            if workload.namespace
        })
        for endpoint in endpoints:
            if (
                endpoint.system == "rest"
                and endpoint.role == "serve"
                and endpoint.framework == "openapi"
            ):
                contract_path = _openapi_contract_evidence_path(endpoint)
                owner_module, module_path = _resolve_openapi_contract_owner(
                    contract_path, all_modules
                )
                if owner_module is None:
                    # No known module encloses this evidence (for example a
                    # federated inventory without the referenced module):
                    # keep the previous best-effort attribution to the
                    # serving module itself, using the raw evidence path.
                    owner_module = module
                    accepted = module is None or module_owns_openapi_file(module, module_path)
                elif owner_module is module:
                    accepted = module_owns_openapi_file(module, module_path)
                else:
                    # A Strategy1 declaration published by ``module`` whose
                    # contract physically lives in a different module (for
                    # example a shared ``model-*`` module). This is a
                    # legitimate cross-module reference, not a duplicate.
                    accepted = True
                if accepted:
                    contract_resources.setdefault(module_path, set()).add(endpoint.topic)
                    if owner_module is not None:
                        contract_owner_identity[module_path] = module_identity(owner_module)
        openapi_files = sorted(
            {
                path
                for path in (module.openapi_files if module else ())
                if module is None or module_owns_openapi_file(module, path)
            }
            | set(contract_resources)
        )
        owner_identity_by_path: dict[str, str] = dict(contract_owner_identity)
        if module is not None:
            for path in module.openapi_files:
                owner_identity_by_path.setdefault(path, module_identity(module))
        nodes.append(
            {
                "id": f"microservice:{name}",
                "kind": "microservice",
                "name": name,
                "layer": module_layer,
                "layer_label": module_layer.replace("_", " ").title(),
                "color": {
                    "application": "#2563eb",
                    "external": "#64748b",
                    "domain": "#7c3aed",
                    "api": "#0891b2",
                    "orchestration": "#9333ea",
                    "infrastructure": "#d97706",
                    "persistence": "#0f766e",
                    "unknown": "#94a3b8",
                }.get(module_layer, "#94a3b8"),
                "runtime_namespaces": module_namespaces,
                "fact_namespaces": sorted(fact_namespaces_by_service.get(name, set())),
                **({"architecture_namespace": module_namespaces[0]} if module_namespaces else {}),
                **({"project_namespace": project_namespace(module, root_path)} if module else {}),
                "architecture_layer": module_layer,
                "kafka_endpoints": [
                    {
                        "role": endpoint.role,
                        "topic": endpoint.topic,
                        "message_type": endpoint.message_type,
                        "location": f"{endpoint.path}:{endpoint.start_line}",
                        "vscode_uri": _endpoint_vscode_uri(endpoint, all_modules, source_roots, root_path),
                    }
                    for endpoint in endpoints
                    if endpoint.system == "kafka"
                ],
                **(
                    {
                        "build_system": module.build_system,
                        "vscode_uri": _vscode_file_uri(module.path, root_path, source_roots),
                    }
                    if module
                    else {}
                ),
                "resources": resources,
                "kubernetes_workloads": [workload.__dict__ for workload in module.kubernetes_workloads] if module else [],
                "openapi_files": openapi_files,
                "openapi_contracts": [
                    {
                        "path": path,
                        "resources": sorted(contract_resources.get(path, set())),
                        **({"spec": openapi_specs[(owner_identity_by_path[path], path)]}
                           if path in owner_identity_by_path
                           and (owner_identity_by_path[path], path) in openapi_specs else {}),
                        **({"vscode_uri": _vscode_file_uri(
                                (module_by_identity.get(owner_identity_by_path.get(path, ""), module)).path / path,
                                root_path, source_roots,
                            )} if module else {}),
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
                                "vscode_uri": _vscode_uri(finding, module, source_roots, root_path),
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
            "persistence_classes": [
                item for item in mongo_persistence_classes
                if item["service"] == service and item["collection"] == collection
                and item["root"]
            ],
            "width": 190,
            "height": 42,
        }
        for service, collection, identity in _mongodb_collection_nodes(collections_by_service)
    ]
    known_node_ids = {str(node["id"]) for node in nodes}
    for fact in graph_facts or []:
        if fact.fact_type == "node" and fact.name is not None:
            fact_visual_kind = "microservice" if fact.kind == "service" else fact.kind
            node_id = f"{fact_visual_kind}:{fact.name}"
            if node_id not in known_node_ids:
                node = {
                    "id": node_id, "kind": fact_visual_kind, "name": fact.name,
                    "label": fact.name, "width": 190, "height": 42,
                    "status": fact.status,
                }
                known_node_ids.add(node_id)
                nodes.append(node)
            node = next(item for item in nodes if item["id"] == node_id)
            if fact.technology:
                node["technology"] = fact.technology
            if fact.metadata:
                node["metadata"] = fact.metadata
                if fact_visual_kind == "microservice":
                    layer = fact.metadata.get("layer")
                    if isinstance(layer, str) and layer:
                        node["layer"] = layer
                        node["layer_label"] = layer.replace("_", " ").title()
                        node["color"] = {
                    "application": "#2563eb",
                    "external": "#64748b",
                            "domain": "#7c3aed",
                    "api": "#0891b2",
                    "orchestration": "#9333ea",
                            "infrastructure": "#d97706",
                            "persistence": "#0f766e",
                        }.get(layer, "#94a3b8")
                    namespaces = fact.metadata.get("namespaces")
                    if not isinstance(namespaces, list):
                        namespaces = [fact.metadata.get("namespace")]
                    node["runtime_namespaces"] = sorted({
                        str(namespace) for namespace in namespaces if namespace
                    })
                    if node["runtime_namespaces"]:
                        node["architecture_namespace"] = node["runtime_namespaces"][0]
                    if node.get("layer"):
                        node["architecture_layer"] = node["layer"]
            if fact_visual_kind == "microservice":
                node.setdefault("fact_namespaces", [])
                if fact.namespace not in node["fact_namespaces"]:
                    node["fact_namespaces"].append(fact.namespace)

    # A resource belongs visually to the boundary of the service that writes
    # or publishes it.  The namespace on a data/topic fact often describes
    # the broker or database infrastructure, which is useful as provenance
    # but is not the architectural ownership boundary used by this view.
    service_architecture = {
        str(node["name"]): {
            "layer": node.get("layer", "unknown"),
            "namespace": (
                (node.get("runtime_namespaces") or [])
                + (node.get("fact_namespaces") or [])
            )[0] if (node.get("runtime_namespaces") or []) + (node.get("fact_namespaces") or []) else None,
        }
        for node in nodes
        if node.get("kind") == "microservice"
    }
    resource_owners: dict[tuple[str, str], str] = {}
    for fact in graph_facts or []:
        if fact.fact_type != "edge" or fact.relation not in {"publishes", "writes"}:
            continue
        source_kind = "microservice" if fact.source_kind == "service" else fact.source_kind
        if source_kind != "microservice" or not fact.source_name or not fact.target_name:
            continue
        resource_owners[(fact.target_kind or "", fact.target_name)] = fact.source_name
    for edge in edges:
        if edge.kind == "kafka" and edge.from_endpoint.topic:
            resource_owners.setdefault(("kafka_topic", edge.from_endpoint.topic), edge.from_service)
    for node in nodes:
        owner = resource_owners.get((str(node.get("kind")), str(node.get("name"))))
        if owner is None and node.get("kind") == "mongodb_collection":
            owner = str(node.get("owner") or "") or None
        if owner is None:
            continue
        owner_data = service_architecture.get(owner)
        if not owner_data:
            continue
        node["owner_service"] = owner
        node["architecture_layer"] = owner_data["layer"]
        if owner_data["namespace"]:
            node["architecture_namespace"] = owner_data["namespace"]
        node["namespace_source"] = "writer"
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
    link_keys = {
        (str(link["source"]), str(link["target"]), str(link["kind"]), str(link["label"]))
        for link in links
    }
    for fact in graph_facts or []:
        if fact.fact_type != "edge":
            continue
        if not all((fact.source_kind, fact.source_name, fact.target_kind, fact.target_name)):
            continue
        source_kind = "microservice" if fact.source_kind == "service" else fact.source_kind
        target_kind = "microservice" if fact.target_kind == "service" else fact.target_kind
        source_id = f"{source_kind}:{fact.source_name}"
        target_id = f"{target_kind}:{fact.target_name}"
        for node_id, node_kind, node_name in (
            (source_id, source_kind, fact.source_name),
            (target_id, target_kind, fact.target_name),
        ):
            if node_id not in known_node_ids:
                nodes.append({
                    "id": node_id, "kind": node_kind, "name": node_name,
                    "label": node_name, "width": 190, "height": 42,
                })
                known_node_ids.add(node_id)
        link = {
            "source": source_id, "target": target_id,
            "kind": f"mcp_{fact.kind}",
            "direction": "outgoing",
            "label": fact.relation or fact.kind,
            "confidence": fact.confidence,
            "status": fact.status,
            "provenance": "MCP graph enrichment",
            **({"technology": fact.technology} if fact.technology else {}),
            **({"metadata": fact.metadata} if fact.metadata else {}),
        }
        link_key = (source_id, target_id, str(link["kind"]), str(link["label"]))
        if link_key not in link_keys:
            links.append(link)
            link_keys.add(link_key)
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
        # Geometry is intentionally uniform: complexity is encoded by color,
        # not by node size, so shape and visual weight remain comparable.
        # The card must leave enough room for a centered name and its corner
        # technology marker. This value is shared by every node kind.
        base_size = 50
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
        node["size"] = base_size
    # A workspace may contain structural directories such as ``services`` or
    # ``libs`` whose children are the actual projects. Keep these containers
    # separate from architecture relations: they are visual ownership/grouping
    # hints, not inferred runtime dependencies.
    service_node_ids = {f"microservice:{name}" for name in ordered_services}
    modules_by_parent: dict[Path, list[tuple[str, DiscoveredModule]]] = {}
    for module in all_modules:
        identity = module_identity(module)
        node_id = f"microservice:{identity}"
        if node_id not in service_node_ids:
            continue
        modules_by_parent.setdefault(module.path.resolve().parent, []).append((node_id, module))
    project_groups = [
        {
            "name": parent.name,
            "namespace": project_namespace(children[0][1], root_path),
            "children": sorted(node_id for node_id, _module in children),
        }
        for parent, children in sorted(modules_by_parent.items(), key=lambda item: str(item[0]))
        if len(children) >= 2
    ]
    if kafka_dto_definitions is None:
        kafka_dtos, project_dto_definitions = kafka_dto_views(endpoints_by_service)
    else:
        for definition in kafka_dto_definitions:
            module = module_by_identity.get(str(definition.get("module") or ""))
            dto_source = definition.get("source")
            if module is not None and isinstance(dto_source, str):
                definition["vscode_uri"] = _vscode_file_uri(
                    module.path / dto_source, root_path, source_roots
                )
        kafka_dtos = [item for item in kafka_dto_definitions if item.get("root", True)]
        project_dto_definitions = [
            item for item in kafka_dto_definitions if not item.get("root", True)
        ]
    graph_data = json.dumps(
        {
            "nodes": nodes,
            "links": links,
            "software_layers": sorted({
                str(node.get("layer"))
                for node in nodes
                if node.get("kind") == "microservice"
            }),
            "runtime_namespaces": runtime_namespaces,
            "fact_namespaces": fact_namespaces,
            "groups": project_groups,
            "build_dependencies": _module_dependency_view(build_modules, module_dependencies),
            "kafka_dtos": kafka_dtos,
            "project_dto_definitions": project_dto_definitions,
            "mongo_persistence_classes": mongo_persistence_classes,
            "indexing_issues": _indexing_issues(
                endpoints_by_service,
                edges,
                indexing_warnings,
                all_modules,
                source_roots,
                root_path,
                diagnostics,
            ),
        },
        ensure_ascii=False,
    ).replace("</", "<\\/")
    return _GRAPH_HTML_TEMPLATE.replace("__GRAPH_DATA__", graph_data)
