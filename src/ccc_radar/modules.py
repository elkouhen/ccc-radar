"""Inventaire de tous les modules Maven et Gradle d'un workspace."""

import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ccc_radar import java_parser
from ccc_radar.gradle import discover_gradle_modules
from ccc_radar.maven import parse_pom, pom_version
from ccc_radar.configuration import service_configuration_example
from ccc_radar.topic_expressions import spring_topic_reference


@dataclass(frozen=True)
class DiscoveredModule:
    name: str
    path: Path
    build_system: str  # maven | gradle
    version: str | None
    kind: str  # library | aggregator
    starts_application: bool
    configuration_example: str
    application_entrypoint: "SourceEvidence | None" = None
    mongo_collections: tuple[str, ...] = ()
    mongo_methods: tuple["MongoMethod", ...] = ()
    openapi_files: tuple[str, ...] = ()
    kafka_methods: tuple["KafkaMethod", ...] = ()
    blocking_points: tuple["BlockingPoint", ...] = ()
    # REST controllers and OpenAPI-generated clients
    rest_controllers: tuple[str, ...] = ()
    openapi_generated_clients: tuple[str, ...] = ()


@dataclass(frozen=True, order=True)
class ModuleDependency:
    """Dépendance de build entre deux modules présents dans le workspace."""

    source: str
    target: str


@dataclass(frozen=True)
class MongoMethod:
    operation: str
    receiver: str
    path: str
    line: int
    collection: str | None = None
    evidence: "SourceEvidence | None" = None
    owner_method: str | None = None


@dataclass(frozen=True)
class KafkaMethod:
    role: str  # send | receive
    mechanism: str
    method: str
    path: str
    line: int
    topic: str | None = None
    evidence: "SourceEvidence | None" = None


@dataclass(frozen=True)
class BlockingPoint:
    mechanism: str
    method: str
    path: str
    line: int
    detail: str
    evidence: "SourceEvidence | None" = None


@dataclass(frozen=True)
class SourceEvidence:
    start_line: int
    end_line: int
    snippet: str
    source_hash: str


class JavaArchitectureExtension(Protocol):
    """Extension d'inventaire appliquée aux sources Java de production."""

    name: str

    def extract(self, files: list[tuple[str, bytes]]) -> tuple[KafkaMethod, ...]: ...


_MONGO_TEMPLATE_OPERATIONS = frozenset({
    "aggregate", "bulkOps", "count", "exists", "find", "findAll", "findAndModify",
    "findAndReplace", "findById", "findOne", "getCollection", "insert", "remove",
    "save", "updateFirst", "updateMulti", "upsert", "watch",
})
_REPOSITORY_OPERATIONS = frozenset({
    "aggregate", "count", "delete", "deleteAll", "deleteAllById", "deleteById",
    "existsById", "find", "findAll", "findById", "findOne", "insert", "insertAll",
    "save", "saveAll",
})
_OPENAPI_FILENAMES = (
    "openapi.yaml", "openapi.yml", "openapi.json", "swagger.yaml", "swagger.yml", "swagger.json",
)
_MAX_NESTED_MODULE_DEPTH = 5


def _is_excluded_module(name: str, module_dir: Path, root: Path) -> bool:
    """Whether a non-production build module must not be indexed.

    Test and mock projects do not describe the production architecture.  Their
    artifact name is not always explicit: a module in ``contract-tests`` may
    still publish an artifact named ``contract-api``. Inspect both the build
    name and every component of its path relative to the workspace, without
    considering the workspace path itself.
    """
    try:
        path_parts = module_dir.resolve().relative_to(root.resolve()).parts
    except ValueError:
        path_parts = (module_dir.name,)
    # A substring match made ordinary artifact names such as ``test-service``
    # (and pytest's temporary workspace names) disappear from discovery.
    # Keep explicit test directories excluded, while retaining production
    # modules whose names merely contain that word.
    test_or_mock = any(
        value.casefold() in {"test", "tests"} or "mock" in value.casefold()
        for value in path_parts
    ) or "mock" in name.casefold()
    # Maven archetypes are templates rather than runtime applications.  This
    # rule intentionally applies to the declared build name only: an ordinary
    # module may legitimately live under a directory containing this word.
    return test_or_mock or "archteype" in name.casefold()


def discover_excluded_module_paths(root: Path) -> tuple[Path, ...]:
    """Return build-module roots excluded from the production index.

    Module inventory already omits test, mock, and ``archteype`` modules. The
    indexer also needs their paths before scanning files: an excluded artifact
    can legitimately contain ``src/main`` sources and would otherwise still
    produce endpoints and findings despite being absent from the inventory.
    """
    root = root.resolve()
    paths: set[Path] = set()
    for pom_path in sorted(root.rglob("pom.xml")):
        if ".git" in pom_path.relative_to(root).parts:
            continue
        module_dir = pom_path.parent.resolve()
        if not _is_module_within_depth(root, module_dir):
            continue
        artifact_id, _, _ = parse_pom(pom_path)
        if _is_excluded_module(artifact_id or module_dir.name, module_dir, root):
            paths.add(module_dir)
    for name, module_dir, _version in discover_gradle_modules(root):
        module_dir = module_dir.resolve()
        if _is_excluded_module(name, module_dir, root):
            paths.add(module_dir)
    return tuple(sorted(paths))


def _trace(stage: str, **fields: object) -> None:
    if os.environ.get("CCCR_TRACE") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(f"CCCR_TRACE ts={time.monotonic():.6f} stage={stage} {details}".rstrip(), file=sys.stderr, flush=True)


def _starts_application(module_dir: Path) -> SourceEvidence | None:
    """Return whether this build module owns a Spring Boot entry point.

    The decision belongs to module parsing, independently of Maven/Gradle. It
    uses Java method boundaries from Tree-sitter and only recognises a
    ``main`` method that invokes ``SpringApplication.run``.
    """
    source_roots = sorted(module_dir.glob("**/src/main/java"))
    if not source_roots:
        return None
    _trace("module.entrypoint.parser.begin", module=module_dir)
    parser = _java_parser("entrypoint")
    _trace("module.entrypoint.parser.end", module=module_dir, roots=len(source_roots))
    for source_root in source_roots:
        for path in source_root.rglob("*.java"):
            try:
                source = path.read_bytes()
            except OSError:
                continue
            _trace("module.entrypoint.parse.begin", module=module_dir, path=path)
            tree = parser.parse(source)
            _trace("module.entrypoint.parse.end", module=module_dir, path=path)
            if tree.root_node.has_error:
                continue
            for node in _walk(tree.root_node):
                if node.type != "method_declaration":
                    continue
                name = node.child_by_field_name("name")
                if name is None or _node_text(source, name) != "main":
                    continue
                for invocation in _walk(node):
                    if invocation.type == "method_invocation" and "SpringApplication.run" in _node_text(source, invocation):
                        return _source_evidence(source, invocation, _module_relative(module_dir, path))
    return None


# Traversée et moteur tree-sitter : factorisés dans `java_parser` (utilisés
# aussi par `scanner.py`). Les alias conservent les noms privés historiques
# pour limiter le bruit de renommage dans ce module.
_walk = java_parser.walk
_node_text = java_parser.node_text
_enclosing_method_name = java_parser.enclosing_method_name
_java_parser = java_parser.java_parser


def _source_evidence(source: bytes, node, rel_path: str) -> SourceEvidence:
    start_line, end_line, snippet, source_hash = java_parser.evidence_fields(
        source, node, rel_path
    )
    return SourceEvidence(
        start_line=start_line,
        end_line=end_line,
        snippet=snippet,
        source_hash=source_hash,
    )


def _first_string_value(node, source: bytes) -> str | None:
    """Return the first string literal nested in a parsed Java expression."""
    if node is None:
        return None
    for child in _walk(node):
        literal = java_parser.string_value(child, source)
        if literal is not None:
            return literal
    return None


def _kafka_topic_value(node, source: bytes) -> str | None:
    literal = _first_string_value(node, source)
    if literal is None:
        return None
    reference = spring_topic_reference(literal)
    if reference is not None:
        return reference.display_name
    return literal


def _module_relative(module_dir: Path, path: Path) -> str:
    return path.relative_to(module_dir).as_posix()


def _mongo_collection_literal(invocation, source: bytes) -> str | None:
    """Return a literal trailing Mongo collection argument, when present."""
    arguments = java_parser.argument_nodes(invocation)
    if not arguments:
        return None
    return java_parser.string_value(arguments[-1], source)


def _invocation_receiver_and_operation(invocation, source: bytes) -> tuple[str, str] | None:
    """Extract a direct Java call receiver and its operation from the AST."""
    receiver, operation, _arguments = java_parser.invocation_parts(invocation, source)
    if receiver is None or receiver.type not in {"identifier", "field_access"}:
        return None
    receiver_text = _node_text(source, receiver)
    return receiver_text.rsplit(".", 1)[-1], operation


def _type_identifiers(node, source: bytes) -> list[str]:
    return [
        _node_text(source, child)
        for child in _walk(node)
        if child.type in {"type_identifier", "scoped_type_identifier"}
    ]


def _repository_entity(type_node, source: bytes) -> str | None:
    identifiers = _type_identifiers(type_node, source)
    if len(identifiers) >= 2 and identifiers[0].rsplit(".", 1)[-1] in {
        "MongoRepository", "ReactiveMongoRepository"
    }:
        return identifiers[1].rsplit(".", 1)[-1]
    return None


def _module_files(module_dir: Path, module_roots: set[Path], pattern: str):
    """Yield files owned by this module, never files of nested modules."""
    for path in sorted(module_dir.rglob(pattern)):
        if not path.is_file():
            continue
        if any(
            parent in module_roots
            and parent != module_dir
            and module_dir in parent.parents
            for parent in path.parents
        ):
            continue
        yield path


def _is_module_within_depth(root: Path, module_dir: Path) -> bool:
    return len(module_dir.relative_to(root).parts) <= _MAX_NESTED_MODULE_DEPTH


class KafkaArchitectureExtension:
    """Extrait les usages Kafka sans dépendre des extracteurs Mongo/JPA."""

    name = "kafka"

    def extract(self, files: list[tuple[str, bytes]]) -> tuple[KafkaMethod, ...]:
        parser = _java_parser("kafka")
        methods: set[KafkaMethod] = set()
        for rel, source in files:
            source_text = source.decode("utf-8", errors="replace")
            tree = parser.parse(source)
            if tree.root_node.has_error:
                continue
            has_kafka_consumer = "KafkaConsumer" in source_text
            has_kafka_producer = "KafkaProducer" in source_text
            for method_node in _walk(tree.root_node):
                if method_node.type != "method_declaration":
                    continue
                method_name_node = method_node.child_by_field_name("name")
                if method_name_node is None:
                    continue
                method_name = _node_text(source, method_name_node)
                for annotation_node in java_parser.annotations_of(method_node):
                    annotation_name = java_parser.annotation_name(annotation_node, source)
                    if annotation_name in {"KafkaListener", "KafkaHandler"}:
                        topic_node = (
                            java_parser.annotation_argument(annotation_node, source, "topics")
                            or java_parser.annotation_argument(annotation_node, source, "topic")
                            or java_parser.annotation_argument(annotation_node, source, "value")
                            or java_parser.annotation_argument(annotation_node, source)
                        )
                        methods.add(KafkaMethod(
                            role="receive", mechanism="spring-kafka-listener", method=method_name,
                            path=rel, line=method_node.start_point.row + 1,
                            topic=_kafka_topic_value(topic_node, source),
                            evidence=_source_evidence(source, annotation_node, rel),
                        ))
                    if annotation_name == "SendTo":
                        methods.add(KafkaMethod(
                            role="send", mechanism="spring-kafka-send-to", method=method_name,
                            path=rel, line=method_node.start_point.row + 1,
                            topic=_kafka_topic_value(
                                java_parser.annotation_argument(annotation_node, source), source
                            ),
                            evidence=_source_evidence(source, annotation_node, rel),
                        ))
                for invocation in _walk(method_node):
                    if invocation.type != "method_invocation":
                        continue
                    call = _invocation_receiver_and_operation(invocation, source)
                    if call is None:
                        continue
                    receiver, operation = call
                    topic = _kafka_topic_value(
                        java_parser.argument_nodes(invocation)[0], source
                    ) if java_parser.argument_nodes(invocation) else None
                    mechanism: str | None = None
                    role: str | None = None
                    if operation in {"send", "sendDefault"} and (
                        receiver.lower().endswith("kafkatemplate") or "KafkaTemplate" in source_text
                    ):
                        role, mechanism = "send", "spring-kafka-template"
                    elif operation == "send" and has_kafka_producer:
                        role, mechanism = "send", "kafka-clients-producer"
                    elif operation == "send" and receiver.lower().endswith("streambridge"):
                        role, mechanism = "send", "spring-cloud-stream"
                    elif operation == "to" and ("StreamsBuilder" in source_text or "KStream" in source_text):
                        role, mechanism = "send", "kafka-streams"
                    elif operation == "poll" and has_kafka_consumer:
                        role, mechanism = "receive", "kafka-clients-poll"
                    elif operation == "stream" and ("StreamsBuilder" in source_text or "KStream" in source_text):
                        role, mechanism = "receive", "kafka-streams"
                    if role is not None and mechanism is not None:
                        methods.add(KafkaMethod(
                            role=role, mechanism=mechanism, method=method_name,
                            path=rel, line=invocation.start_point.row + 1, topic=topic,
                            evidence=_source_evidence(source, invocation, rel),
                        ))
        return tuple(sorted(methods, key=lambda item: (item.path, item.line, item.role, item.mechanism)))


JAVA_ARCHITECTURE_EXTENSIONS: tuple[JavaArchitectureExtension, ...] = (KafkaArchitectureExtension(),)


def _extract_java_architecture(
    module_dir: Path, module_roots: set[Path]
) -> tuple[tuple[str, ...], tuple[MongoMethod, ...], tuple[KafkaMethod, ...], tuple[BlockingPoint, ...]]:
    """Extract Mongo facts from Java syntax trees.

    This deliberately uses AST node boundaries rather than line regexes: comments,
    strings and nested expressions cannot masquerade as annotations or calls. Symbol
    resolution remains out of scope, so each extracted method retains its receiver and
    an optional collection only when a literal is statically present.
    """
    _trace("module.architecture.parser.begin", module=module_dir)
    parser = _java_parser("architecture")
    _trace("module.architecture.parser.end", module=module_dir)
    java_files = list(_module_files(module_dir, module_roots, "*.java"))
    production_files: list[tuple[Path, str, bytes]] = []
    collections: set[str] = set()
    entity_collections: dict[str, str] = {}
    repository_entities: dict[str, str] = {}
    for path in java_files:
        rel = _module_relative(module_dir, path)
        segments = rel.split("/")
        if any(
            segment == "src" and index + 1 < len(segments)
            and (segments[index + 1] == "test" or segments[index + 1].endswith("Test"))
            for index, segment in enumerate(segments)
        ):
            continue
        try:
            source = path.read_bytes()
        except OSError:
            continue
        production_files.append((path, rel, source))
        root_node = parser.parse(source).root_node
        if root_node.has_error:
            continue
        for declaration in java_parser.type_declarations(root_node):
            declaration_name = java_parser.declaration_name(declaration, source)
            if declaration_name is None:
                continue
            document_annotation = next(
                (
                    annotation
                    for annotation in java_parser.annotations_of(declaration)
                    if java_parser.annotation_name(annotation, source) == "Document"
                ),
                None,
            )
            if document_annotation is not None:
                collection = _first_string_value(
                    java_parser.annotation_argument(document_annotation, source, "collection")
                    or java_parser.annotation_argument(document_annotation, source),
                    source,
                )
                if collection is not None:
                    collections.add(collection)
                    entity_collections[declaration_name] = collection
            if declaration.type == "interface_declaration":
                entity = _repository_entity(declaration, source)
                if entity is not None:
                    repository_entities[declaration_name] = entity

    methods: set[MongoMethod] = set()
    kafka_methods = next(
        extension.extract([(rel, source) for _, rel, source in production_files])
        for extension in JAVA_ARCHITECTURE_EXTENSIONS
        if extension.name == "kafka"
    )
    blocking_points: set[BlockingPoint] = set()
    for path, rel, source in production_files:
        _trace("module.architecture.parse.begin", module=module_dir, path=rel)
        tree = parser.parse(source)
        _trace("module.architecture.parse.end", module=module_dir, path=rel)
        _trace("module.architecture.root.begin", module=module_dir, path=rel)
        root_node = tree.root_node
        _trace("module.architecture.root.end", module=module_dir, path=rel, node_type=root_node.type)
        _trace("module.architecture.error_check.begin", module=module_dir, path=rel)
        has_error = root_node.has_error
        _trace("module.architecture.error_check.end", module=module_dir, path=rel, has_error=has_error)
        if has_error:
            continue
        repository_receivers: dict[str, str | None] = {}
        _trace("module.architecture.walk_metadata.begin", module=module_dir, path=rel)
        for node in _walk(root_node):
            if node.type == "field_declaration":
                type_node = node.child_by_field_name("type")
                if type_node is None:
                    continue
                type_names = _type_identifiers(type_node, source)
                type_name = type_names[0].rsplit(".", 1)[-1] if type_names else ""
                entity = repository_entities.get(type_name) or _repository_entity(type_node, source)
                is_crud_repository = type_name in {"CrudRepository", "ReactiveCrudRepository"}
                if entity is None and not is_crud_repository:
                    continue
                for declarator in node.children:
                    if declarator.type != "variable_declarator":
                        continue
                    receiver_node = declarator.child_by_field_name("name")
                    if receiver_node is None:
                        continue
                    receiver = _node_text(source, receiver_node)
                    repository_receivers[receiver] = (
                        entity_collections.get(entity) if entity is not None else None
                    )
        _trace("module.architecture.walk_metadata.end", module=module_dir, path=rel)
        _trace("module.architecture.walk_methods.begin", module=module_dir, path=rel)
        for method_node in _walk(root_node):
            if method_node.type != "method_declaration":
                continue
            method_name_node = method_node.child_by_field_name("name")
            if method_name_node is None:
                continue
            method_name = _node_text(source, method_name_node)
            method_text = _node_text(source, method_node)
            for invocation in _walk(method_node):
                if invocation.type != "method_invocation":
                    continue
                call = _invocation_receiver_and_operation(invocation, source)
                if call is None:
                    continue
                receiver, operation = call
                blocking_mechanism: str | None = None
                detail: str | None = None
                if receiver == "Thread" and operation == "sleep":
                    blocking_mechanism, detail = "thread-sleep", "Thread.sleep"
                elif operation == "wait":
                    blocking_mechanism, detail = "object-wait", "Object.wait"
                elif operation == "get" and any(token in receiver.lower() for token in ("future", "result", "promise")):
                    blocking_mechanism, detail = "future-get", "Future.get sans analyse de timeout"
                elif operation == "join" and any(token in receiver.lower() for token in ("thread", "future", "task")):
                    blocking_mechanism, detail = "thread-or-future-join"
                elif operation in {"lock", "lockInterruptibly"}:
                    blocking_mechanism, detail = "jvm-lock", operation
                elif operation in {"findAndModify", "findOneAndUpdate"} and "lock" in method_text.casefold():
                    blocking_mechanism, detail = "mongo-pessimistic-lock", operation
                if blocking_mechanism is not None and detail is not None:
                    blocking_points.add(BlockingPoint(
                        mechanism=blocking_mechanism, method=method_name, path=rel,
                        line=invocation.start_point.row + 1, detail=detail,
                        evidence=_source_evidence(source, invocation, rel),
                    ))
            for statement in _walk(method_node):
                if statement.type == "synchronized_statement":
                    blocking_points.add(BlockingPoint(
                        mechanism="jvm-synchronized", method=method_name, path=rel,
                        line=statement.start_point.row + 1, detail="synchronized block",
                        evidence=_source_evidence(source, statement, rel),
                    ))
        _trace("module.architecture.walk_methods.end", module=module_dir, path=rel)
        _trace("module.architecture.walk_mongo.begin", module=module_dir, path=rel)
        for node in _walk(root_node):
            if node.type != "method_invocation":
                continue
            call = _invocation_receiver_and_operation(node, source)
            if call is None:
                continue
            receiver, operation = call
            is_template = receiver.lower().endswith(("template", "operations"))
            is_repository = receiver in repository_receivers
            if (is_template and operation in _MONGO_TEMPLATE_OPERATIONS) or (
                is_repository and operation in _REPOSITORY_OPERATIONS
            ):
                collection = (
                    repository_receivers.get(receiver)
                    if is_repository
                    else _mongo_collection_literal(node, source)
                )
                if collection:
                    collections.add(collection)
                methods.add(MongoMethod(
                    operation=operation,
                    receiver=receiver,
                    path=rel,
                    line=node.start_point.row + 1,
                    collection=collection,
                    evidence=_source_evidence(source, node, rel),
                    owner_method=_enclosing_method_name(source, node),
                ))
        _trace("module.architecture.walk_mongo.end", module=module_dir, path=rel)
    _trace("module.architecture.files.end", module=module_dir)
    _trace("module.architecture.sort_collections.begin", module=module_dir, count=len(collections))
    sorted_collections = tuple(sorted(collections))
    _trace("module.architecture.sort_collections.end", module=module_dir)
    _trace("module.architecture.sort_methods.begin", module=module_dir, count=len(methods))
    sorted_methods = tuple(sorted(methods, key=lambda item: (item.path, item.line, item.operation)))
    _trace("module.architecture.sort_methods.end", module=module_dir)
    _trace("module.architecture.sort_blocking.begin", module=module_dir, count=len(blocking_points))
    sorted_blocking_points = tuple(sorted(
        blocking_points, key=lambda item: (item.path, item.line, item.mechanism)
    ))
    _trace("module.architecture.sort_blocking.end", module=module_dir)
    _trace("module.architecture.end", module=module_dir)
    return sorted_collections, sorted_methods, kafka_methods, sorted_blocking_points


def _discover_openapi_files(
    module_dir: Path,
    module_roots: set[Path],
    *,
    rest_controllers: tuple[str, ...] = (),
    build_system: str | None = None,
) -> tuple[str, ...]:
    contracts = {
        _module_relative(module_dir, path)
        for path in _module_files(module_dir, module_roots, "*")
        if path.name.casefold() in _OPENAPI_FILENAMES
    }
    pom_path = module_dir / "pom.xml"
    if build_system == "maven" and rest_controllers and pom_path.is_file():
        from ccc_radar.maven import detect_openapi_generator_input_specs

        contracts.update(detect_openapi_generator_input_specs(pom_path))
    return tuple(sorted(contracts))


def discover_rest_controllers(module_dir: Path, module_roots: set[Path]) -> tuple[str, ...]:
    """Return controller source files belonging to a module.

    This is deliberately a small public discovery API: both the module
    inventory and endpoint scanner need the same definition of a REST-capable
    module. Keeping it here prevents their source-set filtering from
    diverging.
    """
    """Détecte les classes Java annotées avec @RestController.

    Retourne une tuple de chaînes au format "ClassName (relative/path.java)".
    """
    parser = _java_parser("rest_controllers")
    controller_classes: list[str] = []

    for java_file in _module_files(module_dir, module_roots, "*.java"):
        try:
            source = java_file.read_bytes()
        except OSError:
            continue
        root_node = parser.parse(source).root_node
        if root_node.has_error:
            continue
        rel_path = _module_relative(module_dir, java_file)
        for declaration in java_parser.type_declarations(root_node):
            if declaration.type != "class_declaration":
                continue
            if not any(
                java_parser.annotation_name(annotation, source) == "RestController"
                for annotation in java_parser.annotations_of(declaration)
            ):
                continue
            class_name = java_parser.declaration_name(declaration, source) or "Unknown"
            controller_classes.append(f"{class_name} ({rel_path})")

    return tuple(sorted(set(controller_classes)))


def _enrich_module(
    module: DiscoveredModule,
    module_roots: set[Path],
    *,
    enrich_architecture: bool = True,
) -> DiscoveredModule:
    _trace("module.enrich.begin", module=module.path)
    if enrich_architecture:
        collections, methods, kafka_methods, blocking_points = _extract_java_architecture(module.path, module_roots)
    else:
        _trace("module.enrich.architecture.disabled", module=module.path)
        collections = ()
        methods = ()
        kafka_methods = ()
        blocking_points = ()

    # Détecter les contrôleurs REST
    rest_controllers = discover_rest_controllers(module.path, module_roots)

    openapi_files = _discover_openapi_files(
        module.path,
        module_roots,
        rest_controllers=rest_controllers,
        build_system=module.build_system,
    )

    # Détecter les clients OpenAPI générés (Maven uniquement)
    openapi_generated_clients = ()
    if module.build_system == "maven":
        from ccc_radar.maven import detect_openapi_generated_clients
        pom_path = module.path / "pom.xml"
        if pom_path.exists():
            openapi_generated_clients = detect_openapi_generated_clients(pom_path)

    enriched = DiscoveredModule(
        **{**module.__dict__, "mongo_collections": collections, "mongo_methods": methods,
           "openapi_files": openapi_files,
           "kafka_methods": kafka_methods, "blocking_points": blocking_points,
           "rest_controllers": rest_controllers, "openapi_generated_clients": openapi_generated_clients}
    )
    _trace("module.enrich.end", module=module.path)
    return enriched


def discover_modules(
    root: Path,
    *,
    enrich_architecture: bool = True,
    use_tree_sitter: bool = True,
) -> list[DiscoveredModule]:
    """Discover build modules, including libraries and aggregators."""
    root = root.resolve()
    modules: list[DiscoveredModule] = []
    seen_paths: set[Path] = set()
    for pom_path in sorted(root.rglob("pom.xml")):
        if ".git" in pom_path.relative_to(root).parts:
            continue
        module_dir = pom_path.parent.resolve()
        if not _is_module_within_depth(root, module_dir):
            continue
        _trace("module.maven.begin", pom=pom_path)
        artifact_id, _, packaging = parse_pom(pom_path)
        _trace("module.maven.parsed", pom=pom_path, artifact=artifact_id, packaging=packaging)
        module_name = artifact_id or module_dir.name
        if _is_excluded_module(module_name, module_dir, root):
            _trace("module.maven.skipped", pom=pom_path, artifact=module_name)
            continue
        if use_tree_sitter:
            entrypoint = _starts_application(module_dir)
        else:
            _trace("module.maven.entrypoint.disabled", pom=pom_path)
            entrypoint = None
        _trace("module.maven.entrypoint.end", pom=pom_path, found=entrypoint is not None)
        kind = (
            "aggregator"
            if packaging == "pom"
            else "library"
        )
        modules.append(
            DiscoveredModule(
                name=module_name,
                path=module_dir,
                build_system="maven",
                version=pom_version(pom_path),
                kind=kind,
                starts_application=packaging != "pom" and entrypoint is not None,
                configuration_example=service_configuration_example(module_dir),
                application_entrypoint=entrypoint,
            )
        )
        seen_paths.add(module_dir)
    for name, module_dir, version in discover_gradle_modules(root):
        module_dir = module_dir.resolve()
        if module_dir in seen_paths:
            continue
        if _is_excluded_module(name, module_dir, root):
            _trace("module.gradle.skipped", module=module_dir, name=name)
            continue
        has_build_file = any(
            (module_dir / filename).is_file() for filename in ("build.gradle", "build.gradle.kts")
        )
        if use_tree_sitter:
            entrypoint = _starts_application(module_dir)
        else:
            _trace("module.gradle.entrypoint.disabled", module=module_dir)
            entrypoint = None
        modules.append(
            DiscoveredModule(
                name=name,
                path=module_dir,
                build_system="gradle",
                version=version,
                kind=(
                    "library"
                    if has_build_file
                    else "aggregator"
                ),
                starts_application=entrypoint is not None,
                configuration_example=service_configuration_example(module_dir),
                application_entrypoint=entrypoint,
            )
        )
    module_roots = {module.path for module in modules}
    _trace("modules.enrich.begin", count=len(modules))
    enriched = sorted(
        (
            _enrich_module(
                module,
                module_roots,
                enrich_architecture=enrich_architecture and use_tree_sitter,
            )
            for module in modules
        ),
        key=lambda module: str(module.path),
    )
    _trace("modules.enrich.end", count=len(enriched))
    return enriched


def discover_module_dependencies(root: Path, modules: list[DiscoveredModule]) -> list[ModuleDependency]:
    """Retourne les dépendances de build dont les deux extrémités sont locales.

    Les coordonnées Maven sont rapprochées sur l'``artifactId`` des modules
    découverts. Les dépendances Gradle ``project(':...')`` sont rapprochées sur
    leur chemin de projet, puis sur le nom d'artefact. Les dépendances externes
    ne sont volontairement pas ajoutées au graphe.
    """
    names = {module.name for module in modules}
    gradle_projects = {
        ":" + module.path.resolve().relative_to(root.resolve()).as_posix().replace("/", ":"): module.name
        for module in modules
        if module.build_system == "gradle" and module.path.resolve() != root.resolve()
    }
    dependencies: set[ModuleDependency] = set()
    for module in modules:
        if module.build_system == "maven":
            targets = maven_module_dependencies(module.path / "pom.xml")
        else:
            targets = _gradle_module_dependencies(module.path)
        for target in targets:
            resolved = gradle_projects.get(target, target)
            if resolved in names and resolved != module.name:
                dependencies.add(ModuleDependency(source=module.name, target=resolved))
    return sorted(dependencies)


def maven_module_dependencies(pom_path: Path) -> set[str]:
    """Return direct Maven artifact dependencies declared by ``pom_path``."""
    try:
        root = ET.fromstring(pom_path.read_text(encoding="utf-8", errors="replace"))
    except (ET.ParseError, OSError):
        return set()
    namespace = "{http://maven.apache.org/POM/4.0.0}"
    dependencies = root.find(f"{namespace}dependencies")
    if dependencies is None:
        dependencies = root.find("dependencies")
    if dependencies is None:
        return set()
    targets: set[str] = set()
    for dependency in list(dependencies):
        artifact = dependency.findtext(f"{namespace}artifactId") or dependency.findtext("artifactId")
        if artifact:
            targets.add(artifact.strip())
    return targets


_GRADLE_PROJECT_DEPENDENCY_RE = re.compile(
    r"\b(?:api|compileOnly|implementation|runtimeOnly|testImplementation)\s*\(?\s*"
    r"project\s*\(\s*(?:path\s*:\s*)?['\"](:[^'\"]+)['\"]"
)


def _gradle_module_dependencies(module_dir: Path) -> set[str]:
    targets: set[str] = set()
    for filename in ("build.gradle", "build.gradle.kts"):
        path = module_dir / filename
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        targets.update(_GRADLE_PROJECT_DEPENDENCY_RE.findall(text))
    return targets
