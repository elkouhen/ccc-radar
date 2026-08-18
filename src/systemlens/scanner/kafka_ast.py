"""Inférence des endpoints Kafka au niveau AST (BACKLOG-16).

Détecte `@KafkaListener` (consume), `KafkaTemplate.send/sendDefault`,
`new ProducerRecord<...>`, `MessageBuilder...setHeader(TOPIC,...).build()`
puis `.send(msg)`, `builder.stream(...)`/`KafkaConsumer.subscribe` (consume)
et `KStream.to(...)` (produce). Le type de message est inféré depuis les
signatures/generics de l'AST. Point d'entrée public : `infer_kafka_endpoints`."""

from __future__ import annotations

import re
from pathlib import Path

from systemlens import java_parser
from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.scanner._core import (
    _find_first_literal,
    _invocation_receiver,
    _java_qualified_name,
    _message_payload_type,
    _module_for_path,
)
from systemlens.scanner._spring_properties import (
    _resolve_value_annotated_variable,
    resolve_spring_property,
)
from systemlens.topic_expressions import spring_topic_reference

_BARE_TOPIC_VAR_RE = re.compile(
    r"(?:topics\s*=\s*|\.send\(\s*|ProducerRecord\(\s*)([A-Za-z_]\w*)\s*[,)]"
)
# BACKLOG Q25 : `KStream.to("topic")`/`.to("topic", Produced.with(...))` —
# le topic suit directement `.to(`, contrairement au premier littéral
# quelconque du snippet (qui peut appartenir à un `.peek(...)` chaîné avant).
_KAFKA_STREAMS_TO_RE = re.compile(r'\.to\(\s*"([^"]*)"\s*(\+)?')
def _resolve_topic_expression(
    expr: str, repo_root: Path, source_path: str
) -> tuple[str, bool]:
    expr = expr.strip()
    if len(expr) >= 2 and expr[0] == expr[-1] == '"':
        literal = expr[1:-1]
        reference = spring_topic_reference(literal)
        if reference is not None:
            resolved = resolve_spring_property(repo_root, reference.property_key, source_path)
            if resolved is not None:
                return resolved, False
            return reference.display_name, True
        return literal, False
    if re.fullmatch(r"[A-Za-z_]\w*", expr):
        resolved = _resolve_value_annotated_variable(repo_root, source_path, expr)
        if resolved is not None:
            return resolved, False
    return "<dynamic>", True
def _kafka_topic_from_value(value_node, source: bytes, repo_root: Path, rel_path: str) -> tuple[str, bool]:
    """Résout un nœud expression (topic) en (topic, dynamique).

    Littéral `${...}` -> résolu via ``resolve_spring_property`` (jamais au
    hasard) ; identifiant nu -> champ ``@Value`` ; appel imbriqué
    (ex. ``Collections.singletonList("x")``) -> on descend chercher le
    littéral ; sinon ``<dynamic>``."""
    if value_node is None:
        return "<dynamic>", True
    node_type = value_node.type
    if node_type == "string_literal":
        literal = java_parser.string_value(value_node, source)
        if literal is None:
            return "<dynamic>", True
        reference = spring_topic_reference(literal)
        if reference is not None:
            resolved = resolve_spring_property(repo_root, reference.property_key, rel_path)
            if resolved is not None:
                return resolved, False
            return reference.display_name, True
        return literal, False
    if node_type == "identifier":
        resolved = _resolve_value_annotated_variable(
            repo_root, rel_path, java_parser.node_text(source, value_node)
        )
        if resolved is not None:
            return resolved, False
        return "<dynamic>", True
    for descendant in java_parser.walk(value_node):
        if descendant.type == "string_literal":
            return _kafka_topic_from_value(descendant, source, repo_root, rel_path)
    return "<dynamic>", True
def _kafka_topics_from_value(value_node, source: bytes, repo_root: Path, rel_path: str) -> list[tuple[str, bool]]:
    """Resolve one or more topic expressions without silently dropping arrays.

    Spring's ``@KafkaListener(topics = {"a", "b"})`` is a single annotation
    but represents a dependency on every listed topic.  The generic resolver
    deliberately returns one value for invocation arguments; this companion
    keeps that behaviour while expanding only a Java array initializer.
    """
    if value_node is not None and value_node.type in {
        "array_initializer", "element_value_array_initializer"
    }:
        topics = [
            _kafka_topic_from_value(child, source, repo_root, rel_path)
            for child in value_node.children
            if child.type not in {"{", "}", ","}
        ]
        return list(dict.fromkeys(topics))
    return [_kafka_topic_from_value(value_node, source, repo_root, rel_path)]
def _kafka_listener_topics(listener_ann, source: bytes, repo_root: Path, rel_path: str) -> list[tuple[str, bool]]:
    """Extract all concrete topic dependencies of a Spring Kafka listener.

    Besides ``topics``, Spring supports ``topicPartitions``. A ``topicPattern``
    is kept as a dynamic fact: its literal is useful evidence, but it must not
    create an exact producer/consumer dependency in the graph.
    """
    topics_arg = java_parser.annotation_argument(listener_ann, source, key="topics")
    if topics_arg is not None:
        return _kafka_topics_from_value(topics_arg, source, repo_root, rel_path)
    topic_partitions = java_parser.annotation_argument(listener_ann, source, key="topicPartitions")
    if topic_partitions is not None:
        topics: list[tuple[str, bool]] = []
        for node in java_parser.walk(topic_partitions):
            if node.type != "element_value_pair":
                continue
            key = node.child_by_field_name("key")
            if key is None or java_parser.node_text(source, key) != "topic":
                continue
            topics.extend(_kafka_topics_from_value(
                node.child_by_field_name("value"), source, repo_root, rel_path
            ))
        if topics:
            return list(dict.fromkeys(topics))
    topic_pattern = java_parser.annotation_argument(listener_ann, source, key="topicPattern")
    if topic_pattern is not None:
        if topic_pattern.type == "string_literal":
            raw = java_parser.node_text(source, topic_pattern)
            return [(raw[1:-1], True)]
        return [(topic, True) for topic, _dynamic in _kafka_topics_from_value(
            topic_pattern, source, repo_root, rel_path
        )]
    return [("<dynamic>", True)]
def _object_creation_type(source: bytes, node) -> str:
    """Type construit d'un ``new Foo<...>(...)`` : ``ProducerRecord<...>``."""
    type_node = node.child_by_field_name("type")
    return java_parser.node_text(source, type_node) if type_node is not None else ""
def _type_simple_name(source: bytes, type_node) -> str | None:
    """Nom non paramétré d'un type AST : ``ProducerRecord`` pour
    ``ProducerRecord<String, Order>``.

    Avec tree-sitter-java, les arguments génériques sont des enfants du
    ``generic_type`` ; comparer le texte complet du nœud au nom du type
    manquait donc tous les ``new ProducerRecord<K, V>(...)``.
    """
    if type_node is None:
        return None
    text = java_parser.node_text(source, type_node)
    return text.split("<", 1)[0].rsplit(".", 1)[-1].strip() or None
def _declaration_anchor(node):
    """Ancre l'évidence d'un appel dans sa déclaration locale quand il y en a.

    Une invocation imbriquée comme ``builder.stream(...)`` peut commencer à
    la ligne suivant ``KStream<...> joined =``. L'inventaire historique
    pointait la déclaration entière ; conserver cette position évite des
    déplacements artificiels lors de la migration vers l'AST.
    """
    return java_parser.enclosing(node, "local_variable_declaration") or node
def _listener_payload_type(source: bytes, method_node) -> str | None:
    """Type de payload du premier paramètre utile d'un ``@KafkaListener``."""
    params = method_node.child_by_field_name("parameters")
    if params is None:
        return None
    for param in params.children:
        if param.type != "formal_parameter":
            continue
        if any(
            java_parser.annotation_name(ann, source) in {"Header", "Headers"}
            for ann in java_parser.annotations_of(param)
        ):
            continue
        type_node = param.child_by_field_name("type")
        if type_node is None:
            continue
        type_name = java_parser.node_text(source, type_node)
        if type_name in {"Acknowledgment", "Consumer", "ConsumerRecordMetadata"}:
            continue
        payload = _message_payload_type(type_name)
        if payload is not None:
            return payload
    return None
def _method_param_payload_type(source: bytes, method_node, var_name: str) -> str | None:
    params = method_node.child_by_field_name("parameters")
    if params is None:
        return None
    for param in params.children:
        if param.type != "formal_parameter":
            continue
        name_node = param.child_by_field_name("name")
        if name_node is None or java_parser.node_text(source, name_node) != var_name:
            continue
        type_node = param.child_by_field_name("type")
        return _message_payload_type(java_parser.node_text(source, type_node)) if type_node else None
    return None
def _declared_identifier_payload_type(source: bytes, invocation, var_name: str) -> str | None:
    """Resolve an identifier from its closest local declaration, then a field.

    This remains deliberately local to the enclosing Java method/class: an
    unrelated declaration in another method must never supply a Kafka DTO type.
    """
    method = java_parser.enclosing(invocation, "method_declaration")
    if method is None:
        return None
    local_declarations = []
    for declaration in java_parser.walk(method):
        if declaration.type != "local_variable_declaration" or declaration.start_byte > invocation.start_byte:
            continue
        if not any(
            java_parser.node_text(source, name) == var_name
            for declarator in declaration.children
            if declarator.type == "variable_declarator"
            if (name := declarator.child_by_field_name("name")) is not None
        ):
            continue
        local_declarations.append(declaration)
    if local_declarations:
        declaration = max(local_declarations, key=lambda item: item.start_byte)
        type_node = declaration.child_by_field_name("type")
        payload = _message_payload_type(java_parser.node_text(source, type_node)) if type_node else None
        if payload is not None:
            return payload
        for declarator in declaration.children:
            if declarator.type != "variable_declarator":
                continue
            name = declarator.child_by_field_name("name")
            if name is None or java_parser.node_text(source, name) != var_name:
                continue
            value = declarator.child_by_field_name("value")
            if value is not None and value.type == "object_creation_expression":
                return _message_payload_type(_object_creation_type(source, value))

    owner = java_parser.enclosing(method, "class_declaration") or java_parser.enclosing(method, "record_declaration")
    if owner is None:
        return None
    for declaration in java_parser.walk(owner):
        if declaration.type != "field_declaration":
            continue
        type_node = declaration.child_by_field_name("type")
        if type_node is None:
            continue
        for declarator in declaration.children:
            if declarator.type != "variable_declarator":
                continue
            name = declarator.child_by_field_name("name")
            if name is not None and java_parser.node_text(source, name) == var_name:
                return _message_payload_type(java_parser.node_text(source, type_node))
    return None
def _producer_send_payload_type(source: bytes, invocation) -> str | None:
    """Type de payload d'un ``send(topic, payload, ...)`` : 2e argument
    résolu contre une déclaration Java proche."""
    method = java_parser.enclosing(invocation, "method_declaration")
    if method is None:
        return None
    for arg in java_parser.argument_nodes(invocation)[1:]:
        if arg.type == "identifier":
            variable_name = java_parser.node_text(source, arg)
            payload = _method_param_payload_type(source, method, variable_name)
            if payload is not None:
                return payload
            payload = _declared_identifier_payload_type(source, invocation, variable_name)
            if payload is not None:
                return payload
        elif arg.type == "object_creation_expression":
            payload = _message_payload_type(_object_creation_type(source, arg))
            if payload is not None:
                return payload
    return None
def _kafka_endpoint(
    repo_root: Path, rel_path: str, source: bytes, node, role: str, framework: str,
    topic: str, topic_dynamic: bool, message_type: str | None, end_node=None,
) -> MessageEndpoint:
    start_line = node.start_point.row + 1
    end_node = end_node or node
    end_line = end_node.end_point.row + 1
    snippet = source[node.start_byte : end_node.end_byte].decode("utf-8", errors="replace")
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, rel_path, start_line, end_line),
        role=role,
        system="kafka",
        topic=topic,
        topic_dynamic=topic_dynamic,
        source="code",
        framework=framework,
        path=rel_path,
        start_line=start_line,
        end_line=end_line,
        snippet=snippet,
        module=_module_for_path(repo_root, rel_path),
        qualified_name=_java_qualified_name(str(repo_root), rel_path),
        message_type=message_type,
    )
def _message_builder_topic_for(
    source: bytes, send_invocation, var_name: str, repo_root: Path, rel_path: str
) -> tuple[str, bool] | None:
    """Topic posé par ``MessageBuilder...setHeader(TOPIC, ...).build()`` pour
    la variable ``var_name`` déclarée dans la même méthode que l'envoi.

    Scope-aware : la variable du message est locale à la méthode englobant le
    ``.send(msg)``, donc on cherche le ``variable_declarator`` correspondant
    dans cette méthode (et pas globalement — plusieurs méthodes peuvent
    réutiliser le même nom ``message``)."""
    method = java_parser.enclosing(send_invocation, "method_declaration")
    if method is None:
        return None
    for declarator in java_parser.walk(method):
        if declarator.type != "variable_declarator":
            continue
        name_node = declarator.child_by_field_name("name")
        if name_node is None or java_parser.node_text(source, name_node) != var_name:
            continue
        initializer = declarator.child_by_field_name("value")
        if initializer is None:
            continue
        for invocation in java_parser.walk(initializer):
            if invocation.type != "method_invocation":
                continue
            _, method_name, args = java_parser.invocation_parts(invocation, source)
            if method_name != "setHeader" or len(args) < 2:
                continue
            header = java_parser.node_text(source, args[0])
            if header == "TOPIC" or header.endswith(".TOPIC"):
                return _kafka_topic_from_value(args[1], source, repo_root, rel_path)
    return None
def _method_return_payload_type(source: bytes, method_node) -> str | None:
    """Type de payload déduit du type de retour de la méthode englobante
    (ex. ``KStream<Long, Order>`` -> ``Order``), via ``_message_payload_type``."""
    if method_node is None:
        return None
    type_node = method_node.child_by_field_name("type")
    if type_node is None:
        return None
    return _message_payload_type(java_parser.node_text(source, type_node))
def infer_kafka_endpoints(repo_root: Path, files: list[str] | None = None) -> list[MessageEndpoint]:
    """Découvre tous les endpoints Kafka depuis le code Java via tree-sitter.

    Source unique des endpoints Kafka (P2) : ``@KafkaListener`` (consume),
    ``KafkaTemplate.send/sendDefault`` (produce), ``new ProducerRecord<...>``
    (produce), ``MessageBuilder...setHeader(TOPIC,...).build()`` puis
    ``.send(msg)`` (produce), ``builder.stream(...)``/``KafkaConsumer.subscribe``
    (consume) et ``KStream.to(...)`` (produce). Le type de message est inféré
    depuis les signatures/generics de l'AST."""
    if files is None:
        candidate_files = [
            path.relative_to(repo_root).as_posix()
            for path in repo_root.rglob("*.java")
            if path.is_file()
        ]
    else:
        candidate_files = sorted(path for path in files if path.endswith(".java"))

    endpoints: dict[str, MessageEndpoint] = {}
    for rel_path in candidate_files:
        parsed = java_parser.parse_java(str(repo_root), rel_path)
        if parsed is None:
            continue
        source, root = parsed
        source_text = source.decode("utf-8", errors="replace")
        has_kafka_streams = "KStream" in source_text or "StreamsBuilder" in source_text
        has_kafka_consumer = "KafkaConsumer" in source_text
        has_stream_bridge = "StreamBridge" in source_text

        def add(node, role: str, framework: str, topic: str, dynamic: bool, message_type: str | None) -> None:
            endpoint = _kafka_endpoint(
                repo_root, rel_path, source, node, role, framework, topic, dynamic, message_type
            )
            endpoints[endpoint.id] = endpoint

        for method_node in java_parser.walk(root):
            if method_node.type != "method_declaration":
                continue
            listener_ann = next(
                (
                    ann
                    for ann in java_parser.annotations_of(method_node)
                    if java_parser.annotation_name(ann, source) == "KafkaListener"
                ),
                None,
            )
            if listener_ann is None:
                continue
            for topic, dynamic in _kafka_listener_topics(listener_ann, source, repo_root, rel_path):
                endpoint = _kafka_endpoint(
                    repo_root,
                    rel_path,
                    source,
                    listener_ann,
                    "consume",
                    "spring-kafka",
                    topic,
                    dynamic,
                    _listener_payload_type(source, method_node),
                    end_node=method_node,
                )
                endpoints[endpoint.id] = endpoint

        for node in java_parser.walk(root):
            if node.type == "method_invocation":
                object_node, method_name, args = java_parser.invocation_parts(node, source)
                receiver = _invocation_receiver(source, object_node)
                if method_name in {"send", "sendDefault"} and receiver and receiver.lower().endswith("kafkatemplate"):
                    if len(args) >= 2:
                        topic, dynamic = _kafka_topic_from_value(args[0], source, repo_root, rel_path)
                        add(node, "produce", "spring-kafka", topic, dynamic,
                            _producer_send_payload_type(source, node))
                    elif len(args) == 1 and args[0].type == "identifier":
                        built = _message_builder_topic_for(
                            source, node, java_parser.node_text(source, args[0]), repo_root, rel_path
                        )
                        if built is not None:
                            topic, dynamic = built
                            add(node, "produce", "spring-kafka", topic, dynamic, None)
                elif method_name == "send" and receiver and receiver.lower().endswith("streambridge") and has_stream_bridge and args:
                    topic, dynamic = _kafka_topic_from_value(args[0], source, repo_root, rel_path)
                    add(node, "produce", "spring-cloud-stream", topic, dynamic,
                        _producer_send_payload_type(source, node))
                elif method_name == "to" and has_kafka_streams and args:
                    topic, dynamic = _kafka_topic_from_value(args[0], source, repo_root, rel_path)
                    add(node, "produce", "kafka-streams", topic, dynamic,
                        _method_return_payload_type(source, java_parser.enclosing(node, "method_declaration")))
                elif method_name == "stream" and receiver == "builder" and has_kafka_streams and args:
                    topic, dynamic = _kafka_topic_from_value(args[0], source, repo_root, rel_path)
                    add(_declaration_anchor(node), "consume", "kafka-streams", topic, dynamic,
                        _method_return_payload_type(source, java_parser.enclosing(node, "method_declaration")))
                elif (
                    method_name == "subscribe"
                    and receiver
                    and receiver.lower().endswith("consumer")
                    and has_kafka_consumer
                    and args
                ):
                    topic, dynamic = _kafka_topic_from_value(args[0], source, repo_root, rel_path)
                    add(node, "consume", "kafka-clients", topic, dynamic, None)
            elif node.type == "object_creation_expression":
                type_node = node.child_by_field_name("type")
                if _type_simple_name(source, type_node) == "ProducerRecord":
                    first_arg = next(iter(java_parser.argument_nodes(node)), None)
                    topic, dynamic = _kafka_topic_from_value(first_arg, source, repo_root, rel_path)
                    add(node, "produce", "kafka-clients", topic, dynamic,
                        _message_payload_type(_object_creation_type(source, node)))
    return list(endpoints.values())
def _extract_kafka_topic(
    snippet: str, repo_root: Path, source_path: str | None = None
) -> tuple[str, bool]:
    """Renvoie (topic, dynamique). Un littéral `${propriete.imbriquee}`
    (placeholder Spring, ex. `@KafkaListener(topics = "${app.kafka.topics.
    orders}")`) n'est pas un nom de topic mais une clé de configuration :
    tentative de résolution via `resolve_spring_property` avant de retomber
    sur dynamique si la clé est introuvable — jamais résolu au hasard. Une
    variable (pas de littéral du tout, ex. `topics = ordersTopic`) est
    tentée contre les champs `@Value("${...}")` du même fichier source
    (`_resolve_value_annotated_variable`) avant d'abandonner en dynamique.

    BACKLOG Q25 : `KStream.to("topic")` (Kafka Streams) est souvent chaîné
    après un `.peek(...)` dont le lambda peut lui-même contenir un littéral
    (message de log) — le premier littéral du snippet n'est alors pas le
    topic. Un `.to("...")` capté explicitement prime sur la recherche
    générique du premier littéral."""
    streams_to_match = _KAFKA_STREAMS_TO_RE.search(snippet)
    if streams_to_match is not None:
        return streams_to_match.group(1), streams_to_match.group(2) is not None

    literal, dynamic = _find_first_literal(snippet)
    if literal is None:
        if source_path is not None:
            first_line = snippet.splitlines()[0] if snippet else ""
            var_match = _BARE_TOPIC_VAR_RE.search(first_line)
            if var_match is not None:
                resolved = _resolve_value_annotated_variable(
                    repo_root, source_path, var_match.group(1)
                )
                if resolved is not None:
                    return resolved, False
        return "<dynamic>", True

    reference = spring_topic_reference(literal)
    if reference is not None:
        resolved = resolve_spring_property(repo_root, reference.property_key, source_path)
        if resolved is not None:
            return resolved, False
        return reference.display_name, True

    return literal, dynamic
