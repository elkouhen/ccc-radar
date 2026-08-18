"""Primitives Java/texte partagées par les sous-modules `scanner` (BACKLOG-16).

Regroupe les helpers génériques réutilisés par l'inférence REST comme par
l'inférence Kafka : lecture de source Java, résolution de nom qualifié,
parsing de génériques, inférence de type de message, résolution de module
Maven/Gradle depuis un chemin, extraction du premier littéral, et
construction d'un ``MessageEndpoint`` (id, evidence)."""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

from systemlens import java_parser
from systemlens.gradle import gradle_service_for_path
from systemlens.maven import module_name_for_path
from systemlens.models import MessageEndpoint, compute_endpoint_id

def _read_snippet(repo_root: Path, rel_path: str, start_line: int, end_line: int) -> str:
    try:
        lines = (repo_root / rel_path).read_text(
            encoding="utf-8", errors="replace"
        ).splitlines()
    except (OSError, UnicodeError):
        return ""
    start_idx = max(start_line - 1, 0)
    end_idx = min(end_line, len(lines))
    return "\n".join(lines[start_idx:end_idx])


# BACKLOG-13 M1 : module Maven + nom qualifié Java attribués à chaque
# finding/endpoint indexé, en plus de `path` — permet de grouper par module
# sans fédération multi-dépôts (voir `graph.group_endpoints_by_module`).


@lru_cache(maxsize=2048)
def _java_source(repo_root_str: str, rel_path: str) -> str:
    if not rel_path.endswith(".java"):
        return ""
    try:
        return (Path(repo_root_str) / rel_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


@lru_cache(maxsize=2048)
def _java_qualified_name(repo_root_str: str, rel_path: str) -> str | None:
    """Nom Java qualifié (package + stem du fichier) d'un `.java`.

    Le nom de classe suit la convention historique (stem du fichier) ; le
    package est lu sur la déclaration `package` de l'AST tree-sitter (plus
    fiable qu'un regex ancré sur le texte source)."""
    if not rel_path.endswith(".java"):
        return None
    class_name = Path(rel_path).stem
    parsed = java_parser.parse_java(repo_root_str, rel_path)
    if parsed is None:
        return class_name
    source, root = parsed
    for node in java_parser.walk(root):
        if node.type == "package_declaration":
            package = (
                java_parser.node_text(source, node)[len("package") :]
                .strip()
                .rstrip(";")
                .strip()
            )
            return f"{package}.{class_name}" if package else class_name
    return class_name
def _split_java_type_arguments(value: str) -> list[str]:
    arguments: list[str] = []
    depth = 0
    start = 0
    for index, character in enumerate(value):
        if character == "<":
            depth += 1
        elif character == ">":
            depth -= 1
        elif character == "," and depth == 0:
            arguments.append(value[start:index].strip())
            start = index + 1
    arguments.append(value[start:].strip())
    return [argument for argument in arguments if argument]
def _generic_arguments_after(source: str, open_angle: int) -> tuple[list[str], int] | None:
    depth = 0
    for index in range(open_angle, len(source)):
        character = source[index]
        if character == "<":
            depth += 1
        elif character == ">":
            depth -= 1
            if depth == 0:
                return _split_java_type_arguments(source[open_angle + 1:index]), index
    return None
def _generic_value_type(
    source: str, container: str, variable: str | None = None, before: int | None = None
) -> str | None:
    candidates: list[tuple[int, str]] = []
    pattern = re.compile(rf"\b{re.escape(container)}\s*<")
    for match in pattern.finditer(source):
        parsed = _generic_arguments_after(source, match.end() - 1)
        if parsed is None:
            continue
        arguments, end = parsed
        if not arguments:
            continue
        if variable is not None:
            declaration = re.match(rf"\s+{re.escape(variable)}\b", source[end + 1:])
            if declaration is None:
                continue
        candidates.append((match.start(), arguments[-1]))
    if not candidates:
        return None
    if before is None:
        return candidates[-1][1]
    preceding = [candidate for candidate in candidates if candidate[0] <= before]
    return (preceding or candidates)[-1][1]
def _message_payload_type(declared_type: str | None) -> str | None:
    if declared_type is None:
        return None
    normalized = re.sub(r"@\w+(?:\([^)]*\))?\s*", "", declared_type)
    normalized = re.sub(r"\b(?:final|volatile)\b\s*", "", normalized).strip()
    if not normalized or normalized in {"var", "?"}:
        return None
    for container in ("ConsumerRecord", "Message", "KafkaTemplate", "KafkaConsumer", "ProducerRecord", "KStream", "KTable"):
        match = re.fullmatch(rf"(?:[\w.]+\.)?{container}\s*<(.*)>", normalized)
        if match is not None:
            arguments = _split_java_type_arguments(match.group(1))
            return _message_payload_type(arguments[-1] if arguments else None)
    return normalized.replace("...", "[]")
def _first_listener_payload_type(source: str, start_line: int) -> str | None:
    lines = source.splitlines()
    context = "\n".join(lines[max(0, start_line - 1): min(len(lines), start_line + 16)])
    # `public void consume(Message message)` is the project convention. Keep
    # the generic listener fallback for pre-existing Spring listener styles.
    method_patterns = (
        r"\bpublic\s+void\s+consume\s*\(([^()]*)\)\s*(?:throws[^\{]+)?\{",
        r"\b(?:public|protected|private)?\s*void\s+\w+\s*\(([^()]*)\)\s*(?:throws[^\{]+)?\{",
    )
    for pattern in method_patterns:
        for match in re.finditer(pattern, context, re.DOTALL):
            for parameter in _split_java_type_arguments(match.group(1)):
                if "@Header" in parameter or "@Headers" in parameter:
                    continue
                cleaned = re.sub(r"@\w+(?:\([^)]*\))?\s*", "", parameter).strip()
                parts = cleaned.rsplit(None, 1)
                if len(parts) != 2:
                    continue
                payload_type = _message_payload_type(parts[0])
                if payload_type and payload_type not in {"Acknowledgment", "Consumer", "ConsumerRecordMetadata"}:
                    return payload_type
    return None
def _receiver_name(snippet: str, method: str) -> str | None:
    match = re.search(rf"\b([A-Za-z_]\w*)\s*\.{method}\s*\(", snippet)
    return match.group(1) if match else None
def _method_parameter_type(source: str, before: int, parameter_name: str) -> str | None:
    """Find a Java method parameter type for a call occurring before ``before``."""
    signatures = list(
        re.finditer(
            r"\b(?:public|protected|private)?\s*(?:static\s+)?(?:final\s+)?"
            r"[\w.$<>, ?\[\]]+\s+\w+\s*\(([^()]*)\)\s*(?:throws[^\{]+)?\{",
            source[:before],
            re.DOTALL,
        )
    )
    if not signatures:
        return None
    for parameter in _split_java_type_arguments(signatures[-1].group(1)):
        cleaned = re.sub(r"@\w+(?:\([^)]*\))?\s*", "", parameter).strip()
        parts = cleaned.rsplit(None, 1)
        if len(parts) == 2 and parts[1] == parameter_name:
            return _message_payload_type(parts[0])
    return None
def _producer_argument_type(source: str, snippet: str, before: int) -> str | None:
    """Infer a producer payload from ``send(topic, payload)`` method input."""
    constructed = re.search(r",\s*new\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(", snippet)
    if constructed is not None:
        return constructed.group(1)
    argument = re.search(r",\s*([A-Za-z_]\w*)\s*(?:,|\))", snippet)
    if argument is None:
        return None
    return _method_parameter_type(source, before, argument.group(1))
def _infer_kafka_message_type(
    repo_root: Path,
    rel_path: str,
    start_line: int,
    role: str,
    framework: str | None,
    snippet: str,
) -> str | None:
    """Infer a Kafka payload type only from an explicit Java signature.

    The result is the source-level Java type (for example `OrderCreated`), not
    a serializer guess. Returning `None` is preferable to inventing a type.
    """
    source = _java_source(str(repo_root), rel_path)
    if not source:
        return None
    line_offset = sum(len(line) for line in source.splitlines(keepends=True)[:start_line])

    if role == "consume" and (framework == "spring-kafka" or "@KafkaListener" in snippet):
        payload_type = _first_listener_payload_type(source, start_line)
        if payload_type:
            return payload_type
    if framework == "kafka-streams":
        payload_type = _generic_value_type(source, "KStream", before=line_offset)
        return _message_payload_type(payload_type)
    if role == "consume" and framework == "kafka-clients":
        receiver = _receiver_name(snippet, "subscribe")
        payload_type = _generic_value_type(source, "KafkaConsumer", receiver, line_offset)
        return _message_payload_type(payload_type)
    if role == "produce":
        record_match = re.search(r"\b(?:new\s+)?ProducerRecord\s*<", snippet)
        if record_match is not None:
            parsed = _generic_arguments_after(snippet, record_match.end() - 1)
            if parsed is not None:
                arguments, _ = parsed
                return _message_payload_type(arguments[-1] if arguments else None)
        receiver = _receiver_name(snippet, "send") or _receiver_name(snippet, "sendDefault")
        payload_type = _generic_value_type(source, "KafkaTemplate", receiver, line_offset)
        if payload_type:
            return _message_payload_type(payload_type)
        payload_type = _producer_argument_type(source, snippet, line_offset)
        if payload_type:
            return payload_type
    return None
def _module_for_path(repo_root: Path, rel_path: str) -> str | None:
    """Module Maven (`pom.xml`) en priorité (choix explicite, ADR-32) ;
    repli sur la détection de service Gradle (BACKLOG-15 H1, ADR-33) quand
    aucun `pom.xml` n'est trouvé — un repo purement Maven ou purement
    Gradle n'a jamais les deux à interroger, un repo mixte essaie les deux
    dans cet ordre par fichier."""
    return module_name_for_path(repo_root, rel_path) or gradle_service_for_path(
        repo_root, rel_path
    )


# BACKLOG-10 K2/K11 : règles d'inventaire d'endpoints (`metadata.category:
# endpoint-inventory`) — le rôle/système/méthode HTTP viennent des métadonnées
# de la règle (fixes par construction, une règle = une méthode), le
# topic/chemin vient d'une extraction best-effort sur le snippet
# (l'analyse ne dépend d'aucun moteur de règles externe, voir ADR-26).
_QUOTED_STRING_RE = re.compile(r"f?([\"'])(.*?)\1")
def _find_first_literal(snippet: str) -> tuple[str | None, bool]:
    """Cherche le premier texte entre guillemets dans le snippet (annotation
    ou appel), en parcourant ses lignes dans l'ordre — une chaîne fluent
    `WebClient` peut répartir `.get()` et `.uri(...)` sur deux lignes
    (BACKLOG-10 K13) ; le snippet est de toute façon borné exactement par
    `start_line`/`end_line` du nœud AST, jamais de code hors de
    l'appel. Renvoie (littéral, concaténé) ; concaténé=True si
    immédiatement suivi de `+` sur la même ligne (avant la virgule/
    parenthèse fermante), ou si aucun littéral n'est trouvé."""
    for line in snippet.splitlines():
        match = _QUOTED_STRING_RE.search(line)
        if match is not None:
            literal = match.group(2)
            remainder = line[match.end() :].lstrip()
            return literal, remainder.startswith("+")
    return None, True


# BACKLOG Q24 : l'inventaire d'endpoints est borné à la
# méthode annotée (`pattern: @GetMapping(...) $RET $METHOD(...) { ... }`) —
# elle ne voit jamais le `@RequestMapping` porté par la classe englobante,
# alors que Spring MVC le préfixe silencieusement au chemin de la méthode.
# Conséquence observée sur des repos réels (spring-petclinic-microservices,
# microservices-kafka-mq) : soit le chemin sort sous-qualifié (méthode avec
# valeur explicite, préfixe de classe ignoré), soit il sort `<dynamic>`
# (méthode sans valeur explicite : `@GetMapping` seul hérite du chemin de
# classe côté Spring, mais aucun littéral n'est disponible) — dans les
# deux cas, la corrélation caller/callee de `graph.paths_match` échoue sur
# des appels réels. Best-effort ligne par ligne (ADR-26, pas d'AST) : la
# classe/interface la plus proche au-dessus de la méthode, avec ses lignes
# d'annotation contiguës.
def _build_endpoint(
    repo_root: Path,
    rel_path: str,
    start_line: int,
    end_line: int,
    role: str,
    system: str,
    topic: str,
    framework: str,
    snippet: str,
    topic_dynamic: bool = False,
) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, rel_path, start_line, end_line),
        role=role,
        system=system,
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
        message_type=(
            _infer_kafka_message_type(repo_root, rel_path, start_line, role, framework, snippet)
            if system == "kafka"
            else None
        ),
    )


def _invocation_receiver(source: bytes, object_node) -> str | None:
    """Trailing identifier of a call receiver: ``kafkaTemplate`` ou
    ``this.kafkaTemplate`` -> ``kafkaTemplate``."""
    if object_node is None:
        return None
    if object_node.type == "identifier":
        return java_parser.node_text(source, object_node)
    if object_node.type == "field_access":
        field = object_node.child_by_field_name("field")
        return java_parser.node_text(source, field) if field is not None else None
    return None
