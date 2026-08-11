import json
import os
import re
import sys
import time
from dataclasses import replace
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from ccc_radar import gradle as gradle_module
from ccc_radar import java_parser
from ccc_radar import maven as maven_module
from ccc_radar.gradle import gradle_service_for_path
from ccc_radar.maven import module_name_for_path
from ccc_radar.modules import discover_rest_controllers, maven_module_dependencies
from ccc_radar.models import MessageEndpoint, compute_endpoint_id
from ccc_radar.topic_expressions import spring_topic_reference

SEVERITY_ORDER = ["INFO", "WARNING", "ERROR"]

def _trace(stage: str, **fields: object) -> None:
    """Émet des traces opt-in de l'inventaire REST (`CCCR_TRACE=1`)."""
    if os.environ.get("CCCR_TRACE") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(
        f"CCCR_TRACE ts={time.monotonic():.6f} stage={stage} {details}".rstrip(),
        file=sys.stderr,
        flush=True,
    )


def _trace_rest_client(stage: str, **fields: object) -> None:
    """Trace exhaustive de la recherche de clients API.

    Activée séparément avec `CCCR_TRACE_REST_CLIENTS=1`, afin d'éviter le
    volume des fichiers Java parcourus dans la trace générale `CCCR_TRACE`.
    """
    if os.environ.get("CCCR_TRACE_REST_CLIENTS") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(
        f"CCCR_TRACE_REST_CLIENTS ts={time.monotonic():.6f} stage={stage} {details}".rstrip(),
        file=sys.stderr,
        flush=True,
    )


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
_PROPERTY_PLACEHOLDER_RE = re.compile(r"^\$\{([^}]+)\}$")
_MULTI_SLASH_RE = re.compile(r"/{2,}")

# BACKLOG-10 K2 (reliquat) : `@KafkaListener(topics = someVar)` ou
# `kafkaTemplate.send(someVar, ...)` où `someVar` n'est pas un littéral mais
# une variable alimentée ailleurs dans la classe par `@Value("${...}")` —
# retrouver le nom de variable en jeu (pas son contenu, absent du snippet)
# avant de la résoudre contre les champs `@Value` du fichier source.
_BARE_TOPIC_VAR_RE = re.compile(
    r"(?:topics\s*=\s*|\.send\(\s*|ProducerRecord\(\s*)([A-Za-z_]\w*)\s*[,)]"
)
# BACKLOG Q25 : `KStream.to("topic")`/`.to("topic", Produced.with(...))` —
# le topic suit directement `.to(`, contrairement au premier littéral
# quelconque du snippet (qui peut appartenir à un `.peek(...)` chaîné avant).
_KAFKA_STREAMS_TO_RE = re.compile(r'\.to\(\s*"([^"]*)"\s*(\+)?')

_SPRING_BASE_FILENAMES = (
    "application.yml",
    "application.yaml",
    "application.properties",
    "bootstrap.yml",
    "bootstrap.yaml",
    "bootstrap.properties",
)
_SPRING_PROFILE_PATTERNS = (
    "application-*.yml",
    "application-*.yaml",
    "application-*.properties",
    "bootstrap-*.yml",
    "bootstrap-*.yaml",
    "bootstrap-*.properties",
)
_SPRING_CLOUD_CONFIG_DIR_PATTERNS = (
    "src/main/resources/configurations",
    "configurations",
)


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
_MAPPING_ANNOTATION_RE = re.compile(r"@\w+Mapping\s*(?:\(([^)]*)\))?")
_MAPPING_ANNOTATION_BLOCK_RE = re.compile(r"@\w+Mapping\s*(?:\((.*?)\))?", re.DOTALL)
_REQUEST_MAPPING_BLOCK_RE = re.compile(r"@RequestMapping\s*(?:\((.*?)\))?", re.DOTALL)
_REQUEST_PARAM_RE = re.compile(
    r"@RequestParam\s*(?:\((.*?)\))?\s+[\w<>\[\], ?]+\s+(\w+)", re.DOTALL
)
_NON_PATH_MAPPING_ATTRS = {"method", "produces", "consumes", "headers", "params", "name"}
_FEIGN_CLIENT_RE = re.compile(r"@FeignClient\s*\((.*?)\)", re.DOTALL)
_NAMED_STRING_ARG_RE = re.compile(r'(\w+)\s*=\s*"([^"]*)"')
_OPENAPI_HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})
_REST_TEMPLATE_CALL_RE = re.compile(
    r"\.(getForObject|getForEntity|postForObject|postForEntity|put|delete)\(\s*(.+?)\s*(?:,|\))",
    re.DOTALL,
)
_REST_TEMPLATE_EXCHANGE_RE = re.compile(
    r"\.exchange\(\s*(.+?)\s*,\s*(?:HttpMethod\.)?([A-Z]+)\s*,",
    re.DOTALL,
)
_URI_CALL_RE = re.compile(r"\.uri\s*\(")
_GATEWAY_ROUTE_PATH_RE = re.compile(r'\.path\(\s*"([^"]+)"\s*\)')
_GATEWAY_ROUTE_METHOD_RE = re.compile(r'\.method\(\s*(?:"([A-Z]+)"|HttpMethod\.([A-Z]+))\s*\)')
_GATEWAY_ROUTE_URI_RE = re.compile(r"\.uri\(\s*([^)]+?)\s*\)", re.DOTALL)
_ROUTER_FUNCTION_ROUTE_RE = re.compile(
    r"(?:RouterFunctions\.)?route\(\s*(?:RequestPredicates\.)?([A-Z]+)\(\s*\"([^\"]+)\"\s*\)",
    re.DOTALL,
)
_ROUTER_FUNCTION_AND_ROUTE_RE = re.compile(
    r"\.andRoute\(\s*(?:RequestPredicates\.)?([A-Z]+)\(\s*\"([^\"]+)\"\s*\)",
    re.DOTALL,
)
_MARKDOWN_MODULE_HEADING_RE = re.compile(r"^\s*###\s+(.+?)\s*$")
_MARKDOWN_BOLD_SECTION_RE = re.compile(r"^\s*\*\*(Producer|Consumer)\*\*\s*$", re.IGNORECASE)
_MARKDOWN_CODE_RE = re.compile(r"^`(.*)`$")


def _clean_markdown_table_cell(value: str) -> str:
    cleaned = value.strip()
    code = _MARKDOWN_CODE_RE.match(cleaned)
    if code is not None:
        return code.group(1).strip()
    return cleaned


def _normalize_markdown_header(value: str) -> str:
    normalized = _clean_markdown_table_cell(value).lower()
    normalized = normalized.replace("é", "e").replace("è", "e").replace("ê", "e")
    return " ".join(normalized.split())


def _split_markdown_table_row(line: str) -> list[str] | None:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return None
    cells = [_clean_markdown_table_cell(cell) for cell in stripped.strip("|").split("|")]
    return cells


def _is_markdown_separator_row(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell.strip()) for cell in cells)


def _mapping_args_have_only_non_path_attrs(args: str) -> bool:
    """`True` si les arguments d'une annotation `@XMapping(...)` ne portent
    aucun chemin explicite (vide, ou uniquement `method=`/`produces=`/...) —
    dans ce cas le chemin effectif de la méthode est vide (hérite du préfixe
    de classe), pas inconnu."""
    args = args.strip()
    if not args:
        return True
    for part in args.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            return False  # argument positionnel : c'est le chemin lui-même
        key = part.split("=", 1)[0].strip()
        if key not in _NON_PATH_MAPPING_ATTRS:
            return False
    return True


def _next_declaration_line(lines: list[str], start_idx: int) -> int | None:
    for idx in range(start_idx, len(lines)):
        stripped = lines[idx].strip()
        if not stripped or stripped.startswith("@"):
            continue
        return idx
    return None


def _named_string_arg(args: str, key: str) -> str | None:
    for match in _NAMED_STRING_ARG_RE.finditer(args):
        if match.group(1) == key:
            return match.group(2)
    return None


def _split_java_concat(expr: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    escaped = False
    for ch in expr:
        if quote is not None:
            current.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in ('"', "'"):
            quote = ch
            current.append(ch)
            continue
        if ch == "+":
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _resolve_rest_path_expression(
    expr: str, repo_root: Path, source_path: str, *, preserve_dynamic_segments: bool = False
) -> tuple[str, bool]:
    resolved_parts: list[str] = []
    dynamic = False
    for part in _split_java_concat(expr.strip()):
        if len(part) >= 2 and part[0] == part[-1] == '"':
            literal = part[1:-1]
            placeholder = _PROPERTY_PLACEHOLDER_RE.match(literal)
            if placeholder is not None:
                resolved = resolve_spring_property(repo_root, placeholder.group(1), source_path)
                if resolved is None:
                    dynamic = True
                    continue
                resolved_parts.append(resolved)
                continue
            resolved_parts.append(literal)
            continue
        if re.fullmatch(r"[A-Za-z_]\w*", part):
            resolved = _resolve_value_annotated_variable(repo_root, source_path, part)
            if resolved is None:
                dynamic = True
                if preserve_dynamic_segments and resolved_parts:
                    resolved_parts.append(f"{{{part}}}")
                continue
            resolved_parts.append(resolved)
            continue
        dynamic = True
        if preserve_dynamic_segments and resolved_parts:
            resolved_parts.append("{dynamic}")
    raw = "".join(resolved_parts).strip()
    if not raw:
        return "<dynamic>", True
    return _normalize_rest_path(raw), dynamic


@lru_cache(maxsize=1024)
def _class_base_path(repo_root_str: str, source_path: str, start_line: int) -> tuple[str, bool]:
    """Chemin `@RequestMapping` de la classe/interface qui englobe la
    méthode trouvée à `start_line` — renvoie (préfixe, dynamique) ;
    (`""`, `False`) si aucune classe englobante ou aucun `@RequestMapping`
    de classe (rien à préfixer, pas une valeur inconnue)."""
    parsed = java_parser.parse_java(repo_root_str, source_path)
    if parsed is None:
        return "", False
    source, root = parsed
    method = next(
        (
            node for node in java_parser.walk(root)
            if node.type == "method_declaration"
            and any(
                ann.start_point.row + 1 <= start_line <= node.end_point.row + 1
                for ann in java_parser.annotations_of(node)
            )
        ),
        None,
    )
    if method is None:
        return "", False
    owner = java_parser.enclosing(
        method, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration"
    )
    if owner is None:
        return "", False
    mapping = next(
        (ann for ann in java_parser.annotations_of(owner)
         if java_parser.annotation_name(ann, source) == "RequestMapping"),
        None,
    )
    if mapping is not None:
        return _ast_mapping_value(mapping, source, Path(repo_root_str), source_path)
    feign = next(
        (ann for ann in java_parser.annotations_of(owner)
         if java_parser.annotation_name(ann, source) == "FeignClient"),
        None,
    )
    if feign is not None:
        return _ast_feign_base(feign, source, Path(repo_root_str), source_path)
    return "", False


def _extract_rest_path(
    snippet: str,
    repo_root: Path | None = None,
    source_path: str | None = None,
    start_line: int | None = None,
) -> tuple[str, bool]:
    """Renvoie (chemin, dynamique) — jamais résolu silencieusement (même
    esprit que `topic_dynamic` en K2). Fusionne le préfixe `@RequestMapping`
    de la classe englobante (Q24) quand `repo_root`/`source_path`/
    `start_line` sont fournis."""
    prefix, prefix_dynamic = "", False
    if repo_root is not None and source_path is not None and start_line is not None:
        prefix, prefix_dynamic = _class_base_path(str(repo_root), source_path, start_line)

    lines = snippet.splitlines()
    decl_idx = _next_declaration_line(lines, 0)
    annotation_block = "\n".join(lines[:decl_idx]) if decl_idx is not None else snippet
    match = _MAPPING_ANNOTATION_BLOCK_RE.search(annotation_block)
    mapping_args = (match.group(1) or "") if match is not None else None

    if mapping_args is not None:
        literal, method_dynamic = _find_first_literal(mapping_args)
        if literal is None:
            if not _mapping_args_have_only_non_path_attrs(mapping_args or ""):
                return "<dynamic>", True
            literal, method_dynamic = "", False
    else:
        literal, method_dynamic = _find_first_literal(snippet)
        if literal is None:
            return "<dynamic>", True

    if prefix_dynamic:
        return "<dynamic>", True

    method_path = _normalize_rest_path(literal) if literal else "/"
    route = method_path if not prefix else _join_rest_paths(_normalize_rest_path(prefix), method_path)
    query_params = _extract_request_param_names(snippet)
    if query_params:
        route = _with_query_params(route, query_params)
    return route, method_dynamic


def _extract_request_param_names(snippet: str) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for match in _REQUEST_PARAM_RE.finditer(snippet):
        args = match.group(1) or ""
        name = (
            _named_string_arg(args, "name")
            or _named_string_arg(args, "value")
            or _find_first_literal(args)[0]
            or match.group(2)
        )
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _with_query_params(route: str, params: list[str]) -> str:
    if not params or route == "<dynamic>":
        return route
    return f"{route}?{'&'.join(params)}"


def _join_rest_paths(prefix: str, suffix: str) -> str:
    """Assemble deux chemins déjà normalisés (slash de tête unique) sans
    jamais repasser par l'heuristique d'URL protocole-relatif de
    `_normalize_rest_path` : une simple concaténation `"" + "/" +
    "/orders/{id}"` produit `"//orders/{id}"`, que `urlsplit` interprète à
    tort comme `http://orders/{id}` (`orders` avalé comme nom d'hôte)."""
    segments = [s for s in (prefix.strip("/"), suffix.strip("/")) if s]
    return "/" + "/".join(segments) if segments else "/"


def _normalize_rest_path(literal: str) -> str:
    normalized = literal.strip()
    if not normalized:
        return "/"
    if normalized.startswith("//"):
        normalized = urlsplit(f"http:{normalized}").path or "/"
    elif "://" in normalized:
        normalized = urlsplit(normalized).path or "/"
    normalized = normalized.split("?", 1)[0].split("#", 1)[0]
    normalized = _MULTI_SLASH_RE.sub("/", normalized)
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return normalized or "/"


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


def _flatten_properties(data: object, prefix: str = "") -> dict[str, str]:
    flat: dict[str, str] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else str(key)
            flat.update(_flatten_properties(value, full_key))
    elif isinstance(data, (str, int, float, bool)):
        flat[prefix] = str(data)
    return flat


def _parse_dotted_properties_file(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("!"):
            continue
        sep_index = min(
            (i for i in (stripped.find("="), stripped.find(":")) if i != -1), default=-1
        )
        if sep_index == -1:
            continue
        key, value = stripped[:sep_index], stripped[sep_index + 1 :]
        result[key.strip()] = value.strip()
    return result


def _candidate_spring_roots(repo_root: Path, source_path: str | None) -> list[Path]:
    if source_path is None:
        return [repo_root]
    source_abs = (repo_root / source_path).resolve()
    roots: list[Path] = []
    for candidate in [source_abs.parent, *source_abs.parents]:
        if candidate == repo_root or repo_root in candidate.parents:
            roots.append(candidate)
        if candidate == repo_root:
            break
    if repo_root not in roots:
        roots.append(repo_root)
    return roots


def _discover_spring_property_files(
    repo_root_str: str, source_path: str | None
) -> tuple[str, ...]:
    repo_root = Path(repo_root_str)
    discovered: list[str] = []
    seen: set[Path] = set()

    for root in _candidate_spring_roots(repo_root, source_path):
        for config_dir in (root / "src" / "main" / "resources", root):
            for filename in _SPRING_BASE_FILENAMES:
                candidate = config_dir / filename
                if candidate.is_file() and candidate not in seen:
                    seen.add(candidate)
                    discovered.append(str(candidate))
            for pattern in _SPRING_PROFILE_PATTERNS:
                for candidate in sorted(config_dir.glob(pattern)):
                    if candidate.is_file() and candidate not in seen:
                        seen.add(candidate)
                        discovered.append(str(candidate))
    for candidate in _discover_spring_cloud_config_files(repo_root, source_path):
        if candidate not in seen:
            seen.add(candidate)
            discovered.append(str(candidate))
    return tuple(discovered)


def _local_spring_application_names(repo_root: Path, source_path: str | None) -> tuple[str, ...]:
    names: list[str] = []
    seen: set[str] = set()
    for root in _candidate_spring_roots(repo_root, source_path):
        for config_dir in (root / "src" / "main" / "resources", root):
            for filename in _SPRING_BASE_FILENAMES:
                candidate = config_dir / filename
                if not candidate.is_file():
                    continue
                name = _load_flat_spring_properties(str(candidate)).get("spring.application.name")
                if name and name not in seen:
                    seen.add(name)
                    names.append(name)
            for pattern in _SPRING_PROFILE_PATTERNS:
                for candidate in sorted(config_dir.glob(pattern)):
                    if not candidate.is_file():
                        continue
                    name = _load_flat_spring_properties(str(candidate)).get("spring.application.name")
                    if name and name not in seen:
                        seen.add(name)
                        names.append(name)
    return tuple(names)


def _discover_spring_cloud_config_files(
    repo_root: Path, source_path: str | None
) -> tuple[Path, ...]:
    discovered: list[Path] = []
    seen: set[Path] = set()
    for app_name in _local_spring_application_names(repo_root, source_path):
        for config_dir_pattern in _SPRING_CLOUD_CONFIG_DIR_PATTERNS:
            for suffix in (".yml", ".yaml", ".properties"):
                for candidate in sorted(
                    repo_root.glob(f"**/{config_dir_pattern}/{app_name}{suffix}")
                ):
                    if candidate.is_file() and candidate not in seen:
                        seen.add(candidate)
                        discovered.append(candidate)
    return tuple(discovered)


@lru_cache(maxsize=512)
def _load_flat_spring_properties(path_str: str) -> dict[str, str]:
    path = Path(path_str)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}

    if path.suffix == ".properties":
        return _parse_dotted_properties_file(text)
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError:
        return {}
    return _flatten_properties(data or {})


def resolve_spring_property(
    repo_root: Path, property_key: str, source_path: str | None = None
) -> str | None:
    """Cherche `property_key` (ex. `app.kafka.topics.orders`, ou
    `prop:default` — syntaxe de valeur par défaut Spring) dans les fichiers
    de configuration Spring Boot conventionnels du repo. La recherche est
    best-effort mais orientée microservice : on essaie d'abord les configs du
    module contenant `source_path`, puis celles du repo parent ; les fichiers
    sont parsés une seule fois par process via cache."""
    key, _, default = property_key.partition(":")
    for path_str in _discover_spring_property_files(str(repo_root), source_path):
        flat = _load_flat_spring_properties(path_str)
        if key in flat:
            return flat[key]
    return default or None


def _infer_generic_request_mapping_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Infère les annotations de routes Spring et Feign depuis l'AST Java.

    Les anciens parcours ligne par ligne échouaient dès qu'une annotation ou
    une signature était répartie sur plusieurs lignes, ou qu'un commentaire
    imitait une annotation. Ici Tree-sitter associe directement l'annotation
    à sa ``method_declaration``.
    """
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    endpoints: list[MessageEndpoint] = []
    for method in java_parser.walk(root):
        if method.type != "method_declaration":
            continue
        owner = java_parser.enclosing(
            method, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration"
        )
        if owner is None:
            continue
        feign = next(
            (ann for ann in java_parser.annotations_of(owner)
             if java_parser.annotation_name(ann, source) == "FeignClient"),
            None,
        )
        for annotation in java_parser.annotations_of(method):
            methods = _ast_mapping_methods(annotation, source)
            if not methods:
                continue
            route, dynamic = _ast_mapping_route(
                annotation, method, source, repo_root, rel_path, feign
            )
            for http_method in methods:
                endpoints.append(
                    _build_endpoint(
                        repo_root, rel_path, annotation.start_point.row + 1,
                        method.end_point.row + 1,
                        "call" if feign is not None else "serve", "rest",
                        f"{http_method} {route}", "feign" if feign is not None else "spring",
                        java_parser.node_text(source, method), topic_dynamic=dynamic,
                    )
                )
    return endpoints


def _ast_mapping_value(
    annotation, source: bytes, repo_root: Path | None = None, rel_path: str | None = None
) -> tuple[str, bool]:
    """Return a mapping annotation's explicit path, if it has one.

    An annotation with only non-path attributes has the meaningful empty path
    (it inherits its class route); an expression that cannot be resolved is
    dynamic.
    """
    value = (
        java_parser.annotation_argument(annotation, source, key="path")
        or java_parser.annotation_argument(annotation, source, key="value")
        or java_parser.annotation_argument(annotation, source)
    )
    if value is None:
        return "", False
    if value.type != "string_literal":
        return "", True
    if repo_root is not None and rel_path is not None:
        return _resolve_rest_path_expression(
            java_parser.node_text(source, value), repo_root, rel_path, preserve_dynamic_segments=True
        )
    literal = java_parser.string_value(value, source)
    return literal or "", False


def _ast_mapping_methods(annotation, source: bytes) -> list[str]:
    """HTTP methods represented by a Spring mapping annotation."""
    name = java_parser.annotation_name(annotation, source)
    dedicated = {
        "GetMapping": "GET", "PostMapping": "POST", "PutMapping": "PUT",
        "DeleteMapping": "DELETE", "PatchMapping": "PATCH",
    }
    if name in dedicated:
        return [dedicated[name]]
    if name != "RequestMapping":
        return []
    value = java_parser.annotation_argument(annotation, source, key="method")
    if value is None:
        return ["ANY"]
    methods: list[str] = []
    for node in java_parser.walk(value):
        if node.type in {"identifier", "scoped_identifier", "field_access"}:
            candidate = java_parser.node_text(source, node).rsplit(".", 1)[-1]
            if candidate in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
                methods.append(candidate)
    return list(dict.fromkeys(methods))


def _ast_mapping_route(
    annotation, method, source: bytes, repo_root: Path, rel_path: str, feign=None
) -> tuple[str, bool]:
    path, dynamic = _ast_mapping_value(annotation, source, repo_root, rel_path)
    owner = java_parser.enclosing(
        method, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration"
    )
    prefix, prefix_dynamic = "", False
    if owner is not None:
        class_mapping = next(
            (
                ann for ann in java_parser.annotations_of(owner)
                if java_parser.annotation_name(ann, source) == "RequestMapping"
            ),
            None,
        )
        if class_mapping is not None:
            prefix, prefix_dynamic = _ast_mapping_value(class_mapping, source, repo_root, rel_path)
        elif feign is not None:
            prefix, prefix_dynamic = _ast_feign_base(feign, source, repo_root, rel_path)
    if dynamic or prefix_dynamic:
        return "<dynamic>", True
    route = _join_rest_paths(_normalize_rest_path(prefix), _normalize_rest_path(path))
    params = _ast_request_param_names(method, source)
    return _with_query_params(route, params), False


def _ast_feign_base(annotation, source: bytes, repo_root: Path, rel_path: str) -> tuple[str, bool]:
    """Resolve Feign's optional URL/path base without treating ``name`` as a route."""
    value = (
        java_parser.annotation_argument(annotation, source, key="url")
        or java_parser.annotation_argument(annotation, source, key="path")
    )
    if value is None:
        return "", False
    if value.type != "string_literal":
        return "", True
    return _resolve_rest_path_expression(
        java_parser.node_text(source, value), repo_root, rel_path, preserve_dynamic_segments=True
    )


def _ast_request_param_names(method, source: bytes) -> list[str]:
    names: list[str] = []
    params = method.child_by_field_name("parameters")
    if params is None:
        return names
    for parameter in params.children:
        if parameter.type != "formal_parameter":
            continue
        annotation = next(
            (
                ann for ann in java_parser.annotations_of(parameter)
                if java_parser.annotation_name(ann, source) == "RequestParam"
            ),
            None,
        )
        if annotation is None:
            continue
        value = (
            java_parser.annotation_argument(annotation, source, key="name")
            or java_parser.annotation_argument(annotation, source, key="value")
            or java_parser.annotation_argument(annotation, source)
        )
        name = java_parser.string_value(value, source)
        if name is None:
            name_node = parameter.child_by_field_name("name")
            name = java_parser.node_text(source, name_node) if name_node is not None else None
        if name and name not in names:
            names.append(name)
    return names


def _simple_type_name(qualified: str) -> str:
    """`a.b.Foo<Bar>` -> `Foo`."""
    cleaned = qualified.strip().split("<", 1)[0]
    return cleaned.rsplit(".", 1)[-1]


def _pluralize(word: str) -> str:
    """Pluralisation anglaise best-effort, alignée sur Spring Data REST."""
    word = word.strip()
    if not word:
        return word
    if word.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    if word.endswith("y") and len(word) > 1 and word[-2].lower() not in "aeiou":
        return word[:-1] + "ies"
    return word + "s"


@lru_cache(maxsize=256)
def _module_has_spring_data_rest(repo_root_str: str, rel_path: str) -> bool:
    """True si le module Maven contenant ce fichier déclare data-rest.

    Spring Data REST n'auto-expose les repositories qu'avec la dépendance
    `spring-boot-starter-data-rest`. Ce portillon évite les faux positifs sur
    les modules JPA purs (ex. `invoicing` ici, dont `InvoiceRepository` n'est
    pas exposé). On remonte au `pom.xml` du module le plus proche.
    """
    repo_root = Path(repo_root_str)
    current = (repo_root / rel_path).parent
    pom: Path | None = None
    while True:
        candidate = current / "pom.xml"
        if candidate.exists():
            pom = candidate
            break
        if current == repo_root or current.parent == current:
            break
        current = current.parent
    if pom is None:
        return False
    return any("data-rest" in dependency for dependency in maven_module_dependencies(pom))


def _infer_spring_data_rest_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Inventorie les endpoints Spring Data REST d'un repository.

    Deux cas :
    - `@RepositoryRestResource(path = "...")` : chemin explicite (l'annotation
      implique la présence de data-rest, pas de portillon classpath) ;
    - interface Spring Data sans `exported=false` et sans `path` : chemin par
      défaut `/<entité-pluriel>`, uniquement si le module déclare data-rest.
      C'est le cas d'un `UserRepository extends JpaRepository<User, ...>` sans
      annotation, qu'aucun littéral de chemin ne permettait jusque-là de lier.
    """
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    endpoints: list[MessageEndpoint] = []
    for declaration in java_parser.type_declarations(root):
        if declaration.type != "interface_declaration":
            continue
        entity = _repository_entity_type(declaration, source)
        if entity is None:
            continue
        annotation = next(
            (
                ann for ann in java_parser.annotations_of(declaration)
                if java_parser.annotation_name(ann, source) == "RepositoryRestResource"
            ),
            None,
        )
        exported = java_parser.annotation_argument(annotation, source, key="exported") if annotation else None
        if exported is not None and java_parser.node_text(source, exported) == "false":
            continue
        rest_path = java_parser.string_value(
            java_parser.annotation_argument(annotation, source, key="path") if annotation else None,
            source,
        )
        if rest_path is not None:
            assert annotation is not None
            base_path = _normalize_rest_path(rest_path)
            snippet = java_parser.node_text(source, annotation)
            decl_line = annotation.start_point.row + 1
        else:
            if not _module_has_spring_data_rest(str(repo_root), rel_path):
                continue
            base_path = "/" + _pluralize(entity.lower())
            snippet = java_parser.node_text(source, declaration)
            decl_line = declaration.start_point.row + 1

        for topic in (
            f"GET {base_path}",
            f"POST {base_path}",
            f"GET {base_path}/{{id}}",
            f"PUT {base_path}/{{id}}",
            f"PATCH {base_path}/{{id}}",
            f"DELETE {base_path}/{{id}}",
        ):
            endpoints.append(
                _build_endpoint(
                    repo_root,
                    rel_path,
                    decl_line,
                    decl_line,
                    "serve",
                    "rest",
                    topic,
                    "spring-data-rest",
                    snippet,
                )
            )
    return endpoints


def _repository_entity_type(declaration, source: bytes) -> str | None:
    """Entity argument of a Spring Data repository interface, via AST types."""
    for node in java_parser.walk(declaration):
        if node.type != "generic_type":
            continue
        type_node = next(
            (child for child in node.children if child.type in {"type_identifier", "scoped_type_identifier"}),
            None,
        )
        type_name = java_parser.node_text(source, type_node) if type_node is not None else ""
        if not type_name.endswith("Repository"):
            continue
        arguments = next((child for child in node.children if child.type == "type_arguments"), None)
        if arguments is None:
            continue
        for child in arguments.children:
            if child.type not in {"<", ",", ">"}:
                return _simple_type_name(java_parser.node_text(source, child))
    return None


def _infer_swagger_endpoint(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    for declaration in java_parser.type_declarations(root):
        annotation = next(
            (
                ann for ann in java_parser.annotations_of(declaration)
                if java_parser.annotation_name(ann, source) == "EnableSwagger2"
            ),
            None,
        )
        if annotation is not None:
            return [
                _build_endpoint(
                    repo_root,
                    rel_path,
                    annotation.start_point.row + 1,
                    annotation.end_point.row + 1,
                    "serve",
                    "rest",
                    "GET /swagger-ui.html",
                    "swagger-ui",
                    java_parser.node_text(source, annotation),
                )
            ]
    return []


@lru_cache(maxsize=64)
def _openapi_generator_contract_paths(repo_root_str: str) -> tuple[str, ...]:
    repo_root = Path(repo_root_str)
    contracts: set[str] = set()
    for pom_path in sorted(repo_root.rglob("pom.xml")):
        try:
            module_dir = pom_path.parent
            if not discover_rest_controllers(module_dir, set()):
                continue
            for module_relative in maven_module.detect_openapi_generator_input_specs(pom_path):
                contracts.add((module_dir / module_relative).relative_to(repo_root).as_posix())
        except ValueError:
            continue
    return tuple(sorted(contracts))


def _is_strategy1_openapi_declaration_path(rel_path: str) -> bool:
    """Recognize a Strategy1 microservice API publication declaration."""
    path = Path(rel_path)
    parts = path.parts
    return path.suffix.casefold() == ".rest" and any(
        parts[index:index + 4] == ("src", "main", "resources", "openapi")
        for index in range(max(0, len(parts) - 3))
    )


def _is_openapi_contract_path(repo_root: Path, rel_path: str) -> bool:
    path = repo_root / rel_path
    return path.name in {
        "openapi.yaml", "openapi.yml", "openapi.json",
        "swagger.yaml", "swagger.yml", "swagger.json",
    } or rel_path in _openapi_generator_contract_paths(str(repo_root))


def _infer_openapi_generator_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    path = repo_root / rel_path
    if path.name != "pom.xml":
        return []
    if not discover_rest_controllers(path.parent, set()):
        return []
    endpoints: list[MessageEndpoint] = []
    for module_relative in maven_module.detect_openapi_generator_input_specs(path):
        try:
            contract_rel_path = (path.parent / module_relative).relative_to(repo_root).as_posix()
        except ValueError:
            continue
        endpoints.extend(_infer_openapi_endpoints(repo_root, contract_rel_path))
    return endpoints


def _infer_openapi_endpoints(
    repo_root: Path, rel_path: str, *, force_contract: bool = False
) -> list[MessageEndpoint]:
    """Inventory literal operations declared by a production OpenAPI contract.

    Some Spring projects generate their controller interfaces from OpenAPI and
    only implement those interfaces in ``src/main``.  In that layout there is
    no method-level Spring annotation to scan in the checked-in Java sources,
    while the contract remains the authoritative local evidence.  Keep the
    contract file and the operation line as evidence rather than attributing
    the route to an implementation method that does not declare it.
    """
    path = repo_root / rel_path
    if not force_contract and not _is_openapi_contract_path(repo_root, rel_path):
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        document = json.loads(text) if path.suffix == ".json" else yaml.safe_load(text)
    except (OSError, ValueError, yaml.YAMLError):
        return []
    if not isinstance(document, dict) or not isinstance(document.get("paths"), dict):
        return []

    endpoints: list[MessageEndpoint] = []
    lines = text.splitlines()
    for raw_route, operations in document["paths"].items():
        if not isinstance(raw_route, str) or not isinstance(operations, dict):
            continue
        route = _normalize_rest_path(raw_route)
        route_line = next((index + 1 for index, line in enumerate(lines) if line.lstrip().startswith(f"{raw_route}:")), 1)
        for raw_method in operations:
            method = str(raw_method).lower()
            if method not in _OPENAPI_HTTP_METHODS:
                continue
            method_line = next(
                (index + 1 for index in range(route_line, len(lines)) if lines[index].strip() == f"{method}:"),
                route_line,
            )
            snippet = _read_snippet(repo_root, rel_path, method_line, method_line)
            endpoints.append(
                _build_endpoint(
                    repo_root, rel_path, method_line, method_line, "serve", "rest",
                    f"{method.upper()} {route}", "openapi", snippet,
                )
            )
    return endpoints


def _strategy1_model_openapi_contracts(repo_root: Path, api_name: str) -> tuple[str, ...]:
    """Find the ``<api>.yaml``-style contract configured by a ``model-*`` module."""
    contracts: set[str] = set()
    for pom_path in sorted(repo_root.rglob("pom.xml")):
        try:
            artifact_id, _, _ = maven_module.parse_pom(pom_path)
            module_names = {pom_path.parent.name.casefold()}
            if artifact_id:
                module_names.add(artifact_id.casefold())
            if not any(name.startswith("model-") for name in module_names):
                continue
            for module_relative in maven_module.detect_openapi_generator_input_specs(pom_path):
                contract_name = Path(module_relative).stem.casefold().replace("_", "-")
                if contract_name != api_name:
                    continue
                contracts.add((pom_path.parent / module_relative).relative_to(repo_root).as_posix())
        except (OSError, ValueError):
            continue
    return tuple(sorted(contracts))


def _infer_strategy1_declared_openapi_publications(
    repo_root: Path, declaration_rel_path: str
) -> list[MessageEndpoint]:
    """Attribute a ``*.rest`` declaration to its contract in a ``model-*`` module.

    A Strategy1 declaration is deliberately not parsed as OpenAPI.  It tells
    us which microservice publishes the contract with the same base name in a
    ``model-*`` Maven module. Endpoints keep the declaration path so their
    lifecycle follows the publishing service during incremental indexing.
    """
    api_name = Path(declaration_rel_path).stem.casefold().replace("_", "-")
    publisher_module = _module_for_path(repo_root, declaration_rel_path)
    endpoints: list[MessageEndpoint] = []
    for contract_rel_path in _strategy1_model_openapi_contracts(repo_root, api_name):
        for contract_endpoint in _infer_openapi_endpoints(
            repo_root, contract_rel_path, force_contract=True
        ):
            endpoints.append(
                replace(
                    contract_endpoint,
                    id=compute_endpoint_id(
                        contract_endpoint.role,
                        contract_endpoint.topic,
                        declaration_rel_path,
                        1,
                        1,
                    ),
                    path=declaration_rel_path,
                    start_line=1,
                    end_line=1,
                    snippet=(
                        f"Publication OpenAPI declaree par {declaration_rel_path}\n"
                        f"cccr-openapi-contract:{contract_rel_path}\n"
                        f"{contract_endpoint.snippet}"
                    ),
                    module=publisher_module,
                    qualified_name=None,
                )
            )
    return endpoints


def _infer_actuator_endpoint(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    path = repo_root / rel_path
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    value = _load_flat_spring_properties(str(path)).get("management.endpoints.web.exposure.include")
    if value is None or "*" not in value:
        return []
    start_line = 1
    for idx, line in enumerate(text.splitlines(), start=1):
        if "management.endpoints.web.exposure.include" in line:
            start_line = idx
            break
    return [
        _build_endpoint(
            repo_root,
            rel_path,
            start_line,
            start_line,
            "serve",
            "rest",
            "GET /actuator/**",
            "spring-actuator",
            "management.endpoints.web.exposure.include=*",
        )
    ]


@lru_cache(maxsize=512)
def _file_uses_resttemplate(repo_root_str: str, rel_path: str) -> bool:
    path = Path(repo_root_str) / rel_path
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return (
        "org.springframework.web.client.RestTemplate" in text
        or "new RestTemplate(" in text
        or " RestTemplate " in text
    )


@lru_cache(maxsize=512)
def _file_uses_restclient(repo_root_str: str, rel_path: str) -> bool:
    path = Path(repo_root_str) / rel_path
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return "org.springframework.web.client.RestClient" in text or "RestClient " in text


def _uri_argument(snippet: str) -> str | None:
    """Extrait l'argument de `.uri(...)` en tenant compte des appels imbriqués."""
    match = _URI_CALL_RE.search(snippet)
    if match is None:
        return None
    start = match.end()
    depth = 1
    quote: str | None = None
    escaped = False
    for index in range(start, len(snippet)):
        char = snippet[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ('"', "'"):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return snippet[start:index]
    return None


def _extract_restclient_path(
    snippet: str, repo_root: Path, source_path: str
) -> tuple[str, bool] | None:
    expr = _uri_argument(snippet)
    if expr is None:
        return None
    return _resolve_rest_path_expression(
        expr, repo_root, source_path, preserve_dynamic_segments=True
    )


def _extract_resttemplate_path(
    snippet: str, repo_root: Path, source_path: str
) -> tuple[str, bool] | None:
    match = _REST_TEMPLATE_CALL_RE.search(snippet)
    if match is not None:
        return _resolve_rest_path_expression(match.group(2), repo_root, source_path)
    exchange_match = _REST_TEMPLATE_EXCHANGE_RE.search(snippet)
    if exchange_match is not None:
        return _resolve_rest_path_expression(exchange_match.group(1), repo_root, source_path)
    return None


def _infer_resttemplate_exchange_endpoints(
    repo_root: Path, rel_path: str
) -> list[MessageEndpoint]:
    """Infer RestTemplate calls from ``method_invocation`` AST nodes.

    This covers the regular convenience methods as well as ``exchange``. The
    former used to depend on a rule-engine match; taking their first
    argument from the invocation node means nested calls and line wrapping no
    longer affect which expression is considered the URL.
    """
    if not _file_uses_resttemplate(str(repo_root), rel_path):
        return []
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    direct_methods = {
        "getForObject": "GET", "getForEntity": "GET",
        "postForObject": "POST", "postForEntity": "POST",
        "put": "PUT", "delete": "DELETE",
    }
    inferred: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        receiver, method_name, args = java_parser.invocation_parts(invocation, source)
        if method_name not in {*direct_methods, "exchange"} or receiver is None or not args:
            continue
        receiver_name = _invocation_receiver(source, receiver)
        if receiver_name is None or "resttemplate" not in receiver_name.lower():
            continue
        if method_name == "exchange":
            if len(args) < 2:
                continue
            http_method = java_parser.node_text(source, args[1]).rsplit(".", 1)[-1]
            if http_method not in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
                continue
        else:
            http_method = direct_methods[method_name]
        route, dynamic = _resolve_rest_path_expression(
            java_parser.node_text(source, args[0]), repo_root, rel_path
        )
        inferred.append(
            _build_endpoint(
                repo_root, rel_path, invocation.start_point.row + 1,
                invocation.end_point.row + 1, "call", "rest", f"{http_method} {route}",
                "resttemplate", java_parser.node_text(source, invocation), topic_dynamic=dynamic,
            )
        )
    return inferred


def _infer_webclient_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Infer fluent WebClient calls by linking ``.uri`` to its AST receiver."""
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    if b"WebClient" not in source:
        return []
    endpoints: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        receiver, name, args = java_parser.invocation_parts(invocation, source)
        if name != "uri" or receiver is None or not args:
            continue
        verb_call = _webclient_verb_invocation(receiver, source)
        if verb_call is None:
            continue
        http_method, anchor = verb_call
        route, dynamic = _resolve_rest_path_expression(
            java_parser.node_text(source, args[0]), repo_root, rel_path,
            preserve_dynamic_segments=True,
        )
        endpoints.append(
            _build_endpoint(
                repo_root, rel_path, anchor.start_point.row + 1, invocation.end_point.row + 1,
                "call", "rest", f"{http_method} {route}", "webclient",
                java_parser.node_text(source, invocation), topic_dynamic=dynamic,
            )
        )
    return endpoints


def _webclient_verb_invocation(node, source: bytes):
    """Nearest request verb in a fluent receiver chain, with its AST node."""
    if node.type != "method_invocation":
        return None
    receiver, name, _args = java_parser.invocation_parts(node, source)
    verbs = {"get", "post", "put", "delete", "patch", "head", "options"}
    if name in verbs:
        return name.upper(), node
    return _webclient_verb_invocation(receiver, source) if receiver is not None else None


def _infer_configured_api_client_endpoints(
    repo_root: Path, rel_path: str
) -> list[MessageEndpoint]:
    """Émet les dépendances configurées d'une configuration REST Strategy1.

    La convention strategy1 ne dépend ni du nom de factory ni de la forme du
    bean : toute constante majuscule avec underscore présente dans une classe
    ``Rest*Config*`` désigne le microservice homologue en kebab-case.
    """
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    endpoints: dict[str, MessageEndpoint] = {}
    for type_node in java_parser.type_declarations(root):
        if not _is_rest_client_configuration(
            java_parser.declaration_name(type_node, source)
        ):
            continue
        configuration = java_parser.node_text(source, type_node)
        for domain, line in _rest_configuration_domains(type_node, source):
            endpoint = _build_endpoint(
                repo_root,
                rel_path,
                line,
                line,
                "call",
                "rest",
                "ANY <dynamic>",
                "configured-api-client-configuration",
                f"{configuration}\ncccr-api-domain:{domain}",
                topic_dynamic=True,
            )
            endpoints[endpoint.id] = endpoint
            _trace_rest_client(
                "rest_client.search.configuration_dependency",
                path=rel_path,
                line=line,
                domain=domain,
            )
        for service, line in _rest_configuration_external_services(type_node, source):
            endpoint = _build_endpoint(
                repo_root,
                rel_path,
                line,
                line,
                "call",
                "rest",
                "ANY <dynamic>",
                "configured-external-rest-api-properties",
                f"{configuration}\ncccr-external-microservice:{service}",
                topic_dynamic=True,
            )
            endpoints[endpoint.id] = endpoint
            _trace_rest_client(
                "rest_client.search.external_configuration_dependency",
                path=rel_path,
                line=line,
                service=service,
            )
    return list(endpoints.values())


def _infer_spring_cloud_gateway_routes(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    inferred: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        _receiver, name, args = java_parser.invocation_parts(invocation, source)
        if name != "route" or not args or args[0].type != "lambda_expression":
            continue
        path_value: str | None = None
        http_method: str | None = None
        has_uri = False
        for call in java_parser.walk(args[0]):
            if call.type != "method_invocation":
                continue
            _call_receiver, call_name, call_args = java_parser.invocation_parts(call, source)
            if call_name == "path" and call_args and call_args[0].type == "string_literal":
                path_value = java_parser.string_value(call_args[0], source)
            elif call_name == "method" and call_args:
                candidate = java_parser.string_value(call_args[0], source)
                if candidate is None:
                    candidate = java_parser.node_text(source, call_args[0]).rsplit(".", 1)[-1]
                if candidate in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
                    http_method = candidate
            elif call_name == "uri":
                has_uri = True
        if path_value is None or http_method is None or not has_uri:
            continue
        route = _normalize_rest_path(path_value)
        for role in ("serve", "call"):
            inferred.append(
                _build_endpoint(
                    repo_root, rel_path, invocation.start_point.row + 1,
                    invocation.end_point.row + 1, role, "rest", f"{http_method} {route}",
                    "spring-cloud-gateway", java_parser.node_text(source, invocation),
                )
            )
    return inferred


def _gateway_route_entries(data: object) -> list[dict[str, object]]:
    if not isinstance(data, dict):
        return []
    spring = data.get("spring")
    if not isinstance(spring, dict):
        return []
    cloud = spring.get("cloud")
    if not isinstance(cloud, dict):
        return []
    gateway = cloud.get("gateway")
    if not isinstance(gateway, dict):
        return []
    routes = gateway.get("routes")
    if not isinstance(routes, list):
        server = gateway.get("server")
        webflux = server.get("webflux") if isinstance(server, dict) else None
        routes = webflux.get("routes") if isinstance(webflux, dict) else None
    return [route for route in routes if isinstance(route, dict)] if isinstance(routes, list) else []


def _gateway_paths(route: dict[str, object]) -> list[str]:
    predicates = route.get("predicates")
    if not isinstance(predicates, list):
        return []
    paths: list[str] = []
    for predicate in predicates:
        if isinstance(predicate, str) and predicate.startswith("Path="):
            paths.extend(path.strip() for path in predicate[5:].split(",") if path.strip())
        elif isinstance(predicate, dict) and predicate.get("name") == "Path":
            args = predicate.get("args")
            if isinstance(args, dict):
                value = args.get("_genkey_0") or args.get("patterns")
                if isinstance(value, str):
                    paths.append(value)
    return paths


def _gateway_strip_prefix(route: dict[str, object]) -> int:
    filters = route.get("filters")
    if not isinstance(filters, list):
        return 0
    for item in filters:
        if isinstance(item, str) and item.startswith("StripPrefix="):
            try:
                return int(item.partition("=")[2])
            except ValueError:
                return 0
    return 0


def _strip_gateway_path(route: str, prefix_count: int) -> str:
    if prefix_count <= 0:
        return _normalize_rest_path(route)
    parts = [part for part in route.split("/") if part]
    remaining = parts[prefix_count:]
    return "/" + "/".join(remaining) if remaining else "/"


def _infer_spring_cloud_gateway_yaml_routes(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    path = repo_root / rel_path
    try:
        documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8", errors="replace")))
    except (OSError, yaml.YAMLError):
        return []

    inferred: list[MessageEndpoint] = []
    for document in documents:
        for route in _gateway_route_entries(document):
            uri = route.get("uri")
            if not isinstance(uri, str) or not uri.startswith("lb://"):
                continue
            strip_prefix = _gateway_strip_prefix(route)
            line_no = 1
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
                line_no = next(
                    index
                    for index, line in enumerate(text.splitlines(), start=1)
                    if f"uri: {uri}" in line or f"uri:{uri}" in line
                )
            except (OSError, StopIteration):
                pass
            for public_path in _gateway_paths(route):
                public_route = _normalize_rest_path(public_path)
                target_path = _strip_gateway_path(public_route, strip_prefix)
                snippet = f"Path={public_route}; StripPrefix={strip_prefix}; uri={uri}"
                inferred.append(
                    _build_endpoint(
                        repo_root,
                        rel_path,
                        line_no,
                        line_no,
                        "serve",
                        "rest",
                        f"ANY {public_route}",
                        "spring-cloud-gateway",
                        snippet,
                    )
                )
                inferred.append(
                    _build_endpoint(
                        repo_root,
                        rel_path,
                        line_no,
                        line_no,
                        "call",
                        "rest",
                        f"ANY {target_path}",
                        "spring-cloud-gateway",
                        snippet,
                    )
                )
    return inferred


def _infer_spring_webflux_routes(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    inferred: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        _receiver, name, args = java_parser.invocation_parts(invocation, source)
        if name not in {"route", "andRoute"} or not args or args[0].type != "method_invocation":
            continue
        _predicate_receiver, predicate, predicate_args = java_parser.invocation_parts(args[0], source)
        if predicate not in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
            continue
        if not predicate_args or predicate_args[0].type != "string_literal":
            continue
        path = java_parser.string_value(predicate_args[0], source)
        if path is None:
            continue
        inferred.append(
            _build_endpoint(
                repo_root, rel_path, invocation.start_point.row + 1,
                invocation.end_point.row + 1, "serve", "rest",
                f"{predicate} {_normalize_rest_path(path)}", "spring-webflux",
                java_parser.node_text(source, invocation),
            )
        )
    return inferred


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


def _producer_send_payload_type(source: bytes, invocation) -> str | None:
    """Type de payload d'un ``send(topic, payload, ...)`` : 2e argument
    résolu contre le paramètre de la méthode englobante."""
    method = java_parser.enclosing(invocation, "method_declaration")
    if method is None:
        return None
    for arg in java_parser.argument_nodes(invocation)[1:]:
        if arg.type == "identifier":
            payload = _method_param_payload_type(source, method, java_parser.node_text(source, arg))
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


def infer_framework_endpoints(
    repo_root: Path,
    files: list[str] | None = None,
    *,
    configured_api_client_strategy1: bool = False,
) -> list[MessageEndpoint]:
    """Infère les endpoints des frameworks connus.

    Les conventions applicatives de constantes majuscules avec underscore des
    classes ``Rest*Config*`` ne sont actives qu'avec ``strategy1``.
    """
    if files is None:
        candidate_files = [
            path.relative_to(repo_root).as_posix() for path in repo_root.rglob("*") if path.is_file()
        ]
    else:
        candidate_files = sorted(files)

    inferred: dict[str, MessageEndpoint] = {}
    for rel_path in candidate_files:
        if rel_path.endswith(".java"):
            for endpoint in (
                _infer_generic_request_mapping_endpoints(repo_root, rel_path)
                + _infer_spring_data_rest_endpoints(repo_root, rel_path)
                + _infer_swagger_endpoint(repo_root, rel_path)
                + _infer_resttemplate_exchange_endpoints(repo_root, rel_path)
                + _infer_webclient_endpoints(repo_root, rel_path)
                + _infer_spring_cloud_gateway_routes(repo_root, rel_path)
                + _infer_spring_webflux_routes(repo_root, rel_path)
                + (
                    _infer_configured_api_client_endpoints(repo_root, rel_path)
                    if configured_api_client_strategy1
                    else []
                )
            ):
                inferred[endpoint.id] = endpoint
        elif rel_path.endswith("pom.xml"):
            for endpoint in _infer_openapi_generator_endpoints(repo_root, rel_path):
                inferred[endpoint.id] = endpoint
        elif rel_path.endswith((".properties", ".yml", ".yaml")):
            for endpoint in (
                _infer_actuator_endpoint(repo_root, rel_path)
                + _infer_spring_cloud_gateway_yaml_routes(repo_root, rel_path)
                + _infer_openapi_endpoints(repo_root, rel_path)
            ):
                inferred[endpoint.id] = endpoint
        elif configured_api_client_strategy1 and _is_strategy1_openapi_declaration_path(rel_path):
            for endpoint in _infer_strategy1_declared_openapi_publications(repo_root, rel_path):
                inferred[endpoint.id] = endpoint
    return list(inferred.values())


_STRATEGY1_PRODUCER_RE = re.compile(r"\bgetTopics\s*\(\s*\)\s*\.\s*get([A-Z]\w*)\s*\(\s*\)")
_STRATEGY1_KAFKA_KEY_RE = re.compile(
    r"\$\{\s*kafka\.topics\.([A-Za-z_]\w*)\.[^}:]+(?:\s*:[^}]*)?\s*\}"
)


def _strategy1_topic_name(value: str) -> str:
    """Normalize a Java accessor or Spring property segment to a Kafka topic."""
    separated = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", separated)
    return separated.replace("-", "_").upper()


def _kafka_listener_annotation_blocks(source: str) -> list[tuple[int, str]]:
    """Return complete `@KafkaListener(...)` blocks without parsing Java AST."""
    blocks: list[tuple[int, str]] = []
    for match in re.finditer(r"@KafkaListener\s*\(", source):
        depth = 0
        for index in range(match.end() - 1, len(source)):
            character = source[index]
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0:
                    blocks.append((match.start(), source[match.start():index + 1]))
                    break
    return blocks


def infer_kafka_topic_strategy1_endpoints(
    repo_root: Path, files: list[str] | None = None
) -> list[MessageEndpoint]:
    """Infer logical Kafka topics from project conventions selected by strategy1.

    Producers use `getTopics().getXxx()` and listeners use a Spring key shaped
    as `kafka.topics.xxx.<property>`. Both conventions are normalized to the
    physical Kafka name in `SCREAMING_SNAKE_CASE`.
    """
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
        source = _java_source(str(repo_root), rel_path)
        if not source:
            continue
        lines = source.splitlines()
        for match in _STRATEGY1_PRODUCER_RE.finditer(source):
            line_no = source.count("\n", 0, match.start()) + 1
            endpoint = _build_endpoint(
                repo_root,
                rel_path,
                line_no,
                line_no,
                "produce",
                "kafka",
                _strategy1_topic_name(match.group(1)),
                "kafka-topic-strategy1",
                lines[line_no - 1].strip(),
            )
            endpoints[endpoint.id] = endpoint
        for offset, annotation in _kafka_listener_annotation_blocks(source):
            line_no = source.count("\n", 0, offset) + 1
            for key_match in _STRATEGY1_KAFKA_KEY_RE.finditer(annotation):
                endpoint = _build_endpoint(
                    repo_root,
                    rel_path,
                    line_no,
                    line_no + annotation.count("\n"),
                    "consume",
                    "kafka",
                    _strategy1_topic_name(key_match.group(1)),
                    "kafka-topic-strategy1",
                    annotation,
                )
                endpoints[endpoint.id] = endpoint
    return list(endpoints.values())


def apply_kafka_topic_strategy1(
    endpoints: list[MessageEndpoint], strategy_endpoints: list[MessageEndpoint]
) -> list[MessageEndpoint]:
    """Replace standard Kafka extraction at sites covered by strategy1."""
    covered_sites = {
        (endpoint.role, endpoint.path, endpoint.start_line)
        for endpoint in strategy_endpoints
    }
    retained = [
        endpoint
        for endpoint in endpoints
        if endpoint.system != "kafka"
        or (endpoint.role, endpoint.path, endpoint.start_line) not in covered_sites
    ]
    return [*retained, *strategy_endpoints]


def _build_markdown_topic_manifest_endpoint(
    rel_path: str,
    line_no: int,
    line: str,
    module: str | None,
    role: str,
    topic: str,
) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, rel_path, line_no, line_no),
        role=role,
        system="kafka",
        topic=topic,
        topic_dynamic=False,
        source="manifest",
        framework="markdown-topic-manifest",
        path=rel_path,
        start_line=line_no,
        end_line=line_no,
        snippet=line.strip(),
        module=module,
        qualified_name=None,
    )


def _parse_markdown_topic_manifest(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    try:
        lines = (repo_root / rel_path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []

    endpoints: list[MessageEndpoint] = []
    module: str | None = None
    role: str | None = None
    header: list[str] | None = None
    topic_index: int | None = None
    physical_index: int | None = None

    for idx, line in enumerate(lines, start=1):
        heading = _MARKDOWN_MODULE_HEADING_RE.match(line)
        if heading is not None:
            module = heading.group(1).strip()
            role = None
            header = None
            topic_index = None
            physical_index = None
            continue

        section = _MARKDOWN_BOLD_SECTION_RE.match(line)
        if section is not None:
            role = "produce" if section.group(1).lower() == "producer" else "consume"
            header = None
            topic_index = None
            physical_index = None
            continue

        cells = _split_markdown_table_row(line)
        if cells is None:
            continue
        if _is_markdown_separator_row(cells):
            continue
        if role is None:
            continue

        normalized_cells = [_normalize_markdown_header(cell) for cell in cells]
        if "topic" in normalized_cells and "nom physique" in normalized_cells:
            header = cells
            topic_index = normalized_cells.index("topic")
            physical_index = normalized_cells.index("nom physique")
            continue
        if header is None or topic_index is None or physical_index is None:
            continue
        if max(topic_index, physical_index) >= len(cells):
            continue

        physical_name = cells[physical_index].strip()
        logical_name = cells[topic_index].strip()
        topic = physical_name or logical_name
        if not topic:
            continue
        endpoints.append(
            _build_markdown_topic_manifest_endpoint(
                rel_path, idx, line, module, role, topic
            )
        )

    return endpoints


def infer_markdown_topic_manifest_endpoints(
    repo_root: Path, files: list[str] | None = None
) -> list[MessageEndpoint]:
    if files is None:
        candidate_files = [
            path.relative_to(repo_root).as_posix()
            for path in repo_root.rglob("*.md")
            if path.is_file()
        ]
    else:
        candidate_files = sorted(path for path in files if path.endswith(".md"))

    inferred: dict[str, MessageEndpoint] = {}
    for rel_path in candidate_files:
        for endpoint in _parse_markdown_topic_manifest(repo_root, rel_path):
            inferred[endpoint.id] = endpoint
    return list(inferred.values())


def _json_manifest_line_number(text: str, section: str, module: str, topic: str) -> int:
    """Return the best-effort line of a topic declaration in a JSON manifest."""
    section_index = text.find(json.dumps(section, ensure_ascii=False))
    module_index = text.find(json.dumps(module, ensure_ascii=False), max(section_index, 0))
    topic_index = text.find(json.dumps(topic, ensure_ascii=False), max(module_index, 0))
    if topic_index < 0:
        return 1
    return text.count("\n", 0, topic_index) + 1


def _parse_json_kafka_flow_graph_manifest(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Parse the `topics`/`producers`/`consumers` JSON flow-graph schema."""
    try:
        text = (repo_root / rel_path).read_text(encoding="utf-8", errors="replace")
        data = json.loads(text)
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, dict):
        return []

    raw_topics = data.get("topics")
    if not isinstance(raw_topics, dict):
        return []
    topics = {
        logical: physical.strip() or logical
        for logical, physical in raw_topics.items()
        if isinstance(logical, str) and isinstance(physical, str)
    }

    endpoints: dict[str, MessageEndpoint] = {}
    for section, role in (("producers", "produce"), ("consumers", "consume")):
        declarations = data.get(section)
        if not isinstance(declarations, dict):
            continue
        for module, declared_topics in declarations.items():
            if not isinstance(module, str) or not isinstance(declared_topics, list):
                continue
            for logical_topic in declared_topics:
                if not isinstance(logical_topic, str) or not logical_topic.strip():
                    continue
                topic = topics.get(logical_topic, logical_topic)
                line_no = _json_manifest_line_number(text, section, module, logical_topic)
                endpoint = MessageEndpoint(
                    id=compute_endpoint_id(
                        role, topic, f"{rel_path}:{module}", line_no, line_no
                    ),
                    role=role,
                    system="kafka",
                    topic=topic,
                    topic_dynamic=False,
                    source="manifest",
                    framework="json-kafka-flow-graph",
                    path=rel_path,
                    start_line=line_no,
                    end_line=line_no,
                    snippet=f"{section}.{module}: {logical_topic} -> {topic}",
                    module=module,
                    qualified_name=None,
                )
                endpoints[endpoint.id] = endpoint
    return list(endpoints.values())


def infer_json_kafka_flow_graph_endpoints(
    repo_root: Path, files: list[str] | None = None
) -> list[MessageEndpoint]:
    """Infer Kafka endpoints from compatible JSON flow graph manifests."""
    if files is None:
        candidate_files = [
            path.relative_to(repo_root).as_posix()
            for path in repo_root.rglob("*.json")
            if path.is_file()
        ]
    else:
        candidate_files = sorted(path for path in files if path.endswith(".json"))

    inferred: dict[str, MessageEndpoint] = {}
    for rel_path in candidate_files:
        for endpoint in _parse_json_kafka_flow_graph_manifest(repo_root, rel_path):
            inferred[endpoint.id] = endpoint
    return list(inferred.values())


@lru_cache(maxsize=512)
def _load_value_annotated_fields(path_str: str) -> dict[str, str]:
    """Champs `@Value("${clé}")` d'un fichier source Java — variable ->
    clé de propriété (avec éventuel `:défaut`, laissé tel quel pour
    `resolve_spring_property`). Extrait via l'AST tree-sitter : pour chaque
    `field_declaration` annotée `@Value("${...}")`, on associe le nom du
    champ à la clé de propriété (sans les `${ }`)."""
    path = Path(path_str)
    try:
        source = path.read_bytes()
    except OSError:
        return {}
    root = java_parser.java_parser("value_fields").parse(source).root_node
    if root.has_error:
        return {}
    fields: dict[str, str] = {}
    for node in java_parser.walk(root):
        if node.type != "field_declaration":
            continue
        value_ann = next(
            (
                ann
                for ann in java_parser.annotations_of(node)
                if java_parser.annotation_name(ann, source) == "Value"
            ),
            None,
        )
        if value_ann is None:
            continue
        raw = java_parser.first_string_argument(value_ann, source)
        if raw is None or not (raw.startswith("${") and raw.endswith("}")):
            continue
        property_key = raw[2:-1]
        for declarator in node.children:
            if declarator.type != "variable_declarator":
                continue
            name_node = declarator.child_by_field_name("name")
            if name_node is None:
                continue
            fields[java_parser.node_text(source, name_node)] = property_key
    return fields


def _resolve_value_annotated_variable(
    repo_root: Path, source_path: str, var_name: str
) -> str | None:
    fields = _load_value_annotated_fields(str(repo_root / source_path))
    property_key = fields.get(var_name)
    if property_key is None:
        return None
    return resolve_spring_property(repo_root, property_key, source_path)


# Certains microservices ne portent pas l'URL de destination au site d'appel :
# le client HTTP est injecté et construit dans une configuration `Rest*Config*`.
# Dans cette
# convention, un `@Bean` délègue à un helper auquel est passé le domaine qui
# publie l'API. On conserve ce domaine dans l'évidence de l'endpoint pour que
# le graphe puisse restreindre la cible, sans prétendre résoudre une URL.
def _is_rest_client_configuration(class_name: str | None) -> bool:
    """Whether a Java configuration name follows the ``Rest*Config*`` convention."""
    return class_name is not None and class_name.startswith("Rest") and "Config" in class_name


def _is_uppercase_underscore_constant(name: str) -> bool:
    return (
        "_" in name
        and name[0].isupper()
        and all(character.isupper() or character.isdigit() or character == "_" for character in name)
    )


def _rest_configuration_domains(type_node, source: bytes) -> list[tuple[str, int]]:
    """Return uppercase constants of a REST configuration from its Java AST.

    Only identifier nodes are considered.  A constant mentioned in a comment,
    a string literal or an annotation value therefore cannot create a false
    microservice dependency.
    """
    domains = {
        (name.lower().replace("_", "-"), node.start_point.row + 1)
        for node in java_parser.walk(type_node)
        if node.type in {"identifier", "field_identifier"}
        if _is_uppercase_underscore_constant(name := java_parser.node_text(source, node))
    }
    return sorted(domains)


def _rest_configuration_external_services(type_node, source: bytes) -> list[tuple[str, int]]:
    """Return external services referenced through ``getRest().get(...)``.

    Strategy1 treats ``getRest().get("partner")`` in a ``Rest*Config*`` class
    as an explicit HTTP dependency on the external microservice ``partner``.
    The AST shape is checked end-to-end so a matching string in a comment, an
    unrelated ``get`` chain, or another type declaration cannot manufacture an
    architecture relation.
    """
    services: set[tuple[str, int]] = set()
    for invocation in java_parser.walk(type_node):
        if invocation.type != "method_invocation":
            continue
        if java_parser.enclosing(invocation, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration") != type_node:
            continue
        receiver, method_name, arguments = java_parser.invocation_parts(invocation, source)
        if method_name != "get" or len(arguments) != 1 or receiver is None:
            continue
        service = _normalize_api_domain(java_parser.string_value(arguments[0], source) or "")
        if service is None or receiver.type != "method_invocation":
            continue
        rest_receiver, rest_method, rest_arguments = java_parser.invocation_parts(receiver, source)
        if rest_method != "getRest" or rest_arguments:
            continue
        services.add((service, invocation.start_point.row + 1))
    return sorted(services)


def _is_configured_api_client_factory(method_name: str) -> bool:
    """Whether a helper name follows the ``create*ClientApi`` convention."""
    return method_name.startswith("create") and method_name.endswith("ClientApi")


def _simple_java_type(value: str) -> str:
    """Nom simple d'un type Java, sans génériques ni tableau."""
    value = value.strip().rsplit(".", 1)[-1]
    return value.split("<", 1)[0].strip().removesuffix("[]")


def _normalize_api_domain(value: str) -> str | None:
    """Normalise une valeur de domaine vers le nom de microservice attendu."""
    if (
        not value
        or not value[0].isalpha()
        or any(not (character.isalnum() or character in {"_", "-"}) for character in value)
    ):
        return None
    return value.lower().replace("_", "-")


def _api_domain_from_key_invocation(node, source: bytes) -> str | None:
    """Extrait ``DOMAIN_FOO`` de l'expression ``DOMAIN_FOO.getKey()``."""
    if node.type != "method_invocation":
        return None
    receiver, method_name, args = java_parser.invocation_parts(node, source)
    if method_name != "getKey" or args or receiver is None:
        return None
    receiver_text = java_parser.node_text(source, receiver)
    return _normalize_api_domain(receiver_text.rsplit(".", 1)[-1])


def _api_domain_argument(node, source: bytes) -> str | None:
    """Extrait le domaine logique donné à un factory de client HTTP.

    Outre un littéral et la constante terminale de ``XXX.NAME``, la
    convention ``getUriPath(..., DOMAIN_FOO.getKey())`` est acceptée. Les URLs
    et chemins restent exclus : ils ne désignent pas le microservice cible.
    """
    literal = java_parser.string_value(node, source)
    if literal is not None:
        return _normalize_api_domain(literal)

    if node.type == "method_invocation":
        receiver, method_name, args = java_parser.invocation_parts(node, source)
        if method_name == "getKey" and not args and receiver is not None:
            receiver_text = java_parser.node_text(source, receiver)
            return _normalize_api_domain(receiver_text.rsplit(".", 1)[-1])
        if method_name == "getUriPath":
            domains = {
                domain
                for argument in args
                if (domain := _api_domain_from_key_invocation(argument, source)) is not None
            }
            return next(iter(domains)) if len(domains) == 1 else None

    if node.type not in {"identifier", "field_access", "scoped_identifier"}:
        return None
    text = java_parser.node_text(source, node)
    name = text.rsplit(".", 1)[-1]
    return _normalize_api_domain(name)


def _bean_api_domain(method_node, source: bytes, microservice: str) -> str | None:
    """Domaine d'un appel `create*ClientApi(...)` dans un bean.

    La convention applicative est précise : le premier argument est une
    constante de domaine (`YYY.DOMAIN_ANNUAIRE`) et le second l'interface
    d'API. La constante devient `domain-annuaire`, qui correspond au nom du
    microservice. Plusieurs appels de ce type dans un même bean sont ambigus.
    """
    domains: set[str] = set()
    for invocation in java_parser.walk(method_node):
        if invocation.type != "method_invocation":
            continue
        _, method_name, args = java_parser.invocation_parts(invocation, source)
        if not _is_configured_api_client_factory(method_name) or not args:
            continue
        _trace_rest_client(
            "rest_client.search.helper",
            microservice=microservice,
            helper=method_name,
            first_argument=java_parser.node_text(source, args[0]),
            argument_count=len(args),
        )
        domain = _api_domain_argument(args[0], source)
        if domain is not None:
            domains.add(domain)
            _trace_rest_client(
                "rest_client.search.domain", microservice=microservice, domain=domain
            )
        else:
            _trace_rest_client(
                "rest_client.search.domain_ignored",
                microservice=microservice,
                argument=java_parser.node_text(source, args[0]),
            )
    if len(domains) == 1:
        return next(iter(domains))
    if domains:
        _trace_rest_client(
            "rest_client.search.domain_ambiguous",
            microservice=microservice,
            domains=sorted(domains),
        )
    return None


def _rest_client_microservice_name(module_root: Path) -> str:
    """Nom du microservice porté par le POM, ou nom du répertoire en repli."""
    pom_path = module_root / "pom.xml"
    if pom_path.is_file():
        artifact_id, _, _ = maven_module.parse_pom(pom_path)
        if artifact_id:
            return artifact_id
    return module_root.name


def _rest_configuration_module_root(repo_root: Path, source_path: str) -> Path:
    """Racine du microservice contenant `source_path`."""
    caller_path = repo_root / source_path
    for parent in (caller_path.parent, *caller_path.parents):
        if parent == repo_root.parent:
            break
        if (parent / "pom.xml").is_file() or (parent / "build.gradle").is_file() or (parent / "build.gradle.kts").is_file():
            _trace_rest_client(
                "rest_client.search.module",
                caller=source_path,
                module=parent.relative_to(repo_root),
                microservice=_rest_client_microservice_name(parent),
            )
            return parent
    _trace_rest_client(
        "rest_client.search.module_fallback",
        caller=source_path,
        module=".",
        microservice=_rest_client_microservice_name(repo_root),
    )
    return repo_root


@lru_cache(maxsize=512)
def _rest_configuration_client_domains_in_module(
    repo_root_str: str, module_root_rel: str
) -> tuple[tuple[str, str, str], ...]:
    """Retourne `(type_client, nom_bean, domaine)` d'un microservice.

    Le `pom.xml` détermine la frontière et le nom (`artifactId`) du
    microservice Maven. Ainsi, chaque configuration `Rest*Config*` n'est parcourue qu'une fois
    pour ce microservice, sans mélanger les services voisins d'un workspace.
    """
    repo_root = Path(repo_root_str)
    module_root = repo_root / module_root_rel
    service_name = _rest_client_microservice_name(module_root)

    _trace(
        "rest_client.configuration.scan.begin",
        microservice=service_name,
        module=module_root_rel,
    )
    _trace_rest_client(
        "rest_client.search.scan_begin",
        microservice=service_name,
        module=module_root_rel,
    )
    clients: list[tuple[str, str, str]] = []
    for candidate in module_root.rglob("*.java"):
        candidate_rel = candidate.relative_to(repo_root).as_posix()
        _trace_rest_client(
            "rest_client.search.source", microservice=service_name, path=candidate_rel
        )
        parsed = java_parser.parse_java(
            repo_root_str, candidate_rel
        )
        if parsed is None:
            _trace_rest_client(
                "rest_client.search.source_unparsed",
                microservice=service_name,
                path=candidate_rel,
            )
            continue
        source, root = parsed
        for type_node in java_parser.type_declarations(root):
            if not _is_rest_client_configuration(
                java_parser.declaration_name(type_node, source)
            ):
                continue
            _trace_rest_client(
                "rest_client.search.configuration",
                microservice=service_name,
                path=candidate_rel,
            )
            for method_node in java_parser.walk(type_node):
                if method_node.type != "method_declaration":
                    continue
                name_node = method_node.child_by_field_name("name")
                method_name = java_parser.node_text(source, name_node) if name_node is not None else "<anonymous>"
                if not any(
                    java_parser.annotation_name(annotation, source) == "Bean"
                    for annotation in java_parser.annotations_of(method_node)
                ):
                    continue
                _trace_rest_client(
                    "rest_client.search.bean",
                    microservice=service_name,
                    path=candidate_rel,
                    bean=method_name,
                )
                type_node_return = method_node.child_by_field_name("type")
                if type_node_return is None or name_node is None:
                    _trace_rest_client(
                        "rest_client.search.bean_ignored",
                        microservice=service_name,
                        path=candidate_rel,
                        bean=method_name,
                        reason="missing_type_or_name",
                    )
                    continue
                domain = _bean_api_domain(method_node, source, service_name)
                if domain is None:
                    _trace_rest_client(
                        "rest_client.search.bean_ignored",
                        microservice=service_name,
                        path=candidate_rel,
                        bean=method_name,
                        reason="no_unique_create_client_api_domain",
                    )
                    continue
                clients.append(
                    (
                        _simple_java_type(java_parser.node_text(source, type_node_return)),
                        java_parser.node_text(source, name_node),
                        domain,
                    )
                )
                _trace(
                    "rest_client.configuration.bean",
                    configuration=candidate.relative_to(repo_root),
                    bean=java_parser.node_text(source, name_node),
                    api_type=_simple_java_type(java_parser.node_text(source, type_node_return)),
                    domain=domain,
                )
                _trace_rest_client(
                    "rest_client.search.bean_registered",
                    microservice=service_name,
                    bean=java_parser.node_text(source, name_node),
                    api_type=_simple_java_type(java_parser.node_text(source, type_node_return)),
                    domain=domain,
                )
    _trace(
        "rest_client.configuration.scan.end",
        microservice=service_name,
        clients=len(clients),
    )
    _trace_rest_client(
        "rest_client.search.scan_end", microservice=service_name, clients=len(clients)
    )
    return tuple(clients)


def _rest_configuration_client_domains(
    repo_root_str: str, source_path: str
) -> tuple[tuple[str, str, str], ...]:
    """Clients configurés du microservice qui contient `source_path`."""
    repo_root = Path(repo_root_str)
    module_root = _rest_configuration_module_root(repo_root, source_path)
    return _rest_configuration_client_domains_in_module(
        repo_root_str, module_root.relative_to(repo_root).as_posix()
    )


def discover_rest_api_client_configurations(repo_root: Path) -> None:
    """Parcourt proactivement les configurations clients de chaque module.

    Cette phase précède l'analyse des appels : les interfaces d'API générées
    ne donnent pas toujours un résultat REST exploitable, mais leur
    toute configuration `Rest*Config*` doit tout de même être cherchée dans chaque
    microservice Maven du workspace.
    """
    module_roots = sorted({pom_path.parent for pom_path in repo_root.rglob("pom.xml")})
    if not module_roots:
        module_roots = [repo_root]
    _trace_rest_client(
        "rest_client.search.workspace_begin", modules=len(module_roots), root=repo_root
    )
    for module_root in module_roots:
        module_root_rel = module_root.relative_to(repo_root).as_posix()
        _trace_rest_client(
            "rest_client.search.workspace_module",
            microservice=_rest_client_microservice_name(module_root),
            module=module_root_rel,
        )
        _rest_configuration_client_domains_in_module(str(repo_root), module_root_rel)
    _trace_rest_client("rest_client.search.workspace_end", modules=len(module_roots))


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


def clear_analysis_caches() -> None:
    """BACKLOG-16 P2 : vide tous les `lru_cache` d'analyse best-effort
    (package/qualified-name Java, propriétés Spring, champs `@Value`,
    module Maven, service Gradle) — à appeler en tête de chaque
    indexation. Ces caches accélèrent une indexation en cours (un même
    fichier de config lu plusieurs fois), mais un serveur MCP est un
    process long-vivant : sans purge, `reindex_findings` reservirait des
    valeurs résolues avant la modification des fichiers qui a motivé la
    réindexation."""
    _java_qualified_name.cache_clear()
    _openapi_generator_contract_paths.cache_clear()
    _java_source.cache_clear()
    _load_flat_spring_properties.cache_clear()
    _load_value_annotated_fields.cache_clear()
    _class_base_path.cache_clear()
    _rest_configuration_client_domains_in_module.cache_clear()
    _file_uses_resttemplate.cache_clear()
    _file_uses_restclient.cache_clear()
    java_parser.clear_caches()
    maven_module.clear_caches()
    gradle_module.clear_caches()
