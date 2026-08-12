"""Moteur tree-sitter Java partagé et primitives d'extraction AST.

Ce module centralise tout ce qui relève de l'analyse syntaxique Java par
tree-sitter : le `Language`/`Parser` (singletons paresseux), la traversée de
l'arbre et un jeu d'helpers de haut niveau (annotation, invocation, littéraux,
concaténation) utilisés à la fois par `modules.py` (inventaire de modules) et
par `scanner.py` (découverte d'endpoints REST/Kafka).

tree-sitter-java expose les annotations à l'intérieur d'un nœud `modifiers`
enfant de la déclaration (validé à l'usage) ; les arguments nommés d'une
annotation sont des `element_value_pair [key/value]`, les positionnels des
expressions nues ; un `method_invocation` a les champs `object`/`name`/
`arguments` (le chaînage fluent `.a().b()` s'imbrique via `object`) ; une
concaténation de chaînes est un `binary_expression` d'opérateur `+`.
"""

import hashlib
import os
import re
import sys
import time
from functools import lru_cache
from pathlib import Path

from tree_sitter import Language, Node, Parser

import tree_sitter_java

_JAVA_LANGUAGE: Language | None = None
_JAVA_PARSER: Parser | None = None

_ANNOTATION_TYPES = frozenset({"annotation", "marker_annotation"})
_ESCAPE_RE = re.compile(r"\\(.)")


def _trace(stage: str, **fields: object) -> None:
    if os.environ.get("CODEATLAS_TRACE") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(
        f"CODEATLAS_TRACE ts={time.monotonic():.6f} stage={stage} {details}".rstrip(),
        file=sys.stderr,
        flush=True,
    )


def java_language() -> Language:
    """Lazy singleton for the tree-sitter-java `Language`."""
    global _JAVA_LANGUAGE
    if _JAVA_LANGUAGE is None:
        _trace("java_language.begin")
        _JAVA_LANGUAGE = Language(tree_sitter_java.language())
        _trace("java_language.end")
    return _JAVA_LANGUAGE


def java_parser(owner: str = "") -> Parser:
    """Lazy singleton `Parser` (the same instance is reused across all callers).

    ``owner`` is instrumentation-only — it labels the trace so a native crash
    report identifies which extractor was running.
    """
    global _JAVA_PARSER
    _trace("java_parser.begin", owner=owner)
    if _JAVA_PARSER is None:
        _trace("java_parser.create.begin", owner=owner)
        _JAVA_PARSER = Parser()
        _JAVA_PARSER.language = java_language()
        _trace("java_parser.create.end", owner=owner)
    _trace("java_parser.end", owner=owner)
    return _JAVA_PARSER


@lru_cache(maxsize=512)
def parse_java(repo_root_str: str, rel_path: str) -> tuple[bytes, Node] | None:
    """Parse a `.java` file once and return ``(source_bytes, root_node)``.

    Returns ``None`` when the file cannot be read or fails to parse
    (``root_node.has_error``).  Cached: the same tree is reused for every
    extractor that visits the file during one indexation.  Cleared by
    :func:`clear_caches` (wired into ``scanner.clear_analysis_caches``).
    """
    path = Path(repo_root_str) / rel_path
    try:
        source = path.read_bytes()
    except OSError:
        return None
    tree = java_parser("parse_java").parse(source)
    if tree.root_node.has_error:
        return None
    return source, tree.root_node


def clear_caches() -> None:
    """Purge le cache des arbres parsés (à appeler en tête d'indexation)."""
    parse_java.cache_clear()


# --- Traversal primitives ------------------------------------------------


def walk(node: Node):
    """Depth-first generator yielding ``node`` then all its descendants."""
    yield node
    for child in node.children:
        yield from walk(child)


def node_text(source: bytes, node: Node) -> str:
    return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def enclosing(node: Node, *types: str) -> Node | None:
    """Nearest ancestor whose ``type`` is in ``types`` (or ``None``)."""
    current = node.parent
    while current is not None:
        if current.type in types:
            return current
        current = current.parent
    return None


def enclosing_method_name(source: bytes, node: Node) -> str | None:
    """Name of the method enclosing ``node``, if any."""
    method = enclosing(node, "method_declaration")
    if method is None:
        return None
    name = method.child_by_field_name("name")
    return node_text(source, name) if name is not None else None


def has_spring_boot_main_class(source: bytes) -> bool:
    """Whether parsed Java source contains a Spring Boot ``main`` entrypoint."""
    root = java_parser("spring_boot_main").parse(source).root_node
    if root.has_error:
        return False
    for method in walk(root):
        if method.type != "method_declaration":
            continue
        name = method.child_by_field_name("name")
        if name is None or node_text(source, name) != "main":
            continue
        modifiers = modifiers_node(method)
        if modifiers is None or not any(node_text(source, child) == "static" for child in modifiers.children):
            continue
        return_type = method.child_by_field_name("type")
        if return_type is None or node_text(source, return_type) != "void":
            continue
        for invocation in walk(method):
            if invocation.type != "method_invocation":
                continue
            receiver, method_name, _arguments = invocation_parts(invocation, source)
            if receiver is not None and method_name == "run" and node_text(source, receiver) == "SpringApplication":
                return True
    return False


def evidence_fields(source: bytes, node: Node, rel_path: str) -> tuple[int, int, str, str]:
    """1-based ``(start_line, end_line, snippet, source_hash)`` for a node.

    The hash mixes ``rel_path``, ``node.type`` and the snippet so identical
    snippets in different node kinds do not collide.
    """
    snippet = node_text(source, node)
    digest = hashlib.sha256(f"{rel_path}|{node.type}|{snippet}".encode()).hexdigest()
    return node.start_point.row + 1, node.end_point.row + 1, snippet, f"sha256:{digest}"


# --- Structural helpers --------------------------------------------------


def child_by_type(node: Node, type_name: str) -> Node | None:
    """First direct child of ``node`` with the given ``type`` (or ``None``)."""
    for child in node.children:
        if child.type == type_name:
            return child
    return None


_TYPE_DECL_TYPES = frozenset(
    {"class_declaration", "interface_declaration", "record_declaration", "enum_declaration"}
)


def type_declarations(root: Node):
    """Yield every class/interface/record/enum declaration in the tree."""
    for node in walk(root):
        if node.type in _TYPE_DECL_TYPES:
            yield node


def declaration_name(node: Node, source: bytes) -> str | None:
    name = node.child_by_field_name("name")
    return node_text(source, name) if name is not None else None


def modifiers_node(decl: Node) -> Node | None:
    """The `modifiers` child of a declaration (holds annotations + keywords)."""
    return child_by_type(decl, "modifiers")


def annotations_of(decl: Node) -> list[Node]:
    """Annotation nodes (`annotation`/`marker_annotation`) carried by a decl.

    tree-sitter-java nests annotations inside the declaration's `modifiers`
    child rather than as preceding siblings.
    """
    mods = modifiers_node(decl)
    if mods is None:
        return []
    return [c for c in mods.children if c.type in _ANNOTATION_TYPES]


def annotation_name(ann: Node, source: bytes) -> str:
    """Simple (last segment) name of an annotation: ``GetMapping``."""
    name = ann.child_by_field_name("name")
    raw = node_text(source, name) if name is not None else ""
    return raw.rsplit(".", 1)[-1]


def annotation_argument_list(ann: Node) -> Node | None:
    """The `annotation_argument_list` child, or ``None`` for a marker annotation."""
    return child_by_type(ann, "annotation_argument_list")


def annotation_argument_nodes(ann: Node) -> list[Node]:
    """Argument expression nodes of an annotation (positional + named values)."""
    args = annotation_argument_list(ann)
    if args is None:
        return []
    return [c for c in args.children if c.type not in {"(", ")", ","}]


def annotation_argument(ann: Node, source: bytes, key: str | None = None) -> Node | None:
    """A specific annotation argument node.

    With ``key`` set: the ``value`` of the ``element_value_pair`` whose ``key``
    matches.  Without ``key``: the first positional argument, or ``None``.
    """
    args = annotation_argument_list(ann)
    if args is None:
        return None
    positional: Node | None = None
    for child in args.children:
        if child.type == "element_value_pair":
            key_node = child.child_by_field_name("key")
            if key is not None and key_node is not None and node_text(source, key_node) == key:
                return child.child_by_field_name("value")
        elif child.type not in {"(", ")", ","}:
            if positional is None:
                positional = child
    return None if key is not None else positional


def string_value(node: Node | None, source: bytes) -> str | None:
    """Decoded value of a `string_literal` node (minimal escape handling)."""
    if node is None or node.type != "string_literal":
        return None
    fragment = child_by_type(node, "string_fragment")
    if fragment is None:
        return ""
    return _ESCAPE_RE.sub(r"\1", node_text(source, fragment))


def first_string_argument(node: Node, source: bytes) -> str | None:
    """Decoded value of the first positional `string_literal` argument.

    ``node`` is an `annotation`/`marker_annotation` (reads its argument list)
    or a `method_invocation` (reads its `argument_list`).
    """
    container = annotation_argument_list(node)
    if container is None:
        container = child_by_type(node, "argument_list")
    if container is None:
        return None
    for child in container.children:
        if child.type == "string_literal":
            return string_value(child, source)
    return None


def argument_nodes(invocation: Node) -> list[Node]:
    """Top-level argument expression nodes of a `method_invocation`."""
    args = child_by_type(invocation, "argument_list")
    if args is None:
        return []
    return [c for c in args.children if c.type not in {"(", ")", ","}]


def invocation_parts(invocation: Node, source: bytes) -> tuple[Node | None, str, list[Node]]:
    """``(receiver_node, method_name, argument_nodes)`` for a `method_invocation`.

    ``receiver_node`` is ``None`` for an unqualified call; for a fluent chain
    ``a.b().c()`` the outer invocation's receiver is the inner ``a.b()``
    invocation.
    """
    name_node = invocation.child_by_field_name("name")
    name = node_text(source, name_node) if name_node is not None else ""
    return (
        invocation.child_by_field_name("object"),
        name,
        argument_nodes(invocation),
    )


def collect_concat_parts(expr: Node, source: bytes) -> list[tuple[str, str]]:
    """Flatten a Java string-concatenation expression into typed parts.

    Returns ``("lit", value)`` for string literals, ``("id", name)`` for bare
    identifiers, ``("other", text)`` for anything else (method calls, field
    access, …).  A ``+`` chain is a nested ``binary_expression``.
    """
    if expr.type == "binary_expression":
        left = expr.child_by_field_name("left")
        right = expr.child_by_field_name("right")
        parts: list[tuple[str, str]] = []
        if left is not None:
            parts.extend(collect_concat_parts(left, source))
        if right is not None:
            parts.extend(collect_concat_parts(right, source))
        return parts
    if expr.type == "string_literal":
        value = string_value(expr, source)
        return [("lit", value if value is not None else "")]
    if expr.type == "identifier":
        return [("id", node_text(source, expr))]
    return [("other", node_text(source, expr))]
