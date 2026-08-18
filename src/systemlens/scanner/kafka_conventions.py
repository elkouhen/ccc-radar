"""Inférence Kafka par convention de nommage et manifestes (BACKLOG-16).

Regroupe la convention Strategy1 (nom de méthode `send*`, clé Kafka
constante) et les inférences basées sur des manifestes déclaratifs
(tableau Markdown de topics, graphe de flux JSON) qui complètent la
détection AST de `kafka_ast.py` quand le code source n'est pas
disponible/analysable."""

from __future__ import annotations

import json
import re
from pathlib import Path

from systemlens import java_parser
from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.scanner._core import _build_endpoint
from systemlens.scanner.kafka_ast import (
    _kafka_endpoint,
    _kafka_topic_from_value,
    _producer_send_payload_type,
)

_STRATEGY1_PRODUCER_RE = re.compile(r"\bgetTopics\s*\(\s*\)\s*\.\s*get([A-Z]\w*)\s*\(\s*\)")
_STRATEGY1_KAFKA_KEY_RE = re.compile(
    r"\$\{\s*kafka\.topics\.([A-Za-z_]\w*)\.[^}:]+(?:\s*:[^}]*)?\s*\}"
)
_STRATEGY1_SEND_METHOD_PREFIX = "envoyerMessageKafka"
def _strategy1_topic_name(value: str) -> str:
    """Normalize a Java accessor or Spring property segment to a Kafka topic."""
    separated = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", separated)
    return separated.replace("-", "_").upper()
def _strategy1_topic_from_value(value_node, source: bytes, repo_root: Path, rel_path: str) -> tuple[str, bool]:
    """Resolve a Strategy1 `getTopics().getXxx()` argument before fallback."""
    if value_node is not None:
        value = java_parser.node_text(source, value_node)
        if match := _STRATEGY1_PRODUCER_RE.search(value):
            return _strategy1_topic_name(match.group(1)), False
    return _kafka_topic_from_value(value_node, source, repo_root, rel_path)
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

    Producers use `getTopics().getXxx()` or an `envoyerMessageKafka` method
    family call (`envoyerMessageKafka(topic, payload)`,
    `envoyerMessageKafkaRequest(...)`, `envoyerMessageKafkaReply(...)`, etc.).
    Listeners use a Spring key shaped as `kafka.topics.xxx.<property>`.
    Accessor and property conventions are normalized to the physical Kafka
    name in `SCREAMING_SNAKE_CASE`.
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
        parsed = java_parser.parse_java(str(repo_root), rel_path)
        if parsed is None:
            continue
        source_bytes, root = parsed
        source = source_bytes.decode("utf-8", errors="replace")
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
        for node in java_parser.walk(root):
            if node.type != "method_invocation":
                continue
            _object_node, method_name, args = java_parser.invocation_parts(node, source_bytes)
            if (
                method_name is None
                or not method_name.startswith(_STRATEGY1_SEND_METHOD_PREFIX)
                or len(args) < 2
            ):
                continue
            topic, dynamic = _strategy1_topic_from_value(args[0], source_bytes, repo_root, rel_path)
            endpoint = _kafka_endpoint(
                repo_root,
                rel_path,
                source_bytes,
                node,
                "produce",
                "kafka-topic-strategy1",
                topic,
                dynamic,
                _producer_send_payload_type(source_bytes, node),
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

