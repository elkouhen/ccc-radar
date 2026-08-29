"""Load a conservative, AI-produced architecture graph manifest.

The manifest is an input adapter only: it is never persisted in the SQLite
source inventory.  Confirmed and proposed relations are projected onto the
existing HTML graph model; ambiguous and unresolved claims become indexing
issues so an AI cannot silently turn a guess into a dependency.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from systemlens.graph import GraphEdge
from systemlens.models import MessageEndpoint, compute_endpoint_id


MANIFEST_FORMAT = "systemlens-ai-graph-v1"
_NODE_KINDS = {"service", "external_service", "topic", "collection"}
_EDGE_KINDS = {"http", "event", "data"}
_STATUSES = {"confirmed", "proposed", "ambiguous", "unresolved"}
_CONFIDENCES = {"high", "medium", "low", "unknown"}


class AiGraphError(ValueError):
    """A safe, user-actionable AI graph manifest validation error."""


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AiGraphError(f"{field} doit être une chaîne non vide.")
    return value.strip()


def _evidence(value: Any, field: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise AiGraphError(f"{field} doit être une liste.")
    result: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise AiGraphError(f"{field}[{index}] doit être un objet.")
        path = item.get("path")
        if path is not None:
            path = _required_string(path, f"{field}[{index}].path")
            if Path(path).is_absolute() or "\\" in path:
                raise AiGraphError(f"{field}[{index}].path doit être relatif au projet.")
        result.append(dict(item))
    return result


def load_ai_graph(path: Path) -> tuple[dict[str, list[MessageEndpoint]], list[GraphEdge], dict[str, list[str]], list[str]]:
    """Validate and project an AI graph manifest onto the current graph model."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AiGraphError(f"Impossible de lire le manifeste {path}: {exc}") from exc
    if not isinstance(document, dict) or document.get("format") != MANIFEST_FORMAT:
        raise AiGraphError(f"format attendu: {MANIFEST_FORMAT}")

    raw_nodes = document.get("nodes")
    raw_edges = document.get("edges")
    if not isinstance(raw_nodes, list) or not isinstance(raw_edges, list):
        raise AiGraphError("nodes et edges doivent être des listes.")

    nodes: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(raw_nodes):
        if not isinstance(raw, dict):
            raise AiGraphError(f"nodes[{index}] doit être un objet.")
        node_id = _required_string(raw.get("id"), f"nodes[{index}].id")
        kind = _required_string(raw.get("kind"), f"nodes[{index}].kind")
        if kind not in _NODE_KINDS:
            raise AiGraphError(f"nodes[{index}].kind inconnu: {kind}")
        if node_id in nodes:
            raise AiGraphError(f"identifiant de nœud dupliqué: {node_id}")
        node = dict(raw)
        node["name"] = _required_string(raw.get("name"), f"nodes[{index}].name")
        node["evidence"] = _evidence(raw.get("evidence"), f"nodes[{index}].evidence")
        nodes[node_id] = node

    services: dict[str, list[MessageEndpoint]] = {
        str(node["name"]): []
        for node in nodes.values()
        if node["kind"] in {"service", "external_service"}
    }
    collections: dict[str, list[str]] = {}
    for node in nodes.values():
        if node["kind"] == "collection":
            owner = node.get("owner")
            if not isinstance(owner, str) or owner not in services:
                raise AiGraphError(f"la collection {node['id']} doit avoir un owner service valide.")
            collections.setdefault(owner, []).append(str(node["name"]))

    edges: list[GraphEdge] = []
    issues: list[str] = []
    for index, raw in enumerate(raw_edges):
        if not isinstance(raw, dict):
            raise AiGraphError(f"edges[{index}] doit être un objet.")
        edge_id = _required_string(raw.get("id"), f"edges[{index}].id")
        source_id = _required_string(raw.get("source"), f"edges[{index}].source")
        target_id = _required_string(raw.get("target"), f"edges[{index}].target")
        kind = _required_string(raw.get("kind"), f"edges[{index}].kind")
        status = raw.get("status", "confirmed")
        confidence = raw.get("confidence", "unknown")
        if source_id not in nodes or target_id not in nodes:
            raise AiGraphError(f"{edge_id}: source ou target inconnu.")
        if kind not in _EDGE_KINDS or status not in _STATUSES or confidence not in _CONFIDENCES:
            raise AiGraphError(f"{edge_id}: kind, status ou confidence invalide.")
        evidence = _evidence(raw.get("evidence"), f"edges[{index}].evidence")
        if status in {"ambiguous", "unresolved"}:
            reason = raw.get("reason", "relation non résolue")
            issues.append(f"{edge_id}: {reason}")
            continue
        source = nodes[source_id]
        target = nodes[target_id]
        source_name, target_name = str(source["name"]), str(target["name"])
        if kind == "data":
            if source["kind"] not in {"service", "external_service"} or target["kind"] != "collection":
                raise AiGraphError(f"{edge_id}: une relation data doit viser une collection depuis un service.")
            continue
        if source["kind"] not in {"service", "external_service"} or target["kind"] not in {"service", "external_service"}:
            raise AiGraphError(f"{edge_id}: une relation {kind} doit relier deux services.")
        source_site = _endpoint("call" if kind == "http" else "produce", kind, raw, source_name, edge_id, evidence)
        target_site = _endpoint("serve" if kind == "http" else "consume", kind, raw, target_name, edge_id, evidence)
        edges.append(GraphEdge("rest" if kind == "http" else "kafka", source_name, target_name, source_site, target_site))
    return services, edges, collections, issues


def _endpoint(role: str, kind: str, raw: dict[str, Any], service: str, edge_id: str, evidence: list[dict[str, Any]]) -> MessageEndpoint:
    channel = _required_string(raw.get("channel") or raw.get("label") or edge_id, f"{edge_id}.channel")
    path = evidence[0].get("path", "ai-graph.json") if evidence else "ai-graph.json"
    line = evidence[0].get("start_line", 1) if evidence else 1
    if not isinstance(line, int) or line < 1:
        line = 1
    topic = channel if kind == "event" else str(raw.get("label") or channel)
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, str(path), line), role=role,
        system="kafka" if kind == "event" else "rest", topic=topic,
        topic_dynamic=False, source="manifest", framework="ai-analysis",
        path=str(path), start_line=line, end_line=line,
        snippet=f"systemlens-ai-edge:{edge_id}",
        message_type=raw.get("message_type") if isinstance(raw.get("message_type"), str) else None,
    )
