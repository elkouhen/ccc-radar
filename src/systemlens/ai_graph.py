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
from systemlens.models import GraphFact, MessageEndpoint, compute_endpoint_id


MANIFEST_FORMAT = "systemlens-ai-graph-v1"
_NODE_KINDS = {"service", "external_service", "topic", "collection"}
_EDGE_KINDS = {"http", "event", "data"}
_STATUSES = {"confirmed", "proposed", "ambiguous", "unresolved"}
_CONFIDENCES = {"high", "medium", "low", "unknown"}
_FACT_NODE_KINDS = _NODE_KINDS | {"data_schema", "message_channel"}
_FACT_EDGE_KINDS = _EDGE_KINDS | {"serves", "calls", "reads", "writes", "publishes", "consumes", "provides"}


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


def load_fact_manifest(
    path: Path, *, namespace: str | None = None, pass_id: str | None = None,
    source_revision: str | None = None,
) -> tuple[list[GraphFact], str, bool]:
    """Load a JSON manifest as replaceable persisted graph facts.

    This is deliberately separate from ``load_ai_graph``: the latter is a
    legacy read-only HTML projection, while this adapter is the persistence
    contract for iterative enrichment.
    """
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AiGraphError(f"Impossible de lire le manifeste {path}: {exc}") from exc
    if not isinstance(document, dict) or document.get("format") != MANIFEST_FORMAT:
        raise AiGraphError(f"format attendu: {MANIFEST_FORMAT}")
    generated = document.get("generated_by")
    generated = generated if isinstance(generated, dict) else {}
    resolved_namespace = namespace or generated.get("namespace") or "ai-architecture"
    if not isinstance(resolved_namespace, str) or not resolved_namespace.strip():
        raise AiGraphError("namespace doit être une chaîne non vide.")
    resolved_pass = pass_id or generated.get("pass")
    resolved_revision = source_revision or generated.get("source_revision")
    if resolved_pass is not None and not isinstance(resolved_pass, str):
        raise AiGraphError("pass doit être une chaîne.")
    if resolved_revision is not None and not isinstance(resolved_revision, str):
        raise AiGraphError("source_revision doit être une chaîne.")
    raw_nodes = document.get("nodes")
    raw_edges = document.get("edges")
    if not isinstance(raw_nodes, list) or not isinstance(raw_edges, list):
        raise AiGraphError("nodes et edges doivent être des listes.")
    nodes: dict[str, dict[str, Any]] = {}
    facts: list[GraphFact] = []
    used_ids: set[str] = set()
    raw_ids: set[str] = set()

    def evidence_fields(raw: dict[str, Any], field: str) -> tuple[str | None, int | None]:
        evidence = _evidence(raw.get(field), field)
        first = evidence[0] if evidence else {}
        line = first.get("start_line")
        return first.get("path"), line if isinstance(line, int) and line >= 1 else None

    def fact_id(raw_id: str, fact_type: str) -> str:
        # The namespace is part of the storage key because graph_facts has a
        # single primary key and must support independent producers.
        return f"{resolved_namespace}::{fact_type}::{raw_id}"

    for index, raw in enumerate(raw_nodes):
        if not isinstance(raw, dict):
            raise AiGraphError(f"nodes[{index}] doit être un objet.")
        raw_id = _required_string(raw.get("id"), f"nodes[{index}].id")
        kind = _required_string(raw.get("kind"), f"nodes[{index}].kind")
        name = _required_string(raw.get("name"), f"nodes[{index}].name")
        if kind not in _FACT_NODE_KINDS:
            raise AiGraphError(f"nodes[{index}].kind inconnu: {kind}")
        if raw_id in raw_ids:
            raise AiGraphError(f"identifiant de nœud dupliqué: {raw_id}")
        nodes[raw_id] = raw
        raw_ids.add(raw_id)
        stored_id = fact_id(raw_id, "node")
        used_ids.add(stored_id)
        evidence_path, evidence_line = evidence_fields(raw, "evidence")
        status = raw.get("status", "confirmed")
        confidence = raw.get("confidence", "unknown")
        if status not in _STATUSES or confidence not in _CONFIDENCES:
            raise AiGraphError(f"nodes[{index}]: status ou confidence invalide.")
        if status in {"ambiguous", "unresolved"} and not raw.get("reason"):
            raise AiGraphError(f"nodes[{index}].reason est requis pour {status}.")
        metadata = raw.get("metadata", {})
        if not isinstance(metadata, dict):
            raise AiGraphError(f"nodes[{index}].metadata doit être un objet.")
        if raw.get("owner") is not None:
            metadata = {**metadata, "owner": raw["owner"]}
        facts.append(GraphFact(
            id=stored_id, fact_type="node", kind=kind, name=name,
            source_kind=None, source_name=None, target_kind=None, target_name=None,
            relation=None, origin="ai", confidence=confidence,
            evidence_path=evidence_path, evidence_line=evidence_line,
            note=raw.get("reason") if isinstance(raw.get("reason"), str) else None,
            technology=raw.get("technology") if isinstance(raw.get("technology"), str) else None,
            metadata=metadata, namespace=resolved_namespace, status=status,
            pass_id=resolved_pass, source_revision=resolved_revision,
        ))

    for index, raw in enumerate(raw_edges):
        if not isinstance(raw, dict):
            raise AiGraphError(f"edges[{index}] doit être un objet.")
        raw_id = _required_string(raw.get("id"), f"edges[{index}].id")
        source_id = _required_string(raw.get("source"), f"edges[{index}].source")
        target_id = _required_string(raw.get("target"), f"edges[{index}].target")
        kind = _required_string(raw.get("kind"), f"edges[{index}].kind")
        if raw_id in raw_ids:
            raise AiGraphError(f"identifiant de fait dupliqué: {raw_id}")
        if source_id not in nodes or target_id not in nodes:
            raise AiGraphError(f"{raw_id}: source ou target inconnu.")
        relation = raw.get("relation") or kind
        if not isinstance(relation, str) or not relation.strip():
            raise AiGraphError(f"{raw_id}.relation doit être une chaîne non vide.")
        if kind not in _FACT_EDGE_KINDS:
            raise AiGraphError(f"edges[{index}].kind inconnu: {kind}")
        status = raw.get("status", "confirmed")
        confidence = raw.get("confidence", "unknown")
        if status not in _STATUSES or confidence not in _CONFIDENCES:
            raise AiGraphError(f"{raw_id}: status ou confidence invalide.")
        if status in {"ambiguous", "unresolved"} and not raw.get("reason"):
            raise AiGraphError(f"{raw_id}.reason est requis pour {status}.")
        evidence_path, evidence_line = evidence_fields(raw, "evidence")
        metadata = raw.get("metadata", {})
        if not isinstance(metadata, dict):
            raise AiGraphError(f"{raw_id}.metadata doit être un objet.")
        source, target = nodes[source_id], nodes[target_id]
        stored_id = fact_id(raw_id, "edge")
        raw_ids.add(raw_id)
        used_ids.add(stored_id)
        facts.append(GraphFact(
            id=stored_id, fact_type="edge", kind=kind,
            name=None, source_kind=str(source["kind"]), source_name=str(source["name"]),
            target_kind=str(target["kind"]), target_name=str(target["name"]),
            relation=relation, origin="ai", confidence=confidence,
            evidence_path=evidence_path, evidence_line=evidence_line,
            note=raw.get("reason") if isinstance(raw.get("reason"), str) else None,
            technology=raw.get("technology") if isinstance(raw.get("technology"), str) else None,
            metadata=metadata, namespace=resolved_namespace, status=status,
            pass_id=resolved_pass, source_revision=resolved_revision,
        ))
    complete = document.get("mode", "partial") == "complete"
    if document.get("mode", "partial") not in {"partial", "complete"}:
        raise AiGraphError("mode doit être 'partial' ou 'complete'.")
    return facts, resolved_namespace, complete


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
