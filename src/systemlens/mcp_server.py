"""MCP surface for the index-then-enrich architecture workflow."""

import hashlib
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from systemlens.architecture_inventory import load_architecture_inventory
from systemlens.ai_graph import AiGraphError, load_fact_manifest
from systemlens.config import load_config
from systemlens.dependency_analysis import (
    DependencyEdge,
    DependencyGraphResult,
    DependencyNode,
    build_dependency_graph,
)
from systemlens.indexer import IndexReport, index_repo
from systemlens.models import GraphFact
from systemlens.paths import db_path
from systemlens.store import Store

mcp = FastMCP("systemlens")
_FACT_TYPES = {"node", "edge"}
_CONFIDENCES = {"high", "medium", "low", "unknown"}
_STATUSES = {"confirmed", "proposed", "ambiguous", "unresolved"}


def _repo_root() -> Path:
    return Path.cwd()


def _require_index(repo_root: Path) -> None:
    if not db_path(repo_root).is_file():
        raise RuntimeError("Index absent. Lancez d'abord index_repository.")


def _validate_path(path: str | None) -> str | None:
    if path is None:
        return None
    candidate = Path(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("evidence_path doit être relatif au projet, sans '..'.")
    return candidate.as_posix()


def _validate_namespace(namespace: str) -> str:
    value = namespace.strip()
    if not value or any(char.isspace() for char in value):
        raise ValueError("namespace doit être une chaîne non vide sans espace.")
    return value


def _fact_id(*values: str | None) -> str:
    return hashlib.sha256("|".join(value or "" for value in values).encode()).hexdigest()[:16]


def _make_fact(
    fact_type: str, kind: str, name: str | None = None,
    source_kind: str | None = None, source_name: str | None = None,
    target_kind: str | None = None, target_name: str | None = None,
    relation: str | None = None, confidence: str = "medium",
    evidence_path: str | None = None, evidence_line: int | None = None,
    note: str | None = None, technology: str | None = None,
    metadata: dict[str, object] | None = None,
    namespace: str = "manual", status: str = "confirmed",
) -> GraphFact:
    if fact_type not in _FACT_TYPES:
        raise ValueError("fact_type doit être 'node' ou 'edge'.")
    if not kind.strip():
        raise ValueError("kind ne peut pas être vide.")
    if confidence not in _CONFIDENCES:
        raise ValueError(f"confidence doit être l'une de {sorted(_CONFIDENCES)}.")
    if status not in _STATUSES:
        raise ValueError(f"status doit être l'un de {sorted(_STATUSES)}.")
    if fact_type == "node" and not name:
        raise ValueError("name est requis pour un nœud.")
    if fact_type == "edge" and not all((source_kind, source_name, target_kind, target_name, relation)):
        raise ValueError("Une arête requiert les champs source, target et relation.")
    if fact_type == "node" and any((source_kind, source_name, target_kind, target_name, relation)):
        raise ValueError("Les champs source/target/relation sont réservés aux arêtes.")
    if evidence_line is not None and evidence_line < 1:
        raise ValueError("evidence_line doit être supérieur ou égal à 1.")
    resolved_namespace = _validate_namespace(namespace)
    fact_id = _fact_id(resolved_namespace, fact_type, kind, name, source_kind, source_name,
                       target_kind, target_name, relation)
    return GraphFact(
        id=fact_id, fact_type=fact_type, kind=kind.strip(), name=name,
        source_kind=source_kind, source_name=source_name,
        target_kind=target_kind, target_name=target_name, relation=relation,
        origin="mcp", confidence=confidence, evidence_path=_validate_path(evidence_path),
        evidence_line=evidence_line, note=note, technology=technology,
        metadata=metadata or {}, namespace=resolved_namespace, status=status,
    )


@mcp.tool()
def index_repository(topic_strategy: str = "default", full: bool = False) -> IndexReport:
    """Indexe le répertoire courant avant tout enrichissement du graphe."""
    if topic_strategy not in {"default", "strategy1"}:
        raise ValueError("topic_strategy doit être 'default' ou 'strategy1'.")
    repo_root = _repo_root()
    with Store(repo_root) as store:
        return index_repo(repo_root, load_config(repo_root), store,
                          full=full, topic_strategy=topic_strategy)


def reindex_architecture() -> IndexReport:
    """Compatibility helper for Python callers; not exposed as an MCP tool."""
    repo_root = _repo_root()
    with Store(repo_root) as store:
        strategy = store.get_meta("topic_strategy") or "default"
        return index_repo(repo_root, load_config(repo_root), store,
                          topic_strategy=strategy)


@mcp.tool()
def graph_fact_exists(
    fact_type: str, kind: str, name: str | None = None,
    source_kind: str | None = None, source_name: str | None = None,
    target_kind: str | None = None, target_name: str | None = None,
    relation: str | None = None, namespace: str = "manual",
) -> dict[str, object]:
    """Check whether the same semantic enrichment fact is already present."""
    repo_root = _repo_root()
    _require_index(repo_root)
    fact = _make_fact(fact_type, kind, name, source_kind, source_name,
                      target_kind, target_name, relation, namespace=namespace)
    with Store(repo_root, readonly=True) as store:
        existing = store.graph_fact_by_id(fact.id)
    return {"exists": existing is not None, "id": fact.id,
            "fact": dict(existing.__dict__) if existing else None}


@mcp.tool()
def add_graph_fact(
    fact_type: str, kind: str, name: str | None = None,
    source_kind: str | None = None, source_name: str | None = None,
    target_kind: str | None = None, target_name: str | None = None,
    relation: str | None = None, confidence: str = "medium",
    evidence_path: str | None = None, evidence_line: int | None = None,
    note: str | None = None, technology: str | None = None,
    metadata: dict[str, object] | None = None,
    namespace: str = "manual", status: str = "confirmed",
) -> dict[str, object]:
    """Add one assertion; reject an existing semantic duplicate."""
    repo_root = _repo_root()
    _require_index(repo_root)
    fact = _make_fact(fact_type, kind, name, source_kind, source_name,
                      target_kind, target_name, relation, confidence,
                      evidence_path, evidence_line, note, technology, metadata,
                      namespace, status)
    with Store(repo_root) as store:
        if not store.insert_graph_fact(fact):
            raise ValueError(
                f"Le fait existe déjà (id={fact.id}). Utilisez graph_fact_exists "
                "ou remove_graph_fact avant de proposer une nouvelle assertion."
            )
    return dict(fact.__dict__)


@mcp.tool()
def import_graph_facts(
    manifest_path: str, namespace: str | None = None, complete: bool | None = None,
) -> dict[str, object]:
    """Validate and atomically upsert an AI fact manifest into SQLite."""
    repo_root = _repo_root()
    _require_index(repo_root)
    candidate = Path(manifest_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("manifest_path doit être relatif au projet, sans '..'.")
    path = (repo_root / candidate).resolve()
    if repo_root not in path.parents and path != repo_root:
        raise ValueError("manifest_path doit rester dans le projet.")
    try:
        facts, resolved_namespace, manifest_complete = load_fact_manifest(path, namespace=namespace)
    except AiGraphError as exc:
        raise ValueError(str(exc)) from exc
    replace_stale = manifest_complete if complete is None else complete
    inserted = updated = 0
    with Store(repo_root) as store:
        with store.transaction():
            existing = {fact.id for fact in store.graph_facts_by_namespace(resolved_namespace)}
            for fact in facts:
                if fact.id in existing:
                    updated += 1
                else:
                    inserted += 1
                store.upsert_graph_fact(fact)
            removed = store.delete_graph_facts_not_in(
                resolved_namespace, {fact.id for fact in facts}
            ) if replace_stale else 0
    return {
        "namespace": resolved_namespace, "inserted": inserted, "updated": updated,
        "removed": removed, "facts": len(facts), "complete": replace_stale,
    }


@mcp.tool()
def remove_graph_fact(fact_id: str) -> dict[str, object]:
    """Supprime une assertion MCP, jamais un fait extrait du code."""
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root) as store:
        removed = store.delete_graph_fact(fact_id)
    return {"id": fact_id, "removed": removed}


@mcp.tool()
def list_graph_facts() -> list[dict[str, object]]:
    """Liste les faits ajoutés par MCP, séparément des faits du code."""
    repo_root = _repo_root()
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        return [dict(fact.__dict__) for fact in store.all_graph_facts()]


@mcp.tool()
def architecture_graph() -> DependencyGraphResult:
    """Return the complete generic graph, including APIs and data resources."""
    repo_root = _repo_root()
    inventory = load_architecture_inventory(repo_root)
    result = build_dependency_graph(
        inventory.endpoints_by_service, inventory.modules_by_service,
        warnings=inventory.warnings, relations=inventory.relations,
        strategy1=inventory.strategy1,
    )
    with Store(repo_root, readonly=True) as store:
        facts = store.all_graph_facts()
    node_keys = {(node["kind"], node["name"]) for node in result["nodes"]}
    for fact in facts:
        if fact.fact_type == "node" and fact.name is not None:
            if (fact.kind, fact.name) not in node_keys:
                node: DependencyNode = {"id": f"{fact.kind}:{fact.name}", "kind": fact.kind,
                        "name": fact.name, "service": None, "external": False,
                        "status": fact.status}
                if fact.technology:
                    node["technology"] = fact.technology
                if fact.metadata:
                    node["metadata"] = fact.metadata
                result["nodes"].append(node)
                node_keys.add((fact.kind, fact.name))
    known_node_ids = {str(node["id"]) for node in result["nodes"]}
    known_edges = {
        (str(edge["source"]), str(edge["target"]), str(edge["kind"]), str(edge["label"]))
        for edge in result["edges"]
    }
    for fact in facts:
        if fact.fact_type == "edge":
            assert fact.source_kind and fact.source_name and fact.target_kind and fact.target_name
            source = f"{fact.source_kind}:{fact.source_name}"
            target = f"{fact.target_kind}:{fact.target_name}"
            for node_id, node_kind, node_name in (
                (source, fact.source_kind, fact.source_name),
                (target, fact.target_kind, fact.target_name),
            ):
                if node_id not in known_node_ids:
                    result["nodes"].append({
                        "id": node_id, "kind": node_kind, "name": node_name,
                        "service": None, "external": False,
                    })
                    known_node_ids.add(node_id)
                    result["warnings"].append(
                        f"Nœud MCP synthétique créé pour le fait : {node_id}."
                    )
            edge_key = (source, target, fact.kind, fact.relation or "")
            if edge_key in known_edges:
                continue
            edge: DependencyEdge = {"source": source, "target": target, "kind": fact.kind,
                    "label": fact.relation or "", "confidence": fact.confidence,
                    "status": fact.status}
            if fact.technology:
                edge["technology"] = fact.technology
            if fact.metadata:
                edge["metadata"] = fact.metadata
            result["edges"].append(edge)
            known_edges.add(edge_key)
    result["summary"]["relations"] = len(result["edges"])
    return result
