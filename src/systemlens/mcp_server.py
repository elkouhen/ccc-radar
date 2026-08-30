"""MCP surface for the index-then-enrich architecture workflow."""

import hashlib
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from systemlens.architecture_inventory import load_architecture_inventory
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
) -> GraphFact:
    if fact_type not in _FACT_TYPES:
        raise ValueError("fact_type doit être 'node' ou 'edge'.")
    if not kind.strip():
        raise ValueError("kind ne peut pas être vide.")
    if confidence not in _CONFIDENCES:
        raise ValueError(f"confidence doit être l'une de {sorted(_CONFIDENCES)}.")
    if fact_type == "node" and not name:
        raise ValueError("name est requis pour un nœud.")
    if fact_type == "edge" and not all((source_kind, source_name, target_kind, target_name, relation)):
        raise ValueError("Une arête requiert les champs source, target et relation.")
    if fact_type == "node" and any((source_kind, source_name, target_kind, target_name, relation)):
        raise ValueError("Les champs source/target/relation sont réservés aux arêtes.")
    if evidence_line is not None and evidence_line < 1:
        raise ValueError("evidence_line doit être supérieur ou égal à 1.")
    fact_id = _fact_id(fact_type, kind, name, source_kind, source_name,
                       target_kind, target_name, relation)
    return GraphFact(
        id=fact_id, fact_type=fact_type, kind=kind.strip(), name=name,
        source_kind=source_kind, source_name=source_name,
        target_kind=target_kind, target_name=target_name, relation=relation,
        origin="mcp", confidence=confidence, evidence_path=_validate_path(evidence_path),
        evidence_line=evidence_line, note=note, technology=technology,
        metadata=metadata or {},
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
    relation: str | None = None,
) -> dict[str, object]:
    """Check whether the same semantic enrichment fact is already present."""
    repo_root = _repo_root()
    _require_index(repo_root)
    fact = _make_fact(fact_type, kind, name, source_kind, source_name,
                      target_kind, target_name, relation)
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
) -> dict[str, object]:
    """Add one assertion; reject an existing semantic duplicate."""
    repo_root = _repo_root()
    _require_index(repo_root)
    fact = _make_fact(fact_type, kind, name, source_kind, source_name,
                      target_kind, target_name, relation, confidence,
                      evidence_path, evidence_line, note, technology, metadata)
    with Store(repo_root) as store:
        if not store.insert_graph_fact(fact):
            raise ValueError(
                f"Le fait existe déjà (id={fact.id}). Utilisez graph_fact_exists "
                "ou remove_graph_fact avant de proposer une nouvelle assertion."
            )
    return dict(fact.__dict__)


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
                        "name": fact.name, "service": None, "external": False}
                if fact.technology:
                    node["technology"] = fact.technology
                if fact.metadata:
                    node["metadata"] = fact.metadata
                result["nodes"].append(node)
                node_keys.add((fact.kind, fact.name))
        elif fact.fact_type == "edge":
            assert fact.source_kind and fact.source_name and fact.target_kind and fact.target_name
            source = f"{fact.source_kind}:{fact.source_name}"
            target = f"{fact.target_kind}:{fact.target_name}"
            edge: DependencyEdge = {"source": source, "target": target, "kind": fact.kind,
                    "label": fact.relation or "", "confidence": fact.confidence}
            if fact.technology:
                edge["technology"] = fact.technology
            if fact.metadata:
                edge["metadata"] = fact.metadata
            result["edges"].append(edge)
    result["summary"]["relations"] = len(result["edges"])
    return result
