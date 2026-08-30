import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from systemlens.cli import app
from systemlens.mcp_server import (
    add_graph_fact,
    architecture_graph,
    graph_fact_exists,
    import_graph_facts,
    index_repository,
    list_graph_facts,
    remove_graph_fact,
)

RUNNER = CliRunner()
FIXTURE = Path(__file__).parent / "fixtures" / "endpoint_index_repo"


def test_mcp_indexes_then_adds_and_removes_graph_facts(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE, repo)
    monkeypatch.chdir(repo)
    assert RUNNER.invoke(app, ["init"]).exit_code == 0
    index_repository()
    assert not graph_fact_exists("node", "data_schema", name="fraud_events")["exists"]
    node = add_graph_fact(
        "node", "data_schema", name="fraud_events", confidence="low",
        technology="sql", metadata={"database": "risk", "table": "fraud_events"},
    )
    assert graph_fact_exists("node", "data_schema", name="fraud_events")["exists"]
    with pytest.raises(ValueError, match="existe déjà"):
        add_graph_fact("node", "data_schema", name="fraud_events")
    edge = add_graph_fact(
        "edge", "http", source_kind="microservice", source_name="app",
        target_kind="data_schema", target_name="fraud_events", relation="writes",
        technology="sql",
        evidence_path="README.md", evidence_line=1,
    )
    assert {fact["id"] for fact in list_graph_facts()} == {node["id"], edge["id"]}
    graph = architecture_graph()
    assert {item["name"] for item in graph["nodes"]} >= {"fraud_events"}
    assert any(item["target"] == "data_schema:fraud_events" for item in graph["edges"])
    assert remove_graph_fact(str(node["id"])) == {"id": node["id"], "removed": True}
    assert all(fact["id"] != node["id"] for fact in list_graph_facts())


def test_import_graph_facts_upserts_generic_nodes_and_edges(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE, repo)
    monkeypatch.chdir(repo)
    assert RUNNER.invoke(app, ["init"]).exit_code == 0
    index_repository()
    manifest = {
        "format": "systemlens-ai-graph-v1",
        "generated_by": {"namespace": "ai-pass", "pass": "001", "source_revision": "abc"},
        "mode": "partial",
        "nodes": [
            {"id": "orders", "kind": "service", "name": "orders"},
            {"id": "orders-topic", "kind": "message_channel", "name": "orders.created", "technology": "kafka"},
            {"id": "orders-db", "kind": "data_schema", "name": "orders", "technology": "postgresql", "metadata": {"table": "orders"}},
        ],
        "edges": [
            {"id": "publish-orders", "source": "orders", "target": "orders-topic", "kind": "publishes", "relation": "publishes", "confidence": "high"},
            {"id": "write-orders", "source": "orders", "target": "orders-db", "kind": "writes", "relation": "writes", "confidence": "medium"},
        ],
    }
    path = repo / "facts.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    first = import_graph_facts("facts.json")
    assert first["inserted"] == 5
    assert first["updated"] == 0

    manifest["generated_by"]["pass"] = "002"
    manifest["nodes"][2]["metadata"] = {"table": "orders_v2"}
    path.write_text(json.dumps(manifest), encoding="utf-8")
    second = import_graph_facts("facts.json")
    assert second["inserted"] == 0
    assert second["updated"] == 5
    facts = list_graph_facts()
    db_fact = next(fact for fact in facts if fact["name"] == "orders" and fact["kind"] == "data_schema")
    assert db_fact["metadata"] == {"table": "orders_v2"}
    assert db_fact["pass_id"] == "002"

    graph = architecture_graph()
    assert {item["name"] for item in graph["nodes"]} >= {"orders.created", "orders"}

    manifest["mode"] = "complete"
    manifest["nodes"] = manifest["nodes"][:1]
    manifest["edges"] = []
    path.write_text(json.dumps(manifest), encoding="utf-8")
    third = import_graph_facts("facts.json")
    assert third["removed"] == 4
    assert len(list_graph_facts()) == 1
