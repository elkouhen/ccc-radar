import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from systemlens.cli import app
from systemlens.mcp_server import (
    add_graph_fact,
    architecture_graph,
    graph_fact_exists,
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
