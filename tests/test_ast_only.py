from __future__ import annotations

import shutil
from dataclasses import replace
from pathlib import Path

from typer.testing import CliRunner

from ccc_radar.cli import app
from ccc_radar.config import Config
from ccc_radar.flow import group_endpoints_by_module_for_flow, trace_flow
from ccc_radar.graph import build_graph
from ccc_radar.indexer import index_repo
from ccc_radar.scanner import infer_framework_endpoints, infer_kafka_endpoints
from ccc_radar.store import Store


FIXTURES = Path(__file__).parent / "fixtures"
RUNNER = CliRunner()


def test_init_writes_ast_only_configuration(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = RUNNER.invoke(app, ["init"])

    assert result.exit_code == 0
    content = (tmp_path / ".cccr" / "config.yml").read_text()
    assert "include:" in content
    assert "rules:" not in content
    assert "embedding_model:" not in content


def test_ast_extractors_find_rest_and_kafka_facts() -> None:
    rest = infer_framework_endpoints(FIXTURES / "rest_repo")
    kafka = infer_kafka_endpoints(FIXTURES / "kafka_repo")

    assert any(endpoint.role == "serve" and endpoint.system == "rest" for endpoint in rest)
    assert any(endpoint.role == "call" and endpoint.framework == "feign" for endpoint in rest)
    assert any(endpoint.role == "produce" and endpoint.system == "kafka" for endpoint in kafka)
    assert any(endpoint.role == "consume" and endpoint.message_type for endpoint in kafka)


def test_index_is_incremental_without_embeddings(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "endpoint_index_repo", repo)

    with Store(repo) as store:
        first = index_repo(repo, Config(), store)
        second = index_repo(repo, Config(), store)
        endpoints = store.all_endpoints()

    assert first.scanned == 2
    assert first.endpoints_added == 2
    assert len(endpoints) == 2
    assert second.scanned == 0


def test_cli_index_does_not_require_an_embedding_model(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "endpoint_index_repo", repo)
    monkeypatch.chdir(repo)
    assert RUNNER.invoke(app, ["init"]).exit_code == 0
    result = RUNNER.invoke(app, ["index"])

    assert result.exit_code == 0
    assert "+integrations=2" in result.output
    assert "cccr export microservices --html architecture.html" in result.output


def test_indexed_kafka_facts_build_a_traceable_service_edge(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "kafka_repo", repo)

    with Store(repo) as store:
        index_repo(repo, Config(), store)
        endpoints = store.all_endpoints()

    by_service = group_endpoints_by_module_for_flow(endpoints)
    topic = next(endpoint.topic for endpoint in endpoints if endpoint.system == "kafka")
    flow = trace_flow(topic, by_service)

    assert flow.resolved_topic == topic
    assert flow.sites


def test_rest_graph_uses_ast_endpoint_facts() -> None:
    endpoints = infer_framework_endpoints(FIXTURES / "rest_repo")
    served = next(endpoint for endpoint in endpoints if endpoint.role == "serve")
    called = replace(next(endpoint for endpoint in endpoints if endpoint.role == "call"), topic=served.topic)
    graph = build_graph({"server": [served], "caller": [called]})

    assert graph
