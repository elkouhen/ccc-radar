from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from systemlens.apm_overlay import (
    build_microservice_overlay,
    correlate_service_name,
)
from systemlens.cli import app


def test_correlate_service_name_matches_exact_normalized_name() -> None:
    correlation = correlate_service_name("Orders-Service", ["orders_service", "payments"])

    assert correlation.status == "matched"
    assert correlation.node_name == "orders_service"


def test_correlate_service_name_accepts_single_best_containment_candidate() -> None:
    correlation = correlate_service_name("orders", ["orders-api-v2", "payments"])

    assert correlation.status == "heuristic"
    assert correlation.node_name == "orders-api-v2"
    assert correlation.candidates == ["orders-api-v2"]


def test_correlate_service_name_is_ambiguous_on_a_strict_containment_tie() -> None:
    correlation = correlate_service_name("orders", ["orders-api", "orders-bff"])

    assert correlation.status == "ambiguous"
    assert correlation.node_name is None
    assert correlation.candidates == ["orders-api", "orders-bff"]


def test_correlate_service_name_is_unmapped_without_any_candidate() -> None:
    correlation = correlate_service_name("billing", ["orders", "payments"])

    assert correlation.status == "unmapped"
    assert correlation.node_name is None
    assert correlation.candidates == []


def _relation_bucket(
    source: str, target: str, outcome: str, calls: int, duration_us: int
) -> dict[str, object]:
    return {
        "key": {
            "source": source,
            "target": target,
            "target_type": "http",
            "outcome": outcome,
        },
        "calls": {"value": calls},
        "duration_us": {"value": duration_us},
    }


def _latency_bucket(
    key: dict[str, object], calls: int, duration_us: int, p95_us: int, failures: int
) -> dict[str, object]:
    return {
        "key": key,
        "calls": {"value": calls},
        "duration_us": {"value": duration_us},
        "p95": {"values": {"95.0": p95_us}},
        "failures": {"calls": {"value": failures}},
    }


class FakeOverlayClient:
    """Serves the same two aggregate views as ``apm export``/``apm report``."""

    def __init__(self) -> None:
        self.queries: list[dict[str, object]] = []

    def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
        self.queries.append(body)
        metricset = body["query"]["bool"]["filter"][0]["term"]["metricset.name"]  # type: ignore[index]
        if metricset == "service_transaction":
            buckets = [
                _latency_bucket({"service": "orders"}, 10, 500_000, 900_000, 1),
                _latency_bucket({"service": "orders-worker"}, 4, 200_000, 300_000, 0),
                _latency_bucket({"service": "unknown-svc"}, 2, 100_000, 150_000, 0),
            ]
            return {"aggregations": {"items": {"buckets": buckets}}}
        buckets = [
            _relation_bucket("orders", "payments", "success", 8, 400_000),
            _relation_bucket("orders", "payments", "failure", 2, 200_000),
            _relation_bucket("orders", "ghost-service", "success", 3, 90_000),
        ]
        return {"aggregations": {"relations": {"buckets": buckets}}}


def test_build_microservice_overlay_attaches_matched_and_heuristic_observations() -> None:
    client = FakeOverlayClient()

    overlay = build_microservice_overlay(
        client,  # type: ignore[arg-type]
        since="1h",
        environment="production",
        indexed_service_names=["orders", "payments"],
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert overlay["schema_version"] == "apm-microservice-overlay-v1"
    assert overlay["nodes"]["microservice:orders"]["match"] == "matched"
    assert overlay["nodes"]["microservice:orders"]["average_ms"] == 50.0
    assert overlay["edges"]["microservice:orders->microservice:payments"] == {
        "calls": 10,
        "failure_calls": 2,
        "error_rate": 0.2,
        "average_ms": 60.0,
        "match": "matched",
        "call_volume_level": "low",
    }


def test_build_microservice_overlay_routes_unmapped_and_ambiguous_to_unresolved() -> None:
    client = FakeOverlayClient()

    overlay = build_microservice_overlay(
        client,  # type: ignore[arg-type]
        since="1h",
        environment="production",
        indexed_service_names=["orders", "payments"],
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert "microservice:unknown-svc" not in overlay["nodes"]
    assert not any(
        key.endswith("->microservice:ghost-service") for key in overlay["edges"]
    )
    unresolved_names = {entry["observed_name"] for entry in overlay["unresolved"]}
    assert "unknown-svc" in unresolved_names
    assert "ghost-service" in unresolved_names
    ghost_entry = next(
        entry for entry in overlay["unresolved"] if entry["observed_name"] == "ghost-service"
    )
    assert ghost_entry["status"] == "unmapped"
    assert ghost_entry["role"] == "destination"


def test_build_microservice_overlay_never_persists_and_stays_bounded() -> None:
    client = FakeOverlayClient()

    overlay = build_microservice_overlay(
        client,  # type: ignore[arg-type]
        since="1h",
        environment=None,
        indexed_service_names=["orders", "payments"],
        max_relations=1,
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert overlay["coverage"]["dependencies"]["max_results"] == 1
    assert len(overlay["edges"]) <= 1


def test_export_microservices_html_apm_overlay_requires_html(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        ["export", "microservices", "--json", "--apm-overlay"],
    )

    assert result.exit_code == 2
    assert "--apm-overlay" in result.output


def test_export_microservices_html_embeds_apm_overlay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from types import SimpleNamespace

    from systemlens import cli

    graph_data = SimpleNamespace(
        services_by_name={"orders": []},
        edges=[],
        collections_by_service={},
        modules_by_service={},
        warnings=[],
        build_modules=[],
        module_dependencies=[],
        source_roots=[],
        strategy1=False,
        diagnostics=[],
        result={"note": None},
        kafka_dto_definitions=None,
        openapi_contracts=None,
    )
    monkeypatch.setattr(cli, "_load_microservice_graph", lambda *_a, **_k: graph_data)
    fake_overlay = {"schema_version": "apm-microservice-overlay-v1", "nodes": {}, "edges": {}, "unresolved": []}
    monkeypatch.setattr(
        cli, "build_microservice_overlay", lambda *args, **kwargs: fake_overlay
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        cli,
        "render_graph_html",
        lambda *args, **kwargs: captured.update({"kwargs": kwargs}) or "<html></html>",
    )
    output = tmp_path / "graph.html"

    result = CliRunner().invoke(
        app,
        [
            "export", "microservices", "--html", str(output), "--apm-overlay",
            "--endpoint", "https://elastic.example.test", "--api-key", "not-a-real-key",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["kwargs"]["apm_overlay"] is fake_overlay
