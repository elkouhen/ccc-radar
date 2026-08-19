import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from systemlens.apm import (
    ApmError,
    ApmHttpError,
    ElasticApmClient,
    export_curl_command,
    export_digest,
    load_settings,
    parse_since,
)
from systemlens.apm_report import build_runtime_report, render_runtime_report_html
from systemlens.cli import app


class FakeApmClient:
    def __init__(self) -> None:
        self.queries: list[dict[str, object]] = []

    def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
        self.queries.append(body)
        return {
            "aggregations": {
                "relations": {
                    "buckets": [
                        _bucket("orders", "inventory", "success", 10, 500_000),
                        _bucket("orders", "inventory", "failure", 2, 300_000),
                        _bucket("orders", "payments", "success", 5, 1_000_000),
                    ]
                }
            }
        }


def _bucket(
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


def test_export_digest_aggregates_service_destination_metrics() -> None:
    client = FakeApmClient()

    digest = export_digest(
        client,  # type: ignore[arg-type]
        since="1h",
        environment="production",
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert digest["schema_version"] == "apm-digest-v1"
    assert digest["source"] == {
        "kind": "elastic_apm_service_destination_metrics",
        "target_field": "service.target.name",
        "raw_spans_exported": False,
    }
    assert digest["relations"] == [
        {
            "source": "orders",
            "target": "inventory",
            "target_type": "http",
            "calls": 12,
            "failure_calls": 2,
            "error_rate": 0.166667,
            "average_ms": 66.667,
        },
        {
            "source": "orders",
            "target": "payments",
            "target_type": "http",
            "calls": 5,
            "failure_calls": 0,
            "error_rate": 0.0,
            "average_ms": 200.0,
        },
    ]
    query = client.queries[0]
    filters = query["query"]["bool"]["filter"]  # type: ignore[index]
    assert {"term": {"service.environment": "production"}} in filters


def test_export_digest_reports_relation_and_byte_truncation() -> None:
    client = FakeApmClient()
    digest = export_digest(  # type: ignore[arg-type]
        client, since="1h", environment=None, max_relations=1
    )

    assert len(digest["relations"]) == 1
    assert digest["coverage"]["truncated"] is True  # type: ignore[index]
    assert "max_relations" in digest["coverage"]["truncation_reasons"]  # type: ignore[index]

    long_client = FakeApmClient()
    long_client.search_metrics = lambda body: {  # type: ignore[method-assign]
        "aggregations": {
            "relations": {
                "buckets": [
                    _bucket("orders", "a" * 450, "success", 10, 100_000),
                    _bucket("orders", "b" * 450, "success", 9, 100_000),
                ]
            }
        }
    }
    byte_limited = export_digest(  # type: ignore[arg-type]
        long_client, since="1h", environment=None, max_bytes=1_024
    )
    assert (
        len(
            json.dumps(byte_limited, ensure_ascii=False, separators=(",", ":")).encode()
        )
        <= 1_024
    )
    assert "max_bytes" in byte_limited["coverage"]["truncation_reasons"]  # type: ignore[index]


def test_export_digest_falls_back_to_legacy_destination_field() -> None:
    class LegacyTargetClient:
        def __init__(self) -> None:
            self.target_fields: list[str] = []

        def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
            sources = body["aggs"]["relations"]["composite"]["sources"]  # type: ignore[index]
            target_field = sources[1]["target"]["terms"]["field"]  # type: ignore[index]
            self.target_fields.append(target_field)
            buckets = (
                []
                if target_field == "service.target.name"
                else [_bucket("orders", "legacy-payments", "success", 3, 90_000)]
            )
            return {"aggregations": {"relations": {"buckets": buckets}}}

    client = LegacyTargetClient()
    digest = export_digest(client, since="1h", environment=None)  # type: ignore[arg-type]

    assert client.target_fields == [
        "service.target.name",
        "span.destination.service.resource",
    ]
    assert digest["source"]["target_field"] == "span.destination.service.resource"  # type: ignore[index]
    assert digest["relations"][0]["target"] == "legacy-payments"  # type: ignore[index]


def test_export_digest_returns_an_empty_digest_when_no_apm_index_exists() -> None:
    class NoApmIndexClient:
        def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
            return {
                "_shards": {
                    "total": 0,
                    "successful": 0,
                    "skipped": 0,
                    "failed": 0,
                },
                "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
            }

    digest = export_digest(
        NoApmIndexClient(),  # type: ignore[arg-type]
        since="1h",
        environment=None,
        now=datetime(2026, 8, 19, 7, tzinfo=UTC),
    )

    assert digest["relations"] == []
    assert digest["coverage"]["relations_exported"] == 0  # type: ignore[index]
    assert digest["source"]["target_field"] == "service.target.name"  # type: ignore[index]


@pytest.mark.parametrize("value", ["0h", "one-hour", "1w"])
def test_parse_since_rejects_ambiguous_windows(value: str) -> None:
    with pytest.raises(ApmError):
        parse_since(value)


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "https://elastic.example.test:invalid",
        "https://elastic.example.test:",
        "https://user:password@elastic.example.test",
        "https://[not-an-ipv6-address",
    ],
)
def test_load_settings_rejects_invalid_or_credential_bearing_endpoints(
    endpoint: str,
) -> None:
    with pytest.raises(ApmError):
        load_settings(endpoint=endpoint, api_key="not-a-real-key")


def test_load_settings_keeps_an_explicit_value_ahead_of_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEMLENS_ELASTICSEARCH_URL", "https://from-env.example")
    monkeypatch.setenv("SYSTEMLENS_ELASTICSEARCH_API_KEY", "from-env")

    with pytest.raises(ApmError, match="ne doit pas être vide"):
        load_settings(endpoint="https://from-flag.example", api_key="")


def test_load_settings_can_explicitly_disable_tls_verification() -> None:
    settings = load_settings(
        endpoint="https://elastic.example.test",
        api_key="not-a-real-key",
        insecure_tls=True,
    )

    assert settings.insecure_tls is True
    assert ElasticApmClient(settings)._ssl_context is not None


def test_export_digest_rejects_a_budget_smaller_than_required_metadata() -> None:
    with pytest.raises(ApmError, match="métadonnées obligatoires"):
        export_digest(  # type: ignore[arg-type]
            FakeApmClient(),
            since="1h",
            environment="a" * 2_000,
            max_bytes=1_024,
        )


def test_apm_export_writes_machine_readable_digest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    expected = {
        "schema_version": "apm-digest-v1",
        "relations": [],
        "coverage": {"relations_exported": 0},
    }
    monkeypatch.setattr(
        "systemlens.cli.export_digest", lambda *args, **kwargs: expected
    )
    output = tmp_path / "apm-digest.json"

    result = CliRunner().invoke(
        app,
        [
            "apm",
            "export",
            "--endpoint",
            "https://elastic.example.test",
            "--api-key",
            "not-a-real-key",
            "--out",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(output.read_text(encoding="utf-8")) == expected


def test_apm_export_429_prints_safe_cluster_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_export(*args: object, **kwargs: object) -> dict[str, object]:
        raise ApmHttpError(429)

    monkeypatch.setattr("systemlens.cli.export_digest", reject_export)

    result = CliRunner().invoke(
        app,
        [
            "apm",
            "export",
            "--endpoint",
            "https://elastic.example.test",
            "--api-key",
            "not-a-real-key",
            "--export-curl",
            "--insecure",
        ],
    )

    assert result.exit_code == 2
    assert "Échec de l'export APM : Elasticsearch a répondu HTTP 429." in result.output
    assert "systemlens apm doctor" in result.output
    assert "GET _cluster/health" in result.output
    assert "GET _cat/allocation?v" in result.output
    assert "filter_path=**watermark" in result.output
    assert 'POST "${SYSTEMLENS_ELASTICSEARCH_URL%/}/metrics-apm*/_search"' in result.output
    assert 'Authorization: ApiKey ${SYSTEMLENS_ELASTICSEARCH_API_KEY}' in result.output
    assert " --insecure \\" in result.output
    assert "elastic.example.test" not in result.output
    assert "not-a-real-key" not in result.output


def test_export_curl_command_matches_the_first_export_query() -> None:
    command = export_curl_command(
        since="1h",
        environment="production",
        max_buckets=5_000,
        insecure_tls=True,
        now=datetime(2026, 8, 19, 7, tzinfo=UTC),
    )

    assert '"gte": "2026-08-19T06:00:00Z"' in command
    assert '"lt": "2026-08-19T07:00:00Z"' in command
    assert '"service.environment": "production"' in command
    assert '"size": 1000' in command
    assert '"field": "service.target.name"' in command
    assert "span.destination.service.response_time.count" in command
    assert "span.destination.service.response_time.sum.us" in command
    assert "metrics-apm*/_search" in command
    assert "SYSTEMLENS_ELASTICSEARCH_API_KEY" in command
    assert " --insecure \\" in command


def test_runtime_report_uses_histogram_p95_for_services_and_transactions() -> None:
    class RuntimeClient:
        def __init__(self) -> None:
            self.queries: list[dict[str, object]] = []

        def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
            self.queries.append(body)
            metricset = body["query"]["bool"]["filter"][0]["term"]["metricset.name"]  # type: ignore[index]
            if metricset == "service_transaction":
                buckets = [_latency_bucket({"service": "orders"}, 12, 600_000, 900_000, 2)]
                return {"aggregations": {"items": {"buckets": buckets}}}
            if metricset == "transaction":
                buckets = [
                    _latency_bucket(
                        {
                            "service": "orders",
                            "transaction": "POST /checkout",
                            "transaction_type": "request",
                        },
                        8,
                        400_000,
                        750_000,
                        1,
                    )
                ]
                return {"aggregations": {"items": {"buckets": buckets}}}
            return {
                "aggregations": {
                    "relations": {
                        "buckets": [_bucket("orders", "mongo", "success", 5, 250_000)]
                    }
                }
            }

    client = RuntimeClient()
    report = build_runtime_report(
        client,  # type: ignore[arg-type]
        since="1h",
        environment="production",
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert report["services"] == [
        {
            "service": "orders",
            "calls": 12,
            "failure_calls": 2,
            "error_rate": 0.166667,
            "average_ms": 50.0,
            "p95_ms": 900.0,
        }
    ]
    assert report["transactions"] == [
        {
            "service": "orders",
            "calls": 8,
            "failure_calls": 1,
            "error_rate": 0.125,
            "average_ms": 50.0,
            "p95_ms": 750.0,
            "transaction": "POST /checkout",
            "transaction_type": "request",
        }
    ]
    assert report["dependencies"] == [
        {
            "source": "orders",
            "target": "mongo",
            "target_type": "http",
            "calls": 5,
            "failure_calls": 0,
            "error_rate": 0.0,
            "average_ms": 50.0,
        }
    ]
    service_query = client.queries[0]
    service_aggs = service_query["aggs"]["items"]["aggs"]  # type: ignore[index]
    assert service_aggs["calls"] == {  # type: ignore[index]
        "value_count": {"field": "transaction.duration.summary"}
    }
    assert service_aggs["duration_us"] == {  # type: ignore[index]
        "sum": {"field": "transaction.duration.summary"}
    }
    assert service_aggs["failures"] == {  # type: ignore[index]
        "filter": {"term": {"event.outcome": "failure"}},
        "aggs": {
            "calls": {
                "value_count": {"field": "transaction.duration.summary"}
            }
        },
    }
    assert service_aggs["p95"] == {  # type: ignore[index]
        "percentiles": {"field": "transaction.duration.histogram", "percents": [95]}
    }
    assert {"term": {"service.environment": "production"}} in service_query["query"]["bool"]["filter"]  # type: ignore[index]


def _latency_bucket(
    key: dict[str, object],
    calls: int,
    duration_us: int,
    p95_us: int,
    failures: int,
) -> dict[str, object]:
    return {
        "key": key,
        "calls": {"value": calls},
        "duration_us": {"value": duration_us},
        "p95": {"values": {"95.0": p95_us}},
        "failures": {"calls": {"value": failures}},
    }


def test_runtime_report_html_is_self_contained_and_does_not_embed_raw_errors() -> None:
    report = {
        "schema_version": "apm-runtime-report-v1",
        "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
        "environment": None,
        "services": [],
        "transactions": [],
        "dependencies": [{"source": "orders", "target": "payments", "calls": 3, "average_ms": 40.0, "error_rate": 0.0}],
        "coverage": {
            "services": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "transactions": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "dependencies": {"items_seen": 1, "items_exported": 1, "truncated": False, "truncation_reasons": []},
        },
    }

    document = render_runtime_report_html(report)

    assert '<script id="runtime-data" type="application/json">' in document
    assert "Runtime service map" in document
    assert "Directed edges are observed dependencies" in document
    assert 'id="map-mode"' in document
    assert 'id="map-service-filter"' in document
    assert 'id="map-workload-filter"' in document
    assert 'marker-end="url(#arrow)"' in document
    assert "transaction-to-dependency call" in document
    assert "dependency P95 is a separate future pass" in document
    assert "error.message" not in document
    assert "_source" not in document


def test_runtime_report_escapes_a_telemetry_value_before_embedding_json() -> None:
    report = {
        "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
        "environment": None,
        "services": [],
        "transactions": [],
        "dependencies": [{"source": "</script><img src=x>", "target": "payments", "calls": 1, "average_ms": 1.0, "error_rate": 0.0}],
        "coverage": {
            name: {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []}
            for name in ("services", "transactions", "dependencies")
        },
    }

    document = render_runtime_report_html(report)

    assert "</script><img src=x>" not in document
    assert "<\\/script><img src=x>" in document


def test_apm_report_writes_html(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "systemlens.cli.build_runtime_report",
        lambda *args, **kwargs: {
            "services": [], "transactions": [], "dependencies": []
        },
    )
    monkeypatch.setattr(
        "systemlens.cli.render_runtime_report_html", lambda report: "<html>report</html>"
    )
    output = tmp_path / "runtime.html"

    result = CliRunner().invoke(
        app,
        [
            "apm", "report", "--html", str(output), "--endpoint",
            "https://elastic.example.test", "--api-key", "not-a-real-key",
        ],
    )

    assert result.exit_code == 0, result.output
    assert output.read_text(encoding="utf-8") == "<html>report</html>"


def test_apm_doctor_json_is_safe_when_not_configured() -> None:
    result = CliRunner().invoke(app, ["apm", "doctor", "--json"])

    assert result.exit_code == 2
    assert json.loads(result.output)["status"] == "error"
    assert "API_KEY" in result.output


def test_apm_doctor_never_prints_configured_endpoint_or_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "systemlens.cli.apm_doctor",
        lambda settings: {
            "status": "ok",
            "read_access": "ok",
            "service_destination_documents": 0,
        },
    )

    result = CliRunner().invoke(
        app,
        [
            "apm",
            "doctor",
            "--json",
            "--endpoint",
            "https://elastic.example.test/private",
            "--api-key",
            "not-a-real-key",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "elastic.example.test" not in result.output
    assert "not-a-real-key" not in result.output
