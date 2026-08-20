import json
from base64 import b64encode
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from systemlens.apm import (
    ApmError,
    ApmHttpError,
    ApmTimeoutError,
    ElasticApmClient,
    export_curl_command,
    export_digest,
    load_settings,
    parse_since,
)
from systemlens.apm_report import (
    _project_distributed_trace,
    _safe_operation_label,
    _safe_structured_span_label,
    _span_display_label,
    build_runtime_report,
    render_runtime_report_html,
)
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


def test_client_encodes_a_raw_elasticsearch_api_key_and_queries_both_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = []

    class Response:
        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    def fake_urlopen(request: object, **kwargs: object) -> Response:
        requests.append(request)
        return Response()

    monkeypatch.setattr("systemlens.apm.urlopen", fake_urlopen)
    client = ElasticApmClient(
        load_settings(endpoint="https://elastic.example.test", api_key="id:secret")
    )

    client.search_metrics({"size": 0})
    client.search_traces({"size": 0})

    assert requests[0].get_header("Authorization") == f"ApiKey {b64encode(b'id:secret').decode()}"  # type: ignore[union-attr]
    assert requests[0].full_url.endswith("/metrics-apm.service_destination.1m-*,metrics-apm.service_transaction.1m-*,metrics-apm.transaction.1m-*,metrics-service_destination.1m.otel-*,metrics-service_transaction.1m.otel-*,metrics-transaction.1m.otel-*/_search")  # type: ignore[union-attr]
    assert requests[1].full_url.endswith("/traces-apm*,traces-generic.otel-*/_search")  # type: ignore[union-attr]
    curl = client.last_request_curl()
    assert curl is not None
    assert 'POST "${SYSTEMLENS_ELASTICSEARCH_URL%/}/traces-apm*,traces-generic.otel-*/_search"' in curl
    assert 'Authorization: ApiKey ${SYSTEMLENS_ELASTICSEARCH_API_KEY}' in curl
    assert "id:secret" not in curl


def test_apm_report_prints_the_failed_request_curl(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def reject_report(*args: object, **kwargs: object) -> dict[str, object]:
        raise ApmHttpError(400)

    monkeypatch.setattr("systemlens.cli.build_runtime_report", reject_report)
    monkeypatch.setattr(
        "systemlens.cli.ElasticApmClient.last_request_curl",
        lambda self: 'curl -X POST "${SYSTEMLENS_ELASTICSEARCH_URL%/}/traces-apm*/_search"',
    )

    result = CliRunner().invoke(
        app,
        [
            "apm", "report", "--html", str(tmp_path / "report.html"),
            "--endpoint", "https://elastic.example.test", "--api-key", "not-a-real-key",
        ],
    )

    assert result.exit_code == 2
    assert "Elasticsearch a répondu HTTP 400." in result.output
    assert "Requête curl équivalente" in result.output
    assert "${SYSTEMLENS_ELASTICSEARCH_URL%/}" in result.output
    assert "elastic.example.test" not in result.output
    assert "not-a-real-key" not in result.output


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
    assert (
        'POST "${SYSTEMLENS_ELASTICSEARCH_URL%/}/metrics-apm.service_destination.1m-*'
        in result.output
    )
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
    assert "metrics-apm.service_destination.1m-*,metrics-apm.service_transaction.1m-*" in command
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

        def search_traces(self, body: dict[str, object]) -> dict[str, object]:
            self.queries.append(body)
            trace_aggs = body.get("aggs", {}).get("traces", {})  # type: ignore[union-attr]
            if isinstance(trace_aggs, dict) and "spans" in trace_aggs.get("aggs", {}):
                return {"aggregations": {"traces": {"buckets": [{
                    "key": "distributed-trace",
                    "spans": {
                        "hits": {
                            "total": {"value": 1, "relation": "eq"},
                            "hits": [{"fields": {
                                "@timestamp": ["2026-08-15T09:30:00.000Z"],
                                "trace.id": ["distributed-trace"],
                                "span.id": ["root"],
                                "service.name": ["orders"],
                                "processor.event": ["transaction"],
                                "transaction.name": ["POST /checkout"],
                                "transaction.type": ["request"],
                                "transaction.duration.us": [400_000],
                                "event.outcome": ["success"],
                            }}],
                        }
                    },
                }]}}}
            if "aggs" in body:
                return {
                    "aggregations": {
                        "traces": {
                            "buckets": [{
                                "key": "distributed-trace",
                                "services": {"value": 2},
                            }],
                            "sum_other_doc_count": 0,
                        }
                    }
                }
            filters = body["query"]["bool"]["filter"]  # type: ignore[index]
            if {"term": {"processor.event": "span"}} in filters:
                return {
                    "hits": {
                        "hits": [{
                            "fields": {
                                "@timestamp": ["2026-08-15T09:30:00.000Z"],
                                "trace.id": ["distributed-trace"],
                                "service.name": ["orders"],
                                "span.name": ["checkout database query"],
                                "span.type": ["db"],
                                "span.duration.us": [180_000],
                                "event.outcome": ["success"],
                            }
                        }]
                    }
                }
            return {
                "hits": {
                    "hits": [{
                        "fields": {
                            "@timestamp": ["2026-08-15T09:30:00.000Z"],
                            "trace.id": ["distributed-trace"],
                            "service.name": ["orders"],
                            "transaction.name": ["POST /checkout"],
                            "transaction.type": ["request"],
                            "transaction.duration.us": [400_000],
                            "event.outcome": ["success"],
                        }
                    }]
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
            "outcome_calls": 12,
            "error_rate": 0.166667,
            "outcome_coverage": 1.0,
            "average_ms": 50.0,
            "p95_ms": 900.0,
        }
    ]
    assert report["transactions"] == [
        {
            "service": "orders",
            "calls": 8,
            "failure_calls": 1,
            "outcome_calls": 8,
            "error_rate": 0.125,
            "outcome_coverage": 1.0,
            "average_ms": 50.0,
            "p95_ms": 750.0,
            "transaction": "HTTP transaction",
            "transaction_type": "request",
            "p95_scope": "max_operation_p95",
            "operation_count": 1,
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
    assert report["schema_version"] == "apm-runtime-report-v2"
    assert report["generated_at"] == "2026-08-15T10:00:00Z"
    assert report["timeline_spans"] == [{
        "timestamp": "2026-08-15T09:30:00.000Z",
        "service": "orders",
            "span": "Span",
            "span_type": "db",
            "origin": "Database",
            "duration_ms": 180.0,
        "outcome": "success",
        "waterfall_refs": ["waterfall-1"],
    }]
    service_query = client.queries[0]
    service_aggs = service_query["aggs"]["items"]["aggs"]  # type: ignore[index]
    assert service_aggs["calls"] == {  # type: ignore[index]
        "value_count": {"field": "transaction.duration.summary"}
    }
    assert service_aggs["duration_us"] == {  # type: ignore[index]
        "sum": {"field": "transaction.duration.summary"}
    }
    assert service_aggs["outcome_calls"] == {  # type: ignore[index]
        "value_count": {"field": "event.success_count"}
    }
    assert service_aggs["success_calls"] == {  # type: ignore[index]
        "sum": {"field": "event.success_count"}
    }
    assert service_aggs["p95"] == {  # type: ignore[index]
        "percentiles": {"field": "transaction.duration.histogram", "percents": [95]}
    }
    assert {"term": {"service.environment": "production"}} in service_query["query"]["bool"]["filter"]  # type: ignore[index]
    timeline_query = next(query for query in client.queries if "span.name" in query.get("fields", []))
    assert timeline_query["_source"] is False
    assert "trace.id" in timeline_query["fields"]
    assert {"terms": {"trace.id": ["distributed-trace"]}} in timeline_query["query"]["bool"]["filter"]  # type: ignore[index]
    assert {"term": {"processor.event": "span"}} in timeline_query["query"]["bool"]["filter"]  # type: ignore[index]
    assert timeline_query["sort"] == [{"@timestamp": {"order": "desc"}}]
    assert "span.duration.us" in timeline_query["fields"]
    assert "span.type" in timeline_query["fields"]
    trace_id_query = next(
        query for query in client.queries
        if query.get("aggs", {}).get("traces", {}).get("aggs", {}).get("latest")
    )
    assert trace_id_query["runtime_mappings"]["systemlens.trace_id"]["type"] == "keyword"  # type: ignore[index]
    assert trace_id_query["aggs"]["traces"]["terms"]["field"] == "systemlens.trace_id"  # type: ignore[index]
    assert report["coverage"]["timeline"] == {  # type: ignore[index]
        "items_exported": 1,
        "truncated": False,
        "truncation_reasons": [],
        "max_events": 500,
        "available": True,
        "unavailable_reason": None,
    }
    assert report["distributed_traces"] == [{
        "source_kind": "http_service",
        "source": "orders",
        "timestamp": "2026-08-15T09:30:00.000Z",
        "service": "orders",
        "name": "HTTP transaction",
        "duration_ms": 400.0,
        "transaction_type": "request",
        "outcome": "success",
        "route": ["orders"],
        "distributed_operations": ["orders · HTTP transaction"],
        "waterfall_ref": "waterfall-1",
        "truncated": False,
            "spans": [{
                "service": "orders", "name": "HTTP transaction", "kind": "transaction",
                "transaction_type": "request", "origin": "HTTP", "outcome": "success", "depth": 0,
            "offset_ms": 0, "duration_ms": 400.0,
        }],
    }]
    assert report["coverage"]["distributed_traces"] == {  # type: ignore[index]
        "items_exported": 1,
        "max_traces": 20,
        "max_spans_per_trace": 100,
        "truncated": False,
        "truncation_reasons": [],
        "available": True,
        "unavailable_reason": None,
    }
    waterfall_query = next(
        query
        for query in client.queries
        if "spans" in query.get("aggs", {}).get("traces", {}).get("aggs", {})  # type: ignore[union-attr]
    )
    assert waterfall_query["size"] == 0
    assert waterfall_query["aggs"]["traces"]["aggs"]["spans"]["top_hits"]["size"] == 100  # type: ignore[index]
    exported = json.dumps(report)
    assert '"trace.id"' not in exported
    assert '"span.id"' not in exported
    assert '"parent.id"' not in exported
    assert "distributed-trace" not in exported


def test_distributed_trace_uses_producer_span_as_kafka_topic_source() -> None:
    trace = _project_distributed_trace([
        {
            "id": "producer", "parent": None, "timestamp": "2026-08-15T09:30:00Z",
            "service": "api", "name": "POST /work", "kind": "transaction",
            "transaction_type": "request", "duration_ms": 300.0, "outcome": "success",
        },
        {
            "id": "publish", "parent": "producer", "timestamp": "2026-08-15T09:30:00.050Z",
            "service": "api", "name": "work.requested publish", "kind": "span",
            "transaction_type": None, "duration_ms": 10.0, "outcome": "success",
        },
        {
            "id": "consumer", "parent": "publish", "timestamp": "2026-08-15T09:30:00.080Z",
            "service": "worker", "name": "work.requested process", "kind": "transaction",
            "transaction_type": "messaging", "duration_ms": 120.0, "outcome": "success",
        },
    ])

    assert trace is not None
    assert trace["source_kind"] == "topic"
    assert trace["source"] == "Messaging topic"
    assert [span["depth"] for span in trace["spans"]] == [0, 1, 2]  # type: ignore[index]
    assert "producer" not in json.dumps(trace)
    assert "consumer" not in json.dumps(trace)


def test_span_display_labels_use_http_route_and_kafka_topic() -> None:
    http = _span_display_label({
        "span.type": ["http"],
        "http.request.method": ["post"],
        "http.route": ["/orders/{orderId}"],
    })
    kafka = _span_display_label({
        "span.type": ["messaging"],
        "messaging.destination.name": ["orders.created"],
    })

    assert http == "POST /orders/{orderId}"
    assert kafka == "orders.created"
    assert _safe_operation_label(http, "span", None) == "Span"
    assert _safe_structured_span_label(http) == http
    assert _safe_structured_span_label(kafka) == kafka


def test_runtime_report_marks_truncated_distributed_trace_spans() -> None:
    class TruncatedTraceClient:
        def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
            if "relations" in body.get("aggs", {}):
                return {"aggregations": {"relations": {"buckets": []}}}
            return {"aggregations": {"items": {"buckets": []}}}

        def search_traces(self, body: dict[str, object]) -> dict[str, object]:
            trace_aggs = body.get("aggs", {}).get("traces", {})  # type: ignore[union-attr]
            if isinstance(trace_aggs, dict) and "spans" in trace_aggs.get("aggs", {}):
                return {"aggregations": {"traces": {"buckets": [{
                    "key": "trace",
                    "spans": {"hits": {"total": {"value": 501, "relation": "eq"}, "hits": []}},
                }]}}}
            if "aggs" in body:
                return {"aggregations": {"traces": {"buckets": [{
                    "key": "trace", "services": {"value": 2},
                }], "sum_other_doc_count": 0}}}
            return {"hits": {"hits": []}}

    report = build_runtime_report(
        TruncatedTraceClient(),  # type: ignore[arg-type]
        since="1h",
        environment=None,
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    coverage = report["coverage"]["distributed_traces"]  # type: ignore[index]
    assert coverage["truncated"] is True
    assert coverage["truncation_reasons"] == ["max_spans_per_trace"]


def test_runtime_report_groups_indistinguishable_redacted_transactions() -> None:
    class GroupedTransactionsClient:
        def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
            metricset = body["query"]["bool"]["filter"][0]["term"]["metricset.name"]  # type: ignore[index]
            if metricset == "transaction":
                return {"aggregations": {"items": {"buckets": [
                    _latency_bucket(
                        {"service": "orders", "transaction": "GET /orders/1", "transaction_type": "request"},
                        4, 40_000, 12_000, 1,
                    ),
                    _latency_bucket(
                        {"service": "orders", "transaction": "POST /orders", "transaction_type": "request"},
                        6, 120_000, 48_000, 2,
                    ),
                ]}}}
            if metricset == "service_transaction":
                return {"aggregations": {"items": {"buckets": []}}}
            return {"aggregations": {"relations": {"buckets": []}}}

        def search_traces(self, body: dict[str, object]) -> dict[str, object]:
            if "aggs" in body:
                return {"aggregations": {"traces": {"buckets": [
                    {"key": "first", "services": {"value": 2}},
                    {"key": "second", "services": {"value": 2}},
                ], "sum_other_doc_count": 0}}}
            return {"hits": {"hits": [
                {"fields": {
                    "@timestamp": ["2026-08-15T09:30:00.000Z"],
                    "service.name": ["orders"],
                    "transaction.name": ["GET /orders/1"],
                    "transaction.type": ["request"],
                }},
                {"fields": {
                    "@timestamp": ["2026-08-15T09:31:00.000Z"],
                    "service.name": ["orders"],
                    "transaction.name": ["POST /orders"],
                    "transaction.type": ["request"],
                }},
            ]}}

    report = build_runtime_report(
        GroupedTransactionsClient(),  # type: ignore[arg-type]
        since="1h",
        environment=None,
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert report["transactions"] == [{
        "service": "orders",
        "transaction": "HTTP transaction",
        "transaction_type": "request",
        "calls": 10,
        "failure_calls": 3,
        "outcome_calls": 10,
        "error_rate": 0.3,
        "outcome_coverage": 1.0,
        "average_ms": 16.0,
        "p95_ms": 48.0,
        "p95_scope": "max_operation_p95",
        "operation_count": 2,
    }]


def test_runtime_report_excludes_transactions_without_distributed_trace() -> None:
    class DistributedOnlyClient:
        def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
            metricset = body["query"]["bool"]["filter"][0]["term"]["metricset.name"]  # type: ignore[index]
            if metricset == "transaction":
                return {"aggregations": {"items": {"buckets": [
                    _latency_bucket({"service": "orders", "transaction": "distributed", "transaction_type": "request"}, 2, 20_000, 15_000, 0),
                    _latency_bucket({"service": "orders", "transaction": "local", "transaction_type": "request"}, 9, 90_000, 15_000, 0),
                ]}}}
            if metricset == "service_transaction":
                return {"aggregations": {"items": {"buckets": []}}}
            return {"aggregations": {"relations": {"buckets": []}}}

        def search_traces(self, body: dict[str, object]) -> dict[str, object]:
            if "aggs" in body:
                return {"aggregations": {"traces": {"buckets": [
                    {"key": "shared", "services": {"value": 2}},
                    {"key": "local", "services": {"value": 1}},
                ], "sum_other_doc_count": 0}}}
            return {"hits": {"hits": [{"fields": {
                "@timestamp": ["2026-08-15T09:30:00.000Z"],
                "service.name": ["orders"],
                "transaction.name": ["distributed"],
                "transaction.type": ["request"],
            }}]}}

    report = build_runtime_report(
        DistributedOnlyClient(),  # type: ignore[arg-type]
        since="1h",
        environment=None,
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert [item["transaction"] for item in report["transactions"]] == ["HTTP transaction"]  # type: ignore[index]


def test_runtime_report_keeps_aggregates_when_timeline_times_out() -> None:
    class TimelineTimeoutClient:
        def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
            if "relations" in body.get("aggs", {}):
                return {"aggregations": {"relations": {"buckets": []}}}
            return {"aggregations": {"items": {"buckets": []}}}

        def search_traces(self, body: dict[str, object]) -> dict[str, object]:
            raise ApmTimeoutError()

    report = build_runtime_report(
        TimelineTimeoutClient(),  # type: ignore[arg-type]
        since="1h",
        environment=None,
        now=datetime(2026, 8, 15, 10, tzinfo=UTC),
    )

    assert report["services"] == []
    assert report["timeline_spans"] == []
    assert report["coverage"]["timeline"] == {  # type: ignore[index]
        "items_exported": 0,
        "truncated": False,
        "truncation_reasons": [],
        "max_events": 500,
        "available": False,
        "unavailable_reason": "timeout",
    }


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
        "outcome_calls": {"value": calls},
        "success_calls": {"value": calls - failures},
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
    assert "graphology/0.25.4/graphology.umd.min.js" in document
    assert "sigma.js/2.4.0/sigma.min.js" in document
    assert "Runtime service map" in document
    assert 'id="timeline-tab"' in document
    assert 'id="timeline-panel"' in document
    assert "Span execution log" in document
    assert "Directed edges are observed dependencies" in document
    assert 'id="map-mode"' in document
    assert 'id="map-service-filter"' in document
    assert 'id="map-workload-filter"' in document
    assert 'marker-end="url(#arrow)"' in document
    assert "Observed messaging target" in document
    assert "target_kind==='topic'?'send':'HTTP'" in document
    assert "messagingTargetTypes" in document
    assert "new graphology.MultiDirectedGraph()" in document
    assert "new Sigma(network" in document
    assert "transaction-to-dependency call" in document
    assert "dependency P95 is a separate future pass" in document
    assert '"error.message"' not in document
    assert "_source" not in document


def test_runtime_report_exports_only_sanitized_span_error_messages() -> None:
    report = {
        "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
        "services": [], "transactions": [], "dependencies": [], "timeline_spans": [],
        "distributed_traces": [{
            "source_kind": "http_service", "source": "orders",
            "service": "orders", "name": "POST /checkout", "transaction_type": "request",
            "timestamp": "2026-08-15T09:00:00Z", "duration_ms": 10.0, "outcome": "failure",
            "route": ["orders"], "distributed_operations": [], "truncated": False,
            "waterfall_ref": "waterfall-1",
            "spans": [{
                "service": "orders", "name": "POST /checkout", "kind": "span",
                "transaction_type": None, "outcome": "failure",
                "error": {"category": "TimeoutException: customer@example.com"},
                "depth": 0, "offset_ms": 0.0, "duration_ms": 10.0,
            }],
        }],
        "coverage": {},
    }

    document = render_runtime_report_html(report)

    assert "Dependency timed out" in document
    assert "customer@example.com" not in document
    assert "TimeoutException" not in document


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


def test_runtime_report_redacts_telemetry_controlled_operation_values() -> None:
    secret = "https://api.example.test/orders?token=secret-value"
    report = {
        "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
        "services": [],
        "transactions": [{
            "service": "orders", "transaction": secret, "transaction_type": "request",
        }],
        "timeline_events": [{
            "transaction": secret, "transaction_type": "request",
            "result": "Bearer secret-value", "messaging_target": "secret-value",
        }],
        "distributed_traces": [{
            "source_kind": "topic", "source": secret, "name": secret,
            "distributed_operations": [secret],
            "spans": [{"name": secret, "kind": "span"}],
        }],
        "dependencies": [],
        "coverage": {},
    }

    document = render_runtime_report_html(report)

    assert secret not in document
    assert "secret-value" not in document
    assert "HTTP transaction" in document
    assert "Messaging topic" in document


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
