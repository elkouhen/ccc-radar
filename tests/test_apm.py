import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from systemlens.apm import ApmError, export_digest, load_settings, parse_since
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
