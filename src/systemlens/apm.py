"""Read-only, bounded Elastic APM exports for external analysis tools."""

from __future__ import annotations

import base64
import json
import os
import re
import ssl
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

DEFAULT_MAX_BUCKETS = 5_000
DEFAULT_MAX_BYTES = 50_000
DEFAULT_MAX_RELATIONS = 80
_DURATION_PATTERN = re.compile(r"^(?P<amount>[1-9][0-9]*)(?P<unit>[smhd])$")
APM_METRIC_INDEX_PATTERNS = (
    "metrics-apm.service_destination.1m-*",
    "metrics-apm.service_transaction.1m-*",
    "metrics-apm.transaction.1m-*",
    "metrics-service_destination.1m.otel-*",
    "metrics-service_transaction.1m.otel-*",
    "metrics-transaction.1m.otel-*",
)
# Elastic APM stores application events in ``traces-apm*``. Elastic Agent / EDOT
# uses the OTel-native ``traces-<dataset>.otel-<namespace>`` convention; the
# dataset is configurable, so it is not necessarily ``generic``.
APM_TRACE_INDEX_PATTERNS = ("traces-apm*", "traces-*.otel-*")


class ApmError(RuntimeError):
    """A safe error returned by the read-only Elastic APM adapter."""


class ApmHttpError(ApmError):
    """A safe Elasticsearch HTTP failure retaining only its status code."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"Elasticsearch a répondu HTTP {status_code}.")


class ApmTimeoutError(ApmError):
    """A bounded Elasticsearch request exceeded its configured deadline."""

    def __init__(self) -> None:
        super().__init__("La requête Elasticsearch a dépassé son délai d'attente.")


@dataclass(frozen=True)
class ApmSettings:
    """Connection settings, with the credential source retained for doctor."""

    endpoint: str | None
    api_key: str | None
    endpoint_source: str
    api_key_source: str
    insecure_tls: bool = False

    @property
    def configured(self) -> bool:
        return self.endpoint is not None and self.api_key is not None


def load_settings(
    endpoint: str | None = None,
    api_key: str | None = None,
    *,
    insecure_tls: bool = False,
) -> ApmSettings:
    """Load explicit values first, then the documented environment variables."""
    selected_endpoint = (
        endpoint
        if endpoint is not None
        else os.environ.get("SYSTEMLENS_ELASTICSEARCH_URL")
    )
    selected_api_key = (
        api_key
        if api_key is not None
        else os.environ.get("SYSTEMLENS_ELASTICSEARCH_API_KEY")
    )
    if selected_endpoint is not None:
        selected_endpoint = _normalise_endpoint(selected_endpoint)
    if selected_api_key is not None and not selected_api_key.strip():
        raise ApmError("La clé d'API Elasticsearch ne doit pas être vide.")
    if selected_api_key is not None:
        selected_api_key = _normalise_api_key(selected_api_key)
    return ApmSettings(
        endpoint=selected_endpoint,
        api_key=selected_api_key,
        endpoint_source="flag"
        if endpoint is not None
        else ("env" if selected_endpoint else "missing"),
        api_key_source="flag"
        if api_key is not None
        else ("env" if selected_api_key else "missing"),
        insecure_tls=insecure_tls,
    )


def _normalise_endpoint(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ApmError("L'endpoint Elasticsearch doit être une URL HTTP(S) valide.") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or port is None and parsed.netloc.endswith(":")
    ):
        raise ApmError("L'endpoint Elasticsearch doit être une URL HTTP(S) absolue.")
    if parsed.username is not None or parsed.password is not None:
        raise ApmError(
            "L'endpoint Elasticsearch ne doit pas inclure d'identifiants."
        )
    if parsed.query or parsed.fragment:
        raise ApmError(
            "L'endpoint Elasticsearch ne doit pas inclure de requête ni de fragment."
        )
    return value.rstrip("/")


def _normalise_api_key(value: str) -> str:
    """Accept either an Elasticsearch ``id:secret`` key or its header value."""
    key = value.strip()
    if ":" not in key:
        return key
    return base64.b64encode(key.encode("utf-8")).decode("ascii")


class ElasticApmClient:
    """Small Elasticsearch JSON client which never performs write requests."""

    def __init__(self, settings: ApmSettings, *, timeout_seconds: float = 15.0) -> None:
        if not settings.configured:
            raise ApmError(
                "Configuration APM incomplète : définissez SYSTEMLENS_ELASTICSEARCH_URL "
                "et SYSTEMLENS_ELASTICSEARCH_API_KEY."
            )
        assert settings.endpoint is not None
        assert settings.api_key is not None
        self._endpoint = settings.endpoint
        self._api_key = settings.api_key
        self._timeout_seconds = timeout_seconds
        self._ssl_context = (
            ssl._create_unverified_context() if settings.insecure_tls else None
        )
        self._insecure_tls = settings.insecure_tls
        self._last_request: tuple[str, str, dict[str, object] | None] | None = None

    def search_metrics(self, body: dict[str, object]) -> dict[str, object]:
        return self._request_json(
            "POST", f"/{','.join(APM_METRIC_INDEX_PATTERNS)}/_search", body
        )

    def search_traces(self, body: dict[str, object]) -> dict[str, object]:
        """Read a bounded projection of recorded APM trace events."""
        return self._request_json(
            "POST", f"/{','.join(APM_TRACE_INDEX_PATTERNS)}/_search", body
        )

    def search_all_traces(self, body: dict[str, object]) -> list[dict[str, object]]:
        """Read at most the caller's bounded number of trace events by scroll."""
        # Elasticsearch rejects track_total_hits in a scroll context. ``size``
        # is a total cap supplied by the report, not an invitation to walk the
        # entire time window: a long ``--since`` range must remain bounded.
        requested_limit = body.get("size")
        max_events = requested_limit if isinstance(requested_limit, int) else 1_000
        max_events = max(1, max_events)
        request_body = {
            key: value for key, value in body.items() if key != "track_total_hits"
        }
        request_body.update({"size": min(1_000, max_events), "sort": ["_doc"]})
        response = self._request_json(
            "POST", f"/{','.join(APM_TRACE_INDEX_PATTERNS)}/_search?scroll=1m", request_body
        )
        scroll_id = response.get("_scroll_id")
        events: list[dict[str, object]] = []
        try:
            while True:
                hits = response.get("hits")
                page = hits.get("hits") if isinstance(hits, dict) else None
                if not isinstance(page, list) or not page:
                    break
                remaining = max_events - len(events)
                events.extend(hit for hit in page[:remaining] if isinstance(hit, dict))
                if len(events) >= max_events:
                    break
                if not isinstance(scroll_id, str):
                    break
                response = self._request_json(
                    "POST", "/_search/scroll", {"scroll": "1m", "scroll_id": scroll_id}
                )
                next_scroll_id = response.get("_scroll_id")
                if isinstance(next_scroll_id, str):
                    scroll_id = next_scroll_id
        finally:
            if isinstance(scroll_id, str):
                try:
                    self._request_json("DELETE", "/_search/scroll", {"scroll_id": [scroll_id]})
                except ApmError:
                    pass
        return events

    def check_metrics_access(self) -> int | None:
        """Validate index-read access without requiring Elasticsearch monitor rights."""
        response = self.search_metrics(
            {
                "size": 0,
                "track_total_hits": False,
                "query": {"term": {"metricset.name": "service_destination"}},
            }
        )
        hits = response.get("hits")
        if not isinstance(hits, dict):
            raise ApmError("Réponse Elasticsearch invalide : hits absents.")
        total = hits.get("total")
        if isinstance(total, dict) and isinstance(total.get("value"), int):
            return total["value"]
        if isinstance(total, int):
            return total
        return None

    def last_request_curl(self) -> str | None:
        """Render the latest request with shell variables instead of secrets."""
        if self._last_request is None:
            return None
        method, path, body = self._last_request
        payload = json.dumps(body, ensure_ascii=False, indent=2) if body is not None else None
        insecure_option = " --insecure \\\n" if self._insecure_tls else ""
        command = (
            "curl --silent --show-error --max-time 15 \\\n"
            f"{insecure_option}"
            f'  -X {method} "${{SYSTEMLENS_ELASTICSEARCH_URL%/}}{path}" \\\n'
            '  -H "Accept: application/json" \\\n'
            '  -H "Authorization: ApiKey ${SYSTEMLENS_ELASTICSEARCH_API_KEY}"'
        )
        if payload is None:
            return command
        return (
            f"{command} \\\n"
            '  -H "Content-Type: application/json" \\\n'
            "  --data-binary @- <<'SYSTEMLENS_APM_QUERY'\n"
            f"{payload}\n"
            "SYSTEMLENS_APM_QUERY"
        )

    def _request_json(
        self, method: str, path: str, body: dict[str, object] | None = None
    ) -> dict[str, object]:
        self._last_request = (method, path, body)
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {
            "Accept": "application/json",
            "Authorization": f"ApiKey {self._api_key}",
        }
        if payload is not None:
            headers["Content-Type"] = "application/json"
        request = Request(
            f"{self._endpoint}{path}", data=payload, headers=headers, method=method
        )
        try:
            with urlopen(
                request, timeout=self._timeout_seconds, context=self._ssl_context
            ) as response:
                decoded = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise ApmHttpError(exc.code) from exc
        except TimeoutError as exc:
            raise ApmTimeoutError() from exc
        except URLError as exc:
            if isinstance(exc.reason, TimeoutError):
                raise ApmTimeoutError() from exc
            raise ApmError("Elasticsearch est inaccessible.") from exc
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApmError(
                "La réponse Elasticsearch n'est pas un JSON exploitable."
            ) from exc
        if not isinstance(decoded, dict):
            raise ApmError("La réponse Elasticsearch doit être un objet JSON.")
        # Elasticsearch can report its own expired search deadline in a 200
        # response. A partial page must not make a report look complete.
        if decoded.get("timed_out") is True:
            raise ApmTimeoutError()
        return decoded


def doctor(settings: ApmSettings) -> dict[str, object]:
    """Report configuration and read access without exposing credentials."""
    result: dict[str, object] = {
        "endpoint": {
            "configured": settings.endpoint is not None,
            "source": settings.endpoint_source,
        },
        "api_key": {
            "configured": settings.api_key is not None,
            "source": settings.api_key_source,
        },
        "read_access": "not_checked",
    }
    if not settings.configured:
        result["status"] = "error"
        result["detail"] = (
            "Définissez SYSTEMLENS_ELASTICSEARCH_URL et "
            "SYSTEMLENS_ELASTICSEARCH_API_KEY."
        )
        return result
    document_count = ElasticApmClient(settings).check_metrics_access()
    result["status"] = "ok"
    result["read_access"] = "ok"
    result["service_destination_documents"] = document_count
    return result


def parse_since(
    value: str, *, now: datetime | None = None
) -> tuple[datetime, datetime]:
    """Convert a small, deterministic duration syntax into a UTC time window."""
    match = _DURATION_PATTERN.fullmatch(value)
    if match is None:
        raise ApmError("`--since` attend une durée positive comme 15m, 1h ou 1d.")
    amount = int(match.group("amount"))
    unit = match.group("unit")
    seconds = amount * {"s": 1, "m": 60, "h": 3_600, "d": 86_400}[unit]
    end = (now or datetime.now(UTC)).astimezone(UTC)
    return end - timedelta(seconds=seconds), end


def export_digest(
    client: ElasticApmClient,
    *,
    since: str,
    environment: str | None,
    max_relations: int = DEFAULT_MAX_RELATIONS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_buckets: int = DEFAULT_MAX_BUCKETS,
    now: datetime | None = None,
) -> dict[str, object]:
    """Export a deterministic, bounded service-destination summary for a model."""
    if max_relations < 1:
        raise ApmError("`--max-relations` doit être supérieur à zéro.")
    if max_bytes < 1_024:
        raise ApmError("`--max-bytes` doit être au moins 1024.")
    if max_buckets < 1:
        raise ApmError("`--max-buckets` doit être supérieur à zéro.")
    start, end = parse_since(since, now=now)
    buckets, query_truncated, target_field = _read_relation_buckets(
        client, start, end, environment, max_buckets
    )
    relations = _aggregate_relations(buckets)
    return _bounded_digest(
        relations,
        start=start,
        end=end,
        environment=environment,
        target_field=target_field,
        query_truncated=query_truncated,
        max_relations=max_relations,
        max_bytes=max_bytes,
    )


def export_curl_command(
    *,
    since: str,
    environment: str | None,
    max_buckets: int,
    insecure_tls: bool = False,
    now: datetime | None = None,
) -> str:
    """Render the first read request issued by ``export_digest`` as safe curl.

    Endpoint and credentials intentionally remain shell variable references:
    diagnostic output must never disclose either value, even when the export
    was configured through CLI flags rather than environment variables.
    """
    if max_buckets < 1:
        raise ApmError("`--max-buckets` doit être supérieur à zéro.")
    start, end = parse_since(since, now=now)
    query = _service_destination_query(
        start,
        end,
        environment,
        "service.target.name",
        min(1_000, max_buckets),
        None,
    )
    payload = json.dumps(query, ensure_ascii=False, indent=2)
    insecure_option = " --insecure \\\n" if insecure_tls else ""
    return (
        "curl --silent --show-error --max-time 15 \\\n"
        f"{insecure_option}"
        '  -X POST "${SYSTEMLENS_ELASTICSEARCH_URL%/}/metrics-apm.service_destination.1m-*,metrics-apm.service_transaction.1m-*,metrics-apm.transaction.1m-*,metrics-service_destination.1m.otel-*,metrics-service_transaction.1m.otel-*,metrics-transaction.1m.otel-*/_search" \\\n'
        '  -H "Accept: application/json" \\\n'
        '  -H "Content-Type: application/json" \\\n'
        '  -H "Authorization: ApiKey ${SYSTEMLENS_ELASTICSEARCH_API_KEY}" \\\n'
        "  --data-binary @- <<'SYSTEMLENS_APM_QUERY'\n"
        f"{payload}\n"
        "SYSTEMLENS_APM_QUERY"
    )


def _read_relation_buckets(
    client: ElasticApmClient,
    start: datetime,
    end: datetime,
    environment: str | None,
    max_buckets: int,
) -> tuple[list[dict[str, object]], bool, str]:
    for target_field in ("service.target.name", "span.destination.service.resource"):
        buckets, truncated = _read_buckets_for_target_field(
            client, start, end, environment, max_buckets, target_field
        )
        if buckets or truncated:
            return buckets, truncated, target_field
    return [], False, "service.target.name"


def _read_buckets_for_target_field(
    client: ElasticApmClient,
    start: datetime,
    end: datetime,
    environment: str | None,
    max_buckets: int,
    target_field: str,
) -> tuple[list[dict[str, object]], bool]:
    buckets: list[dict[str, object]] = []
    after_key: dict[str, object] | None = None
    while len(buckets) < max_buckets:
        remaining = max_buckets - len(buckets)
        response = client.search_metrics(
            _service_destination_query(
                start,
                end,
                environment,
                target_field,
                min(1_000, remaining),
                after_key,
            )
        )
        aggregations = response.get("aggregations")
        if not isinstance(aggregations, dict):
            shards = response.get("_shards")
            if isinstance(shards, dict) and shards.get("total") == 0:
                # Elasticsearch omits ``aggregations`` from a successful
                # wildcard search when no metrics-apm* index exists yet.
                return [], False
            raise ApmError("Réponse Elasticsearch invalide : agrégations absentes.")
        relation_aggregation = aggregations.get("relations")
        if not isinstance(relation_aggregation, dict):
            raise ApmError(
                "Réponse Elasticsearch invalide : agrégation relations absente."
            )
        page = relation_aggregation.get("buckets")
        if not isinstance(page, list):
            raise ApmError("Réponse Elasticsearch invalide : buckets absents.")
        page_buckets = [item for item in page if isinstance(item, dict)]
        buckets.extend(page_buckets)
        next_after = relation_aggregation.get("after_key")
        if not page_buckets or not isinstance(next_after, dict):
            return buckets, False
        after_key = next_after
    return buckets, True


def _service_destination_query(
    start: datetime,
    end: datetime,
    environment: str | None,
    target_field: str,
    size: int,
    after_key: dict[str, object] | None,
) -> dict[str, object]:
    filters: list[dict[str, object]] = [
        {"term": {"metricset.name": "service_destination"}},
        {
            "range": {
                "@timestamp": {
                    "gte": _iso8601(start),
                    "lt": _iso8601(end),
                }
            }
        },
    ]
    if environment:
        filters.append({"term": {"service.environment": environment}})
    composite: dict[str, object] = {
        "size": size,
        "sources": [
            {"source": {"terms": {"field": "service.name"}}},
            {"target": {"terms": {"field": target_field}}},
            {
                "target_type": {
                    "terms": {"field": "service.target.type", "missing_bucket": True}
                }
            },
            {"outcome": {"terms": {"field": "event.outcome", "missing_bucket": True}}},
        ],
    }
    if after_key is not None:
        composite["after"] = after_key
    return {
        "size": 0,
        "track_total_hits": False,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            "relations": {
                "composite": composite,
                "aggs": {
                    "calls": {
                        "sum": {"field": "span.destination.service.response_time.count"}
                    },
                    "duration_us": {
                        "sum": {
                            "field": "span.destination.service.response_time.sum.us"
                        }
                    },
                },
            }
        },
    }


def _aggregate_relations(buckets: list[dict[str, object]]) -> list[dict[str, object]]:
    aggregated: dict[tuple[str, str, str | None], dict[str, object]] = {}
    for bucket in buckets:
        key = bucket.get("key")
        if not isinstance(key, dict):
            continue
        source = key.get("source")
        target = key.get("target")
        if (
            not isinstance(source, str)
            or not source
            or not isinstance(target, str)
            or not target
        ):
            continue
        target_type = key.get("target_type")
        normalized_target_type = target_type if isinstance(target_type, str) else None
        calls = _metric_value(bucket, "calls")
        duration_us = _metric_value(bucket, "duration_us")
        relation = aggregated.setdefault(
            (source, target, normalized_target_type),
            {
                "source": source,
                "target": target,
                "target_type": normalized_target_type,
                "calls": 0.0,
                "failure_calls": 0.0,
                "duration_us": 0.0,
            },
        )
        relation["calls"] = _as_number(relation["calls"]) + calls
        relation["duration_us"] = _as_number(relation["duration_us"]) + duration_us
        if key.get("outcome") == "failure":
            relation["failure_calls"] = _as_number(relation["failure_calls"]) + calls
    result: list[dict[str, object]] = []
    for relation in aggregated.values():
        calls = round(_as_number(relation["calls"]))
        failures = round(_as_number(relation["failure_calls"]))
        duration_us = _as_number(relation["duration_us"])
        item: dict[str, object] = {
            "source": relation["source"],
            "target": relation["target"],
            "calls": calls,
            "failure_calls": failures,
            "error_rate": round(failures / calls, 6) if calls else None,
            "average_ms": round(duration_us / calls / 1_000, 3) if calls else None,
        }
        if relation["target_type"] is not None:
            item["target_type"] = relation["target_type"]
        result.append(item)
    return sorted(
        result,
        key=lambda item: (
            -round(_as_number(item["calls"])),
            str(item["source"]),
            str(item["target"]),
        ),
    )


def _metric_value(bucket: dict[str, object], name: str) -> float:
    metric = bucket.get(name)
    if not isinstance(metric, dict):
        return 0.0
    return _as_number(metric.get("value"))


def _as_number(value: object) -> float:
    return float(value) if isinstance(value, (int, float)) else 0.0


def _bounded_digest(
    relations: list[dict[str, object]],
    *,
    start: datetime,
    end: datetime,
    environment: str | None,
    target_field: str,
    query_truncated: bool,
    max_relations: int,
    max_bytes: int,
) -> dict[str, object]:
    selected: list[dict[str, object]] = []
    truncation_reasons = ["query_bucket_limit"] if query_truncated else []
    for relation in relations:
        if len(selected) >= max_relations:
            truncation_reasons.append("max_relations")
            break
        candidate = selected + [relation]
        digest = _digest_payload(
            candidate,
            start,
            end,
            environment,
            target_field,
            len(relations),
            truncation_reasons,
            max_relations,
            max_bytes,
        )
        if len(_compact_json(digest)) > max_bytes:
            truncation_reasons.append("max_bytes")
            break
        selected = candidate
    digest = _digest_payload(
        selected,
        start,
        end,
        environment,
        target_field,
        len(relations),
        truncation_reasons,
        max_relations,
        max_bytes,
    )
    # The truncation marker itself consumes bytes.  Keep the public contract
    # strict even at the boundary by removing lower-priority relations until
    # the final payload fits the requested budget.
    while selected and len(_compact_json(digest)) > max_bytes:
        selected.pop()
        if "max_bytes" not in truncation_reasons:
            truncation_reasons.append("max_bytes")
        digest = _digest_payload(
            selected,
            start,
            end,
            environment,
            target_field,
            len(relations),
            truncation_reasons,
            max_relations,
            max_bytes,
        )
    if len(_compact_json(digest)) > max_bytes:
        raise ApmError(
            "`--max-bytes` est trop petit pour encoder les métadonnées obligatoires du digest."
        )
    return digest


def _digest_payload(
    relations: list[dict[str, object]],
    start: datetime,
    end: datetime,
    environment: str | None,
    target_field: str,
    relations_seen: int,
    truncation_reasons: list[str],
    max_relations: int,
    max_bytes: int,
) -> dict[str, object]:
    return {
        "schema_version": "apm-digest-v1",
        "source": {
            "kind": "elastic_apm_service_destination_metrics",
            "target_field": target_field,
            "raw_spans_exported": False,
        },
        "window": {"from": _iso8601(start), "to": _iso8601(end)},
        "environment": environment,
        "relations": relations,
        "coverage": {
            "relations_seen": relations_seen,
            "relations_exported": len(relations),
            "truncated": bool(truncation_reasons),
            "truncation_reasons": truncation_reasons,
            "max_relations": max_relations,
            "max_bytes": max_bytes,
        },
    }


def compact_json(digest: dict[str, object]) -> str:
    """Serialize digest JSON predictably so the byte budget is meaningful."""
    return _compact_json(digest).decode("utf-8")


def _compact_json(value: dict[str, object]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _iso8601(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
