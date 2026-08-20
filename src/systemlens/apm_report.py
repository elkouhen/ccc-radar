"""Bounded aggregate Elastic APM runtime report with a Sigma.js graph view."""

from __future__ import annotations

import json
import re
from datetime import datetime
from html import escape
from typing import Callable

from systemlens.apm import (
    APM_METRIC_INDEX_PATTERNS,
    APM_TRACE_INDEX_PATTERNS,
    ApmError,
    ApmTimeoutError,
    ElasticApmClient,
    _aggregate_relations,
    _as_number,
    _iso8601,
    _metric_value,
    _read_relation_buckets,
    parse_since,
)

DEFAULT_MAX_SERVICES = 30
DEFAULT_MAX_TRANSACTIONS = 50
DEFAULT_MAX_DEPENDENCIES = 80
DEFAULT_MAX_BUCKETS_PER_VIEW = 1_000
DEFAULT_MAX_TIMELINE_EVENTS = 500
MAX_DISTRIBUTED_TRACE_CANDIDATES = 10_000
DEFAULT_MAX_DISTRIBUTED_TRACES = 20
# Les data streams APM et OTel du POC limitent ``top_hits`` à 100 résultats
# par bucket (`index.max_inner_result_window`). Cette projection reste bornée
# et ne doit pas modifier les réglages d'index de production.
MAX_SPANS_PER_DISTRIBUTED_TRACE = 100


def build_runtime_report(
    client: ElasticApmClient,
    *,
    since: str,
    environment: str | None,
    max_services: int = DEFAULT_MAX_SERVICES,
    max_transactions: int = DEFAULT_MAX_TRANSACTIONS,
    max_dependencies: int = DEFAULT_MAX_DEPENDENCIES,
    max_buckets: int = DEFAULT_MAX_BUCKETS_PER_VIEW,
    max_timeline_events: int = DEFAULT_MAX_TIMELINE_EVENTS,
    now: datetime | None = None,
) -> dict[str, object]:
    """Read three aggregate metric projections for the runtime HTML report.

    Every Elasticsearch request has ``size: 0`` and returns aggregation buckets
    only.  The returned object is intentionally transient; the CLI may embed it
    in an explicitly requested report file but it is never put in SQLite.
    """
    for option, value in (
        ("--max-services", max_services),
        ("--max-transactions", max_transactions),
        ("--max-dependencies", max_dependencies),
        ("--max-buckets", max_buckets),
        ("--max-timeline-events", max_timeline_events),
    ):
        if value < 1:
            raise ApmError(f"`{option}` doit être supérieur à zéro.")

    start, end = parse_since(since, now=now)
    service_buckets, services_query_truncated = _read_composite_buckets(
        client,
        _latency_query("service", start, end, environment),
        max_buckets,
    )
    transaction_buckets, transactions_query_truncated = _read_composite_buckets(
        client,
        _latency_query("transaction", start, end, environment),
        max_buckets,
    )
    dependency_buckets, dependencies_query_truncated, target_field = (
        _read_relation_buckets(client, start, end, environment, max_buckets)
    )
    (
        timeline_spans,
        distributed_transactions,
        timeline_truncated,
        timeline_truncation_reasons,
        timeline_unavailable_reason,
    ) = _read_timeline_spans(
        client, start, end, environment, max_timeline_events
    )
    (
        distributed_traces,
        traces_truncated,
        traces_truncation_reasons,
        traces_unavailable_reason,
    ) = _read_distributed_traces(client, start, end, environment)
    waterfall_refs_by_trace_id: dict[str, list[str]] = {}
    for index, trace in enumerate(distributed_traces, start=1):
        trace_id = trace.pop("_trace_id", None)
        waterfall_ref = f"waterfall-{index}"
        trace["waterfall_ref"] = waterfall_ref
        if isinstance(trace_id, str):
            waterfall_refs_by_trace_id.setdefault(trace_id, []).append(waterfall_ref)

    services = _rank_latency_buckets(service_buckets, kind="service")
    transactions = _rank_latency_buckets(transaction_buckets, kind="transaction")
    transactions = [
        item
        for item in transactions
        if (
            item.get("service"),
            item.get("_transaction_identity"),
            item.get("transaction_type"),
        )
        in distributed_transactions
    ]
    for item in transactions:
        item.pop("_transaction_identity", None)
    linked_timeline_spans: list[dict[str, object]] = []
    for item in timeline_spans:
        trace_id = item.pop("_trace_id", None)
        if isinstance(trace_id, str) and trace_id in waterfall_refs_by_trace_id:
            item["waterfall_refs"] = waterfall_refs_by_trace_id[trace_id]
            linked_timeline_spans.append(item)
    # Every displayed execution-log span opens one of the embedded waterfalls.
    # Retaining unrelated candidates would render non-interactive timeline rows.
    timeline_spans = linked_timeline_spans
    transactions = _group_redacted_transactions(transactions)
    dependencies = _aggregate_relations(dependency_buckets)
    dependencies.sort(
        key=lambda item: (
            -_as_number(item.get("average_ms")),
            -_as_number(item.get("calls")),
            str(item.get("source")),
            str(item.get("target")),
        )
    )
    return {
        "schema_version": "apm-runtime-report-v2",
        "source": {
            "kind": "elastic_apm_and_otel_metric_aggregates",
            "raw_event_source_exported": False,
            "recorded_transaction_projection": True,
            "transaction_view": "distributed_traces_only",
            "service_metricset": "service_transaction",
            "transaction_metricset": "transaction",
            "dependency_metricset": "service_destination",
            "dependency_target_field": target_field,
            "metric_index_patterns": list(APM_METRIC_INDEX_PATTERNS),
            "timeline_trace_index_patterns": list(APM_TRACE_INDEX_PATTERNS),
        },
        "window": {"from": _iso8601(start), "to": _iso8601(end)},
        # The end of the explicit query window is also the report snapshot
        # instant.  Keeping it in the projection makes a saved HTML report
        # auditable without adding a second clock or persisting history.
        "generated_at": _iso8601(end),
        "environment": environment,
        "services": services[:max_services],
        "transactions": transactions[:max_transactions],
        "dependencies": dependencies[:max_dependencies],
        "timeline_spans": timeline_spans,
        "distributed_traces": distributed_traces,
        "coverage": {
            "services": _view_coverage(
                len(services), max_services, max_buckets, services_query_truncated
            ),
            "transactions": _view_coverage(
                len(transactions),
                max_transactions,
                max_buckets,
                transactions_query_truncated,
            ),
            "dependencies": _view_coverage(
                len(dependencies),
                max_dependencies,
                max_buckets,
                dependencies_query_truncated,
            ),
            "timeline": {
                "items_exported": len(timeline_spans),
                "truncated": timeline_truncated,
                "truncation_reasons": timeline_truncation_reasons,
                "max_events": max_timeline_events,
                "available": timeline_unavailable_reason is None,
                "unavailable_reason": timeline_unavailable_reason,
            },
            "distributed_traces": {
                "items_exported": len(distributed_traces),
                "max_traces": DEFAULT_MAX_DISTRIBUTED_TRACES,
                "max_spans_per_trace": MAX_SPANS_PER_DISTRIBUTED_TRACE,
                "truncated": traces_truncated,
                "truncation_reasons": traces_truncation_reasons,
                "available": traces_unavailable_reason is None,
                "unavailable_reason": traces_unavailable_reason,
            },
        },
    }


def _read_distributed_traces(
    client: ElasticApmClient,
    start: datetime,
    end: datetime,
    environment: str | None,
) -> tuple[list[dict[str, object]], bool, list[str], str | None]:
    """Return recent distributed trace waterfalls without exporting identifiers.

    Trace, span and parent identifiers are required briefly to rebuild the tree,
    but are deliberately omitted from the resulting report projection.
    """
    search_traces = getattr(client, "search_traces", None)
    if not callable(search_traces):
        return [], False, [], "unsupported_client"
    filters: list[dict[str, object]] = [
        {"range": {"@timestamp": {"gte": _iso8601(start), "lt": _iso8601(end)}}},
    ]
    if environment:
        filters.append({"term": {"service.environment": environment}})
    try:
        trace_ids, candidates_truncated = _read_distributed_trace_ids(
            search_traces, filters, DEFAULT_MAX_DISTRIBUTED_TRACES
        )
        reasons = ["max_trace_candidates"] if candidates_truncated else []
        if len(trace_ids) > DEFAULT_MAX_DISTRIBUTED_TRACES:
            reasons.append("max_distributed_traces")
        trace_ids = trace_ids[:DEFAULT_MAX_DISTRIBUTED_TRACES]
        if not trace_ids:
            return [], bool(reasons), reasons, None
        response = search_traces({
            "size": 0,
            "track_total_hits": False,
            "_source": False,
            "query": {"bool": {"filter": [*filters, {"terms": {"trace.id": trace_ids}}]}},
            "aggs": {
                "traces": {
                    "terms": {"field": "trace.id", "size": len(trace_ids)},
                    "aggs": {
                        "spans": {
                            "top_hits": {
                                "size": MAX_SPANS_PER_DISTRIBUTED_TRACE,
                                "_source": False,
                                "sort": [{"@timestamp": {"order": "asc"}}],
                                "fields": [
                                    "@timestamp", "span.id", "parent.id",
                                    "span.name", "span.type", "span.subtype", "service.name", "processor.event",
                                    "transaction.name", "transaction.type",
                                    "transaction.duration.us", "span.duration.us",
                                    "event.outcome", "error.type", "http.request.method",
                                    "http.route", "url.path", "messaging.destination.name",
                                    "messaging.kafka.destination", "messaging.system",
                                ],
                            }
                        }
                    },
                }
            },
        })
    except ApmTimeoutError:
        return [], False, [], "timeout"
    aggregations = response.get("aggregations")
    traces_aggregation = aggregations.get("traces") if isinstance(aggregations, dict) else None
    trace_buckets = traces_aggregation.get("buckets") if isinstance(traces_aggregation, dict) else None
    if not isinstance(trace_buckets, list):
        return [], bool(reasons), reasons, None
    by_trace: dict[str, list[dict[str, object]]] = {}
    truncated_trace_ids: set[str] = set()
    for trace_bucket in trace_buckets:
        if not isinstance(trace_bucket, dict):
            continue
        trace_id = trace_bucket.get("key")
        spans = trace_bucket.get("spans")
        hits = spans.get("hits") if isinstance(spans, dict) else None
        raw_hits = hits.get("hits") if isinstance(hits, dict) else None
        total = hits.get("total") if isinstance(hits, dict) else None
        total_value = total.get("value") if isinstance(total, dict) else total
        if isinstance(total_value, int) and total_value > MAX_SPANS_PER_DISTRIBUTED_TRACE:
            reasons.append("max_spans_per_trace")
            if isinstance(trace_id, str):
                truncated_trace_ids.add(trace_id)
        if not isinstance(trace_id, str) or not isinstance(raw_hits, list):
            continue
        for hit in raw_hits:
            fields = hit.get("fields") if isinstance(hit, dict) else None
            if not isinstance(fields, dict):
                continue
            span_id = _timeline_field_string(fields, "span.id")
            timestamp = _timeline_field_string(fields, "@timestamp")
            service = _timeline_field_string(fields, "service.name")
            transaction_name = _timeline_field_string(fields, "transaction.name")
            span_label = _span_display_label(fields)
            name = transaction_name or span_label or _timeline_field_string(fields, "span.name")
            if not span_id or not timestamp or not service or name is None:
                continue
            duration_us = _timeline_field_number(fields, "transaction.duration.us")
            if duration_us is None:
                duration_us = _timeline_field_number(fields, "span.duration.us")
            by_trace.setdefault(trace_id, []).append({
                "id": span_id,
                "parent": _timeline_field_string(fields, "parent.id"),
                "timestamp": timestamp,
                "service": service,
                "name": name,
                "structured_span_label": span_label is not None and transaction_name is None,
                "kind": "transaction" if _timeline_field_string(fields, "processor.event") == "transaction" else "span",
                "transaction_type": _timeline_field_string(fields, "transaction.type"),
                "origin": _span_origin(fields),
                "duration_ms": round(duration_us / 1_000, 3) if duration_us is not None else None,
                "outcome": _timeline_field_string(fields, "event.outcome"),
                "error": _safe_error_details(
                    _timeline_field_string(fields, "error.type")
                ),
            })
    traces: list[dict[str, object]] = []
    for trace_id, raw_trace in by_trace.items():
        trace = _project_distributed_trace(raw_trace)
        if trace is not None:
            trace["_trace_id"] = trace_id
            trace["truncated"] = trace_id in truncated_trace_ids
            traces.append(trace)
    return sorted(
        traces,
        key=lambda item: (
            {"http_service": 0, "topic": 1, "service": 2}.get(str(item["source_kind"]), 3),
            str(item["source"]).lower(),
            str(item["timestamp"]),
        ),
    ), bool(reasons), list(dict.fromkeys(reasons)), None


def _project_distributed_trace(
    raw_spans: list[dict[str, object]],
) -> dict[str, object] | None:
    """Turn one trace's internal span graph into a safe waterfall projection."""
    if not raw_spans:
        return None
    ordered = sorted(
        raw_spans,
        key=lambda item: _iso_timestamp_milliseconds(str(item["timestamp"])),
    )
    by_id = {str(item["id"]): item for item in ordered}
    roots = [item for item in ordered if not item.get("parent") or str(item["parent"]) not in by_id]
    root = roots[0] if roots else ordered[0]
    depths: dict[str, int] = {}
    def depth(item: dict[str, object]) -> int:
        identifier = str(item["id"])
        if identifier in depths:
            return depths[identifier]
        parent = by_id.get(str(item.get("parent")))
        depths[identifier] = 0 if parent is None or parent is item else min(depth(parent) + 1, 20)
        return depths[identifier]
    source_kind = "service"
    source = str(root["service"])
    # Messaging instrumentation does not always expose destination.name.  The
    # producer span label is a conservative fallback for this POC's Kafka flow.
    for item in ordered:
        if item.get("kind") != "transaction" or item.get("transaction_type") != "messaging":
            continue
        parent = by_id.get(str(item.get("parent")))
        match = re.match(r"^.+ publish$", str(parent.get("name")) if parent else "")
        if match:
            source_kind, source = "topic", "Messaging topic"
            break
    if source_kind == "service" and str(root.get("transaction_type") or "").lower() in {"request", "http"}:
        source_kind = "http_service"
    start_ms = _iso_timestamp_milliseconds(str(root["timestamp"]))
    spans: list[dict[str, object]] = []
    for item in ordered:
        timestamp_ms = _iso_timestamp_milliseconds(str(item["timestamp"]))
        span: dict[str, object] = {
            "service": item["service"],
            "name": _safe_operation_label(
                str(item["name"]),
                str(item["kind"]),
                item["transaction_type"]
                if isinstance(item["transaction_type"], str)
                else None,
            ),
            "kind": item["kind"],
            "transaction_type": item["transaction_type"],
            "origin": _safe_span_origin(item.get("origin")),
            "outcome": item["outcome"],
            "depth": depth(item),
            "offset_ms": round(max(0, timestamp_ms - start_ms), 3),
            "duration_ms": item["duration_ms"],
        }
        if item.get("structured_span_label") is True:
            span["structured_span_label"] = True
        if item.get("error") is not None:
            span["error"] = item["error"]
        spans.append(span)
    duration = root.get("duration_ms")
    if not isinstance(duration, (int, float)):
        duration = max(
            (_as_number(span["offset_ms"]) + _as_number(span["duration_ms"])
             for span in spans),
            default=0,
        )
    services = list(dict.fromkeys(str(item["service"]) for item in ordered))
    operations = [
        f"{item['service']} · {_safe_operation_label(str(item['name']), 'transaction', item['transaction_type'] if isinstance(item['transaction_type'], str) else None)}"
        for item in ordered
        if item.get("kind") == "transaction"
    ]
    return {
        "source_kind": source_kind, "source": source, "timestamp": root["timestamp"],
        "service": root["service"],
        "name": _safe_operation_label(
            str(root["name"]),
            "transaction",
            root["transaction_type"]
            if isinstance(root["transaction_type"], str)
            else None,
        ),
        "duration_ms": duration,
        "transaction_type": root["transaction_type"],
        "outcome": root["outcome"], "route": services,
        "distributed_operations": list(dict.fromkeys(operations)), "spans": spans,
    }


def _iso_timestamp_milliseconds(value: str) -> float:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1_000
    except ValueError:
        return 0.0


def _read_timeline_spans(
    client: ElasticApmClient,
    start: datetime,
    end: datetime,
    environment: str | None,
    max_events: int,
) -> tuple[
    list[dict[str, object]],
    set[tuple[str, str | None, str | None]],
    bool,
    list[str],
    str | None,
]:
    """Read a bounded, field-projected span execution log from traces.

    Trace IDs are used only in memory to select traces spanning multiple
    services. The event query explicitly disables ``_source`` and never
    requests identifiers, request data, headers, bodies, stack traces, or
    error messages.
    """
    search_traces = getattr(client, "search_traces", None)
    if not callable(search_traces):
        return [], set(), False, [], "unsupported_client"
    filters: list[dict[str, object]] = [
        {"range": {"@timestamp": {"gte": _iso8601(start), "lt": _iso8601(end)}}},
    ]
    if environment:
        filters.append({"term": {"service.environment": environment}})
    try:
        trace_ids, candidates_truncated = _read_distributed_trace_ids(
            search_traces, filters, max_events
        )
        if not trace_ids:
            reasons = ["max_trace_candidates"] if candidates_truncated else []
            return [], set(), candidates_truncated, reasons, None
        transaction_filters = [
            {"term": {"processor.event": "transaction"}},
            *filters,
            {"terms": {"trace.id": trace_ids}},
        ]
        transaction_response = search_traces({
            "size": max_events,
            "track_total_hits": False,
            "_source": False,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "fields": [
                "@timestamp", "trace.id", "service.name", "transaction.name", "transaction.type",
                "transaction.duration.us", "event.outcome",
            ],
            "query": {"bool": {"filter": transaction_filters}},
        })
        span_filters = [
            {"term": {"processor.event": "span"}},
            *filters,
            {"terms": {"trace.id": trace_ids}},
        ]
        response = search_traces({
            "size": max_events,
            "track_total_hits": False,
            "_source": False,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "fields": [
                "@timestamp", "trace.id", "service.name", "span.name", "span.type", "span.subtype",
                "span.duration.us", "event.outcome", "http.request.method", "http.route",
                "url.path", "messaging.destination.name", "messaging.kafka.destination",
                "messaging.system",
            ],
            "query": {"bool": {"filter": span_filters}},
        })
    except ApmTimeoutError:
        return [], set(), False, [], "timeout"
    transaction_hits = transaction_response.get("hits")
    transaction_raw_hits = (
        transaction_hits.get("hits") if isinstance(transaction_hits, dict) else None
    )
    distributed_transactions: set[tuple[str, str | None, str | None]] = set()
    if isinstance(transaction_raw_hits, list):
        for hit in transaction_raw_hits:
            if not isinstance(hit, dict) or not isinstance(hit.get("fields"), dict):
                continue
            fields = hit["fields"]
            service = _timeline_field_string(fields, "service.name")
            if service:
                distributed_transactions.add((
                    service,
                    _timeline_field_string(fields, "transaction.name"),
                    _timeline_field_string(fields, "transaction.type"),
                ))
    hits = response.get("hits")
    if not isinstance(hits, dict):
        return [], distributed_transactions, False, [], None
    raw_hits = hits.get("hits")
    if not isinstance(raw_hits, list):
        return [], distributed_transactions, False, [], None
    spans: list[dict[str, object]] = []
    for hit in raw_hits:
        if not isinstance(hit, dict) or not isinstance(hit.get("fields"), dict):
            continue
        fields = hit["fields"]
        timestamp = _timeline_field_string(fields, "@timestamp")
        service = _timeline_field_string(fields, "service.name")
        span = _span_display_label(fields) or _safe_operation_label(
            _timeline_field_string(fields, "span.name"), "span", None
        )
        if not timestamp or not service or span is None:
            continue
        duration = _timeline_field_number(fields, "span.duration.us")
        span_event: dict[str, object] = {
            "timestamp": timestamp,
            "service": service,
            "span": span,
            "_trace_id": _timeline_field_string(fields, "trace.id"),
            "span_type": _safe_span_type(_timeline_field_string(fields, "span.type")),
            "origin": _span_origin(fields),
            "duration_ms": round(duration / 1_000, 3) if duration is not None else None,
            "outcome": _timeline_field_string(fields, "event.outcome"),
        }
        spans.append(span_event)
    reasons = ["max_trace_candidates"] if candidates_truncated else []
    if len(raw_hits) >= max_events:
        reasons.append("max_timeline_events")
    return spans, distributed_transactions, bool(reasons), reasons, None


def _read_distributed_trace_ids(
    search_traces: Callable[[dict[str, object]], dict[str, object]],
    filters: list[dict[str, object]],
    max_events: int,
) -> tuple[list[str], bool]:
    """Select recent trace IDs that contain spans from multiple services."""
    candidate_limit = min(MAX_DISTRIBUTED_TRACE_CANDIDATES, max_events * 10)
    response = search_traces({
        "size": 0,
        "track_total_hits": False,
        "query": {"bool": {"filter": filters}},
        "aggs": {
            "traces": {
                "terms": {
                    "field": "trace.id",
                    "size": candidate_limit,
                    "order": {"latest": "desc"},
                },
                "aggs": {
                    "latest": {"max": {"field": "@timestamp"}},
                    "services": {
                        "cardinality": {
                            "field": "service.name",
                            "precision_threshold": 100,
                        }
                    },
                },
            }
        },
    })
    aggregations = response.get("aggregations")
    traces = aggregations.get("traces") if isinstance(aggregations, dict) else None
    if not isinstance(traces, dict):
        raise ApmError("Réponse Elasticsearch invalide : agrégation traces absente.")
    buckets = traces.get("buckets")
    if not isinstance(buckets, list):
        raise ApmError("Réponse Elasticsearch invalide : buckets traces absents.")
    trace_ids: list[str] = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        trace_id = bucket.get("key")
        services = bucket.get("services")
        service_count = services.get("value") if isinstance(services, dict) else 0
        if isinstance(trace_id, str) and isinstance(service_count, int) and service_count > 1:
            trace_ids.append(trace_id)
    return trace_ids, bool(traces.get("sum_other_doc_count"))


def _timeline_field_string(fields: dict[str, object], name: str) -> str | None:
    value = fields.get(name)
    if isinstance(value, list) and value and isinstance(value[0], str):
        return value[0]
    return value if isinstance(value, str) else None


def _timeline_field_number(fields: dict[str, object], name: str) -> float | None:
    value = fields.get(name)
    if isinstance(value, list) and value:
        value = value[0]
    return float(value) if isinstance(value, (int, float)) else None


def _span_display_label(fields: dict[str, object]) -> str | None:
    """Return the two safe, structured labels exposed for span execution."""
    span_type = _timeline_field_string(fields, "span.type")
    span_subtype = _timeline_field_string(fields, "span.subtype")
    method = _timeline_field_string(fields, "http.request.method")
    resource = (
        _timeline_field_string(fields, "http.route")
        or _timeline_field_string(fields, "url.path")
    )
    if method and resource:
        normalized_method = method.upper()
        if normalized_method in {"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"}:
            normalized_resource = resource.split("?", 1)[0].split("#", 1)[0]
            if re.fullmatch(r"/[A-Za-z0-9._~!$&'()*+,;=:@%{}\-/]{1,200}", normalized_resource):
                return f"{normalized_method} {normalized_resource}"
    messaging_system = _timeline_field_string(fields, "messaging.system")
    if (
        str(messaging_system).lower() == "kafka"
        or str(span_subtype).lower() == "kafka"
        or str(span_type).lower() == "messaging"
    ):
        topic = (
            _timeline_field_string(fields, "messaging.destination.name")
            or _timeline_field_string(fields, "messaging.kafka.destination")
        )
        if topic and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,248}", topic):
            return topic
    return None


def _span_origin(fields: dict[str, object]) -> str:
    """Classify a span using stable HTTP, Kafka and span-type attributes."""
    if _timeline_field_string(fields, "http.request.method"):
        return "HTTP"
    transaction_type = str(_timeline_field_string(fields, "transaction.type") or "").lower()
    if transaction_type in {"request", "http"}:
        return "HTTP"
    if transaction_type == "messaging":
        return "Messaging"
    if (
        str(_timeline_field_string(fields, "messaging.system")).lower() == "kafka"
        or str(_timeline_field_string(fields, "span.subtype")).lower() == "kafka"
    ):
        return "Kafka"
    span_type = str(_timeline_field_string(fields, "span.type") or "").lower()
    if span_type == "messaging":
        return "Messaging"
    if span_type == "db":
        return "Database"
    if span_type == "external":
        return "External"
    return "Application"


def _safe_span_origin(value: object) -> str:
    allowed = {"HTTP", "Kafka", "Messaging", "Database", "External", "Application"}
    return value if value in allowed else "Application"


def _safe_operation_label(
    value: str | None, processor_event: str | None, transaction_type: str | None
) -> str | None:
    """Replace instrumentation-controlled operation values before export."""
    if value is None:
        return None
    if processor_event == "span":
        return "Span"
    if str(transaction_type).lower() in {"request", "http"}:
        return "HTTP transaction"
    if str(transaction_type).lower() == "messaging":
        return "Messaging transaction"
    return "Transaction"


def _safe_structured_span_label(value: str | None) -> str | None:
    """Validate a label produced from approved HTTP or Kafka fields only."""
    if value is None:
        return None
    if re.fullmatch(
        r"(?:DELETE|GET|HEAD|OPTIONS|PATCH|POST|PUT) /[A-Za-z0-9._~!$&'()*+,;=:@%{}\-/]{1,200}",
        value,
    ) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,248}", value):
        return value
    return None


def _safe_span_type(value: str | None) -> str:
    """Keep only stable, non-telemetry-controlled span-type categories."""
    normalized = str(value or "").lower()
    if normalized in {"request", "http"}:
        return "request"
    if normalized == "messaging":
        return "messaging"
    if normalized in {"db", "external"}:
        return normalized
    return "other"


def _safe_error_details(error_type: str | None) -> dict[str, str] | None:
    """Map telemetry error types to a small, non-sensitive user-facing taxonomy."""
    normalized = str(error_type or "").lower()
    if "timeout" in normalized:
        return {"category": "timeout", "message": "Dependency timed out"}
    if "connect" in normalized or "connection" in normalized:
        return {"category": "connection", "message": "Dependency connection failed"}
    if "valid" in normalized:
        return {"category": "validation", "message": "Request validation failed"}
    if "authoriz" in normalized or "forbidden" in normalized:
        return {"category": "authorization", "message": "Access was denied"}
    if "throttl" in normalized or "rate" in normalized:
        return {"category": "throttling", "message": "Dependency rate limit reached"}
    if error_type:
        return {"category": "failure", "message": "Operation failed"}
    return None


def _latency_query(
    kind: str, start: datetime, end: datetime, environment: str | None
) -> Callable[[int, dict[str, object] | None], dict[str, object]]:
    sources: list[dict[str, object]]
    if kind == "service":
        sources = [{"service": {"terms": {"field": "service.name"}}}]
        metricset = "service_transaction"
    elif kind == "transaction":
        sources = [
            {"service": {"terms": {"field": "service.name"}}},
            {"transaction": {"terms": {"field": "transaction.name"}}},
            {
                "transaction_type": {
                    "terms": {
                        "field": "transaction.type",
                        "missing_bucket": True,
                    }
                }
            },
        ]
        metricset = "transaction"
    else:  # pragma: no cover - internal caller only
        raise ValueError(f"Unknown latency report kind: {kind}")

    def build(size: int, after_key: dict[str, object] | None) -> dict[str, object]:
        composite: dict[str, object] = {"size": size, "sources": sources}
        if after_key is not None:
            composite["after"] = after_key
        filters: list[dict[str, object]] = [
            {"term": {"metricset.name": metricset}},
            {
                "range": {
                    "@timestamp": {"gte": _iso8601(start), "lt": _iso8601(end)}
                }
            },
        ]
        if environment:
            filters.append({"term": {"service.environment": environment}})
        return {
            "size": 0,
            "track_total_hits": False,
            "query": {
                "bool": {
                    "filter": filters
                }
            },
            "aggs": {
                "items": {
                    "composite": composite,
                    "aggs": {
                        "calls": {
                            "value_count": {
                                "field": "transaction.duration.summary"
                            }
                        },
                        "duration_us": {
                            "sum": {"field": "transaction.duration.summary"}
                        },
                        "p95": {
                            "percentiles": {
                                "field": "transaction.duration.histogram",
                                "percents": [95],
                            }
                        },
                        "outcome_calls": {
                            "value_count": {"field": "event.success_count"}
                        },
                        "success_calls": {
                            "sum": {"field": "event.success_count"}
                        },
                    },
                }
            },
        }

    return build


def _read_composite_buckets(
    client: ElasticApmClient,
    query_builder: Callable[[int, dict[str, object] | None], dict[str, object]],
    max_buckets: int,
) -> tuple[list[dict[str, object]], bool]:
    """Page a composite aggregate without retrieving any raw event documents."""
    buckets: list[dict[str, object]] = []
    after_key: dict[str, object] | None = None
    while len(buckets) < max_buckets:
        remaining = max_buckets - len(buckets)
        response = client.search_metrics(query_builder(min(1_000, remaining), after_key))
        aggregations = response.get("aggregations")
        if not isinstance(aggregations, dict):
            raise ApmError("Réponse Elasticsearch invalide : agrégations absentes.")
        aggregation = aggregations.get("items")
        if not isinstance(aggregation, dict):
            raise ApmError("Réponse Elasticsearch invalide : agrégation items absente.")
        page = aggregation.get("buckets")
        if not isinstance(page, list):
            raise ApmError("Réponse Elasticsearch invalide : buckets absents.")
        page_buckets = [item for item in page if isinstance(item, dict)]
        buckets.extend(page_buckets)
        next_after = aggregation.get("after_key")
        if not page_buckets or not isinstance(next_after, dict):
            return buckets, False
        after_key = next_after
    return buckets, True


def _rank_latency_buckets(
    buckets: list[dict[str, object]], *, kind: str
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for bucket in buckets:
        key = bucket.get("key")
        if not isinstance(key, dict):
            continue
        service = key.get("service")
        if not isinstance(service, str) or not service:
            continue
        calls = round(_metric_value(bucket, "calls"))
        duration_us = _metric_value(bucket, "duration_us")
        outcome_calls = round(_metric_value(bucket, "outcome_calls"))
        success_calls = round(_metric_value(bucket, "success_calls"))
        failures = max(0, outcome_calls - success_calls)
        item: dict[str, object] = {
            "service": service,
            "calls": calls,
            "failure_calls": round(failures),
            "outcome_calls": outcome_calls,
            "error_rate": round(failures / outcome_calls, 6) if outcome_calls else None,
            "outcome_coverage": round(outcome_calls / calls, 6) if calls else None,
            "average_ms": round(duration_us / calls / 1_000, 3) if calls else None,
            "p95_ms": _percentile_ms(bucket),
        }
        if kind == "transaction":
            name = key.get("transaction")
            if not isinstance(name, str) or not name:
                continue
            transaction_type = key.get("transaction_type")
            item["_transaction_identity"] = name
            item["transaction"] = _safe_operation_label(
                name, "transaction", transaction_type if isinstance(transaction_type, str) else None
            )
            if isinstance(transaction_type, str):
                item["transaction_type"] = transaction_type
        result.append(item)
    return sorted(
        result,
        key=lambda item: (
            -_as_number(item.get("p95_ms")),
            -_as_number(item.get("error_rate")),
            -_as_number(item.get("calls")),
            str(item["service"]),
            str(item.get("transaction", "")),
        ),
    )


def _group_redacted_transactions(
    transactions: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Merge raw operation buckets after their safe labels have been assigned."""
    grouped: dict[tuple[str, str, str | None], list[dict[str, object]]] = {}
    for item in transactions:
        service = item.get("service")
        transaction = item.get("transaction")
        transaction_type = item.get("transaction_type")
        if not isinstance(service, str) or not isinstance(transaction, str):
            continue
        key = (
            service,
            transaction,
            transaction_type if isinstance(transaction_type, str) else None,
        )
        grouped.setdefault(key, []).append(item)
    result: list[dict[str, object]] = []
    for (service, transaction, transaction_type), items in grouped.items():
        calls = sum(round(_as_number(item.get("calls"))) for item in items)
        failures = sum(round(_as_number(item.get("failure_calls"))) for item in items)
        outcome_calls = sum(
            round(_as_number(item.get("outcome_calls"))) for item in items
        )
        duration_us = sum(
            _as_number(item.get("average_ms")) * _as_number(item.get("calls")) * 1_000
            for item in items
        )
        p95_values = [
            value
            for candidate in items
            if isinstance((value := candidate.get("p95_ms")), (int, float))
        ]
        grouped_item: dict[str, object] = {
            "service": service,
            "transaction": transaction,
            "calls": calls,
            "failure_calls": failures,
            "outcome_calls": outcome_calls,
            "error_rate": round(failures / outcome_calls, 6)
            if outcome_calls
            else None,
            "outcome_coverage": round(outcome_calls / calls, 6) if calls else None,
            "average_ms": round(duration_us / calls / 1_000, 3) if calls else None,
            "p95_ms": max(p95_values) if p95_values else None,
            "p95_scope": "max_operation_p95",
            "operation_count": len(items),
        }
        if transaction_type is not None:
            grouped_item["transaction_type"] = transaction_type
        result.append(grouped_item)
    return sorted(
        result,
        key=lambda item: (
            -_as_number(item.get("p95_ms")),
            -_as_number(item.get("error_rate")),
            -_as_number(item.get("calls")),
            str(item["service"]),
            str(item["transaction"]),
        ),
    )


def _nested_metric_value(bucket: dict[str, object], *names: str) -> float:
    value: object = bucket
    for name in names:
        if not isinstance(value, dict):
            return 0.0
        value = value.get(name)
    if not isinstance(value, dict):
        return 0.0
    return _as_number(value.get("value"))


def _percentile_ms(bucket: dict[str, object]) -> float | None:
    p95 = bucket.get("p95")
    if not isinstance(p95, dict):
        return None
    values = p95.get("values")
    if not isinstance(values, dict):
        return None
    value = values.get("95.0")
    if not isinstance(value, (int, float)):
        return None
    return round(value / 1_000, 3)


def _view_coverage(
    seen: int, exported: int, max_buckets: int, query_truncated: bool
) -> dict[str, object]:
    reasons: list[str] = []
    if query_truncated:
        reasons.append("query_bucket_limit")
    if seen > exported:
        reasons.append("view_result_limit")
    return {
        "items_seen": seen,
        "items_exported": min(seen, exported),
        "truncated": bool(reasons),
        "truncation_reasons": reasons,
        "max_results": exported,
        "max_buckets": max_buckets,
    }


def render_runtime_report_html(report: dict[str, object]) -> str:
    """Render an aggregate-only runtime report with the shared Sigma.js view."""
    data = json.dumps(
        _redact_report_operations(report), ensure_ascii=False, separators=(",", ":")
    ).replace(
        "</", "<\\/"
    )
    title = escape("SystemLens · APM runtime overview")
    document = _RUNTIME_REPORT_HTML.replace("__TITLE__", title).replace(
        "__RUNTIME_DATA__", data
    )
    return document.replace("</body>", _RUNTIME_REPORT_ENHANCEMENTS + "</body>")


def _redact_report_operations(report: dict[str, object]) -> dict[str, object]:
    """Defend the HTML boundary from telemetry-controlled operation values."""
    projection = json.loads(json.dumps(report))
    transactions = projection.get("transactions")
    if isinstance(transactions, list):
        for item in transactions:
            if isinstance(item, dict):
                item["transaction"] = _safe_operation_label(
                    item.get("transaction") if isinstance(item.get("transaction"), str) else None,
                    "transaction",
                    item.get("transaction_type")
                    if isinstance(item.get("transaction_type"), str)
                    else None,
                )
    timeline_spans = projection.get("timeline_spans")
    if isinstance(timeline_spans, list):
        for span in timeline_spans:
            if isinstance(span, dict):
                span["span"] = _safe_operation_label(
                    span.get("span") if isinstance(span.get("span"), str) else None,
                    "span",
                    None,
                )
                span["span_type"] = _safe_span_type(
                    span.get("span_type") if isinstance(span.get("span_type"), str) else None
                )
                span["origin"] = _safe_span_origin(span.get("origin"))
                span.pop("_trace_id", None)
    legacy_timeline_events = projection.get("timeline_events")
    if isinstance(legacy_timeline_events, list):
        for event in legacy_timeline_events:
            if isinstance(event, dict):
                event["transaction"] = _safe_operation_label(
                    event.get("transaction")
                    if isinstance(event.get("transaction"), str)
                    else None,
                    "transaction",
                    event.get("transaction_type")
                    if isinstance(event.get("transaction_type"), str)
                    else None,
                )
                event.pop("result", None)
                event.pop("messaging_target", None)
                event.pop("_trace_id", None)
    traces = projection.get("distributed_traces")
    if isinstance(traces, list):
        for trace in traces:
            if not isinstance(trace, dict):
                continue
            trace.pop("_trace_id", None)
            trace["name"] = _safe_operation_label(
                trace.get("name") if isinstance(trace.get("name"), str) else None,
                "transaction",
                trace.get("transaction_type")
                if isinstance(trace.get("transaction_type"), str)
                else None,
            )
            if trace.get("source_kind") == "topic":
                trace["source"] = "Messaging topic"
            trace["distributed_operations"] = ["Distributed transaction"]
            spans = trace.get("spans")
            if isinstance(spans, list):
                for span in spans:
                    if isinstance(span, dict):
                        span["name"] = _safe_operation_label(
                            span.get("name") if isinstance(span.get("name"), str) else None,
                            span.get("kind") if isinstance(span.get("kind"), str) else None,
                            span.get("transaction_type")
                            if isinstance(span.get("transaction_type"), str)
                            else None,
                        ) if span.pop("structured_span_label", False) is not True else (
                            _safe_structured_span_label(
                                span.get("name") if isinstance(span.get("name"), str) else None
                            ) or "Span"
                        )
                        span["origin"] = _safe_span_origin(span.get("origin"))
                        error = span.get("error")
                        safe_error = _safe_error_details(
                            error.get("category") if isinstance(error, dict)
                            and isinstance(error.get("category"), str)
                            else None
                        )
                        if safe_error is None:
                            span.pop("error", None)
                        else:
                            span["error"] = safe_error
    return projection


_RUNTIME_REPORT_ENHANCEMENTS = """<style>
.distributed-trace-group { margin:0 0 18px; } .distributed-trace-group h3 { margin:0 0 8px; font-size:13px; color:#243e99; }
.distributed-trace { border:1px solid #dbe3ef; border-radius:10px; margin:8px 0; overflow:auto; } .distributed-trace header { min-width:760px; display:flex; justify-content:space-between; gap:14px; padding:10px 12px; border-bottom:1px solid #edf0f5; }
.trace-identity { display:flex; flex-wrap:wrap; gap:6px; margin-top:5px; } .trace-identity span,.trace-origin { border-radius:999px; background:#eef3ff; color:#243e99; padding:2px 7px; font-size:12px; } .trace-origin { flex:0 0 auto; font-weight:650; }
.trace-waterfall { min-width:760px; padding:8px 12px; } .trace-span { display:grid; grid-template-columns: minmax(260px,.8fr) minmax(280px,1.2fr) 80px; align-items:center; gap:10px; min-height:28px; } .trace-span.is-failure { border-left:3px solid #b42318; background:#fff6f5; } .timeline-item.is-waterfall-link { cursor:pointer; }
.trace-span-name { display:flex; align-items:center; gap:6px; padding-left:calc(var(--depth) * 14px); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; } .trace-span-toggle { flex:0 0 22px; height:22px; border:1px solid #cbd7e8; border-radius:5px; background:#fff; color:#3156d3; font:inherit; font-weight:750; cursor:pointer; } .trace-span-toggle:hover { background:#eef3ff; } .trace-span-details { overflow:hidden; border:0; padding:0; background:transparent; color:inherit; font:inherit; text-align:left; text-overflow:ellipsis; white-space:nowrap; cursor:pointer; } .trace-span-details:hover,.trace-span-details:focus-visible { color:#243e99; text-decoration:underline; outline:0; } .trace-span-details.is-selected { color:#243e99; font-weight:750; } .trace-error-badge { flex:0 0 auto; border-radius:999px; padding:1px 6px; background:#fee4e2; color:#b42318; font-size:11px; font-weight:750; } .trace-track { position:relative; height:15px; border-left:1px solid #dce3ee; background:repeating-linear-gradient(90deg,#f7f9fc 0,#f7f9fc calc(25% - 1px),#e7edf6 calc(25% - 1px),#e7edf6 25%); }
.trace-bar { position:absolute; top:2px; height:11px; min-width:2px; border-radius:3px; background:#7c92c9; } .trace-bar.is-transaction { background:#11b9b4; } .trace-bar.is-failure { background:#b42318; } .trace-span small { color:#5f6b82; font-variant-numeric:tabular-nums; text-align:right; } .trace-span-detail { margin:0 12px 12px; padding:10px; border:1px solid #dbe3ef; border-radius:8px; background:#f8faff; } .trace-span-detail dl { display:grid; grid-template-columns:max-content 1fr; gap:4px 12px; margin:0; } .trace-span-detail dt { color:#5f6b82; } .trace-span-detail dd { margin:0; } .trace-span-detail .is-failure { color:#b42318; font-weight:750; }
</style><script>
(() => {
  const data = JSON.parse(document.getElementById('runtime-data').textContent);
  const byId = id => document.getElementById(id);
  const sharedServiceFilters = [...document.querySelectorAll('.tab-service-filter')];
  const sharedService = () => byId('global-service-filter').value;
  const rootTransactionType = value => {
    const type = String(value || '').toLowerCase();
    return type === 'request' || type === 'http' ? 'request' : type === 'messaging' ? 'messaging' : 'other';
  };
  const number = value => typeof value === 'number' ? value : 0;
  const esc = value => String(value ?? '—').replace(/[&<>"']/g, character => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[character]));
  const formatMs = value => typeof value === 'number' ? `${value.toLocaleString(undefined,{maximumFractionDigits:3})} ms` : '—';
  const summary = document.createElement('div');
  summary.id = 'timeline-summary';
  summary.className = 'timeline-summary';
  byId('timeline-events').before(summary);
  function selectView(view) {
    const services = view === 'overview';
    const transactions = view === 'details';
    const dependencies = view === 'map';
    const timeline = view === 'timeline';
    const distributed = view === 'distributed';
    byId('services-panel').hidden = !services;
    document.querySelectorAll('.service-panel').forEach(panel => { panel.hidden = !services; });
    document.querySelectorAll('.transaction-panel').forEach(panel => { panel.hidden = !transactions; });
    document.querySelectorAll('.dependency-panel').forEach(panel => { panel.hidden = !dependencies; });
    document.querySelectorAll('.distributed-panel').forEach(panel => { panel.hidden = !distributed; });
    byId('timeline-panel').hidden = !timeline;
    byId('coverage').hidden = !services;
    ['overview', 'map', 'details', 'distributed', 'timeline'].forEach(name => byId(`${name}-tab`).classList.toggle('is-active', name === view));
  }
  ['overview', 'map', 'details', 'distributed', 'timeline'].forEach(name => byId(`${name}-tab`).addEventListener('click', () => selectView(name)));
  selectView('overview');
  function visibleTimelineSpans() {
    const service = byId('timeline-service-filter').value;
    const type = byId('timeline-span-type-filter').value;
    let spans = (Array.isArray(data.timeline_spans) ? data.timeline_spans : []).filter(span =>
      (!service || span.service === service) &&
      (!type || span.span_type === type)
    );
    if (byId('timeline-top-ten-error-impact-filter').checked) {
      const groups = new Map();
      spans.filter(span => span.outcome === 'failure').forEach(span => {
        const key = `${span.service}|${span.span_type}|${span.span}`;
        const group = groups.get(key) || {errors: 0, exemplar: span};
        group.errors += 1;
        if (number(span.duration_ms) > number(group.exemplar.duration_ms)) group.exemplar = span;
        groups.set(key, group);
      });
      spans = [...groups.values()].map(group => ({
        ...group.exemplar, _errorImpact: {errors: group.errors},
      })).sort((left, right) =>
        number(right._errorImpact.errors) - number(left._errorImpact.errors) ||
        number(right.duration_ms) - number(left.duration_ms)
      ).slice(0, 10).sort((left, right) => Date.parse(left.timestamp) - Date.parse(right.timestamp));
    } else if (byId('timeline-top-ten-longest-filter').checked) {
      spans = spans.sort((left, right) => number(right.duration_ms) - number(left.duration_ms)).slice(0, 10).sort((left, right) => Date.parse(left.timestamp) - Date.parse(right.timestamp));
    }
    return spans;
  }
  function renderTimelineTriage() {
    const timelineCoverage = (data.coverage && data.coverage.timeline) || {};
    if (timelineCoverage.available === false) {
      summary.innerHTML = '';
      byId('timeline-events').innerHTML = '<p class="empty">The bounded span execution log is unavailable because its query timed out. Aggregate views are still available.</p>';
      return;
    }
    const spans = visibleTimelineSpans();
    summary.innerHTML = `<div class="timeline-bucket"><b>${spans.length.toLocaleString()}</b>matching spans</div>`;
    byId('timeline-events').innerHTML = spans.length ? spans.map(span => `<article class="timeline-item ${span.outcome === 'failure' ? 'is-failure' : ''}"><time>${esc(span.timestamp)}</time><div><div class="timeline-name">${esc(span.service)} · ${esc(span.span)}</div><div class="subtle">${esc(span.origin)} · ${esc(span.span_type)}${span.outcome ? ` · ${esc(span.outcome)}` : ''}${span._errorImpact ? ` · ${span._errorImpact.errors.toLocaleString()} observed failed executions` : ''}</div></div><strong>${formatMs(span.duration_ms)}</strong></article>`).join('') : '<p class="empty">No recorded span matches this selection.</p>';
    const waterfallCount = Array.isArray(data.distributed_traces) ? data.distributed_traces.length : 0;
    byId('timeline-coverage').textContent = `Showing ${spans.length.toLocaleString()} recorded span${spans.length === 1 ? '' : 's'} from ${waterfallCount.toLocaleString()} distributed waterfall exemplar${waterfallCount === 1 ? '' : 's'}${timelineCoverage.truncated ? ' (span result limit reached)' : ''}.`;
  }
  const timelineServices = [...new Set((data.timeline_spans || []).map(span => span.service).filter(Boolean))].sort();
  byId('timeline-service-filter').innerHTML = `<option value="">All observed services</option>${timelineServices.map(service => `<option value="${esc(service)}">${esc(service)}</option>`).join('')}`;
  const timelineSpanTypes = [...new Set((data.timeline_spans || []).map(span => span.span_type).filter(Boolean))].sort();
  byId('timeline-span-type-filter').innerHTML = `<option value="">All span types</option>${timelineSpanTypes.map(type => `<option value="${esc(type)}">${esc(type)}</option>`).join('')}`;
  ['timeline-service-filter', 'timeline-span-type-filter', 'timeline-top-ten-longest-filter', 'timeline-top-ten-error-impact-filter'].forEach(id => ['input', 'change'].forEach(event => byId(id).addEventListener(event, renderTimelineTriage)));
  renderTimelineTriage();
  const sourceLabel = trace => trace.source_kind === 'topic' ? `Kafka topic · ${trace.source}` : trace.source_kind === 'http_service' ? `HTTP service · ${trace.source}` : `Source service · ${trace.source}`;
  const distributedSourceFilter = byId('distributed-trace-source-filter');
  const distributedRootTypeFilter = byId('distributed-root-type-filter');
  const rootTypeLabels = {request: 'HTTP/request', messaging: 'Messaging', other: 'Other'};
  const distributedRootTypes = [...new Set((data.distributed_traces || []).map(trace => rootTransactionType(trace.transaction_type)))].sort();
  distributedRootTypeFilter.innerHTML = `<option value="">All root types</option>${distributedRootTypes.map(type => `<option value="${esc(type)}">${esc(rootTypeLabels[type])}</option>`).join('')}`;
  let timelineWaterfallRefs = new Set();
  const clearTimelineWaterfallFocus = document.createElement('button');
  clearTimelineWaterfallFocus.id = 'clear-timeline-waterfall-focus';
  clearTimelineWaterfallFocus.type = 'button';
  clearTimelineWaterfallFocus.hidden = true;
  clearTimelineWaterfallFocus.textContent = 'Clear Timeline waterfall filter';
  distributedSourceFilter.closest('.panel-head').append(clearTimelineWaterfallFocus);
  function visibleDistributedTraces() {
    const allTraces = Array.isArray(data.distributed_traces) ? data.distributed_traces : [];
    let traces = allTraces.filter(trace =>
      (!distributedSourceFilter.value || `${trace.source_kind}:${trace.source}` === distributedSourceFilter.value) &&
      (!sharedService() || trace.service === sharedService() || (trace.route || []).includes(sharedService())) &&
      (!distributedRootTypeFilter.value || rootTransactionType(trace.transaction_type) === distributedRootTypeFilter.value) &&
      (!timelineWaterfallRefs.size || timelineWaterfallRefs.has(trace.waterfall_ref))
    );
    if (byId('distributed-top-ten-error-impact-filter').checked) {
      const groups = new Map();
      traces.filter(trace => trace.outcome === 'failure').forEach(trace => {
        const key = `${trace.service}|${trace.transaction_type}|${(trace.route || []).join('→')}`;
        const group = groups.get(key) || {errors: 0, exemplar: trace};
        group.errors += 1;
        if (number(trace.duration_ms) > number(group.exemplar.duration_ms)) group.exemplar = trace;
        groups.set(key, group);
      });
      traces = [...groups.values()].map(group => ({
        ...group.exemplar, _errorImpact: {errors: group.errors},
      })).sort((left, right) =>
        number(right._errorImpact.errors) - number(left._errorImpact.errors) ||
        number(right.duration_ms) - number(left.duration_ms)
      ).slice(0, 10);
    } else if (byId('distributed-top-ten-impact-filter').checked) {
      const groups = new Map();
      traces.forEach(trace => {
        const key = `${trace.service}|${trace.transaction_type}|${(trace.route || []).join('→')}`;
        const group = groups.get(key) || {traces: [], totalDuration: 0};
        group.traces.push(trace);
        group.totalDuration += number(trace.duration_ms);
        groups.set(key, group);
      });
      traces = traces.map(trace => {
        const key = `${trace.service}|${trace.transaction_type}|${(trace.route || []).join('→')}`;
        const group = groups.get(key);
        return {...trace, _impact: {executions: group.traces.length, totalDuration: group.totalDuration}};
      }).sort((left, right) =>
        number(right._impact.totalDuration) - number(left._impact.totalDuration) ||
        number(right.duration_ms) - number(left.duration_ms)
      ).slice(0, 10);
    }
    return traces;
  }
  function renderTraceSpanRows(trace) {
    const spans = Array.isArray(trace.spans) ? trace.spans : [];
    const total = Math.max(1, number(trace.duration_ms), ...spans.map(span => number(span.offset_ms) + number(span.duration_ms)));
    const dense = spans.length > 20;
    return spans.map((span, index) => {
      const depth = number(span.depth);
      let descendants = 0;
      for (let child = index + 1; child < spans.length && number(spans[child].depth) > depth; child += 1) descendants += 1;
      const expandable = descendants > 0;
      const collapsed = dense && expandable;
      const left = Math.min(100, 100 * number(span.offset_ms) / total);
      const width = Math.max(1, Math.min(100 - left, 100 * Math.max(number(span.duration_ms), 1) / total));
      const toggle = expandable ? `<button class="trace-span-toggle" type="button" aria-expanded="${collapsed ? 'false' : 'true'}" aria-label="${collapsed ? 'Expand' : 'Collapse'} ${descendants} subspans">${collapsed ? '+' : '−'}</button>` : '';
      const failure = span.outcome === 'failure';
      return `<div class="trace-span${collapsed ? ' is-collapsed' : ''}${failure ? ' is-failure' : ''}" data-depth="${depth}" style="--depth:${depth}"><span class="trace-span-name">${toggle}<button class="trace-span-details" type="button" data-span-index="${index}" aria-expanded="false" aria-label="Show details for ${esc(span.service)} ${esc(span.name)}">${esc(span.service)} · ${esc(span.name)}</button><span class="trace-origin">${esc(span.origin)}</span>${failure ? '<span class="trace-error-badge">Error</span>' : ''}</span><span class="trace-track"><i class="trace-bar ${span.kind === 'transaction' ? 'is-transaction' : ''}${failure ? ' is-failure' : ''}" style="left:${left}%;width:${width}%"></i></span><small>${formatMs(span.duration_ms)}</small></div>`;
    }).join('');
  }
  function updateTraceSpanVisibility(card) {
    const rows = [...card.querySelectorAll('.trace-span')];
    rows.forEach((row, index) => {
      let depth = Number(row.dataset.depth);
      let hidden = false;
      for (let ancestor = index - 1; ancestor >= 0; ancestor -= 1) {
        const ancestorDepth = Number(rows[ancestor].dataset.depth);
        if (ancestorDepth >= depth) continue;
        if (rows[ancestor].classList.contains('is-collapsed')) {
          hidden = true;
          break;
        }
        depth = ancestorDepth;
      }
      row.hidden = hidden;
    });
  }
  function renderTraceSpanDetail(span) {
    const outcome = span.outcome || 'unknown';
    const error = span.error || (outcome === 'failure' ? {category: 'failure', message: 'Operation failed'} : null);
    const failureNote = error ? `<dt>Error category</dt><dd>${esc(error.category)}</dd><dt>Error message</dt><dd class="is-failure">${esc(error.message)}</dd>` : '';
    const privacyNote = outcome === 'failure' ? '<p class="note">Error messages are sanitized categories; raw exception data is not included in this report.</p>' : '';
    return `<dl><dt>Service</dt><dd>${esc(span.service)}</dd><dt>Operation</dt><dd>${esc(span.name)}</dd><dt>Origin</dt><dd>${esc(span.origin)}</dd><dt>Kind</dt><dd>${esc(span.kind)}</dd><dt>Type</dt><dd>${esc(span.transaction_type || 'other')}</dd><dt>Outcome</dt><dd class="${outcome === 'failure' ? 'is-failure' : ''}">${esc(outcome)}</dd>${failureNote}<dt>Start offset</dt><dd>${formatMs(span.offset_ms)}</dd><dt>Duration</dt><dd>${formatMs(span.duration_ms)}</dd></dl>${privacyNote}`;
  }
  function bindTraceSpanTrees(container, traces) {
    const tracesByRef = new Map(traces.map(trace => [trace.waterfall_ref, trace]));
    container.querySelectorAll('.distributed-trace').forEach(card => {
      updateTraceSpanVisibility(card);
      const trace = tracesByRef.get(card.dataset.waterfallRef);
      const detail = card.querySelector('.trace-span-detail');
      card.querySelectorAll('.trace-span-details').forEach(button => button.addEventListener('click', () => {
        if (!trace || !detail) return;
        const index = Number(button.dataset.spanIndex);
        const span = trace.spans[index];
        if (!span) return;
        const selected = button.classList.toggle('is-selected');
        card.querySelectorAll('.trace-span-details').forEach(candidate => {
          if (candidate !== button) {
            candidate.classList.remove('is-selected');
            candidate.setAttribute('aria-expanded', 'false');
          }
        });
        button.setAttribute('aria-expanded', String(selected));
        detail.hidden = !selected;
        detail.innerHTML = selected ? renderTraceSpanDetail(span) : '';
      }));
      card.querySelectorAll('.trace-span-toggle').forEach(toggle => toggle.addEventListener('click', () => {
        const row = toggle.closest('.trace-span');
        if (!row) return;
        const collapsed = row.classList.toggle('is-collapsed');
        toggle.textContent = collapsed ? '+' : '−';
        toggle.setAttribute('aria-expanded', String(!collapsed));
        toggle.setAttribute('aria-label', `${collapsed ? 'Expand' : 'Collapse'} ${[...row.parentElement.querySelectorAll('.trace-span')].filter(candidate => Number(candidate.dataset.depth) > Number(row.dataset.depth)).length} subspans`);
        updateTraceSpanVisibility(card);
      }));
    });
  }
  function renderDistributedTraces() {
    const container = byId('distributed-traces');
    const allTraces = Array.isArray(data.distributed_traces) ? data.distributed_traces : [];
    if (!distributedSourceFilter.options.length) { const sourceOptions = [...new Map(allTraces.map(trace => [`${trace.source_kind}:${trace.source}`, sourceLabel(trace)])).entries()]; distributedSourceFilter.innerHTML = `<option value="">All origins</option>${sourceOptions.map(([value,label]) => `<option value="${esc(value)}">${esc(label)}</option>`).join('')}`; }
    const traces = visibleDistributedTraces();
    if (!traces.length) { container.innerHTML = '<p class="empty">No distributed trace was observed in this window.</p>'; return; }
    const groups = new Map();
    traces.forEach(trace => { const key = `${trace.source_kind}:${trace.source}`; if (!groups.has(key)) groups.set(key, []); groups.get(key).push(trace); });
    container.innerHTML = [...groups.entries()].map(([,items]) => `<section class="distributed-trace-group"><h3>${esc(sourceLabel(items[0]))}</h3>${items.map(trace => { const operations = (trace.distributed_operations || []).join(' → '); const route = (trace.route || []).join(' → '); const impact = trace._impact ? ` · ${trace._impact.executions.toLocaleString()} executions · ${formatMs(trace._impact.totalDuration)} cumulative duration` : trace._errorImpact ? ` · ${trace._errorImpact.errors.toLocaleString()} observed failed executions` : ''; return `<article class="distributed-trace" data-waterfall-ref="${esc(trace.waterfall_ref)}"><header><div><b>${esc(trace.service)} · ${esc(trace.name)}</b><div class="trace-identity"><span>${esc(route)}</span><span>${esc(operations)}</span></div><div class="subtle">${esc(trace.timestamp)} · ${trace.spans.length} spans${impact}</div></div><strong>${formatMs(trace.duration_ms)}</strong></header><div class="trace-waterfall">${renderTraceSpanRows(trace)}</div><section class="trace-span-detail" hidden aria-live="polite"></section></article>`; }).join('')}</section>`).join('');
    bindTraceSpanTrees(container, traces);
  }
  ['distributed-trace-source-filter', 'distributed-root-type-filter', 'distributed-top-ten-impact-filter', 'distributed-top-ten-error-impact-filter'].forEach(id => ['input', 'change'].forEach(event => byId(id).addEventListener(event, () => {
    renderDistributedTraces();
    renderTraceCoverage();
  })));
  clearTimelineWaterfallFocus.addEventListener('click', () => {
    timelineWaterfallRefs = new Set();
    clearTimelineWaterfallFocus.hidden = true;
    renderDistributedTraces();
  });
  renderDistributedTraces();
  function renderOutcomeSummary() {
    const services = Array.isArray(data.services) ? data.services : [];
    const calls = services.reduce((sum, item) => sum + number(item.calls), 0);
    const outcomes = services.reduce((sum, item) => sum + number(item.outcome_calls), 0);
    const failures = services.reduce((sum, item) => sum + number(item.failure_calls), 0);
    const failureRate = outcomes ? failures / outcomes : null;
    const outcomeCoverage = calls ? outcomes / calls : null;
    byId('cards').innerHTML = [
      ['Observed services', services.length.toLocaleString()],
      ['Observed calls', calls.toLocaleString()],
      ['Failure rate (known outcomes)', typeof failureRate === 'number' ? `${(failureRate * 100).toFixed(1)}%` : '—'],
      ['Outcome coverage', typeof outcomeCoverage === 'number' ? `${(outcomeCoverage * 100).toFixed(1)}%` : '—'],
      ['Observed dependencies', (Array.isArray(data.dependencies) ? data.dependencies.length : 0).toLocaleString()],
    ].map(([label, value]) => `<article class="card"><span class="subtle">${label}</span><b>${value}</b></article>`).join('');
  }
  function renderTraceCoverage() {
    const coverage = (data.coverage && data.coverage.distributed_traces) || {};
    const status = byId('report-status');
    if (coverage.truncated && !status.querySelector('[data-distributed-trace-coverage]')) {
      status.insertAdjacentHTML('beforeend', `<span class="status status-limited" data-distributed-trace-coverage>Limited distributed-trace coverage: ${esc((coverage.truncation_reasons || []).join(', '))}. Waterfalls may be partial.</span>`);
    }
    const coverageNote = byId('coverage');
    if (coverageNote && !coverageNote.dataset.distributedTraceCoverage) {
      coverageNote.textContent += ` · Distributed traces: ${number(coverage.items_exported)}/${number(coverage.max_traces)}${coverage.truncated ? ` (truncated: ${(coverage.truncation_reasons || []).join(', ')})` : ''}.`;
      coverageNote.dataset.distributedTraceCoverage = 'true';
    }
    const visible = visibleDistributedTraces();
    document.querySelectorAll('.distributed-trace').forEach((card, index) => {
      if (visible[index] && visible[index].truncated) {
        const detail = card.querySelector('.subtle');
        if (detail) detail.textContent += ' · Partial waterfall';
      }
    });
  }
  function labelTransactionP95() {
    document.querySelectorAll('#transactions th').forEach(header => {
      if (header.textContent === 'P95') header.textContent = 'Highest operation P95';
    });
    document.querySelectorAll('#transaction-graph .transaction-metrics').forEach(metric => {
      metric.innerHTML = metric.innerHTML.replace('P95 ', 'Highest operation P95 ');
    });
  }
  function openTimelineWaterfalls(refs) {
    timelineWaterfallRefs = new Set(refs);
    clearTimelineWaterfallFocus.textContent = `Clear exact Timeline waterfall filter (${refs.length})`;
    clearTimelineWaterfallFocus.hidden = false;
    selectView('distributed');
    renderDistributedTraces();
    renderTraceCoverage();
    byId('details-distributed-traces').scrollIntoView({block:'start'});
  }
  function decorateTimelineEvents() {
    const visibleSpans = visibleTimelineSpans();
    byId('timeline-events').querySelectorAll('.timeline-item').forEach((event, index) => {
      const refs = visibleSpans[index] && Array.isArray(visibleSpans[index].waterfall_refs) ? visibleSpans[index].waterfall_refs : [];
      if (!refs.length) return;
      event.classList.add('is-waterfall-link');
      event.tabIndex = 0;
      event.setAttribute('role', 'button');
      event.setAttribute('aria-label', `Show the ${refs.length} matching distributed waterfall${refs.length === 1 ? '' : 's'}`);
      event.dataset.waterfallRefs = refs.join(',');
    });
  }
  byId('timeline-events').addEventListener('click', event => {
    const target = event.target.closest('.timeline-item');
    if (target && target.dataset.waterfallRefs) openTimelineWaterfalls(target.dataset.waterfallRefs.split(','));
  });
  byId('timeline-events').addEventListener('keydown', event => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    const target = event.target.closest('.timeline-item');
    if (!target || !target.dataset.waterfallRefs) return;
    event.preventDefault();
    openTimelineWaterfalls(target.dataset.waterfallRefs.split(','));
  });
  ['timeline-service-filter', 'timeline-span-type-filter', 'timeline-top-ten-longest-filter', 'timeline-top-ten-error-impact-filter'].forEach(id => ['input', 'change'].forEach(event => byId(id).addEventListener(event, decorateTimelineEvents)));
  decorateTimelineEvents();
  renderOutcomeSummary();
  renderTraceCoverage();
  labelTransactionP95();
  ['transaction-service-filter', 'transaction-kind-filter'].forEach(id => byId(id).addEventListener('change', labelTransactionP95));
  sharedServiceFilters.forEach(filter => {
    filter.innerHTML = byId('global-service-filter').innerHTML;
    filter.value = sharedService();
    if (filter === byId('global-service-filter')) return;
    filter.addEventListener('change', () => {
      byId('global-service-filter').value = filter.value;
      byId('global-service-filter').dispatchEvent(new Event('change'));
    });
  });
  byId('global-service-filter').addEventListener('change', () => {
    sharedServiceFilters.forEach(filter => { filter.value = sharedService(); });
    renderDistributedTraces();
    renderTraceCoverage();
  });
})();
</script>"""


_RUNTIME_REPORT_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title><script src="https://cdnjs.cloudflare.com/ajax/libs/graphology/0.25.4/graphology.umd.min.js"></script><script src="https://cdnjs.cloudflare.com/ajax/libs/sigma.js/2.4.0/sigma.min.js"></script><style>
:root { color-scheme: light; --ink:#182033; --muted:#5f6b82; --line:#dde3ee; --panel:#fff; --canvas:#f5f7fb; --blue:#3156d3; --amber:#a65100; --red:#b42318; --green:#087443; }
* { box-sizing:border-box; } [hidden] { display:none !important; } body { margin:0; background:var(--canvas); color:var(--ink); font:14px/1.45 Inter,ui-sans-serif,system-ui,sans-serif; }
main { max-width:1440px; margin:auto; padding:24px; } h1,h2 { margin:0; } h1 { font-size:25px; } h2 { font-size:16px; } .subtle { color:var(--muted); margin:4px 0 0; } .context { display:flex; flex-wrap:wrap; gap:8px; margin:18px 0; } .pill { background:#eaf0ff; color:#243e99; border-radius:999px; padding:5px 9px; font-size:12px; } .cards { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-bottom:12px; } .card,.panel { background:var(--panel); border:1px solid var(--line); border-radius:12px; box-shadow:0 1px 2px #1820330b; } .card { padding:14px; } .card b { display:block; font-size:24px; margin-top:4px; } .panel { padding:16px; margin-top:12px; overflow:auto; } .panel-head { display:flex; justify-content:space-between; align-items:baseline; gap:12px; margin-bottom:10px; } select { border:1px solid var(--line); border-radius:7px; background:#fff; padding:6px; max-width:260px; } table { width:100%; border-collapse:collapse; white-space:nowrap; } th { color:var(--muted); font-weight:600; text-align:left; font-size:12px; } td,th { padding:9px 8px; border-bottom:1px solid #edf0f5; } tr:last-child td { border:0; } .metric { font-variant-numeric:tabular-nums; text-align:right; } .danger { color:var(--red); font-weight:650; } .warm { color:var(--amber); font-weight:650; } .flow { display:grid; gap:7px; min-width:660px; } .flow-row { display:grid; grid-template-columns:1fr 26px 1fr 150px; align-items:center; gap:8px; } .node { overflow:hidden; text-overflow:ellipsis; padding:7px 9px; border-radius:7px; background:#f1f4fa; } .arrow { color:var(--blue); font-size:18px; text-align:center; } .edge { height:8px; min-width:14px; border-radius:99px; background:var(--blue); opacity:.25; } .edge-wrap { display:flex; align-items:center; gap:7px; } .transaction-graph { display:grid; gap:14px; min-width:660px; } .transaction-service { display:grid; grid-template-columns:minmax(150px,.3fr) 30px 1fr; align-items:stretch; gap:8px; } .transaction-service-node { display:flex; align-items:center; justify-content:center; padding:12px; border:1px solid #cbd5e1; border-radius:10px; background:#eef3ff; color:#243e99; font-weight:750; overflow-wrap:anywhere; } .transaction-service-edge { display:flex; align-items:center; justify-content:center; color:var(--blue); font-size:22px; } .transaction-nodes { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:8px; } .transaction-node { min-height:86px; padding:10px; border:1px solid #dbe3ef; border-left:5px solid #5b74db; border-radius:9px; background:#f8faff; } .transaction-node.is-warm { border-left-color:#d48a22; background:#fffaf1; } .transaction-node.is-hot { border-left-color:#c13b31; background:#fff6f5; } .transaction-name { overflow-wrap:anywhere; font-weight:750; } .transaction-metrics { margin-top:7px; color:var(--muted); font-size:12px; } .note { color:var(--muted); font-size:12px; margin:12px 0 0; } .empty { color:var(--muted); padding:8px 0; } @media (max-width:800px) { main { padding:14px; } .cards { grid-template-columns:repeat(2,minmax(0,1fr)); } .panel { padding:12px; } } @media (max-width:460px) { .cards { grid-template-columns:1fr; } }
.transaction-kinds { display:grid; gap:10px; } .transaction-kind { border:1px solid #e2e8f0; border-radius:10px; padding:9px; background:#fbfcff; } .transaction-kind h3 { margin:0 0 8px; color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; } .transaction-kind.kind-http { border-color:#bfdbfe; background:#f6faff; } .transaction-kind.kind-messaging { border-color:#bbf7d0; background:#f6fff9; } .kind-pill { display:inline-block; margin-left:6px; padding:1px 5px; border-radius:99px; background:#eaf0ff; color:#243e99; font-size:11px; font-weight:650; }
.service-map { min-height:410px; border:1px solid #e4eaf3; border-radius:11px; background:radial-gradient(circle at 50% 45%,#fff 0,#f6f8fc 70%); overflow:auto; } .service-map svg { display:block; min-width:700px; width:100%; height:410px; } .service-map .map-edge { stroke:#7890bd; fill:none; cursor:pointer; } .service-map .map-edge.is-risk { stroke:var(--red); } .service-map .map-edge-label { fill:var(--muted); font-size:11px; pointer-events:none; } .service-map .map-node { cursor:pointer; } .service-map .map-node circle { fill:#eef3ff; stroke:#7890bd; stroke-width:2; } .service-map .map-node.is-selected circle { fill:#3156d3; stroke:#1d3b9e; stroke-width:4; } .service-map .map-node.is-risk circle { stroke:#c13b31; } .service-map .map-node text { fill:#172554; font-size:12px; font-weight:700; pointer-events:none; } .service-map .map-node.is-selected text { fill:#fff; } .service-map-details { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:12px; } .service-map-details h3 { margin:0 0 7px; font-size:14px; } .service-map-details h4 { color:var(--muted); font-size:12px; margin:12px 0 6px; text-transform:uppercase; } .detail-card { border:1px solid #e4eaf3; border-radius:9px; padding:11px; background:#fff; } .detail-list { display:grid; gap:6px; } .detail-item { display:flex; justify-content:space-between; gap:10px; padding:6px 0; border-bottom:1px solid #eef2f7; } .detail-item:last-child { border-bottom:0; } @media (max-width:800px) { .service-map-details { grid-template-columns:1fr; } }
.service-map .map-topic polygon { fill:#ecfdf3; stroke:#15945c; stroke-width:2; } .service-map .map-topic.is-selected polygon { fill:#087443; stroke:#065f46; stroke-width:4; } .service-map .map-topic text { fill:#065f46; } .service-map .map-topic.is-selected text { fill:#fff; } .map-legend { display:flex; gap:12px; margin:0 0 8px; color:var(--muted); font-size:12px; } .map-legend span::before { content:''; display:inline-block; width:10px; height:10px; margin-right:5px; vertical-align:-1px; background:#eef3ff; border:1px solid #7890bd; border-radius:50%; } .map-legend .legend-topic::before { background:#ecfdf3; border-color:#15945c; border-radius:1px; transform:rotate(45deg); }
.runtime-tabs { display:flex; gap:6px; margin:0 0 12px; } .runtime-tab { border:1px solid #cbd5e1; border-radius:7px; padding:7px 11px; color:#334155; background:#fff; font:inherit; font-weight:700; cursor:pointer; } .runtime-tab.is-active { color:#fff; border-color:#3156d3; background:#3156d3; } .timeline { display:grid; gap:8px; } .timeline-item { display:grid; grid-template-columns:160px 1fr auto; gap:10px; align-items:center; padding:9px; border-left:4px solid #3156d3; border-radius:7px; background:#f8faff; } .timeline-item.is-failure { border-left-color:#b42318; background:#fff6f5; } .timeline-item .timeline-name { overflow-wrap:anywhere; font-weight:700; } @media (max-width:800px) { .timeline-item { grid-template-columns:1fr; gap:3px; } }
.report-status { display:flex; flex-wrap:wrap; gap:8px; margin:0 0 14px; } .status { border-radius:7px; padding:6px 9px; font-size:12px; font-weight:700; } .status-ok { background:#eaf8f1; color:#087443; } .status-limited { background:#fff4e5; color:#a65100; } .triage-items { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; } .triage-item { border-left:4px solid var(--blue); border-radius:8px; background:#f8faff; padding:10px; } .triage-item.is-danger { border-left-color:var(--red); background:#fff6f5; } .triage-item b,.triage-item span { display:block; } .triage-item span { color:var(--muted); font-size:12px; } .filter-bar { display:grid; grid-template-columns:repeat(auto-fit,minmax(155px,1fr)); align-items:end; gap:10px 12px; margin:12px 0; padding:12px; border-color:#d8e2f2; background:linear-gradient(135deg,#f8faff,#fff); } .filter-bar label { display:grid; gap:4px; color:#475569; font-size:11px; font-weight:750; letter-spacing:.02em; } .filter-bar select,.filter-bar input:not([type=checkbox]) { width:100%; min-width:0; max-width:none; min-height:34px; border-color:#cbd7e8; background:#fff; box-shadow:inset 0 1px 1px #18203308; color:var(--ink); font:inherit; font-size:13px; } .filter-bar label:has(input[type=checkbox]) { display:flex; min-height:34px; align-items:center; gap:8px; padding:0 10px; border:1px solid #cbd7e8; border-radius:7px; background:#fff; color:#334155; font-size:12px; letter-spacing:0; cursor:pointer; } .filter-bar input[type=checkbox] { width:15px; height:15px; margin:0; accent-color:var(--blue); } .filter-bar button { min-height:34px; border:1px solid #cbd7e8; border-radius:7px; background:#fff; color:#334155; padding:6px 10px; font:inherit; cursor:pointer; } .filter-bar select:focus,.filter-bar input:focus,.filter-bar button:focus { outline:3px solid #bfdbfe; outline-offset:1px; } .timeline-summary { display:grid; grid-template-columns:repeat(auto-fit,minmax(110px,1fr)); gap:7px; margin:0 0 10px; } .timeline-bucket { border-radius:7px; background:#f1f4fa; padding:7px; color:var(--muted); font-size:12px; } .timeline-bucket b { color:var(--ink); display:block; font-size:15px; } @media (max-width:800px) { .triage-items { grid-template-columns:1fr; } .filter-bar { grid-template-columns:1fr 1fr; } } @media (max-width:460px) { .filter-bar { grid-template-columns:1fr; } }
#overview-layout { display:grid; gap:12px; } .overview-sidebar { display:grid; gap:12px; } .overview-sidebar .panel { margin-top:0; } .map-panel { display:grid; grid-template-columns:minmax(0,1fr) minmax(250px,.42fr); gap:12px; align-items:start; overflow:visible; } .map-panel .panel-head { grid-column:1/-1; } .map-panel .service-map { grid-column:1; } .map-panel .service-map-details { grid-column:2; grid-row:2; grid-template-columns:1fr; margin-top:0; max-height:410px; overflow:auto; } @media (max-width:800px) { .map-panel { grid-template-columns:1fr; } .map-panel .service-map,.map-panel .service-map-details { grid-column:1; grid-row:auto; } .map-panel .service-map-details { max-height:none; } }
#map-panel[hidden] { display:grid !important; position:absolute; left:-100000px; width:calc(100vw - 48px); visibility:hidden; }
</style></head><body><main>
<header><h1>APM runtime overview</h1><p class="subtle">Bounded aggregates plus a minimal recorded-transaction projection. No event source, IDs, headers, bodies, traces, or raw error messages are included.</p><div id="context" class="context"></div><div id="report-status" class="report-status"></div></header>
<nav class="runtime-tabs" aria-label="Runtime report views"><button id="overview-tab" class="runtime-tab is-active" type="button">Services</button><button id="details-tab" class="runtime-tab" type="button">Transaction workloads</button><button id="timeline-tab" class="runtime-tab" type="button">Span execution log</button><button id="distributed-tab" class="runtime-tab" type="button">Distributed traces</button><button id="map-tab" class="runtime-tab" type="button">Dependencies</button></nav>
<section id="services-panel"><section id="cards" class="cards" aria-label="Runtime summary"></section><div id="overview-layout"><aside class="overview-sidebar">
<section id="triage" class="panel triage"><div class="panel-head"><div><h2>Investigation priorities</h2><p class="subtle">Impact combines observed volume, error rate, and tail latency; it is a triage aid, not an SLO verdict.</p></div></div><div id="triage-items" class="triage-items"></div></section>
<section class="panel filter-bar"><label>Observed service <select id="global-service-filter" class="tab-service-filter" aria-label="Filter Services by observed service"></select></label><select id="global-workload-filter" hidden></select><input id="failures-only-filter" type="checkbox" hidden><button id="clear-filters" type="button" hidden>Clear filters</button></section>
</aside></div></section><section id="map-panel" class="panel map-panel dependency-panel" hidden><div class="panel-head"><div><h2>Runtime service map</h2><p class="subtle">Directed edges are observed dependencies. Select a service to inspect aggregate workload details; this does not assert a transaction-to-dependency call.</p></div><div><label>View <select id="map-mode" aria-label="Filter service map"></select></label> <label>Service <select id="map-service-filter" aria-label="Focus service map"></select></label> <label>Workload <select id="map-workload-filter" aria-label="Filter workload details"></select></label></div></div><div id="service-map" class="service-map"></div><div id="service-map-details" class="service-map-details"></div></section>
<section id="details-hotspots" class="panel service-panel" hidden><div class="panel-head"><h2>Service hotspots</h2><span class="subtle">Ranked by P95, then error rate and volume</span></div><div id="services"></div></section>
<section id="timeline-panel" class="panel" hidden><div class="filter-bar"><label>Observed service <select id="timeline-observed-service-filter" class="tab-service-filter" aria-label="Filter Timeline by observed service"></select></label></div><div class="panel-head"><div><h2>Span execution log</h2><p class="subtle">Bounded cross-service spans. Filters apply only to the spans embedded in this report.</p></div></div><div class="filter-bar"><label>Service <select id="timeline-service-filter" aria-label="Filter span log by service"></select></label><label>Type <select id="timeline-span-type-filter" aria-label="Filter span log by type"></select></label><label><input id="timeline-top-ten-longest-filter" type="checkbox"> 10 longest spans</label><label><input id="timeline-top-ten-error-impact-filter" type="checkbox"> 10 spans with most errors</label></div><div id="timeline-events" class="timeline"></div><p id="timeline-coverage" class="note"></p></section>
<section class="panel filter-bar transaction-panel detail-panel" hidden><label>Observed service <select id="transactions-observed-service-filter" class="tab-service-filter" aria-label="Filter Transactions by observed service"></select></label></section>
<section id="details-slow-transactions" class="panel service-panel" hidden><div class="panel-head"><h2>Slow transactions</h2><span class="subtle">P95 is an approximate percentile from APM histogram metrics</span></div><div id="transactions"></div></section>
<section id="details-transactions" class="panel transaction-panel detail-panel" hidden><div class="panel-head"><div><h2>Transaction graph</h2><p class="subtle">Transactions remain owned by their service; no transaction-to-dependency call is asserted.</p></div><div><label>Service <select id="transaction-service-filter" aria-label="Filter transaction graph by service"></select></label> <label>Type <select id="transaction-kind-filter" aria-label="Filter transaction graph by type"></select></label></div></div><div id="transaction-graph" class="transaction-graph"></div></section>
<section class="panel filter-bar distributed-panel" hidden><label>Observed service <select id="distributed-observed-service-filter" class="tab-service-filter" aria-label="Filter Distributed transactions by observed service"></select></label><label>Root type <select id="distributed-root-type-filter" aria-label="Filter distributed transactions by root span type"></select></label><label><input id="distributed-top-ten-impact-filter" type="checkbox"> 10 highest-impact transactions</label><label><input id="distributed-top-ten-error-impact-filter" type="checkbox"> 10 transactions with most errors</label></section>
<section id="details-distributed-traces" class="panel distributed-panel" hidden><div class="panel-head"><div><h2>Distributed transactions</h2><p class="subtle">Recent multi-service traces, grouped by HTTP source service or inferred Kafka source topic. Identifiers are not included in this export.</p></div><label>Origin <select id="distributed-trace-source-filter" aria-label="Filter distributed traces by origin"></select></label></div><div id="distributed-traces"></div></section>
<section class="panel filter-bar dependency-panel" hidden><label>Observed service <select id="dependencies-observed-service-filter" class="tab-service-filter" aria-label="Filter Dependencies by observed service"></select></label></section>
<section id="details-flows" class="panel dependency-panel" hidden><div class="panel-head"><h2>Focused dependency flow</h2><label>Service <select id="service-filter" aria-label="Filter dependency flow by service"></select></label></div><div id="flows" class="flow"></div></section>
<section id="details-dependencies" class="panel dependency-panel" hidden><div class="panel-head"><h2>Dependencies</h2><span class="subtle">Average latency only; dependency P95 is a separate future pass</span></div><div id="dependencies"></div></section>
<section id="details-failures" class="panel service-panel" hidden><div class="panel-head"><h2>Recurring failures</h2><span class="subtle">Aggregated failure counts only</span></div><div id="failures"></div></section>
<p id="coverage" class="note"></p></main><script id="runtime-data" type="application/json">__RUNTIME_DATA__</script><script>
const data=JSON.parse(document.getElementById('runtime-data').textContent); const $=id=>document.getElementById(id); const n=v=>typeof v==='number'?v:null; const ms=v=>n(v)===null?'—':`${v.toLocaleString(undefined,{maximumFractionDigits:3})} ms`; const pct=v=>n(v)===null?'—':`${(v*100).toFixed(1)}%`; const count=v=>n(v)===null?'—':v.toLocaleString(); const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const rows=(items,columns)=>items.length?`<table><thead><tr>${columns.map(c=>`<th class="${c.metric?'metric':''}">${c.label}</th>`).join('')}</tr></thead><tbody>${items.map(item=>`<tr>${columns.map(c=>`<td class="${c.metric?'metric':''} ${c.className?c.className(item):''}">${c.html?c.html(item):esc(item[c.key])}</td>`).join('')}</tr>`).join('')}</tbody></table>`:'<p class="empty">No aggregate metric was observed in this window.</p>';
const failures=[...data.services.map(x=>({...x,label:x.service,kind:'Service'})),...data.transactions.map(x=>({...x,label:`${x.service} / ${x.transaction}`,kind:'Transaction'}))].filter(x=>x.failure_calls>0).sort((a,b)=>b.failure_calls-a.failure_calls||b.error_rate-a.error_rate).slice(0,20);
$('context').innerHTML=[`Window: ${esc(data.window.from)} → ${esc(data.window.to)}`,data.environment?`Environment: ${esc(data.environment)}`:'All environments',data.generated_at?`Snapshot: ${esc(data.generated_at)}`:''].filter(Boolean).map(x=>`<span class="pill">${x}</span>`).join('');
const coverageViews=['services','transactions','dependencies','timeline']; const limited=coverageViews.filter(name=>data.coverage&&data.coverage[name]&&data.coverage[name].truncated); $('report-status').innerHTML=limited.length?`<span class="status status-limited">Limited coverage: ${esc(limited.join(', '))}. Rankings may be incomplete.</span>`:'<span class="status status-ok">Coverage complete within the configured report limits.</span>';
const p95s=data.services.map(x=>x.p95_ms).filter(v=>n(v)!==null); const hot=p95s.length?Math.max(...p95s):null; const serviceFailureCount=data.services.reduce((sum,x)=>sum+(n(x.failure_calls)||0),0); const totalCalls=data.services.reduce((sum,x)=>sum+(n(x.calls)||0),0); const overallRate=totalCalls?serviceFailureCount/totalCalls:null; $('cards').innerHTML=[['Observed services',count(data.services.length)],['Observed calls',count(totalCalls)],['Failure rate',pct(overallRate)],['Observed dependencies',count(data.dependencies.length)]].map(([label,value])=>`<article class="card"><span class="subtle">${label}</span><b>${value}</b></article>`).join('');
const score=x=>((n(x.calls)||0)*(n(x.error_rate)||0)*1000)+(n(x.p95_ms)||n(x.average_ms)||0)*Math.log10(Math.max(1,n(x.calls)||0)); const triage=[...data.services.map(x=>({...x,kind:'Service',name:x.service})),...data.transactions.map(x=>({...x,kind:'Transaction',name:`${x.service} / ${x.transaction}`})),...data.dependencies.map(x=>({...x,kind:'Dependency',name:`${x.source} → ${x.target}`}))].sort((a,b)=>score(b)-score(a)).slice(0,3); $('triage-items').innerHTML=triage.length?triage.map(x=>`<article class="triage-item ${x.error_rate?'is-danger':''}"><span>${esc(x.kind)}</span><b>${esc(x.name)}</b><span>${count(x.calls)} calls · ${pct(x.error_rate)} errors · ${ms(x.p95_ms??x.average_ms)}</span></article>`).join(''):'<p class="empty">No aggregate metric was observed in this window.</p>';
$('services').innerHTML=rows(data.services,[{label:'Service',key:'service'},{label:'Calls',key:'calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms===hot?'danger':'warm',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
const transactionSelect=$('transaction-service-filter'); const transactionServices=[...new Set(data.transactions.map(x=>x.service))].sort(); transactionSelect.innerHTML=`<option value="">All observed services</option>${transactionServices.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')}`; function transactionGraph(){const selected=transactionSelect.value; const items=data.transactions.filter(x=>!selected||x.service===selected); const byService=new Map(); items.forEach(item=>{const group=byService.get(item.service)||[]; group.push(item); byService.set(item.service,group);}); const maxP95=Math.max(1,...items.map(x=>n(x.p95_ms)||0)); $('transaction-graph').innerHTML=items.length?[...byService.entries()].sort(([left],[right])=>left.localeCompare(right)).map(([service,transactions])=>`<div class="transaction-service"><div class="transaction-service-node">${esc(service)}</div><div class="transaction-service-edge">→</div><div class="transaction-nodes">${transactions.map(transaction=>{const ratio=(n(transaction.p95_ms)||0)/maxP95; const heat=ratio>=.75?'is-hot':ratio>=.4?'is-warm':''; return `<article class="transaction-node ${heat}"><div class="transaction-name">${esc(transaction.transaction)}</div><div class="transaction-metrics">P95 ${ms(transaction.p95_ms)} · ${count(transaction.calls)} calls · ${pct(transaction.error_rate)} errors</div></article>`;}).join('')}</div></div>`).join(''):'<p class="empty">No transaction aggregate for this selection.</p>';} transactionSelect.addEventListener('change',transactionGraph); transactionGraph();
$('transactions').innerHTML=rows(data.transactions,[{label:'Service',key:'service'},{label:'Transaction',html:x=>`${esc(x.transaction)}${x.transaction_type?` <span class="subtle">${esc(x.transaction_type)}</span>`:''}`},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms?'warm':'',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
const transactionKind=x=>{const type=String(x.transaction_type||'').toLowerCase();return type==='messaging'?'Messaging':(type==='request'||type==='http'?'HTTP':'Other');}; const transactionKindSelect=$('transaction-kind-filter'); transactionKindSelect.innerHTML='<option value="">HTTP, messaging, and other</option><option value="HTTP">HTTP</option><option value="Messaging">Messaging</option><option value="Other">Other</option>'; function renderTransactionGraph(){const selected=transactionSelect.value;const selectedKind=transactionKindSelect.value;const items=data.transactions.filter(x=>(!selected||x.service===selected)&&(!selectedKind||transactionKind(x)===selectedKind));const byService=new Map();items.forEach(item=>{const group=byService.get(item.service)||[];group.push(item);byService.set(item.service,group);});const maxP95=Math.max(1,...items.map(x=>n(x.p95_ms)||0));$('transaction-graph').innerHTML=items.length?[...byService.entries()].sort(([left],[right])=>left.localeCompare(right)).map(([service,transactions])=>{const byKind=new Map();transactions.forEach(transaction=>{const kind=transactionKind(transaction);const group=byKind.get(kind)||[];group.push(transaction);byKind.set(kind,group);});return `<div class="transaction-service"><div class="transaction-service-node">${esc(service)}</div><div class="transaction-service-edge">→</div><div class="transaction-kinds">${['HTTP','Messaging','Other'].filter(kind=>byKind.has(kind)).map(kind=>`<section class="transaction-kind kind-${kind.toLowerCase()}"><h3>${kind}</h3><div class="transaction-nodes">${byKind.get(kind).map(transaction=>{const ratio=(n(transaction.p95_ms)||0)/maxP95;const heat=ratio>=.75?'is-hot':ratio>=.4?'is-warm':'';return `<article class="transaction-node ${heat}"><div class="transaction-name">${esc(transaction.transaction)}</div><div class="transaction-metrics">P95 ${ms(transaction.p95_ms)} · ${count(transaction.calls)} calls · ${pct(transaction.error_rate)} errors</div></article>`;}).join('')}</div></section>`).join('')}</div></div>`;}).join(''):'<p class="empty">No transaction aggregate for this selection.</p>';} transactionSelect.addEventListener('change',renderTransactionGraph);transactionKindSelect.addEventListener('change',renderTransactionGraph);renderTransactionGraph();
$('transactions').innerHTML=rows(data.transactions,[{label:'Service',key:'service'},{label:'Transaction',html:x=>`${esc(x.transaction)} <span class="kind-pill">${esc(transactionKind(x))}</span>${x.transaction_type?` <span class="subtle">${esc(x.transaction_type)}</span>`:''}`},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms?'warm':'',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
$('dependencies').innerHTML=rows(data.dependencies,[{label:'Source',key:'source'},{label:'Target',html:x=>`${esc(x.target)}${x.target_type?` <span class="subtle">${esc(x.target_type)}</span>`:''}`},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,className:x=>x.average_ms?'warm':'',html:x=>ms(x.average_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
$('failures').innerHTML=rows(failures,[{label:'Kind',key:'kind'},{label:'Workload',key:'label'},{label:'Failures',metric:true,className:()=> 'danger',html:x=>count(x.failure_calls)},{label:'Error rate',metric:true,className:()=> 'danger',html:x=>pct(x.error_rate)},{label:'P95',metric:true,html:x=>ms(x.p95_ms)}]);
const select=$('service-filter'); const services=[...new Set(data.dependencies.flatMap(x=>[x.source,x.target]))].sort(); select.innerHTML=`<option value="">All observed flows</option>${services.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')}`; function flows(){const selected=select.value;const items=data.dependencies.filter(x=>!selected||x.source===selected||x.target===selected); const max=Math.max(1,...items.map(x=>x.calls)); $('flows').innerHTML=items.length?items.map(x=>`<div class="flow-row"><div class="node">${esc(x.source)}</div><div class="arrow">→</div><div class="node">${esc(x.target)}</div><div class="edge-wrap"><span class="edge" style="width:${Math.max(8,Math.round(100*x.calls/max))}px"></span><span class="subtle">${ms(x.average_ms)} · ${count(x.calls)}</span></div></div>`).join(''):'<p class="empty">No dependency flow for this selection.</p>';} select.addEventListener('change',flows); flows();
const mapMode=$('map-mode'),mapService=$('map-service-filter'),mapWorkload=$('map-workload-filter');const mapServices=[...new Set([...data.services.map(x=>x.service),...data.dependencies.flatMap(x=>[x.source,x.target])])].sort();mapMode.innerHTML='<option value="hotspots">Hotspots</option><option value="all">All observed dependencies</option>';mapService.innerHTML=`<option value="">All services</option>${mapServices.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')}`;mapWorkload.innerHTML='<option value="">All workloads</option><option value="HTTP">HTTP</option><option value="Messaging">Messaging</option><option value="Other">Other</option>';const detailRows=(items,render)=>items.length?`<div class="detail-list">${items.map(render).join('')}</div>`:'<p class="empty">No aggregate metric was observed.</p>';function runtimeMap(){const focused=mapService.value;const riskAverage=Math.max(1,...data.dependencies.map(x=>n(x.average_ms)||0))*.7;let edges=data.dependencies.filter(x=>!focused||x.source===focused||x.target===focused);if(mapMode.value==='hotspots'){edges=edges.filter(x=>x.error_rate>0||(n(x.average_ms)||0)>=riskAverage);if(!edges.length)edges=data.dependencies.filter(x=>!focused||x.source===focused||x.target===focused).slice(0,12);}const names=[...new Set([...edges.flatMap(x=>[x.source,x.target]),...(focused?[focused]:[])])].sort();if(!names.length){$('service-map').innerHTML='<p class="empty">No observed dependency in this window.</p>';$('service-map-details').innerHTML='';return;}const centerX=450,centerY=205,radius=Math.min(145,Math.max(75,names.length*20));const points=new Map(names.map((name,index)=>{const angle=-Math.PI/2+(2*Math.PI*index/names.length);return [name,{x:centerX+radius*Math.cos(angle),y:centerY+radius*Math.sin(angle)}];}));const selected=focused||names[0];const edgeSvg=edges.filter(x=>x.source!==x.target).map((edge,index)=>{const from=points.get(edge.source),to=points.get(edge.target);if(!from||!to)return '';const risk=edge.error_rate>0||(n(edge.average_ms)||0)>=riskAverage;const width=Math.max(2,Math.min(10,1+Math.log10(Math.max(1,edge.calls))*2));const mx=(from.x+to.x)/2,my=(from.y+to.y)/2;return `<g class="map-edge ${risk?'is-risk':''}" data-edge="${index}"><line x1="${from.x}" y1="${from.y}" x2="${to.x}" y2="${to.y}" stroke-width="${width}" marker-end="url(#arrow)"></line><text class="map-edge-label" x="${mx}" y="${my}">${count(edge.calls)} · ${ms(edge.average_ms)}</text></g>`;}).join('');const nodeSvg=names.map(name=>{const point=points.get(name),metric=data.services.find(x=>x.service===name),risk=metric&&metric.error_rate>0;return `<g class="map-node ${name===selected?'is-selected':''} ${risk?'is-risk':''}" data-service="${esc(name)}"><circle cx="${point.x}" cy="${point.y}" r="45"></circle><text x="${point.x}" y="${point.y-4}" text-anchor="middle">${esc(name)}</text><text x="${point.x}" y="${point.y+14}" text-anchor="middle">${metric?`${ms(metric.p95_ms)} · ${pct(metric.error_rate)}`:'observed'}</text></g>`;}).join('');$('service-map').innerHTML=`<svg viewBox="0 0 900 410" role="img" aria-label="Observed service dependency map"><defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#7890bd"></path></marker></defs>${edgeSvg}${nodeSvg}</svg>`;document.querySelectorAll('.map-node').forEach(node=>node.addEventListener('click',()=>{mapService.value=node.dataset.service||'';runtimeMap();}));const tx=data.transactions.filter(x=>x.service===selected&&(!mapWorkload.value||transactionKind(x)===mapWorkload.value));const outbound=data.dependencies.filter(x=>x.source===selected),incoming=data.dependencies.filter(x=>x.target===selected);$('service-map-details').innerHTML=`<article class="detail-card"><h3>${esc(selected)} workloads</h3>${detailRows(tx,x=>`<div class="detail-item"><span>${esc(x.transaction)} <span class="kind-pill">${esc(transactionKind(x))}</span></span><span>${ms(x.p95_ms)} · ${count(x.calls)}</span></div>`)}</article><article class="detail-card"><h3>Observed dependencies</h3><h4>Outbound</h4>${detailRows(outbound,x=>`<div class="detail-item"><span>→ ${esc(x.target)}</span><span>${count(x.calls)} · ${pct(x.error_rate)}</span></div>`)}<h4>Inbound</h4>${detailRows(incoming,x=>`<div class="detail-item"><span>${esc(x.source)} →</span><span>${count(x.calls)} · ${pct(x.error_rate)}</span></div>`)}</article>`;}mapMode.addEventListener('change',runtimeMap);mapService.addEventListener('change',runtimeMap);mapWorkload.addEventListener('change',runtimeMap);runtimeMap();
const messagingTargetTypes=new Set(['amqp','jms','kafka','messaging','nats','pulsar','rabbitmq','sqs']);let typedSelectedNode=null;const nodeKey=(name,kind)=>`${kind}:${name}`;function typedServiceMap(){const focused=mapService.value;const riskAverage=Math.max(1,...data.dependencies.map(x=>n(x.average_ms)||0))*.7;let edges=data.dependencies.filter(x=>!focused||x.source===focused||x.target===focused).map(edge=>({...edge,target_kind:messagingTargetTypes.has(String(edge.target_type||'').toLowerCase())?'topic':'service'}));if(mapMode.value==='hotspots'){edges=edges.filter(x=>x.error_rate>0||(n(x.average_ms)||0)>=riskAverage);if(!edges.length)edges=data.dependencies.filter(x=>!focused||x.source===focused||x.target===focused).slice(0,12).map(edge=>({...edge,target_kind:messagingTargetTypes.has(String(edge.target_type||'').toLowerCase())?'topic':'service'}));}const nodes=new Map();edges.forEach(edge=>{nodes.set(nodeKey(edge.source,'service'),{name:edge.source,kind:'service'});nodes.set(nodeKey(edge.target,edge.target_kind),{name:edge.target,kind:edge.target_kind});});if(focused)nodes.set(nodeKey(focused,'service'),{name:focused,kind:'service'});const items=[...nodes.entries()].sort(([,left],[,right])=>left.name.localeCompare(right.name));if(!items.length){$('service-map').innerHTML='<p class="empty">No observed dependency in this window.</p>';$('service-map-details').innerHTML='';return;}if(!typedSelectedNode||!nodes.has(typedSelectedNode))typedSelectedNode=nodeKey(focused||items[0][1].name,'service');const centerX=450,centerY=205,radius=Math.min(145,Math.max(75,items.length*20));const points=new Map(items.map(([key,node],index)=>{const angle=-Math.PI/2+(2*Math.PI*index/items.length);return [key,{...node,x:centerX+radius*Math.cos(angle),y:centerY+radius*Math.sin(angle)}];}));const edgeSvg=edges.map(edge=>{const from=points.get(nodeKey(edge.source,'service')),to=points.get(nodeKey(edge.target,edge.target_kind));if(!from||!to)return '';const risk=edge.error_rate>0||(n(edge.average_ms)||0)>=riskAverage;const width=Math.max(2,Math.min(10,1+Math.log10(Math.max(1,edge.calls))*2));const mx=(from.x+to.x)/2,my=(from.y+to.y)/2;const direction=edge.target_kind==='topic'?'send':'HTTP';return `<g class="map-edge ${risk?'is-risk':''}"><line x1="${from.x}" y1="${from.y}" x2="${to.x}" y2="${to.y}" stroke-width="${width}" marker-end="url(#arrow)"></line><text class="map-edge-label" x="${mx}" y="${my}">${direction} → ${count(edge.calls)} · ${ms(edge.average_ms)}</text></g>`;}).join('');const nodeSvg=[...points.entries()].map(([key,node])=>{const selected=key===typedSelectedNode,metric=node.kind==='service'?data.services.find(x=>x.service===node.name):null,risk=metric&&metric.error_rate>0;const shape=node.kind==='topic'?`<polygon points="${node.x},${node.y-37} ${node.x+37},${node.y} ${node.x},${node.y+37} ${node.x-37},${node.y}"></polygon>`:`<circle cx="${node.x}" cy="${node.y}" r="45"></circle>`;return `<g class="map-node ${node.kind==='topic'?'map-topic':''} ${selected?'is-selected':''} ${risk?'is-risk':''}" data-node="${esc(key)}">${shape}<text x="${node.x}" y="${node.y-4}" text-anchor="middle">${esc(node.name)}</text><text x="${node.x}" y="${node.y+14}" text-anchor="middle">${node.kind==='topic'?'messaging target':metric?`${ms(metric.p95_ms)} · ${pct(metric.error_rate)}`:'external target'}</text></g>`;}).join('');$('service-map').innerHTML=`<div class="map-legend"><span>Observed service</span><span class="legend-topic">Observed messaging target</span></div><svg viewBox="0 0 900 410" role="img" aria-label="Directed observed service and messaging-target map"><defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#7890bd"></path></marker></defs>${edgeSvg}${nodeSvg}</svg>`;document.querySelectorAll('.map-node').forEach(node=>node.addEventListener('click',()=>{typedSelectedNode=node.dataset.node||null;typedServiceMap();}));const selected=points.get(typedSelectedNode);if(!selected)return;const tx=selected.kind==='service'?data.transactions.filter(x=>x.service===selected.name&&(!mapWorkload.value||transactionKind(x)===mapWorkload.value)):[];const outbound=selected.kind==='service'?edges.filter(x=>x.source===selected.name):[];const incoming=edges.filter(x=>x.target===selected.name&&x.target_kind===selected.kind);$('service-map-details').innerHTML=`<article class="detail-card"><h3>${esc(selected.name)} ${selected.kind==='topic'?'messaging target':'service'}</h3>${selected.kind==='service'?detailRows(tx,x=>`<div class="detail-item"><span>${esc(x.transaction)} <span class="kind-pill">${esc(transactionKind(x))}</span></span><span>${ms(x.p95_ms)} · ${count(x.calls)}</span></div>`):'<p class="note">The target type identifies messaging; APM does not prove that this name is a confirmed topic.</p>'}</article><article class="detail-card"><h3>Direction</h3><h4>${selected.kind==='topic'?'Observed senders':'Outbound'}</h4>${detailRows(selected.kind==='topic'?incoming:outbound,x=>`<div class="detail-item"><span>${selected.kind==='topic'?`${esc(x.source)} sends →`: `→ ${esc(x.target)}`}</span><span>${count(x.calls)} · ${pct(x.error_rate)}</span></div>`)}${selected.kind==='service'?`<h4>Inbound</h4>${detailRows(incoming,x=>`<div class="detail-item"><span>${esc(x.source)} →</span><span>${count(x.calls)} · ${pct(x.error_rate)}</span></div>`)}`:''}</article>`;}mapMode.addEventListener('change',typedServiceMap);mapService.addEventListener('change',()=>{typedSelectedNode=null;typedServiceMap();});mapWorkload.addEventListener('change',typedServiceMap);typedServiceMap();
if(!mapService.value&&data.dependencies.length){typedSelectedNode=nodeKey(data.dependencies[0].source,'service');typedServiceMap();}
let apmMapRenderer=null;function sigmaServiceMap(){if(!window.graphology||!window.Sigma)return;const focused=mapService.value;const riskAverage=Math.max(1,...data.dependencies.map(x=>n(x.average_ms)||0))*.7;let edges=data.dependencies.filter(x=>!focused||x.source===focused||x.target===focused).map(edge=>({...edge,target_kind:messagingTargetTypes.has(String(edge.target_type||'').toLowerCase())?'topic':'service'}));if(mapMode.value==='hotspots'){edges=edges.filter(x=>x.error_rate>0||(n(x.average_ms)||0)>=riskAverage);if(!edges.length)edges=data.dependencies.filter(x=>!focused||x.source===focused||x.target===focused).slice(0,12).map(edge=>({...edge,target_kind:messagingTargetTypes.has(String(edge.target_type||'').toLowerCase())?'topic':'service'}));}const nodes=new Map();edges.forEach(edge=>{nodes.set(nodeKey(edge.source,'service'),{name:edge.source,kind:'service'});nodes.set(nodeKey(edge.target,edge.target_kind),{name:edge.target,kind:edge.target_kind});});if(focused)nodes.set(nodeKey(focused,'service'),{name:focused,kind:'service'});const items=[...nodes.entries()].sort(([,left],[,right])=>left.name.localeCompare(right.name));if(!items.length)return;const network=new graphology.MultiDirectedGraph();const radius=Math.max(1,items.length/5);items.forEach(([key,node],index)=>{const angle=-Math.PI/2+(2*Math.PI*index/items.length);const metric=node.kind==='service'?data.services.find(x=>x.service===node.name):null;network.addNode(key,{label:`${node.kind==='topic'?'◇ ':''}${node.name}`,x:radius*Math.cos(angle),y:radius*Math.sin(angle),size:node.kind==='topic'?10:12,color:node.kind==='topic'?'#087443':metric&&metric.error_rate>0?'#b42318':'#3156d3'});});edges.forEach((edge,index)=>{const direction=edge.target_kind==='topic'?'send':'HTTP';network.addEdgeWithKey(`apm-edge-${index}`,nodeKey(edge.source,'service'),nodeKey(edge.target,edge.target_kind),{label:`${direction} · ${count(edge.calls)} calls · ${ms(edge.average_ms)}`,size:Math.max(1,Math.min(6,1+Math.log10(Math.max(1,edge.calls)))),color:edge.error_rate>0?'#b42318':'#7890bd',type:'arrow'});});apmMapRenderer?.kill();$('service-map').innerHTML='<div class="map-legend"><span>Observed service</span><span class="legend-topic">◇ Observed messaging target</span></div><div id="service-map-canvas" style="height:400px;min-width:700px"></div>';apmMapRenderer=new Sigma(network,$('service-map-canvas'),{renderEdgeLabels:true,labelDensity:.08,labelGridCellSize:160,labelRenderedSizeThreshold:8});apmMapRenderer.on('clickNode',({node})=>{typedSelectedNode=node;typedServiceMap();sigmaServiceMap();});}mapMode.addEventListener('change',sigmaServiceMap);mapService.addEventListener('change',sigmaServiceMap);mapWorkload.addEventListener('change',sigmaServiceMap);sigmaServiceMap();
const timelineEvents=Array.isArray(data.timeline_events)?data.timeline_events:[];const timelineService=$('timeline-service-filter');const timelineServices=[...new Set(timelineEvents.map(x=>x.service).filter(Boolean))].sort();timelineService.innerHTML=`<option value="">All observed services</option>${timelineServices.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')}`;function renderTimeline(){const items=timelineEvents.filter(x=>!timelineService.value||x.service===timelineService.value);$('timeline-events').innerHTML=items.length?items.map(item=>`<article class="timeline-item ${item.outcome==='failure'?'is-failure':''}"><time>${esc(item.timestamp)}</time><div><div class="timeline-name">${esc(item.service)} · ${esc(item.transaction)}</div><div class="subtle">${esc(item.transaction_type||'other')}${item.messaging_target?` · ${esc(item.messaging_target)}`:''}${item.result?` · ${esc(item.result)}`:''}</div></div><strong>${ms(item.duration_ms)}</strong></article>`).join(''):'<p class="empty">No recorded transaction event was available in this window.</p>';const coverage=data.coverage.timeline||{};$('timeline-coverage').textContent=`Recorded events: ${count(coverage.items_exported||0)}${coverage.truncated?' (truncated at the configured event limit)':''}.`;}timelineService.addEventListener('change',renderTimeline);renderTimeline();function setRuntimeTab(tab){const timeline=tab==='timeline';document.querySelectorAll('main > .panel').forEach(panel=>{panel.hidden=timeline?panel.id!=='timeline-panel':panel.id==='timeline-panel';});$('coverage').hidden=timeline;$('overview-tab').classList.toggle('is-active',!timeline);$('timeline-tab').classList.toggle('is-active',timeline);}$('overview-tab').addEventListener('click',()=>setRuntimeTab('overview'));$('timeline-tab').addEventListener('click',()=>setRuntimeTab('timeline'));
const cv=data.coverage; const view=(name,c)=>`${name}: ${c.items_exported}/${c.items_seen}${c.truncated?` (truncated: ${c.truncation_reasons.join(', ')})`:''}`; $('coverage').textContent=`Coverage — ${view('services',cv.services)} · ${view('transactions',cv.transactions)} · ${view('dependencies',cv.dependencies)}. A zero result means no matching aggregate was observed in the selected window.`;
const globalService=$('global-service-filter'),globalWorkload=$('global-workload-filter'),failuresOnly=$('failures-only-filter');const allServices=[...new Set([...data.services.map(x=>x.service),...data.transactions.map(x=>x.service),...data.dependencies.flatMap(x=>[x.source,x.target])])].sort();globalService.innerHTML=`<option value="">All observed services</option>${allServices.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')}`;globalWorkload.innerHTML='<option value="">All workloads</option><option value="HTTP">HTTP</option><option value="Messaging">Messaging</option><option value="Other">Other</option>';function filtered(items){return items.filter(x=>{const service=globalService.value;const owned=!service||x.service===service||x.source===service||x.target===service;const workload=!globalWorkload.value||!x.transaction_type||transactionKind(x)===globalWorkload.value;const failed=!failuresOnly.checked||(n(x.error_rate)||0)>0;return owned&&workload&&failed;});}function renderFilteredTables(){$('services').innerHTML=rows(filtered(data.services),[{label:'Service',key:'service'},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms===hot?'danger':'warm',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);$('transactions').innerHTML=rows(filtered(data.transactions),[{label:'Service',key:'service'},{label:'Transaction',html:x=>`${esc(x.transaction)} <span class="kind-pill">${esc(transactionKind(x))}</span>`},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms?'warm':'',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);$('dependencies').innerHTML=rows(filtered(data.dependencies),[{label:'Source',key:'source'},{label:'Target',key:'target'},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);$('failures').innerHTML=rows(filtered(failures),[{label:'Kind',key:'kind'},{label:'Workload',key:'label'},{label:'Failures',metric:true,className:()=> 'danger',html:x=>count(x.failure_calls)},{label:'Error rate',metric:true,className:()=> 'danger',html:x=>pct(x.error_rate)},{label:'P95',metric:true,html:x=>ms(x.p95_ms)}]);}function applyGlobalFilters(){const service=globalService.value,workload=globalWorkload.value;[mapService,transactionSelect,select,timelineService].forEach(control=>{if([...control.options].some(option=>option.value===service))control.value=service;});mapWorkload.value=workload;transactionKindSelect.value=workload;renderFilteredTables();mapService.dispatchEvent(new Event('change'));transactionSelect.dispatchEvent(new Event('change'));select.dispatchEvent(new Event('change'));timelineService.dispatchEvent(new Event('change'));}globalService.addEventListener('change',applyGlobalFilters);globalWorkload.addEventListener('change',applyGlobalFilters);failuresOnly.addEventListener('change',applyGlobalFilters);$('clear-filters').addEventListener('click',()=>{globalService.value='';globalWorkload.value='';failuresOnly.checked=false;applyGlobalFilters();});renderFilteredTables();$('details-tab').addEventListener('click',()=>{document.querySelectorAll('main > .panel:not(#timeline-panel):not(.detail-panel)').forEach(panel=>panel.hidden=true);$('timeline-panel').hidden=true;document.querySelectorAll('.detail-panel').forEach(panel=>panel.hidden=false);$('coverage').hidden=true;$('overview-tab').classList.remove('is-active');$('timeline-tab').classList.remove('is-active');$('details-tab').classList.add('is-active');});$('overview-tab').addEventListener('click',()=>{document.querySelectorAll('.detail-panel').forEach(panel=>panel.hidden=true);$('details-tab').classList.remove('is-active');});$('timeline-tab').addEventListener('click',()=>{document.querySelectorAll('.detail-panel').forEach(panel=>panel.hidden=true);$('details-tab').classList.remove('is-active');});
</script></body></html>"""
