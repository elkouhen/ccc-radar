"""Bounded aggregate Elastic APM runtime report with a Sigma.js graph view."""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from typing import Callable

from systemlens.apm import (
    ApmError,
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
    timeline_events, timeline_truncated = _read_timeline_events(
        client, start, end, environment, max_timeline_events
    )

    services = _rank_latency_buckets(service_buckets, kind="service")
    transactions = _rank_latency_buckets(transaction_buckets, kind="transaction")
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
            "kind": "elastic_apm_metric_aggregates",
            "raw_event_source_exported": False,
            "recorded_transaction_projection": True,
            "service_metricset": "service_transaction",
            "transaction_metricset": "transaction",
            "dependency_metricset": "service_destination",
            "dependency_target_field": target_field,
            "timeline_trace_index": "traces-apm*",
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
        "timeline_events": timeline_events,
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
                "items_exported": len(timeline_events),
                "truncated": timeline_truncated,
                "truncation_reasons": ["max_timeline_events"] if timeline_truncated else [],
                "max_events": max_timeline_events,
            },
        },
    }


def _read_timeline_events(
    client: ElasticApmClient,
    start: datetime,
    end: datetime,
    environment: str | None,
    max_events: int,
) -> tuple[list[dict[str, object]], bool]:
    """Read a bounded, field-projected transaction timeline from traces.

    The query explicitly disables ``_source`` and never requests identifiers,
    request data, headers, bodies, stack traces, or error messages.
    """
    search_traces = getattr(client, "search_traces", None)
    if not callable(search_traces):
        return [], False
    filters: list[dict[str, object]] = [
        {"term": {"processor.event": "transaction"}},
        {"range": {"@timestamp": {"gte": _iso8601(start), "lt": _iso8601(end)}}},
    ]
    if environment:
        filters.append({"term": {"service.environment": environment}})
    response = search_traces({
        "size": max_events,
        "track_total_hits": False,
        "_source": False,
        "sort": [{"@timestamp": {"order": "asc"}}],
        "fields": [
            "@timestamp", "service.name", "transaction.name", "transaction.type",
            "transaction.duration.us", "transaction.result", "event.outcome",
            "transaction.message.queue.name",
        ],
        "query": {"bool": {"filter": filters}},
    })
    hits = response.get("hits")
    if not isinstance(hits, dict):
        return [], False
    raw_hits = hits.get("hits")
    if not isinstance(raw_hits, list):
        return [], False
    events: list[dict[str, object]] = []
    for hit in raw_hits:
        if not isinstance(hit, dict) or not isinstance(hit.get("fields"), dict):
            continue
        fields = hit["fields"]
        timestamp = _timeline_field_string(fields, "@timestamp")
        service = _timeline_field_string(fields, "service.name")
        transaction = _timeline_field_string(fields, "transaction.name")
        if not timestamp or not service or not transaction:
            continue
        duration = _timeline_field_number(fields, "transaction.duration.us")
        event: dict[str, object] = {
            "timestamp": timestamp,
            "service": service,
            "transaction": transaction,
            "transaction_type": _timeline_field_string(fields, "transaction.type"),
            "duration_ms": round(duration / 1_000, 3) if duration is not None else None,
            "result": _timeline_field_string(fields, "transaction.result"),
            "outcome": _timeline_field_string(fields, "event.outcome"),
        }
        queue = _timeline_field_string(fields, "transaction.message.queue.name")
        if queue:
            event["messaging_target"] = queue
        events.append(event)
    return events, len(raw_hits) >= max_events


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
                        "failures": {
                            "filter": {"term": {"event.outcome": "failure"}},
                            "aggs": {
                                "calls": {
                                    "value_count": {
                                        "field": "transaction.duration.summary"
                                    }
                                }
                            },
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
        failures = _nested_metric_value(bucket, "failures", "calls")
        item: dict[str, object] = {
            "service": service,
            "calls": calls,
            "failure_calls": round(failures),
            "error_rate": round(failures / calls, 6) if calls else None,
            "average_ms": round(duration_us / calls / 1_000, 3) if calls else None,
            "p95_ms": _percentile_ms(bucket),
        }
        if kind == "transaction":
            name = key.get("transaction")
            if not isinstance(name, str) or not name:
                continue
            item["transaction"] = name
            transaction_type = key.get("transaction_type")
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
    data = json.dumps(report, ensure_ascii=False, separators=(",", ":")).replace(
        "</", "<\\/"
    )
    title = escape("SystemLens · APM runtime overview")
    document = _RUNTIME_REPORT_HTML.replace("__TITLE__", title).replace(
        "__RUNTIME_DATA__", data
    )
    return document.replace("</body>", _RUNTIME_REPORT_ENHANCEMENTS + "</body>")


_RUNTIME_REPORT_ENHANCEMENTS = """<script>
(() => {
  const data = JSON.parse(document.getElementById('runtime-data').textContent);
  const byId = id => document.getElementById(id);
  const number = value => typeof value === 'number' ? value : 0;
  const esc = value => String(value ?? '—').replace(/[&<>"']/g, character => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[character]));
  const formatMs = value => typeof value === 'number' ? `${value.toLocaleString(undefined,{maximumFractionDigits:3})} ms` : '—';
  const workload = event => {
    const type = String(event.transaction_type || '').toLowerCase();
    return type === 'messaging' ? 'Messaging' : (type === 'request' || type === 'http' ? 'HTTP' : 'Other');
  };
  const summary = document.createElement('div');
  summary.id = 'timeline-summary';
  summary.className = 'timeline-summary';
  byId('timeline-events').before(summary);
  function renderTimelineTriage() {
    const service = byId('global-service-filter').value;
    const kind = byId('global-workload-filter').value;
    const failuresOnly = byId('failures-only-filter').checked;
    const events = (Array.isArray(data.timeline_events) ? data.timeline_events : []).filter(event =>
      (!service || event.service === service) &&
      (!kind || workload(event) === kind) &&
      (!failuresOnly || event.outcome === 'failure')
    );
    const buckets = [['Under 100 ms', 0], ['100–500 ms', 0], ['500 ms or more', 0]];
    events.forEach(event => { const duration = number(event.duration_ms); buckets[duration < 100 ? 0 : duration < 500 ? 1 : 2][1] += 1; });
    summary.innerHTML = buckets.map(([label, count]) => `<div class="timeline-bucket"><b>${count.toLocaleString()}</b>${label}</div>`).join('');
    byId('timeline-events').innerHTML = events.length ? events.map(event => `<article class="timeline-item ${event.outcome === 'failure' ? 'is-failure' : ''}"><time>${esc(event.timestamp)}</time><div><div class="timeline-name">${esc(event.service)} · ${esc(event.transaction)}</div><div class="subtle">${esc(event.transaction_type || 'other')}${event.messaging_target ? ` · ${esc(event.messaging_target)}` : ''}${event.result ? ` · ${esc(event.result)}` : ''}</div></div><strong>${formatMs(event.duration_ms)}</strong></article>`).join('') : '<p class="empty">No recorded transaction event matches this selection.</p>';
  }
  ['global-service-filter', 'global-workload-filter', 'failures-only-filter', 'timeline-service-filter'].forEach(id => byId(id).addEventListener('change', renderTimelineTriage));
  renderTimelineTriage();
})();
</script>"""


_RUNTIME_REPORT_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title><script src="https://cdnjs.cloudflare.com/ajax/libs/graphology/0.25.4/graphology.umd.min.js"></script><script src="https://cdnjs.cloudflare.com/ajax/libs/sigma.js/2.4.0/sigma.min.js"></script><style>
:root { color-scheme: light; --ink:#182033; --muted:#5f6b82; --line:#dde3ee; --panel:#fff; --canvas:#f5f7fb; --blue:#3156d3; --amber:#a65100; --red:#b42318; --green:#087443; }
* { box-sizing:border-box; } body { margin:0; background:var(--canvas); color:var(--ink); font:14px/1.45 Inter,ui-sans-serif,system-ui,sans-serif; }
main { max-width:1440px; margin:auto; padding:24px; } h1,h2 { margin:0; } h1 { font-size:25px; } h2 { font-size:16px; } .subtle { color:var(--muted); margin:4px 0 0; } .context { display:flex; flex-wrap:wrap; gap:8px; margin:18px 0; } .pill { background:#eaf0ff; color:#243e99; border-radius:999px; padding:5px 9px; font-size:12px; } .cards { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-bottom:12px; } .card,.panel { background:var(--panel); border:1px solid var(--line); border-radius:12px; box-shadow:0 1px 2px #1820330b; } .card { padding:14px; } .card b { display:block; font-size:24px; margin-top:4px; } .panel { padding:16px; margin-top:12px; overflow:auto; } .panel-head { display:flex; justify-content:space-between; align-items:baseline; gap:12px; margin-bottom:10px; } select { border:1px solid var(--line); border-radius:7px; background:#fff; padding:6px; max-width:260px; } table { width:100%; border-collapse:collapse; white-space:nowrap; } th { color:var(--muted); font-weight:600; text-align:left; font-size:12px; } td,th { padding:9px 8px; border-bottom:1px solid #edf0f5; } tr:last-child td { border:0; } .metric { font-variant-numeric:tabular-nums; text-align:right; } .danger { color:var(--red); font-weight:650; } .warm { color:var(--amber); font-weight:650; } .flow { display:grid; gap:7px; min-width:660px; } .flow-row { display:grid; grid-template-columns:1fr 26px 1fr 150px; align-items:center; gap:8px; } .node { overflow:hidden; text-overflow:ellipsis; padding:7px 9px; border-radius:7px; background:#f1f4fa; } .arrow { color:var(--blue); font-size:18px; text-align:center; } .edge { height:8px; min-width:14px; border-radius:99px; background:var(--blue); opacity:.25; } .edge-wrap { display:flex; align-items:center; gap:7px; } .transaction-graph { display:grid; gap:14px; min-width:660px; } .transaction-service { display:grid; grid-template-columns:minmax(150px,.3fr) 30px 1fr; align-items:stretch; gap:8px; } .transaction-service-node { display:flex; align-items:center; justify-content:center; padding:12px; border:1px solid #cbd5e1; border-radius:10px; background:#eef3ff; color:#243e99; font-weight:750; overflow-wrap:anywhere; } .transaction-service-edge { display:flex; align-items:center; justify-content:center; color:var(--blue); font-size:22px; } .transaction-nodes { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:8px; } .transaction-node { min-height:86px; padding:10px; border:1px solid #dbe3ef; border-left:5px solid #5b74db; border-radius:9px; background:#f8faff; } .transaction-node.is-warm { border-left-color:#d48a22; background:#fffaf1; } .transaction-node.is-hot { border-left-color:#c13b31; background:#fff6f5; } .transaction-name { overflow-wrap:anywhere; font-weight:750; } .transaction-metrics { margin-top:7px; color:var(--muted); font-size:12px; } .note { color:var(--muted); font-size:12px; margin:12px 0 0; } .empty { color:var(--muted); padding:8px 0; } @media (max-width:800px) { main { padding:14px; } .cards { grid-template-columns:repeat(2,minmax(0,1fr)); } .panel { padding:12px; } } @media (max-width:460px) { .cards { grid-template-columns:1fr; } }
.transaction-kinds { display:grid; gap:10px; } .transaction-kind { border:1px solid #e2e8f0; border-radius:10px; padding:9px; background:#fbfcff; } .transaction-kind h3 { margin:0 0 8px; color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; } .transaction-kind.kind-http { border-color:#bfdbfe; background:#f6faff; } .transaction-kind.kind-messaging { border-color:#bbf7d0; background:#f6fff9; } .kind-pill { display:inline-block; margin-left:6px; padding:1px 5px; border-radius:99px; background:#eaf0ff; color:#243e99; font-size:11px; font-weight:650; }
.service-map { min-height:410px; border:1px solid #e4eaf3; border-radius:11px; background:radial-gradient(circle at 50% 45%,#fff 0,#f6f8fc 70%); overflow:auto; } .service-map svg { display:block; min-width:700px; width:100%; height:410px; } .service-map .map-edge { stroke:#7890bd; fill:none; cursor:pointer; } .service-map .map-edge.is-risk { stroke:var(--red); } .service-map .map-edge-label { fill:var(--muted); font-size:11px; pointer-events:none; } .service-map .map-node { cursor:pointer; } .service-map .map-node circle { fill:#eef3ff; stroke:#7890bd; stroke-width:2; } .service-map .map-node.is-selected circle { fill:#3156d3; stroke:#1d3b9e; stroke-width:4; } .service-map .map-node.is-risk circle { stroke:#c13b31; } .service-map .map-node text { fill:#172554; font-size:12px; font-weight:700; pointer-events:none; } .service-map .map-node.is-selected text { fill:#fff; } .service-map-details { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:12px; } .service-map-details h3 { margin:0 0 7px; font-size:14px; } .service-map-details h4 { color:var(--muted); font-size:12px; margin:12px 0 6px; text-transform:uppercase; } .detail-card { border:1px solid #e4eaf3; border-radius:9px; padding:11px; background:#fff; } .detail-list { display:grid; gap:6px; } .detail-item { display:flex; justify-content:space-between; gap:10px; padding:6px 0; border-bottom:1px solid #eef2f7; } .detail-item:last-child { border-bottom:0; } @media (max-width:800px) { .service-map-details { grid-template-columns:1fr; } }
.service-map .map-topic polygon { fill:#ecfdf3; stroke:#15945c; stroke-width:2; } .service-map .map-topic.is-selected polygon { fill:#087443; stroke:#065f46; stroke-width:4; } .service-map .map-topic text { fill:#065f46; } .service-map .map-topic.is-selected text { fill:#fff; } .map-legend { display:flex; gap:12px; margin:0 0 8px; color:var(--muted); font-size:12px; } .map-legend span::before { content:''; display:inline-block; width:10px; height:10px; margin-right:5px; vertical-align:-1px; background:#eef3ff; border:1px solid #7890bd; border-radius:50%; } .map-legend .legend-topic::before { background:#ecfdf3; border-color:#15945c; border-radius:1px; transform:rotate(45deg); }
.runtime-tabs { display:flex; gap:6px; margin:0 0 12px; } .runtime-tab { border:1px solid #cbd5e1; border-radius:7px; padding:7px 11px; color:#334155; background:#fff; font:inherit; font-weight:700; cursor:pointer; } .runtime-tab.is-active { color:#fff; border-color:#3156d3; background:#3156d3; } .timeline { display:grid; gap:8px; } .timeline-item { display:grid; grid-template-columns:160px 1fr auto; gap:10px; align-items:center; padding:9px; border-left:4px solid #3156d3; border-radius:7px; background:#f8faff; } .timeline-item.is-failure { border-left-color:#b42318; background:#fff6f5; } .timeline-item .timeline-name { overflow-wrap:anywhere; font-weight:700; } @media (max-width:800px) { .timeline-item { grid-template-columns:1fr; gap:3px; } }
.report-status { display:flex; flex-wrap:wrap; gap:8px; margin:0 0 14px; } .status { border-radius:7px; padding:6px 9px; font-size:12px; font-weight:700; } .status-ok { background:#eaf8f1; color:#087443; } .status-limited { background:#fff4e5; color:#a65100; } .triage-items { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; } .triage-item { border-left:4px solid var(--blue); border-radius:8px; background:#f8faff; padding:10px; } .triage-item.is-danger { border-left-color:var(--red); background:#fff6f5; } .triage-item b,.triage-item span { display:block; } .triage-item span { color:var(--muted); font-size:12px; } .filter-bar { display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin:12px 0; } .filter-bar label { display:flex; align-items:center; gap:6px; } .filter-bar button { border:1px solid var(--line); border-radius:7px; background:#fff; color:var(--ink); padding:6px 10px; font:inherit; cursor:pointer; } .timeline-summary { display:grid; grid-template-columns:repeat(auto-fit,minmax(110px,1fr)); gap:7px; margin:0 0 10px; } .timeline-bucket { border-radius:7px; background:#f1f4fa; padding:7px; color:var(--muted); font-size:12px; } .timeline-bucket b { color:var(--ink); display:block; font-size:15px; } @media (max-width:800px) { .triage-items { grid-template-columns:1fr; } }
</style></head><body><main>
<header><h1>APM runtime overview</h1><p class="subtle">Bounded aggregates plus a minimal recorded-transaction projection. No event source, IDs, headers, bodies, traces, or error messages are included.</p><div id="context" class="context"></div><div id="report-status" class="report-status"></div></header>
<section id="cards" class="cards" aria-label="Runtime summary"></section>
<section id="triage" class="panel triage"><div class="panel-head"><div><h2>Investigation priorities</h2><p class="subtle">Impact combines observed volume, error rate, and tail latency; it is a triage aid, not an SLO verdict.</p></div></div><div id="triage-items" class="triage-items"></div></section>
<section class="panel filter-bar"><label>Focus service <select id="global-service-filter" aria-label="Focus every report view by service"></select></label><label>Workload <select id="global-workload-filter" aria-label="Filter every report view by workload"></select></label><label><input id="failures-only-filter" type="checkbox"> Failures only</label><button id="clear-filters" type="button">Clear filters</button></section>
<nav class="runtime-tabs" aria-label="Runtime report views"><button id="overview-tab" class="runtime-tab is-active" type="button">Overview</button><button id="details-tab" class="runtime-tab" type="button">Details</button><button id="timeline-tab" class="runtime-tab" type="button">Timeline</button></nav>
<section class="panel"><div class="panel-head"><h2>Service hotspots</h2><span class="subtle">Ranked by P95, then error rate and volume</span></div><div id="services"></div></section>
<section class="panel"><div class="panel-head"><div><h2>Runtime service map</h2><p class="subtle">Directed edges are observed dependencies. Select a service to inspect aggregate workload details; this does not assert a transaction-to-dependency call.</p></div><div><label>View <select id="map-mode" aria-label="Filter service map"></select></label> <label>Service <select id="map-service-filter" aria-label="Focus service map"></select></label> <label>Workload <select id="map-workload-filter" aria-label="Filter workload details"></select></label></div></div><div id="service-map" class="service-map"></div><div id="service-map-details" class="service-map-details"></div></section>
<section id="timeline-panel" class="panel" hidden><div class="panel-head"><div><h2>Recorded transaction timeline</h2><p class="subtle">Bounded field projection from recorded transaction events; no event source, IDs, headers, bodies, traces, or error messages.</p></div><label>Service <select id="timeline-service-filter" aria-label="Filter timeline by service"></select></label></div><div id="timeline-events" class="timeline"></div><p id="timeline-coverage" class="note"></p></section>
<section id="details-transactions" class="panel detail-panel" hidden><div class="panel-head"><div><h2>Transaction graph</h2><p class="subtle">Transactions remain owned by their service; no transaction-to-dependency call is asserted.</p></div><div><label>Service <select id="transaction-service-filter" aria-label="Filter transaction graph by service"></select></label> <label>Type <select id="transaction-kind-filter" aria-label="Filter transaction graph by type"></select></label></div></div><div id="transaction-graph" class="transaction-graph"></div></section>
<section id="details-flows" class="panel detail-panel" hidden><div class="panel-head"><h2>Focused dependency flow</h2><label>Service <select id="service-filter" aria-label="Filter dependency flow by service"></select></label></div><div id="flows" class="flow"></div></section>
<section class="panel"><div class="panel-head"><h2>Slow transactions</h2><span class="subtle">P95 is an approximate percentile from APM histogram metrics</span></div><div id="transactions"></div></section>
<section class="panel"><div class="panel-head"><h2>Dependencies</h2><span class="subtle">Average latency only; dependency P95 is a separate future pass</span></div><div id="dependencies"></div></section>
<section class="panel"><div class="panel-head"><h2>Recurring failures</h2><span class="subtle">Aggregated failure counts only</span></div><div id="failures"></div></section>
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
