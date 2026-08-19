"""Bounded aggregate Elastic APM runtime report and self-contained HTML view."""

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


def build_runtime_report(
    client: ElasticApmClient,
    *,
    since: str,
    environment: str | None,
    max_services: int = DEFAULT_MAX_SERVICES,
    max_transactions: int = DEFAULT_MAX_TRANSACTIONS,
    max_dependencies: int = DEFAULT_MAX_DEPENDENCIES,
    max_buckets: int = DEFAULT_MAX_BUCKETS_PER_VIEW,
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
        "schema_version": "apm-runtime-report-v1",
        "source": {
            "kind": "elastic_apm_metric_aggregates",
            "raw_events_exported": False,
            "service_metricset": "service_transaction",
            "transaction_metricset": "transaction",
            "dependency_metricset": "service_destination",
            "dependency_target_field": target_field,
        },
        "window": {"from": _iso8601(start), "to": _iso8601(end)},
        "environment": environment,
        "services": services[:max_services],
        "transactions": transactions[:max_transactions],
        "dependencies": dependencies[:max_dependencies],
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
        },
    }


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
    """Render a self-contained, aggregate-only runtime investigation report."""
    data = json.dumps(report, ensure_ascii=False, separators=(",", ":")).replace(
        "</", "<\\/"
    )
    title = escape("SystemLens · APM runtime overview")
    return _RUNTIME_REPORT_HTML.replace("__TITLE__", title).replace(
        "__RUNTIME_DATA__", data
    )


_RUNTIME_REPORT_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title><style>
:root { color-scheme: light; --ink:#182033; --muted:#5f6b82; --line:#dde3ee; --panel:#fff; --canvas:#f5f7fb; --blue:#3156d3; --amber:#a65100; --red:#b42318; --green:#087443; }
* { box-sizing:border-box; } body { margin:0; background:var(--canvas); color:var(--ink); font:14px/1.45 Inter,ui-sans-serif,system-ui,sans-serif; }
main { max-width:1440px; margin:auto; padding:24px; } h1,h2 { margin:0; } h1 { font-size:25px; } h2 { font-size:16px; } .subtle { color:var(--muted); margin:4px 0 0; } .context { display:flex; flex-wrap:wrap; gap:8px; margin:18px 0; } .pill { background:#eaf0ff; color:#243e99; border-radius:999px; padding:5px 9px; font-size:12px; } .cards { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-bottom:12px; } .card,.panel { background:var(--panel); border:1px solid var(--line); border-radius:12px; box-shadow:0 1px 2px #1820330b; } .card { padding:14px; } .card b { display:block; font-size:24px; margin-top:4px; } .panel { padding:16px; margin-top:12px; overflow:auto; } .panel-head { display:flex; justify-content:space-between; align-items:baseline; gap:12px; margin-bottom:10px; } select { border:1px solid var(--line); border-radius:7px; background:#fff; padding:6px; max-width:260px; } table { width:100%; border-collapse:collapse; white-space:nowrap; } th { color:var(--muted); font-weight:600; text-align:left; font-size:12px; } td,th { padding:9px 8px; border-bottom:1px solid #edf0f5; } tr:last-child td { border:0; } .metric { font-variant-numeric:tabular-nums; text-align:right; } .danger { color:var(--red); font-weight:650; } .warm { color:var(--amber); font-weight:650; } .flow { display:grid; gap:7px; min-width:660px; } .flow-row { display:grid; grid-template-columns:1fr 26px 1fr 150px; align-items:center; gap:8px; } .node { overflow:hidden; text-overflow:ellipsis; padding:7px 9px; border-radius:7px; background:#f1f4fa; } .arrow { color:var(--blue); font-size:18px; text-align:center; } .edge { height:8px; min-width:14px; border-radius:99px; background:var(--blue); opacity:.25; } .edge-wrap { display:flex; align-items:center; gap:7px; } .transaction-graph { display:grid; gap:14px; min-width:660px; } .transaction-service { display:grid; grid-template-columns:minmax(150px,.3fr) 30px 1fr; align-items:stretch; gap:8px; } .transaction-service-node { display:flex; align-items:center; justify-content:center; padding:12px; border:1px solid #cbd5e1; border-radius:10px; background:#eef3ff; color:#243e99; font-weight:750; overflow-wrap:anywhere; } .transaction-service-edge { display:flex; align-items:center; justify-content:center; color:var(--blue); font-size:22px; } .transaction-nodes { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:8px; } .transaction-node { min-height:86px; padding:10px; border:1px solid #dbe3ef; border-left:5px solid #5b74db; border-radius:9px; background:#f8faff; } .transaction-node.is-warm { border-left-color:#d48a22; background:#fffaf1; } .transaction-node.is-hot { border-left-color:#c13b31; background:#fff6f5; } .transaction-name { overflow-wrap:anywhere; font-weight:750; } .transaction-metrics { margin-top:7px; color:var(--muted); font-size:12px; } .note { color:var(--muted); font-size:12px; margin:12px 0 0; } .empty { color:var(--muted); padding:8px 0; } @media (max-width:800px) { main { padding:14px; } .cards { grid-template-columns:repeat(2,minmax(0,1fr)); } .panel { padding:12px; } } @media (max-width:460px) { .cards { grid-template-columns:1fr; } }
.transaction-kinds { display:grid; gap:10px; } .transaction-kind { border:1px solid #e2e8f0; border-radius:10px; padding:9px; background:#fbfcff; } .transaction-kind h3 { margin:0 0 8px; color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; } .transaction-kind.kind-http { border-color:#bfdbfe; background:#f6faff; } .transaction-kind.kind-messaging { border-color:#bbf7d0; background:#f6fff9; } .kind-pill { display:inline-block; margin-left:6px; padding:1px 5px; border-radius:99px; background:#eaf0ff; color:#243e99; font-size:11px; font-weight:650; }
</style></head><body><main>
<header><h1>APM runtime overview</h1><p class="subtle">Aggregate-only workload analysis. No raw events, traces, or error messages are included.</p><div id="context" class="context"></div></header>
<section id="cards" class="cards" aria-label="Runtime summary"></section>
<section class="panel"><div class="panel-head"><h2>Service hotspots</h2><span class="subtle">Ranked by P95, then error rate and volume</span></div><div id="services"></div></section>
<section class="panel"><div class="panel-head"><div><h2>Transaction graph</h2><p class="subtle">HTTP and messaging lanes show service ownership only; they do not assert a transaction-to-dependency call.</p></div><div><label>Service <select id="transaction-service-filter" aria-label="Filter transaction graph by service"></select></label> <label>Type <select id="transaction-kind-filter" aria-label="Filter transaction graph by type"></select></label></div></div><div id="transaction-graph" class="transaction-graph"></div></section>
<section class="panel"><div class="panel-head"><h2>Focused dependency flow</h2><label>Service <select id="service-filter" aria-label="Filter dependency flow by service"></select></label></div><div id="flows" class="flow"></div></section>
<section class="panel"><div class="panel-head"><h2>Slow transactions</h2><span class="subtle">P95 is an approximate percentile from APM histogram metrics</span></div><div id="transactions"></div></section>
<section class="panel"><div class="panel-head"><h2>Dependencies</h2><span class="subtle">Average latency only; dependency P95 is a separate future pass</span></div><div id="dependencies"></div></section>
<section class="panel"><div class="panel-head"><h2>Recurring failures</h2><span class="subtle">Aggregated failure counts only</span></div><div id="failures"></div></section>
<p id="coverage" class="note"></p></main><script id="runtime-data" type="application/json">__RUNTIME_DATA__</script><script>
const data=JSON.parse(document.getElementById('runtime-data').textContent); const $=id=>document.getElementById(id); const n=v=>typeof v==='number'?v:null; const ms=v=>n(v)===null?'—':`${v.toLocaleString(undefined,{maximumFractionDigits:3})} ms`; const pct=v=>n(v)===null?'—':`${(v*100).toFixed(1)}%`; const count=v=>n(v)===null?'—':v.toLocaleString(); const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const rows=(items,columns)=>items.length?`<table><thead><tr>${columns.map(c=>`<th class="${c.metric?'metric':''}">${c.label}</th>`).join('')}</tr></thead><tbody>${items.map(item=>`<tr>${columns.map(c=>`<td class="${c.metric?'metric':''} ${c.className?c.className(item):''}">${c.html?c.html(item):esc(item[c.key])}</td>`).join('')}</tr>`).join('')}</tbody></table>`:'<p class="empty">No aggregate metric was observed in this window.</p>';
const failures=[...data.services.map(x=>({...x,label:x.service,kind:'Service'})),...data.transactions.map(x=>({...x,label:`${x.service} / ${x.transaction}`,kind:'Transaction'}))].filter(x=>x.failure_calls>0).sort((a,b)=>b.failure_calls-a.failure_calls||b.error_rate-a.error_rate).slice(0,20);
$('context').innerHTML=[`Window: ${esc(data.window.from)} → ${esc(data.window.to)}`,data.environment?`Environment: ${esc(data.environment)}`:'All environments'].map(x=>`<span class="pill">${x}</span>`).join('');
const p95s=data.services.map(x=>x.p95_ms).filter(v=>n(v)!==null); const hot=p95s.length?Math.max(...p95s):null; const failureCount=failures.reduce((sum,x)=>sum+x.failure_calls,0); $('cards').innerHTML=[['Services',count(data.services.length)],['Transactions',count(data.transactions.length)],['Dependencies',count(data.dependencies.length)],['Aggregated failures',count(failureCount)]].map(([label,value])=>`<article class="card"><span class="subtle">${label}</span><b>${value}</b></article>`).join('');
$('services').innerHTML=rows(data.services,[{label:'Service',key:'service'},{label:'Calls',key:'calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms===hot?'danger':'warm',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
const transactionSelect=$('transaction-service-filter'); const transactionServices=[...new Set(data.transactions.map(x=>x.service))].sort(); transactionSelect.innerHTML=`<option value="">All observed services</option>${transactionServices.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')}`; function transactionGraph(){const selected=transactionSelect.value; const items=data.transactions.filter(x=>!selected||x.service===selected); const byService=new Map(); items.forEach(item=>{const group=byService.get(item.service)||[]; group.push(item); byService.set(item.service,group);}); const maxP95=Math.max(1,...items.map(x=>n(x.p95_ms)||0)); $('transaction-graph').innerHTML=items.length?[...byService.entries()].sort(([left],[right])=>left.localeCompare(right)).map(([service,transactions])=>`<div class="transaction-service"><div class="transaction-service-node">${esc(service)}</div><div class="transaction-service-edge">→</div><div class="transaction-nodes">${transactions.map(transaction=>{const ratio=(n(transaction.p95_ms)||0)/maxP95; const heat=ratio>=.75?'is-hot':ratio>=.4?'is-warm':''; return `<article class="transaction-node ${heat}"><div class="transaction-name">${esc(transaction.transaction)}</div><div class="transaction-metrics">P95 ${ms(transaction.p95_ms)} · ${count(transaction.calls)} calls · ${pct(transaction.error_rate)} errors</div></article>`;}).join('')}</div></div>`).join(''):'<p class="empty">No transaction aggregate for this selection.</p>';} transactionSelect.addEventListener('change',transactionGraph); transactionGraph();
$('transactions').innerHTML=rows(data.transactions,[{label:'Service',key:'service'},{label:'Transaction',html:x=>`${esc(x.transaction)}${x.transaction_type?` <span class="subtle">${esc(x.transaction_type)}</span>`:''}`},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms?'warm':'',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
const transactionKind=x=>{const type=String(x.transaction_type||'').toLowerCase();return type==='messaging'?'Messaging':(type==='request'||type==='http'?'HTTP':'Other');}; const transactionKindSelect=$('transaction-kind-filter'); transactionKindSelect.innerHTML='<option value="">HTTP, messaging, and other</option><option value="HTTP">HTTP</option><option value="Messaging">Messaging</option><option value="Other">Other</option>'; function renderTransactionGraph(){const selected=transactionSelect.value;const selectedKind=transactionKindSelect.value;const items=data.transactions.filter(x=>(!selected||x.service===selected)&&(!selectedKind||transactionKind(x)===selectedKind));const byService=new Map();items.forEach(item=>{const group=byService.get(item.service)||[];group.push(item);byService.set(item.service,group);});const maxP95=Math.max(1,...items.map(x=>n(x.p95_ms)||0));$('transaction-graph').innerHTML=items.length?[...byService.entries()].sort(([left],[right])=>left.localeCompare(right)).map(([service,transactions])=>{const byKind=new Map();transactions.forEach(transaction=>{const kind=transactionKind(transaction);const group=byKind.get(kind)||[];group.push(transaction);byKind.set(kind,group);});return `<div class="transaction-service"><div class="transaction-service-node">${esc(service)}</div><div class="transaction-service-edge">→</div><div class="transaction-kinds">${['HTTP','Messaging','Other'].filter(kind=>byKind.has(kind)).map(kind=>`<section class="transaction-kind kind-${kind.toLowerCase()}"><h3>${kind}</h3><div class="transaction-nodes">${byKind.get(kind).map(transaction=>{const ratio=(n(transaction.p95_ms)||0)/maxP95;const heat=ratio>=.75?'is-hot':ratio>=.4?'is-warm':'';return `<article class="transaction-node ${heat}"><div class="transaction-name">${esc(transaction.transaction)}</div><div class="transaction-metrics">P95 ${ms(transaction.p95_ms)} · ${count(transaction.calls)} calls · ${pct(transaction.error_rate)} errors</div></article>`;}).join('')}</div></section>`).join('')}</div></div>`;}).join(''):'<p class="empty">No transaction aggregate for this selection.</p>';} transactionSelect.addEventListener('change',renderTransactionGraph);transactionKindSelect.addEventListener('change',renderTransactionGraph);renderTransactionGraph();
$('transactions').innerHTML=rows(data.transactions,[{label:'Service',key:'service'},{label:'Transaction',html:x=>`${esc(x.transaction)} <span class="kind-pill">${esc(transactionKind(x))}</span>${x.transaction_type?` <span class="subtle">${esc(x.transaction_type)}</span>`:''}`},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,html:x=>ms(x.average_ms)},{label:'P95',metric:true,className:x=>x.p95_ms?'warm':'',html:x=>ms(x.p95_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
$('dependencies').innerHTML=rows(data.dependencies,[{label:'Source',key:'source'},{label:'Target',html:x=>`${esc(x.target)}${x.target_type?` <span class="subtle">${esc(x.target_type)}</span>`:''}`},{label:'Calls',metric:true,html:x=>count(x.calls)},{label:'Average',metric:true,className:x=>x.average_ms?'warm':'',html:x=>ms(x.average_ms)},{label:'Errors',metric:true,className:x=>x.error_rate?'danger':'',html:x=>pct(x.error_rate)}]);
$('failures').innerHTML=rows(failures,[{label:'Kind',key:'kind'},{label:'Workload',key:'label'},{label:'Failures',metric:true,className:()=> 'danger',html:x=>count(x.failure_calls)},{label:'Error rate',metric:true,className:()=> 'danger',html:x=>pct(x.error_rate)},{label:'P95',metric:true,html:x=>ms(x.p95_ms)}]);
const select=$('service-filter'); const services=[...new Set(data.dependencies.flatMap(x=>[x.source,x.target]))].sort(); select.innerHTML=`<option value="">All observed flows</option>${services.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')}`; function flows(){const selected=select.value;const items=data.dependencies.filter(x=>!selected||x.source===selected||x.target===selected); const max=Math.max(1,...items.map(x=>x.calls)); $('flows').innerHTML=items.length?items.map(x=>`<div class="flow-row"><div class="node">${esc(x.source)}</div><div class="arrow">→</div><div class="node">${esc(x.target)}</div><div class="edge-wrap"><span class="edge" style="width:${Math.max(8,Math.round(100*x.calls/max))}px"></span><span class="subtle">${ms(x.average_ms)} · ${count(x.calls)}</span></div></div>`).join(''):'<p class="empty">No dependency flow for this selection.</p>';} select.addEventListener('change',flows); flows();
const cv=data.coverage; const view=(name,c)=>`${name}: ${c.items_exported}/${c.items_seen}${c.truncated?` (truncated: ${c.truncation_reasons.join(', ')})`:''}`; $('coverage').textContent=`Coverage — ${view('services',cv.services)} · ${view('transactions',cv.transactions)} · ${view('dependencies',cv.dependencies)}. A zero result means no matching aggregate was observed in the selected window.`;
</script></body></html>"""
