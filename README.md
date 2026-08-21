# SystemLens (`systemlens`)

Local Java/Spring architecture context for coding agents.

`systemlens` gives an agent source-evidenced dependencies, impact paths and
unresolved facts before it changes a Java/Spring system. It indexes REST and
Kafka integrations, Maven/Gradle modules, OpenAPI contracts, MongoDB
collections and derived architecture relations in a local SQLite database. It
does not invoke an external code-analysis engine or send source code to a
service.

## Install

```bash
uv tool install systemlens
```

## Quick start

Run these commands at the root of the Java/Spring repository before connecting
a coding agent through MCP.

```bash
systemlens init
systemlens doctor
systemlens index
```

Start `systemlens mcp` and let the agent query the catalog, graph, coverage and
trace tools before an edit. For human review, generate an interactive export:

```bash
systemlens export microservices --html architecture.html
```

Use `systemlens microservices`, `systemlens topics`, `systemlens apis`, and
`systemlens analyze audit` for terminal-oriented exploration.

Indexing is incremental. Use `systemlens index --full` after a broad change, and
`systemlens index --topic-strategy strategy1` only for repositories that follow the
documented Strategy1 Kafka and REST conventions.

## What is extracted

- Spring MVC/WebFlux routes and Spring Data REST exposure.
- Feign, RestTemplate, WebClient and gateway HTTP calls.
- Spring Kafka and Spring Cloud Stream producers/consumers, including explicit
  payload types when available.
- Maven/Gradle modules and dependencies, OpenAPI contracts, MongoDB usage and
  Spring properties.
- Optional Kafka facts from Markdown and JSON manifests.
- Optional Deployment and StatefulSet CPU/RAM dimensions from the active local
  Kubernetes context with `systemlens index --kubernetes`.

Dynamic paths and topic values are retained as dynamic facts; the tool does not
guess a concrete dependency.

## Elastic APM digest for external AI agents (for example, Pi)

When read-only Elasticsearch access is available, SystemLens can export a
small runtime-behaviour digest for an external agent such as Pi, a
CLI-based coding-agent tool that can be pointed at a local file. It reads aggregated Elastic
APM `service_destination` metrics, never raw spans, logs, request headers, or
source code. It does not modify the local index.

Set credentials in your shell rather than putting them in a command history:

```bash
export SYSTEMLENS_ELASTICSEARCH_URL=https://elastic.example
# Raw Elasticsearch id:secret API keys are accepted and encoded by SystemLens.
export SYSTEMLENS_ELASTICSEARCH_API_KEY=...
systemlens apm doctor --json
systemlens apm export --since 1h --environment production --out apm-digest.json
pi -p @apm-digest.json "Analyse the service dependencies, error rates and latency hotspots."

# Self-contained runtime overview for human investigation
systemlens apm report --since 1h --environment production --html apm-runtime.html

# Lightweight HTML plus a JSON sidecar, useful with --all-spans
systemlens apm report --since 1h --all-spans --html apm-runtime.html --data apm-runtime.json
```

The export defaults to 80 relations and 50 KB. Its `coverage` object states
when either the Elasticsearch aggregation or the output budget truncated the
result, so Pi can distinguish absence from incomplete coverage.

`apm report` creates an explicit HTML file for human review. Its interactive
graph uses the same Graphology/Sigma.js CDN assets as the architecture export;
the embedded SVG fallback remains available if those assets cannot load.
It starts with action-oriented anomaly signals based on observed error rates,
latency, outcome coverage, and failed trace exemplars, then ranks services and transactions by P95 latency
(with their averages, volume, and aggregate failure rate), includes a directed
service map and lists recurring aggregate failures. Its modes are separated into
Services, Transaction workloads, Spans, Traces, and Dependencies tabs; each ranking and its
filters live in the matching operational view.
Without `--data`, the report is self-contained. With `--data`, the HTML stays
small and loads the adjacent JSON file; serve their directory over HTTP (for
example in WSL: `python3 -m http.server 8000`) then open
`http://localhost:8000/apm-runtime.html` from the Windows browser.
The report also
records its snapshot window and whether any view was truncated, so it can be
shared without mistaking incomplete rankings for absence. The Transactions tab
includes all observed aggregate transaction workloads, including local ones;
it also shows recent distributed-trace waterfalls, grouped by their HTTP source
service or a generic messaging source, with nested service and database spans.
Each card shows the cross-service route and safe workload labels before the
detailed waterfall, and can be filtered by origin. Traces also groups embedded
failures into signatures and a service-by-step matrix; its duration explanation
is an investigation aid, not a calculated critical path. IDs and arbitrary
operation values are used only in memory to form the tree and are never embedded.
The Spans tab shows a bounded projection of recorded spans. It
never embeds `_source`, trace IDs, request data, headers, bodies, error
messages, results, messaging targets, or arbitrary operation values. Dependency P95 is
deliberately not estimated, and the one-shot report has no historical baseline.
Span records are limited to 500 by default (2,000 maximum). HTTP
spans show their method and route; Kafka spans show their topic. Each
distributed trace waterfall is limited to 100 spans to respect Elasticsearch's
default `index.max_inner_result_window`. If a bounded Elasticsearch query
times out (including a server-side `timed_out` response), report generation
fails without writing a partial file. The Traces tab lists the bounded
distributed-trace waterfalls: selecting a Span entry filters to its matching
trace, and a span selected in a trace can open its matching Spans entry while
its trace detail remains visible.

### Inspect the source aggregation

To validate the input data independently, query the same read-only aggregate
that `systemlens apm export` consumes. Keep the API key in the environment; do
not paste it into the command or a report. Supply a trusted CA with `--cacert`.
For a disposable local POC with a self-signed ingress certificate only, replace
that option with `--insecure`.

```bash
curl --fail --silent --show-error \
  --cacert /path/to/elasticsearch-ca.pem \
  --header "Authorization: ApiKey ${SYSTEMLENS_ELASTICSEARCH_API_KEY:?set the API key}" \
  --header 'Content-Type: application/json' \
  --request POST \
  --data '{
    "size": 0,
    "track_total_hits": false,
    "query": {
      "bool": {
        "filter": [
          {"term": {"metricset.name": "service_destination"}},
          {"range": {"@timestamp": {"gte": "now-1h", "lt": "now"}}}
        ]
      }
    },
    "aggs": {
      "relations": {
        "composite": {
          "size": 1000,
          "sources": [
            {"source": {"terms": {"field": "service.name"}}},
            {"target": {"terms": {"field": "service.target.name"}}},
            {"target_type": {"terms": {"field": "service.target.type", "missing_bucket": true}}},
            {"outcome": {"terms": {"field": "event.outcome", "missing_bucket": true}}}
          ]
        },
        "aggs": {
          "calls": {"sum": {"field": "span.destination.service.response_time.count"}},
          "duration_us": {"sum": {"field": "span.destination.service.response_time.sum.us"}}
        }
      }
    }
  }' \
  "${SYSTEMLENS_ELASTICSEARCH_URL}/metrics-apm.service_destination.1m-*/_search"
```

The response contains aggregate source/destination buckets only. The CLI reads
both Elastic APM and compatible OpenTelemetry runtime data streams; the curl
example remains the Elastic APM-only projection. A zero bucket
count means no observed outgoing destination metrics in the selected window;
it does not prove that a static dependency is absent.

For Kibana **Dev Tools → Console**, paste and execute the standalone
[service-destination request](docs/apm-service-destination-query.http). Adjust
the `@timestamp` range or add a `service.environment` term filter as needed.

For Elastic Stack Monitoring, an importable Kibana dashboard for a primary
Elasticsearch cluster is available in
[`docs/kibana-primary-cluster-dashboard.ndjson`](docs/kibana-primary-cluster-dashboard.ndjson).
See [its import guide](docs/KIBANA-PRIMARY-CLUSTER-DASHBOARD.md).

## MCP

Start the stdio server from an initialized repository:

```bash
systemlens mcp
```

For example, register it in an MCP client configuration as:

```json
{
  "mcpServers": {
    "systemlens": {
      "command": "systemlens",
      "args": ["mcp"]
    }
  }
}
```

The main tools are `list_endpoints`, `architecture_catalog`, `graph`,
`dependency_graph`, `audit_dependency_graph`, `architecture_audit`,
`architecture_coverage`, `trace_message_flow`, `list_modules`,
`list_workspace_services`, `list_request_reply_patterns`, and
`reindex_architecture`.

## Documentation

- [Functional specification](docs/SPEC-FONC.md) — CLI and MCP contracts.
- [Technical specification](docs/SPEC-TECH.md) — extraction and storage design.
- [REST detection coverage](docs/REST_DETECTION.md).
- [Architecture map](docs/ARCHITECTURE.md).
- [ADRs](docs/ADR.md) — includes historical decisions.
