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

Start `systemlens mcp` and let the agent index the repository, enrich the graph
with repository-specific facts, then read the merged graph. For human review,
generate an interactive export:

```bash
systemlens export microservices --html architecture.html
```

Use `systemlens microservices`, `systemlens topics`, `systemlens apis`, and
`systemlens analyze audit` for terminal-oriented exploration.

To use the static architecture export as a local Python web application, start:

```bash
systemlens web
```

Open `http://127.0.0.1:8765/`. The Architecture page renders the current
persisted index snapshot on each request. Use `--host` and `--port` to adjust
the local server. The default loopback address avoids exposing indexed
architecture details on the network.

When an HTML export loads adjacent JSON data, serve its output directory with:

```bash
cd report-directory
simpleweb
```

It serves the current directory at `http://127.0.0.1:8000/`; use `--port` or
`--host` to change the local address.

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
```

`SYSTEMLENS_ELASTICSEARCH_URL` and `SYSTEMLENS_ELASTICSEARCH_API_KEY` take
precedence. For compatibility with Elastic tooling, SystemLens also accepts
`ELASTICSEARCH_URL` and `ELASTICSEARCH_API_KEY`; this lets a shell session that
has sourced an Elastic credentials loader run SystemLens without copying those
values.

The export defaults to 80 relations and 50 KB. Its `coverage` object states
when either the Elasticsearch aggregation or the output budget truncated the
result, so Pi can distinguish absence from incomplete coverage.

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

The MCP control surface is `index_repository`, `graph_fact_exists`, `add_graph_fact`,
`remove_graph_fact`, `list_graph_facts`, and `architecture_graph`. Indexing
creates source-derived facts; graph facts add an explicit, persistent
enrichment layer without modifying or deleting source evidence.
Use `data_schema` and `message_channel` node kinds with a `technology` field
to enrich graphs with SQL, Redis, RabbitMQ, SQS or other middleware facts.

## Documentation

- [Functional specification](docs/SPEC-FONC.md) — CLI and MCP contracts.
- [Technical specification](docs/SPEC-TECH.md) — extraction and storage design.
- [REST detection coverage](docs/REST_DETECTION.md).
- [Architecture map](docs/ARCHITECTURE.md).
- [ADRs](docs/ADR.md) — includes historical decisions.
