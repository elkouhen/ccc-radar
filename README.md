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

For iterative AI enrichment, validate and replace facts in the separate local
enrichment layer:

```bash
systemlens import-facts architecture.ai-graph.pass-001.json \
  --namespace ai-architecture
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
