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
