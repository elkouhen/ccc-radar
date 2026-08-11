# ccc-radar (`cccr`)

Local Java/Spring architecture discovery from source ASTs.

`cccr` indexes REST and Kafka integrations, Maven/Gradle modules, OpenAPI
contracts, MongoDB collections and derived architecture relations in a local
SQLite database. It does not invoke an external code-analysis engine.

## Install

```bash
uv tool install ccc-radar
```

## Quick start

Run these commands at the root of the Java/Spring repository. The HTML export
is the recommended first result: it provides an interactive view of the
services, APIs, Kafka topics and DTOs.

```bash
cccr init
cccr doctor
cccr index
cccr export microservices --html architecture.html
```

Open `architecture.html` in a browser. From there, start with a question such
as “who produces this Kafka topic?” or “what depends on this service?”. Use
`cccr microservices`, `cccr topics`, `cccr apis`, and `cccr analyze audit` for
terminal-oriented exploration.

Indexing is incremental. Use `cccr index --full` after a broad change, and
`cccr index --topic-strategy strategy1` only for repositories that follow the
documented Strategy1 Kafka and REST conventions.

## What is extracted

- Spring MVC/WebFlux routes and Spring Data REST exposure.
- Feign, RestTemplate, WebClient and gateway HTTP calls.
- Spring Kafka and Spring Cloud Stream producers/consumers, including explicit
  payload types when available.
- Maven/Gradle modules and dependencies, OpenAPI contracts, MongoDB usage and
  Spring properties.
- Optional Kafka facts from Markdown and JSON manifests.

Dynamic paths and topic values are retained as dynamic facts; the tool does not
guess a concrete dependency.

## MCP

Start the stdio server from an initialized repository:

```bash
cccr mcp
```

The main tools are `list_endpoints`, `architecture_catalog`, `graph`,
`dependency_graph`, `audit_dependency_graph`, `trace_message_flow`,
`list_modules`, and `list_workspace_services`.

## Documentation

- [Functional specification](docs/SPEC-FONC.md) — CLI and MCP contracts.
- [Technical specification](docs/SPEC-TECH.md) — extraction and storage design.
- [REST detection coverage](docs/REST_DETECTION.md).
- [Architecture map](docs/ARCHITECTURE.md).
- [ADRs](docs/ADR.md) — includes historical decisions.
