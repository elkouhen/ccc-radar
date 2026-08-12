# Product requirements — systemlens

## Purpose

`systemlens` gives developers and coding agents a local, queryable view of a
Java/Spring architecture. It derives facts directly from source ASTs, without
starting an external rule engine or sending source code to a service.

The product answers questions such as:

- Which services expose or call an HTTP API?
- Which Kafka topics are produced and consumed, and with which payload type?
- What are the dependencies and likely impact paths between services?
- Which Maven/Gradle modules, OpenAPI contracts, MongoDB collections and
  Spring properties belong to a service?

## Users and primary workflows

| User | Need | Surface |
|---|---|---|
| Coding agent | Obtain bounded, evidenced architecture context before an edit | MCP tools |
| Developer | Inspect one service, API, topic or module | CLI catalog commands |
| Architect | Assess topology, dependencies and risks across services | `analyze`, graph export |

The normal workflow is `systemlens init`, `systemlens index`, then one of `microservices`,
`topics`, `apis`, `modules`, `analyze`, or the equivalent MCP tool. Indexing is
incremental; `--full` refreshes every eligible source file.

## Scope

Delivered:

- Tree-sitter Java AST extraction for Spring MVC/WebFlux, Feign,
  RestTemplate/WebClient, Spring Cloud Gateway and Spring Data REST endpoints.
- Kafka producers and consumers, dynamic-topic evidence and explicit Java
  payload types.
- Maven/Gradle module discovery, OpenAPI and MongoDB inventory.
- Local SQLite persistence, architecture relations, graph/audit views and
  workspace federation.
- Markdown/JSON Kafka manifests and the opt-in Strategy1 conventions.

Not delivered:

- Security or quality scans, severity filtering or automated remediation.
- Runtime tracing, bytecode analysis, cross-repository source analysis, or a
  hosted service.
- Guaranteed resolution of dynamic values; unresolved values remain explicitly
  marked as dynamic rather than guessed.

## Product requirements

1. Source facts must be derived from local AST parsing and deterministic local
   configuration only.
2. Each fact must carry enough evidence to navigate to its file and line range.
3. A changed or deleted source file must update or remove its facts on the next
   index run.
4. The tool must continue to operate without network access once its local
   architecture inventory is available.
5. Graph, catalog and audit output must make uncertainty visible rather than
   inventing a dependency.

## Success measures

- A developer can produce a usable REST/Kafka topology after one local index.
- Incremental indexing touches only changed files unless an extractor signature
  or selected convention changes.
- Every emitted integration is traceable to a concrete source location or an
  explicitly named manifest entry.

For observable command and MCP contracts, see
[SPEC-FONC.md](./SPEC-FONC.md). For implementation details, see
[SPEC-TECH.md](./SPEC-TECH.md). Historical decisions, including the retired
external-analyzer design, remain in [ADR.md](./ADR.md).
