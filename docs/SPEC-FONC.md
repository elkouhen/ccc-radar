# Functional specification — ccc-radar (`cccr`)

`cccr` builds a local architecture inventory from Java/Spring source ASTs.
All commands operate on the current repository unless an explicit workspace
root is accepted. There is no external code-analysis process in this workflow.

## Configuration

`cccr init` creates `.cccr/config.yml`:

```yaml
include: ["**/*"]
exclude: [".git/**", ".venv/**", "node_modules/**", ".cccr/**"]
min_severity: INFO
embedding_model: ~/models/jina-code-embeddings-1.5b
```

`include` and `exclude` control source inventory. Maven/Gradle test source
sets (`src/test`, `src/componentTest`, and names ending in `Test`) are always
excluded. The `min_severity` setting remains accepted for database compatibility
but does not alter AST endpoint extraction.

## CLI

| Command | Behaviour |
|---|---|
| `cccr init` | Creates `.cccr/config.yml`; it never overwrites an existing file. |
| `cccr doctor [--json]` | Read-only check of configuration, local AST readiness, embedding model and index state. |
| `cccr index [--full] [--topic-strategy default\|strategy1] [--manifest FILE]...` | Incrementally extracts and persists architecture facts. |
| `cccr microservices`, `topics`, `apis`, `dtos`, `mongodb`, `modules` | Browse the indexed catalog; each supports the documented list/show/neighbors actions and JSON output where applicable. |
| `cccr analyze audit` | Reports static architecture risks. |
| `cccr analyze microservices impact NAME` | Lists direct and transitive impact paths. |
| `cccr analyze microservices path FROM TO` | Lists bounded paths between services. |
| `cccr analyze request-reply` | Lists Strategy1 Kafka request/reply candidates. |
| `cccr export microservices` / `cccr export modules` | Emits JSON, HTML or LikeC4 views. |
| `cccr mcp` | Starts the stdio MCP server. |

`cccr index` reports its file delta, AST analysis stage, persisted endpoint
count and materialized relations. Its final line is:

```text
scanned=<N> skipped=<N> +integrations=<N> -integrations=<N>
```

The first AST-only run removes stale results from the retired analyzer.

`--topic-strategy strategy1` is opt-in. `--disable` accepts `properties`,
`module-architecture`, and `module-tree-sitter`.

## Extraction contract

An endpoint has a role (`serve`/`call` for REST, `produce`/`consume` for Kafka),
a system, a topic (`METHOD /path` for REST), source location, framework and
optional module, qualified name and Java message type. A value that cannot be
resolved statically is flagged `topic_dynamic=true`; it is never fabricated.

The Java AST extractor covers Spring MVC/WebFlux, Feign, RestTemplate,
WebClient, Spring Cloud Gateway, Spring Data REST, Spring Kafka and Spring
Cloud Stream. Markdown and JSON Kafka manifests are supported as explicit
sources and are labelled `source=manifest`.

The HTML microservice export provides an inspector for each statically typed
Kafka message. It shows the message topic, producer and consumer services, and
allows navigation through recursively referenced project DTO fields.

`--topic-strategy strategy1` adds opt-in convention extraction for selected
`getTopics()` accessors, `${kafka.topics.*.name}` expressions and configured
REST client constants. It may also derive a high-confidence request/reply pair
when both sides follow the `retour_<request-topic>` convention.

## Incrementality and freshness

The index stores SHA-256 values for eligible files. A normal run parses added or
changed files and purges facts for deleted files. A full refresh is forced when
the endpoint extractor signature, analysis configuration signature or selected
topic strategy changes. Explicit manifests are included even when otherwise
excluded.

The index is `.cccr/findings.db` for compatibility with prior releases. It is a
local implementation detail, not a contract for direct SQL writes.

## MCP

The MCP server exposes the same indexed architecture. Its primary tools are:

| Tool | Purpose |
|---|---|
| `list_endpoints` | Filter raw REST/Kafka facts. |
| `architecture_catalog` | List, show and navigate services, modules, topics, DTOs, APIs and collections. |
| `graph` | Return an inter-service topology. |
| `dependency_graph` | Return typed HTTP, Kafka, MongoDB and external API relations. |
| `audit_dependency_graph` | Return static topology risks. |
| `trace_message_flow` | Trace a topic or route through its source sites. |
| `list_modules` | Return the persisted module inventory. |
| `list_workspace_services` | Discover and load a multi-service Maven/Gradle workspace read-only. |
| `reindex_architecture` | Incrementally refresh AST facts after a source change. |

MCP tools require an existing index where they query persisted facts. Errors are
returned through the standard MCP tool-error path.

## Boundaries

The inventory is static. Reflection, arbitrary string construction, runtime
routing and undeclared external contracts can remain unresolved. Consumers must
use `topic_dynamic`, confidence and source evidence when interpreting the graph.
