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
```

`include` and `exclude` control source inventory. Maven/Gradle test source
sets (`src/test`, `src/componentTest`, and names ending in `Test`) are always
excluded. The `min_severity` setting remains accepted for database compatibility
but does not alter AST endpoint extraction.

## CLI

| Command | Behaviour |
|---|---|
| `cccr init` | Creates `.cccr/config.yml`; it never overwrites an existing file. |
| `cccr doctor [--json]` | Read-only check of configuration, local AST readiness and index state. |
| `cccr index [--full] [--topic-strategy default\|strategy1] [--manifest FILE]...` | Incrementally extracts and persists architecture facts. |
| `cccr microservices`, `topics`, `apis`, `dtos`, `mongodb`, `modules` | Browse the indexed catalog; each supports the documented list/show/neighbors actions and JSON output where applicable. |
| `cccr analyze audit` | Reports static architecture risks. |
| `cccr analyze indexing-issues [--json]` | Lists unresolved indexing facts. JSON includes source evidence suitable for reviewing proposed heuristics. |
| `cccr analyze microservices impact NAME` | Lists direct and transitive impact paths. |
| `cccr analyze microservices path FROM TO` | Lists bounded paths between services. |
| `cccr analyze request-reply` | Lists Strategy1 Kafka request/reply candidates. |
| `cccr export microservices` / `cccr export modules` | Emits JSON, HTML or LikeC4 views. |
| `cccr mcp` | Starts the stdio MCP server. |

`cccr index` reports its file delta, AST analysis stage, persisted endpoint
count and materialized relations. It then prints a next-step hint towards the
interactive microservice HTML export. Its result line is:

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
allows navigation through recursively referenced project DTO fields and enums.
Its initial view foregrounds task-oriented entry points (Kafka topic, service
dependencies, service-to-service path and Kafka messages). Relation/resource
filters, graph layouts, specialized reference views and build dependencies are
available as advanced controls.
The Explore search accepts either one exact, unambiguous graph-node name or an
itinerary written with `->`. An itinerary accepts every indexed node type and
highlights the shortest directed path through the supplied stops in order.
Invalid, ambiguous, repeated, or unreachable stops leave the current graph
unchanged and produce an actionable message. The itinerary detail is an
ordered, clickable list of node names and types. A Kafka topic lists its
associated DTO names in parentheses. Selecting any path stop reveals its
ordinary detail view, including the indexed Kafka source links where present.
The microservice detail starts with functional counts, then separates `API`
and `Kafka` into consumed and published resources. Each Kafka topic lists its
applicable DTOs. A collapsed `Sources` section lists the indexed OpenAPI and
Kafka files that provide the evidence, avoiding repetition in every topic.
The topic detail lists resolved Kafka DTOs once. It lists message types only
when no matching DTO has been resolved, avoiding duplicate published and
consumed type lists when they describe the same contract.
Indexing issues that have a source endpoint expose a VS Code link to the
associated file and line. The `Resources` view groups distinct OpenAPI and
Kafka DTO sections; both support filtering their complete list (OpenAPI by
path or service, DTOs by simple name or package). Request/reply and build
dependencies remain available as complementary analyses from that view.

`--topic-strategy strategy1` adds opt-in convention extraction for selected
`getTopics()` accessors, `envoyerMessageKafka(kafkaProperties.getTopics().getXxx(), payload)` calls,
`${kafka.topics.*.name}` expressions and configured REST client constants. It
may also derive a high-confidence request/reply pair
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
