# Functional specification — systemlens (`systemlens`)

`systemlens` builds a local architecture inventory from Java/Spring source ASTs.
All commands operate on the current repository unless an explicit workspace
root is accepted. There is no external code-analysis process in this workflow.

## Configuration

`systemlens init` creates `.systemlens/config.yml`:

```yaml
include: ["**/*"]
exclude: [".git/**", ".venv/**", "node_modules/**", ".systemlens/**"]
min_severity: INFO
```

This is a breaking rename from `cccr`, `archlens`, and `codeatlas`: SystemLens
does not read an existing `.cccr/`, `.archlens/`, or `.codeatlas/` directory.
Run `systemlens init` and
`systemlens index` to create a new local inventory in `.systemlens/`.

`include` and `exclude` control source inventory. Maven/Gradle test source
sets (`src/test`, `src/componentTest`, and names ending in `Test`) are always
excluded. The `min_severity` setting remains accepted for database compatibility
but does not alter AST endpoint extraction.

## CLI

| Command | Behaviour |
|---|---|
| `systemlens init` | Creates `.systemlens/config.yml`; it never overwrites an existing file. |
| `systemlens doctor [--json]` | Read-only check of configuration, local AST readiness and index state. |
| `systemlens index [MANIFEST]... [--full] [--topic-strategy default\|strategy1] [--manifest FILE]...` | Incrementally extracts and persists architecture facts. Kafka manifests may be passed positionally or through repeatable `--manifest FILE`. |
| `systemlens microservices`, `topics`, `apis`, `dtos`, `mongodb`, `modules` | Browse the indexed catalog; `microservices`, `topics` and `mongodb` list the corresponding architecture objects directly, each with a `kind` and `name`, and support the documented list/show/neighbors actions and JSON output where applicable. |
| `systemlens analyze audit` | Reports static architecture risks. |
| `systemlens analyze coverage [--json]` | Reports inventory coverage and unresolved integrations. |
| `systemlens analyze indexing-issues [--json]` | Lists unresolved indexing facts. JSON includes source evidence suitable for reviewing proposed heuristics. |
| `systemlens analyze microservices impact NAME` | Lists direct and transitive impact paths. |
| `systemlens analyze microservices path FROM TO` | Lists bounded paths between services. |
| `systemlens analyze request-reply` | Lists Strategy1 Kafka request/reply candidates. |
| `systemlens export microservices (--html FILE \| --c4 DIRECTORY \| --json)` | Exports the microservice, Kafka-topic and MongoDB-collection topology. |
| `systemlens export modules --html FILE` | Exports the Maven/Gradle build-dependency view. |
| `systemlens export request-reply --html FILE` | Exports Strategy1 Kafka request/reply candidates. |
| `systemlens mcp` | Starts the stdio MCP server. |

`systemlens index` reports its file delta, AST analysis stage, persisted endpoint
count and materialized relations. It then prints a next-step hint towards the
interactive microservice HTML export. Its result line is:

```text
scanned=<N> skipped=<N> +integrations=<N> -integrations=<N>
```

The first AST-only run removes stale results from the retired analyzer.

`--topic-strategy strategy1` is opt-in. The selected strategy is persisted with
the index and reused by incremental MCP reindexing and all derived views.
`--disable` accepts `properties`,
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

A REST call forms an internal architecture relation only when its target
service is identified by an exact normalized explicit alias, such as an HTTP
host, an `lb://` service name, or a configured client domain. A matching HTTP
method and route only refines a resource within that already identified
service; it never identifies a service by itself. Calls without a unique target
remain indexed as unresolved evidence and are reported by coverage and indexing
issues rather than being linked to a coincidentally similar route.

The HTML microservice export provides an inspector for each statically typed
Kafka message. It shows the indexed payload-type identity, message topic, and
producer and consumer services. DTO fields, enum values, and recursive project
type navigation are not part of the current indexed contract.
Its initial view foregrounds task-oriented entry points (Kafka topic, service
dependencies, service-to-service path and Kafka messages). Relation/resource
filters, graph layouts, specialized reference views and build dependencies are
available as advanced controls. Changing a relation-type filter rebuilds and
relayouts the graph from only the selected dependency types; excluded relations
do not influence the resulting graph layout.
Microservices with no indexed inter-service relation remain visible in a
separate isolated area of the graph, so their absence of dependencies is not
confused with an absent service.
Microservices, Kafka topics, and MongoDB collections are marked by their
relative connectivity. In the HTML graph, the shape and neutral interior
identify the resource type, while the coloured outline alone communicates its
relative connectivity. The relation count includes their indexed HTTP, Kafka,
and MongoDB dependencies, while low/medium/high relative tiers are calculated separately
for each resource type. For a microservice, the count is its distinct direct
HTTP clients and targets, Kafka producer/consumer topic relations, and MongoDB
collection relations; multiple HTTP routes between the same client and target
are counted once. The HTML complexity badge exposes the HTTP, Kafka, and
MongoDB breakdown as a tooltip, along with the resource rank and its soft
tercile bounds. The lowest third is blue, the middle third orange, and the
highest third red; the terciles are recalculated separately for each resource
type in every export. The graph label of each coloured resource also displays
its connectivity score directly (for example, `orders.created · 4`).
The Explore search suggests indexed resource names and accepts either one
exact, unambiguous graph-node name or a Kafka itinerary written with `->`.
An itinerary starts and ends with a
microservice and follows only directed Kafka relations through Kafka topics;
it never traverses HTTP or MongoDB dependencies.
Invalid, ambiguous, repeated, or unreachable stops leave the current graph
unchanged and produce an actionable message. The itinerary detail is an
ordered, clickable list of node names and types. A Kafka topic lists its
associated DTO names in parentheses. Selecting any path stop reveals its
ordinary detail view, including the indexed Kafka source links where present.
Every graph resource detail starts with indexed and visible relation counts,
then a `Relations` section. For a microservice, that section separates
consumed and published API and Kafka resources, plus MongoDB collections. Each
Kafka topic lists its applicable DTOs. A collapsed `Sources` section lists the
indexed OpenAPI and Kafka files that provide the evidence, avoiding repetition
in every topic.
The topic detail lists resolved Kafka DTOs once. It lists message types only
when no matching DTO has been resolved, avoiding duplicate published and
consumed type lists when they describe the same contract.
Indexing issues that have a source endpoint expose a VS Code link to the
associated file and line. The `Resources` view groups distinct OpenAPI and
Kafka DTO sections; both support filtering their complete list (OpenAPI by
path or service, DTOs by simple name or package). Request/reply and build
dependencies remain available as complementary analyses from that view. A
persistent inventory status reports whether unresolved indexing facts exist and
opens their review view.

`--topic-strategy strategy1` adds opt-in convention extraction for selected
`getTopics()` accessors and `envoyerMessageKafka*(kafkaProperties.getTopics().getXxx(), payload)` calls
(including `envoyerMessageKafkaRequest` and `envoyerMessageKafkaReply`),
`${kafka.topics.*.name}` expressions and configured REST client constants. It
also enables the `getXxxServiceUrl()` REST target-name convention.
It may also derive a high-confidence request/reply pair
when both sides follow the `retour_<request-topic>` convention.

## Incrementality and freshness

The index stores SHA-256 values for eligible files. A normal run parses added or
changed files and purges facts for deleted files. A full refresh is forced when
the endpoint extractor signature, analysis configuration signature, selected
topic strategy, Spring configuration file, or Maven/Gradle build descriptor
changes. Spring properties and build descriptors can affect facts attributed to
otherwise unchanged Java source files. Explicit manifests are included even when
otherwise excluded.

The index is `.systemlens/findings.db` for compatibility with prior releases. It is a
local implementation detail, not a contract for direct SQL writes.

## MCP

The MCP server exposes the same indexed architecture. Its primary tools are:

| Tool | Purpose |
|---|---|
| `list_endpoints` | Filter raw REST/Kafka facts. |
| `architecture_catalog` | List, show and navigate services, modules, topics, DTOs, APIs and collections. |
| `architecture_audit` | Return the structured equivalent of `systemlens analyze audit`. |
| `architecture_coverage` | Return inventory coverage and unresolved integration counts. |
| `graph` | Return an inter-service topology. |
| `dependency_graph` | Return typed HTTP, Kafka, MongoDB and external API relations. |
| `audit_dependency_graph` | Return static topology risks. |
| `trace_message_flow` | Trace a topic or route through its source sites. |
| `list_modules` | Return the persisted module inventory. |
| `list_workspace_services` | Discover and load a multi-service Maven/Gradle workspace read-only. |
| `list_request_reply_patterns` | Return Strategy1 Kafka request/reply candidates. |
| `reindex_architecture` | Incrementally refresh AST facts after a source change. |

MCP tools require an existing index where they query persisted facts. Errors are
returned through the standard MCP tool-error path.

## Boundaries

The inventory is static. Reflection, arbitrary string construction, runtime
routing and undeclared external contracts can remain unresolved. Consumers must
use `topic_dynamic`, confidence and source evidence when interpreting the graph.
