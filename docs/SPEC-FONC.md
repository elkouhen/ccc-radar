# Functional specification — systemlens (`systemlens`)

`systemlens` builds a local architecture inventory from Java/Spring source ASTs.
The inventory commands operate on the current repository unless an explicit
workspace root is accepted. There is no external code-analysis process in this
workflow. The separate, opt-in `apm` command group reads configured Elastic APM
metric aggregates and neither indexes source nor writes the inventory.

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
| `systemlens apm doctor [--endpoint URL] [--api-key KEY] [--json]` | Read-only validation of Elasticsearch access to the APM metrics. It reports only whether the endpoint and key are configured and their source (`flag` or `env`); it never prints the URL or key. |
| `systemlens apm export [--since DURATION] [--environment NAME] [--endpoint URL] [--api-key KEY] [--max-relations N] [--max-bytes N] [--max-buckets N] [--out FILE]` | Reads `service_destination` aggregates from Elasticsearch and emits compact JSON to stdout, or writes it to `FILE`. It does not export raw spans, logs, headers, identifiers, or source content. |
| `systemlens apm report --html FILE [--since DURATION] [--environment NAME] [--endpoint URL] [--api-key KEY] [--max-services N] [--max-transactions N] [--max-dependencies N] [--max-buckets N]` | Reads bounded APM metric aggregates and writes a self-contained human investigation report. It never reads raw events, spans, logs, request data, trace identifiers, or unredacted error values. |
| `systemlens index [MANIFEST]... [--full] [--topic-strategy default\|strategy1] [--manifest FILE]... [--kubernetes] [--kubernetes-namespace NAME]` | Incrementally extracts and persists architecture facts. `--kubernetes` queries the active `kubectl` context for Deployments and StatefulSets; `--kubernetes-namespace` restricts it to one namespace. |
| `systemlens microservices`, `topics`, `apis`, `dtos`, `mongodb`, `modules` | Browse the indexed catalog; `microservices`, `topics` and `mongodb` list the corresponding architecture objects directly, each with a `kind` and `name`, and support the documented list/show/neighbors actions and JSON output where applicable. |
| `systemlens analyze audit` | Reports static architecture risks. |
| `systemlens analyze coverage [--json]` | Reports inventory coverage and unresolved integrations. |
| `systemlens analyze indexing-issues [--json]` | Lists unresolved indexing facts. JSON includes source evidence suitable for reviewing proposed heuristics. |
| `systemlens analyze microservices impact NAME` | Lists direct and transitive impact paths. |
| `systemlens analyze microservices path FROM TO` | Lists bounded paths between services. |
| `systemlens analyze request-reply` | Lists Strategy1 Kafka request/reply candidates. |
| `systemlens export microservices (--html FILE \| --c4 DIRECTORY \| --json) [--root-path DIRECTORY] [--apm-overlay --since DURATION --environment NAME --endpoint URL --api-key KEY --max-relations N --max-buckets N]` | Exports the microservice, Kafka-topic and MongoDB-collection topology. `--root-path` provides the local source root for HTML VS Code links. `--apm-overlay` (HTML only) additionally queries bounded Elastic APM aggregates and overlays them on the graph; see "Elastic APM microservice overlay". |
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
producer and consumer services. When the matching Java type is indexed, its
inspector also shows its source, declared fields, enum values, and conservative
recursive project-type navigation.
The export uses a responsive workspace layout with eight navigation tabs:
Explorer, Paths, OpenAPI, Kafka, Mongo, Request/reply, Build, and
Quality. It includes compact architecture counters, task-oriented starting
actions, a floating context panel, and a full-size resource inspector. On
narrow viewports the controls and context panel use separate bounded regions so
the graph remains visible while either panel scrolls independently.
Its initial view foregrounds task-oriented entry points (Kafka topic, service
dependencies, service-to-service path and Kafka messages). Relation/resource
filters and graph layouts are available as advanced controls. Dedicated
OpenAPI, Kafka, Mongo, Request/reply, and Build views keep their domain
inventories separate. Changing a relation-type filter rebuilds and relayouts
the graph from only the selected dependency types; excluded relations do not
influence the resulting graph layout.
Microservices with no indexed inter-service relation remain visible in a
separate isolated area of the graph, so their absence of dependencies is not
confused with an absent service.
Microservices, Kafka topics, and MongoDB collections are marked by their
relative connectivity. In the HTML graph, the shape identifies the resource
type. Microservices, Kafka topics, and MongoDB collections use a coloured
outline around a neutral interior; the fill never carries connectivity or risk
meaning. The relation count includes their indexed HTTP, Kafka,
and MongoDB dependencies, while low/medium/high relative tiers are calculated separately
for each resource type. For a microservice, the count is its distinct direct
HTTP clients and targets, Kafka producer/consumer topic relations, and MongoDB
collection relations; multiple HTTP routes between the same client and target
are counted once. The HTML complexity badge exposes the HTTP, Kafka, and
MongoDB breakdown as a tooltip, along with the resource rank and its soft
tercile bounds. The lowest third is blue, the middle third orange, and the
highest third red; the terciles are recalculated separately for each resource
type in every export. The graph label of each coloured resource also displays
its relative connectivity through its coloured outline; the exact details are
available after selecting the node.
The Explore search suggests indexed resource names and accepts either one
exact, unambiguous graph-node name or a Kafka itinerary written with `->`.
An itinerary starts and ends with a microservice and follows only directed Kafka
relations through Kafka topics; it never traverses HTTP or MongoDB dependencies.
When a microservice and another resource have the same display name, a direct
resource search remains ambiguous, while an itinerary endpoint resolves the
unique microservice candidate required by the itinerary grammar.
Invalid, ambiguous, repeated, or unreachable stops leave the current graph
unchanged and produce an actionable message. The itinerary detail is an
ordered, clickable list of node names and types. A Kafka topic lists its
associated DTO names in parentheses. Selecting any path stop reveals its
ordinary detail view, including the indexed Kafka source links where present.
Every graph resource detail starts with indexed and visible relation counts,
then a `Relations` section. For a microservice, that section separates
consumed and published API and Kafka resources, plus MongoDB collections. Each
microservice resolved to an indexed Maven or Gradle module also provides a
visible action that opens the module root directory in VS Code. Kafka topics
list their applicable DTOs. A collapsed `Sources` section lists the indexed
OpenAPI and Kafka files that provide the evidence, avoiding repetition in every
topic.
At constrained viewport sizes, an empty floating context panel remains visible
as guidance but does not intercept pointer interaction with the active tab;
once it contains selected-node or path details, its normal controls remain
interactive.
Indexing persists source evidence only as paths relative to the project root.
HTML export joins those paths to `--root-path` (the current directory by
default) when building VS Code links. No WSL distribution or absolute local
source path is stored in the index.
When several modules reference one OpenAPI/Swagger file, it is listed once for
the module that directly contains the source file.
MongoDB collection details list the services using the collection once, followed
by indexed Java persistence classes resolved from
`@Document`, Mongo repository entity generics, or unambiguous `Type.class`
arguments passed to `MongoTemplate`. They include their qualified name, source
location, and declared fields. The Persistence view provides the same inventory
with filtering by class, package, collection, or service and opens a dedicated
inspector. Fields whose type resolves uniquely to another indexed project class
are navigable recursively; the inspector provides a return action to the
containing class. External and ambiguous field types remain plain text.
Persistence classes declared in a dependent build module are attached to the
owning service collection through the indexed module-dependency graph. When
dependency metadata is unavailable, a workspace-wide class is used only if it
is the unique candidate for that collection name; ambiguous candidates remain
unassociated rather than being guessed.
The topic detail lists resolved Kafka DTOs once. It lists message types only
when no matching DTO has been resolved, avoiding duplicate published and
consumed type lists when they describe the same contract.
Indexing issues that have a source endpoint expose a VS Code link to the
associated file and line. The HTML export provides dedicated OpenAPI, Kafka,
Mongo, Request/reply, and Build views. OpenAPI and Kafka both support
filtering their complete list (OpenAPI by path or service, DTOs by simple name
or package); Persistence filters by class, package, collection, or service. A
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

## Elastic APM digest

`apm export` is an explicit read-only integration for a configured Elasticsearch
endpoint. It accepts `--since` as a positive `s`, `m`, `h`, or `d` duration
(default `1h`) and an optional exact `service.environment` filter. Command-line
connection values take precedence over `SYSTEMLENS_ELASTICSEARCH_URL` and
`SYSTEMLENS_ELASTICSEARCH_API_KEY`. An endpoint must be an absolute HTTP(S) URL.

The command queries only `metrics-apm*` documents with
`metricset.name=service_destination`. It aggregates source service, destination
service, destination type, outcome, call count, failure count, error rate, and
average latency. It first uses `service.target.name`, then retries the legacy
`span.destination.service.resource` field only when the first query is empty.

The JSON contract has schema version `apm-digest-v1`, a UTC `window`, optional
`environment`, ordered `relations`, and `coverage`. `coverage` reports the
number of relations seen and exported plus each truncation reason. Output is
bounded by `--max-relations` (80 by default), `--max-bytes` (50,000 by default),
and a protective Elasticsearch `--max-buckets` read limit (5,000 by default).
No digest result is persisted in SQLite, surfaced through MCP, or merged into
an architecture HTML export.

## Elastic APM runtime report

`apm report` is an explicit read-only command which requires `--html FILE`.
It accepts the same bounded UTC `--since` duration and optional exact
environment filter as `apm export`. It queries only `metrics-apm*`, always with
`size: 0`, and emits a versioned `apm-runtime-report-v1` aggregate observation
embedded in the requested self-contained HTML file. It is not persisted in
SQLite, merged into the static architecture snapshot, or exposed through MCP.

The service view reads `metricset.name=service_transaction`; the transaction
view reads `metricset.name=transaction`; both aggregate the `value_count` and
`sum` metrics of `transaction.duration.summary`, and the P95 of
`transaction.duration.histogram`, plus aggregate failure counts. The dependency
view reads `metricset.name=service_destination` and reports call count, average
latency, and aggregate failure rate. It first uses `service.target.name`, then
retries the legacy `span.destination.service.resource` field only when the
first query is empty. Dependency P95 is not returned: it requires a separate,
explicitly approved second pass over sampled span aggregates.

The report has service, transaction, dependency, and recurring-failure tables.
Its transaction graph groups transactions beneath their observed service and
colours them by relative P95; its service filter is client-side. Its edges show
only service ownership, never a transaction-to-dependency call. The dependency
table provides a separate client-side source/target flow filter. Each ranking
reports `items_seen`, `items_exported`, its result limit, bucket limit, and
truncation reasons. A zero result means no matching aggregate was observed in
the covered window; it does not prove a static HTTP, Kafka, MongoDB, or S3
dependency is absent. P95 values are approximate histogram percentiles.

The report does not correlate observed names with static identities in this
release. Its analysis is limited to aggregate metrics and failure counts: raw
spans, trace IDs, request data, headers, log messages, credentials, and
unredacted exception values are excluded.

Kafka latency, consumer failures, MongoDB activity, S3 activity, and Kubernetes
capacity signals require their own documented source fields and availability
checks. Their absence must be reported as unavailable coverage rather than
estimated from another telemetry type.

## Elastic APM microservice overlay

`export microservices --html FILE --apm-overlay` decorates the static
microservice graph with the same bounded, read-only APM aggregates as
`apm export`/`apm report`, using the same `--since` (default `1h`),
`--environment`, connection, and result-limit flags. It is opt-in and requires
network access; without `--apm-overlay` the export behaves exactly as before,
fully offline.

The overlay reads `service_destination` for edge call volume and error rate,
and `service_transaction` for node average latency. Every observed service
name is correlated to an indexed microservice name: an exact,
case/separator-normalized match is **matched**; otherwise a normalized
substring containment (either direction) is accepted as a **heuristic** match
against the single best-scoring indexed candidate; a tie between
equally-scored candidates is **ambiguous**; no candidate at all is
**unmapped**. Only `matched` and unambiguous `heuristic` observations are
drawn on the graph. A heuristic match is visually distinguished from an exact
match (for example a dashed badge outline) so a viewer never mistakes a guess
for a confirmed identity.

A matched microservice node shows its average observed transaction latency as
a separate badge/ring from its existing static-connectivity outline, so
runtime latency is never confused with static relation count. A matched edge
between two indexed microservices shows call volume and error rate; call
volume uses its own low/medium/high tercile, computed only across overlaid
edges, independent of the static complexity terciles.

`ambiguous` and `unmapped` observations never appear on the graph. They are
listed in a dedicated side panel section, each with its observed name, role
(`service` for a node-level observation, `source`/`destination` for an
edge-level observation whose corresponding node did not resolve, or
`dependency` for an edge whose two ends both resolved but that lost a
conflicting claim on an already-attached edge), call volume/error rate, and —
for `ambiguous` entries — every tied candidate name. This keeps their absence
from the graph distinguishable from a genuinely idle service.

A zero-observation window (no matching aggregate at all) is reported in the
same panel as "no APM activity observed in this window", never silently, and
never as evidence that a static dependency is absent.

The overlay is computed only in memory at export time. It is never persisted
to `.systemlens/findings.db`, merged into `architecture_relations`, exposed
through MCP, or considered by `analyze audit`, `analyze microservices impact`,
or `analyze microservices path`, all of which remain purely static.

For a future Kubernetes workload correlation, an exact workload/service name
match remains preferred. A normalized, token-bounded inclusion (for example,
`orders` in `orders-api-v2`) may be used only as a fallback when it identifies
one indexed service. A broad substring match, or a workload containing two or
more candidate service names, is unresolved and must be exposed in coverage.
