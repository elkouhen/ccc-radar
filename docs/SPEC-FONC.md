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
| `systemlens version` | Prints the installed `systemlens` package version. |
| `systemlens apm doctor [--endpoint URL] [--api-key KEY] [--insecure] [--json]` | Read-only validation of Elasticsearch access to the APM metrics. It reports only whether the endpoint and key are configured and their source (`flag` or `env`); it never prints the URL or key. `--insecure` explicitly accepts a self-signed TLS certificate. |
| `systemlens apm export [--since DURATION] [--environment NAME] [--endpoint URL] [--api-key KEY] [--insecure] [--max-relations N] [--max-bytes N] [--max-buckets N] [--export-curl] [--out FILE]` | Reads `service_destination` aggregates from Elasticsearch and emits compact JSON to stdout, or writes it to `FILE`. It does not export raw spans, logs, headers, identifiers, or source content. A failed export preserves the safe HTTP/access error and prints read-only diagnostic guidance; HTTP 429 guidance includes Kibana Dev Tools commands for cluster health, disk allocation, and watermark settings, never the endpoint or API key. `--insecure` explicitly accepts a self-signed TLS certificate; `--export-curl` includes curl's equivalent `--insecure` flag and prints a reproducible curl command for the export's first APM query, using environment-variable references rather than credential values. |
| `systemlens apm report --html FILE [--since DURATION] [--environment NAME] [--endpoint URL] [--api-key KEY] [--insecure] [--max-services N] [--max-transactions N] [--max-dependencies N] [--max-buckets N] [--max-timeline-events N]` | Reads bounded APM metric aggregates plus a bounded field projection of recorded transaction events and writes an HTML human investigation report. Its interactive graph uses the same Graphology/Sigma.js CDN assets as the architecture export and falls back to embedded SVG when unavailable. It never reads `_source`, request data, headers, bodies, logs, credentials, or unredacted error values; trace identifiers are held only in memory to create report-local waterfall references and are never exported. `--insecure` explicitly accepts a self-signed TLS certificate. |
| `systemlens index [MANIFEST]... [--full] [--topic-strategy default\|strategy1] [--manifest FILE]... [--kubernetes] [--kubernetes-namespace NAME]` | Incrementally extracts and persists architecture facts. `--kubernetes` queries the active `kubectl` context for Deployments and StatefulSets; `--kubernetes-namespace` restricts it to one namespace. |
| `systemlens microservices`, `topics`, `apis`, `dtos`, `mongodb`, `modules` | Browse the indexed catalog; `microservices`, `topics` and `mongodb` list the corresponding architecture objects directly, each with a `kind` and `name`, and support the documented list/show/neighbors actions and JSON output where applicable. |
| `systemlens microservices topics\|apis\|mongodb\|properties\|openapi NAME [--root DIR] [--json]` | Follow one linked object kind from a single named microservice. |
| `systemlens microservices implementation KIND ID [--root DIR] [--json]` | Jump to the source implementation of one identified integration. |
| `systemlens modules integrations MODULE [--json]` | Lists the integrations owned by one module. |
| `systemlens modules graph [--json]` | Prints the Maven/Gradle build-dependency graph between modules. |
| `systemlens analyze audit [--workspace DIR]` | Reports static architecture risks; `--workspace` analyzes a parent workspace of independently indexed services instead of the current repository. |
| `systemlens analyze coverage [--root DIR] [--json]` | Reports inventory coverage and unresolved integrations. |
| `systemlens analyze indexing-issues [--root DIR] [--json]` | Lists unresolved indexing facts. JSON includes source evidence suitable for reviewing proposed heuristics. |
| `systemlens analyze microservices calls\|dependencies\|external-apis\|orphan-integrations [NAME] [--root DIR] [--json]` | Lists a service's outgoing calls, dependencies, external APIs, or integrations with no resolved caller/callee, depending on the subcommand. `external-apis` and `orphan-integrations` accept an optional `NAME` to scope the result to one service. |
| `systemlens analyze microservices impact NAME [--root DIR] [--json]` | Lists direct and transitive impact paths. |
| `systemlens analyze microservices path FROM TO [--root DIR] [--json] [--max-depth N] [--limit N]` | Lists bounded paths between services. |
| `systemlens analyze request-reply [--root DIR] [--json]` | Lists Strategy1 Kafka request/reply candidates. |
| `systemlens export microservices (--html FILE \| --c4 DIRECTORY \| --json) [--root-path DIRECTORY] [--apm-overlay --since DURATION --environment NAME --endpoint URL --api-key KEY --insecure --max-relations N --max-buckets N]` | Exports the microservice, Kafka-topic and MongoDB-collection topology. `--root-path` provides the local source root for HTML VS Code links. `--apm-overlay` (HTML only) additionally queries bounded Elastic APM aggregates and overlays them on the graph; `--insecure` explicitly accepts a self-signed TLS certificate; see "Elastic APM microservice overlay". |
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
For a `src/main/resources/openapi/xxx.rest` publication declaration, it
searches the entire indexed repository for same-named `xxx.yaml`,
`xxx.yml`, or `xxx.json` OpenAPI contracts, including contracts in a sibling
shared module without an `openapi-generator` Maven configuration. It also
searches every YAML or JSON document below
`model-xxx/src/main/resources/openapi/`, so that module may contain several
contracts with distinct names. Only a valid OpenAPI document is attached, and
its resulting endpoints remain attributed to the module that owns the `.rest`
declaration.

Independently of Strategy1, each build module inventories every valid YAML or
JSON OpenAPI document under its own `src/main/resources/openapi/` directory;
contract file names do not need to follow an `openapi.*` or `swagger.*`
pattern.
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

The command queries the Elastic APM metric streams and the compatible
OpenTelemetry metric streams (`metrics-service_destination.1m.otel-*`,
`metrics-service_transaction.1m.otel-*`, and `metrics-transaction.1m.otel-*`)
with
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
environment filter as `apm export`. It queries the same APM and OpenTelemetry
metric streams with `size: 0` for aggregate views and both `traces-apm*` and
`traces-*.otel-*` for at most 500 recorded transaction
projections by default (configurable with `--max-timeline-events`, capped at
2,000), then emits a versioned
`apm-runtime-report-v2` observation embedded in the requested HTML file. It is not persisted in
SQLite, merged into the static architecture snapshot, or exposed through MCP.
After writing the file, the CLI prints a generation summary with the displayed
service, aggregate-transaction, dependency, execution-log-span, and
distributed-trace counts. The span and trace figures are bounded report
projections, not exhaustive counts of the selected APM window.

The service view reads `metricset.name=service_transaction`; the transaction
view reads `metricset.name=transaction`; both aggregate the `value_count` and
`sum` metrics of `transaction.duration.summary`, and the P95 of
`transaction.duration.histogram`. They derive failures from known outcomes
minus `event.success_count` successes and expose outcome coverage. The summary
failure rate uses known outcomes only and displays its coverage. To avoid
exporting arbitrary operation names, transaction rows with the same safe
service/type category are merged; their displayed `Highest operation P95` is
the maximum P95 among the merged operations, not a recomputed category
percentile. The dependency
view reads `metricset.name=service_destination` and reports call count, average
latency, and aggregate failure rate. It first uses `service.target.name`, then
retries the legacy `span.destination.service.resource` field only when the
first query is empty. Dependency P95 is not returned: it requires a separate,
explicitly approved second pass over sampled span aggregates.

The report starts with a bounded investigation-priority ranking. It combines
observed volume, aggregate error rate, and service/transaction P95 or dependency
average latency only as a triage aid; it is not an SLO verdict or a causal
claim. Its context shows the UTC window, environment, snapshot instant, and a
visible complete/limited coverage state. The summary failure rate is calculated
from the service aggregate only and is therefore not double-counted across the
service and transaction views.

The Transactions tab additionally displays up to 20 recent distributed-trace
waterfalls. They are grouped and sorted by the HTTP source service, a generic
messaging source when a producer span follows the observed publish convention,
or their root service. Trace, span, and parent identifiers are used only in
memory to reconstruct the tree and are never embedded in the HTML or persisted.
Each waterfall exports only timestamp, service, safe workload labels,
transaction kind, outcome, relative timing, duration, and tree depth; it is
therefore an exemplar for investigation, not a complete trace archive. Trace
selection and each per-trace span limit expose truncation in the report status
and on each partial waterfall. Its card
starts with the cross-service route and distributed transaction operations, and
the tab offers an origin filter so database spans can be read as details of the
selected flow.
For traces with more than 20 spans, nested spans are collapsed by default.
Expanding a span reveals only its descendants, so dense traces remain
navigable. The report retrieves at most 500 spans per waterfall and continues
to mark any truncated waterfall as partial.

The report separates its investigation modes into Services, Transaction
workloads, Spans, Traces, and Dependencies tabs. Each
tab starts with
the same observed-service filter. Selecting a service in any tab synchronizes
the selection across all tabs and their view-specific controls. Services contains
the context, priority ranking, service hotspot table, and recurring failures.
Services contains the slow-transaction ranking. Transaction workloads provides
the ownership graph. Traces provides the waterfall exemplars and
their origin filter. Its observed-service and root type (`HTTP/request`,
`messaging`, or other) filters narrow the bounded waterfall examples locally;
they do not issue a new Elasticsearch request. The optional `10
highest-impact traces` filter groups matching waterfalls by their safe
service/type/route category, ranks categories by observed cumulative duration
(duration across all recorded executions), then displays up to ten distinct
exemplars with their execution count and cumulative duration.
The `10 traces with most errors` filter instead retains failed waterfalls,
groups them by the same safe category, and ranks up to ten exemplars by their
observed error count. The count shown on each card is limited to the embedded
waterfall data and is not an exhaustive error total; it represents failed
executions of the grouped operation, not errors on one individual span.
Spans offers equivalent `10 spans with most errors` filtering:
it groups failed spans by safe service/type/label, ranks those groups by
observed error count, and shows one exemplar and count per group.
Selecting a span in a distributed waterfall reveals its safe execution details:
service, operation, kind, type, observed outcome, relative start offset, and
duration. For failures, the report explicitly states that raw error messages
and exception data are excluded. It displays a sanitized error category and
controlled message when the telemetry error type can be classified (for
example, `Dependency timed out`); it never displays the raw telemetry error
message. Failed spans are also visually distinct in
the waterfall through a red row, red timing bar, and an `Error` badge.
Services also presents a bounded, action-oriented anomaly summary for observed
service and dependency error rates, failed trace exemplars, and incomplete
outcome coverage. Selecting a signal opens Traces with the relevant local
focus; it is an investigation priority, not a causal claim. Traces groups
failed span examples into safe failure signatures (service, origin, sanitized
category, and trace step) and displays a service-by-step failure matrix. Both
explicitly count only currently embedded examples. A selected span also shows
the trace duration, longest recorded span, and non-exclusive recorded duration
by service; nested spans may overlap, so this is a timing explanation rather
than a calculated critical path. Trace and span duration badges compare only
with bounded embedded examples of the same safe grouping.
Dependencies contains the directed map, its map mode,
service, and workload selectors, the selected node's aggregate workloads and
observed directions, the dependency table, and focused flows. The shared
observed-service selector is duplicated for access at the top of every tab;
other selectors remain owned by their view. Its primary visual is
an interactive directed service map: circle nodes are
observed services and diamond nodes are observed messaging targets. Every arrow
is directed from the observed source service to its target; it is labelled HTTP
for a non-messaging target and `send` for a recognized outgoing messaging target.
Edge thickness represents volume and risk colour errors or comparatively high
average latency. It offers hotspot/all, service, and workload-type filters. Selecting a service opens its
HTTP (`transaction.type=request` or `http`), messaging
(`transaction.type=messaging`), and other transaction aggregates alongside its
inbound and outbound dependencies. A messaging diamond is an APM target whose
type indicates messaging (`kafka`, `rabbitmq`, `jms`, etc.); its name is not
claimed to be a confirmed broker topic. The map does not assert a transaction-
to-dependency call. Each ranking
reports `items_seen`, `items_exported`, its result limit, bucket limit, and
truncation reasons. A zero result means no matching aggregate was observed in
the covered window; it does not prove a static HTTP, Kafka, MongoDB, or S3
dependency is absent. Service P95 values are approximate histogram percentiles;
merged transaction rows explicitly show the highest constituent-operation P95.

The Spans tab is a chronological, bounded view for selected
cross-service traces. It displays safe span labels, service, span type,
outcome, timestamp, and duration. Its filters narrow the embedded span set by
service and type (including `request` and `messaging` when observed); they do
not query Elasticsearch again and therefore cannot extend the report window. The view
states its displayed span count and waterfall-exemplar count, so it cannot be
mistaken for an exhaustive execution archive. Its Elasticsearch query sets
`_source: false` and retrieves only `@timestamp`, service name, span name/type,
duration, outcome, and a transient trace ID. Span names are replaced with the
safe `Span` label and types are allowlisted before export. It never exports
request or response data, headers, bodies, stack traces, logs, error values,
result values, or messaging queue/topic names.
The optional `10 longest spans` filter retains the ten longest spans after all
other Timeline filters and keeps their chronological display order.
Selecting a Span entry opens the Traces view and filters waterfalls to the
exact associated trace. Selecting a span in a trace keeps its safe execution
details visible there and, when that span is present in the bounded Spans view,
offers an action to open the exact matching Span entry. SystemLens receives
trace and span IDs only in memory to create report-local opaque references,
then removes the original IDs before serializing the report. The report exposes
a clear action to remove the exact-trace filter; it never displays a route name
or trace ID. When a trace opens a matching Span, Spans offers a return action
to the exact originating trace.

The report does not correlate observed names with static identities in this
release. Its aggregate views remain separate from the Spans view's bounded
transaction-field projection; trace IDs, request data, headers, bodies, log
messages, credentials, unredacted exception values, and telemetry-controlled
operation labels are excluded.

The Transactions view contains all observed aggregate transaction workloads in
the selected window, including local operations. Distributed-trace waterfalls
remain a separate, bounded view: SystemLens uses trace IDs only transiently to
select those multi-service exemplars; it never exports, persists, or displays
the identifiers.

The APM HTTP client bounds each Elasticsearch request to 15 seconds. If the
bounded Timeline query times out, `apm report` still writes the aggregate
report and marks Timeline coverage as unavailable with reason `timeout`; it
does not silently treat this as a zero-observation window.

An `apm report` file is a one-shot snapshot and does not persist or infer a
historical baseline. It must not present a regression comparison unless a future
explicit comparison input and coverage contract are added.

Kafka latency, consumer failures, MongoDB activity, S3 activity, and Kubernetes
capacity signals require their own documented source fields and availability
checks. Their absence must be reported as unavailable coverage rather than
estimated from another telemetry type.

## Elastic APM microservice overlay

`export microservices --html FILE --apm-overlay` decorates the static
microservice graph with the same bounded, read-only APM aggregates as
`apm export`/`apm report`, using the same `--since` (default `1h`),
`--environment`, connection, and result-limit flags — except `--max-buckets`,
which defaults to 2,000 for the overlay versus 5,000 for `apm export`, because
the overlay reads two aggregate queries per export. It is opt-in and requires
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
