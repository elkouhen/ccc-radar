# Technical specification — systemlens (`systemlens`)

## Architecture

The pipeline is deliberately local:

```text
repository files → file hashes → Tree-sitter Java AST extractors
                 → endpoints/modules/properties → SQLite facts and relations
                 → CLI, MCP, graph and audit views
```

When explicitly enabled with `--kubernetes`, indexing also invokes the local
`kubectl` CLI once to list Deployments and StatefulSets. It aggregates the
declared requests and limits of regular containers (init containers are
excluded) and attaches a workload only when its Kubernetes name exactly matches
the indexed module name. This optional step can contact the current Kubernetes
API context; it is never enabled by default.

`scanner/` (a package; see `docs/ARCHITECTURE.md`) owns Java/Spring extraction.
`java_parser.py` provides cached
Tree-sitter parsing and syntax helpers. `modules.py`, `maven.py` and `gradle.py`
discover build units; `relations.py` derives typed architecture relations from
modules, endpoints and build dependencies. `indexer.py` orchestrates the
incremental transaction. `store.py` owns SQLite persistence.

`cli.py` and `mcp_server.py` are thin delivery layers over the domain modules.

### Elastic APM digest adapter

`apm.py` is deliberately outside the indexing pipeline and SQLite model. The
explicit `systemlens apm` commands use the Python standard library HTTPS client
with an Elasticsearch API key to perform only `POST .../_search` requests.
Credentials come from one-off flags or environment variables, are not persisted,
and are not included in errors or digest JSON. `apm doctor` uses the same
read query, so it does not require Elasticsearch `monitor` privileges.

The adapter queries `metrics-apm*` with a composite aggregation over Elastic
APM `service_destination` metrics. It pages at most `max_buckets` aggregate
buckets, combines success/failure outcomes by source, target, and target type,
then ranks relations by call volume. It retains only the prefix that fits both
the relation and serialized-byte limits. The resulting `apm-digest-v1` payload
contains its time window, metric field used, aggregate relations, and explicit
coverage/truncation metadata. It contains no raw span, trace, log, request, or
source data and is not merged with a snapshot.

### Elastic APM runtime report projection

`apm_report.py` constructs an in-memory `apm-runtime-report-v1` observation
from three explicit read-only `metrics-apm*` aggregation queries. Its service
and transaction views page composite buckets and aggregate transaction count,
duration sum, aggregate failure count from the `aggregate_metric_double`
field `transaction.duration.summary`, and the P95 percentile of
`transaction.duration.histogram`. Its dependency view reuses the bounded
`service_destination` aggregate adapter and therefore intentionally reports
only average latency, call count, and aggregate failure rate.

Every query uses `size: 0`; it never retrieves `_source` documents. Each view
has an independent composite-bucket guard and output-result guard. The
projection records the UTC window, environment, source metricsets/field,
per-view coverage, limits, and truncation. `render_runtime_report_html` embeds
only this versioned aggregate projection in an explicitly requested,
self-contained HTML file. It does not write to SQLite, snapshots, or MCP cache.
Service names and transaction names are inserted through JSON data and rendered
with HTML escaping before insertion, so a telemetry value cannot create markup.
The report's transaction graph is a client-side grouping of transaction buckets
by `service.name`; an edge represents only that ownership. It intentionally
does not connect a transaction bucket to a dependency bucket because the three
metric aggregate queries do not prove that causal relationship.

The report is a standalone runtime view and does not join observed names to
static identities. A later correlation remains a delivery-layer join, not a
topology derivation: it may attach one observed name only through exact
persisted evidence, retain mapping state and confidence, and never infer a
static HTTP, Kafka, MongoDB, or S3 relation from a name coincidence.

### Elastic APM microservice overlay

The overlay is computed by the HTML export path (`render/html_export.py`)
when `--apm-overlay` is set, reusing `apm.py`'s existing bounded
`service_destination` reader/aggregator for edges and `apm_report.py`'s
existing bounded `service_transaction` composite-bucket reader for node
average latency. No new Elasticsearch query shape is introduced; the overlay
only combines the two existing aggregate result sets with the already-indexed
microservice name list held in memory for that export.

Correlation is a pure, delivery-layer function over
`(observed_name, indexed_names)` that never touches `store.py` or
`architecture_relations`:

1. Normalize both sides (case-fold, collapse `-`, `_`, `.` separators).
2. An exact normalized equality is `matched`.
3. Otherwise, an indexed name is a candidate when one normalized string
   contains the other; its score is the containment ratio (matched length /
   longer string length). The highest-scoring candidate is used; a strict tie
   between the top score across two or more candidates makes the observation
   `ambiguous` instead.
4. No candidate at all is `unmapped`.

Only `matched` and singular `heuristic` results contribute a node/edge
enrichment record; `ambiguous` and `unmapped` results are collected into a
separate list rendered as the side-panel section, each retaining every tied
candidate name for `ambiguous` entries. This mirrors the existing future
Kubernetes correlation rule below (exact-first, single-candidate-only
containment fallback) but is deliberately looser: it accepts a genuinely
single best-scoring containment candidate as `heuristic` rather than leaving
it unresolved, because this join is visual, opt-in, and never persisted (see
ADR-16). A future SL-010 alias-based correlation adapter remains the
conservative, exact-alias-only mechanism for any persisted or MCP-exposed
join; when both exist, an explicit alias always takes precedence over this
overlay's heuristic containment result for the same name.

Because two or more distinct observed names can each resolve to the same
indexed node or edge (exactly or heuristically), attachment is resolved in
priority passes rather than a single overwrite-prone pass: exact `matched`
claims are attached first, then `heuristic` claims fill only still-unclaimed
nodes/edges, and any observation that would otherwise silently overwrite an
already-attached target is instead routed to the unresolved list (`ambiguous`
for a losing node claim, `dependency` role for a losing edge claim whose two
ends both resolved).

The enrichment record carries, per matched node, `average_ms` from
`transaction.duration.summary`; per matched edge, `calls`, `failure_calls`,
and `error_rate` from `service_destination`, identical in shape to the
`apm-digest-v1` relation fields. It is embedded in the exported HTML's
in-memory graph model only; it is not written to SQLite, not part of
`ArchitectureSnapshot`, and not reachable from any MCP tool.

S3 support requires a separate conservative Java extractor for explicit AWS SDK
v1/v2 operations and configured bucket names, with dynamic bucket expressions
preserved as unresolved evidence. Kafka, MongoDB, S3, and Kubernetes runtime
signals require source-specific aggregate adapters; they cannot be synthesized
from `service_destination` metrics when their telemetry is absent.

Future Kubernetes correlation must first use the current exact workload/service
name match. Its only fallback is a normalized token-sequence containment check
between a Deployment or StatefulSet name and an indexed service name. The
fallback succeeds only for one candidate; it records the matching strategy and
leaves zero or multiple candidates unresolved. It must never use an arbitrary
substring search or change persisted source topology.

## Data model

`MessageEndpoint` is the primary extracted fact. It records role, system,
topic, dynamic status, source (`code` or `manifest`), framework, location,
snippet, module, qualified name and optional Kafka message type. Its identifier
is stable for a source location:

```text
sha256(role | topic | path | start_line:end_line)[:16]
```

`ArchitectureRelation` records an evidenced link between source and target
objects. It includes origin (`code`, `manifest` or `derived`), confidence,
module and source location. It is the persisted source of truth for the
architecture snapshot, including conservative resolved inter-service REST and
Kafka topology relations. `ArchitectureSnapshot` derives its topology edges
from those persisted relations; adapters may use indexed endpoints only to add
route, topic, and source presentation details and do not re-resolve targets or
rescan source.

`AnalysisProfile` carries persisted extraction choices with the loaded
inventory, currently the `default` or `strategy1` topic convention. CLI, MCP,
export, graph, and audit adapters consume this profile. A workspace federation
retains source profiles and rejects a mixture of incompatible topic strategies.

MongoDB persistence-class metadata is extracted at index time from Java
`@Document` declarations, entity generic types of Mongo repositories, and
unambiguous `Type.class` arguments of `MongoTemplate` operations. Repository
entities without `@Document` use Spring Data's lower-camel simple-name default;
ambiguous simple names are not resolved. The immutable snapshot records the
collection, qualified class name, source location, and declared fields so HTML
exports never reopen Java sources to build this view.
The HTML snapshot resolves these classes from the collection-owning module and
its transitive build dependencies. A unique collection-wide fallback covers
snapshots without dependency metadata while preserving ambiguity when several
modules declare the same collection name.
Persistence-class extraction is part of schema version 21. Older snapshots are
rejected on read and must be regenerated, preventing a valid-looking HTML
export from silently presenting the empty pre-extractor inventory.
For each MongoDB root class, indexing persists the recursive closure of uniquely
resolved project field types. Nested definitions retain source locations and
declared fields but are marked as non-root so collection inventories list only
actual persistence roots while inspectors can navigate the complete closure.

`ExtractionDiagnostic` is a safe, persisted extraction outcome with its file
path, extractor, category, severity and a non-source-code detail. The initial
implementation records Tree-sitter Java parse failures; `analyze
indexing-issues` exposes them alongside unresolved architecture facts.
MongoDB extraction keeps structurally valid declarations and invocations from
a partially parsed Java file while ignoring subtrees that contain an error or
missing token. The file-level diagnostic remains visible so partial coverage is
never presented as a complete parse.

The SQLite store is standard-library-only: it does not load native vector
extensions or persist/query vector representations. The retained findings
search compatibility path uses deterministic lexical matching.

The index persists source evidence only as paths relative to the indexed
project root. HTML export receives a local `--root-path` and joins it to these
relative paths when building VS Code URIs. This keeps an index portable across
machines and avoids persisting WSL or other host-specific path context.

Kafka DTO definitions and OpenAPI/Swagger document contents are materialized at
index time. Each OpenAPI/Swagger source file is attributed and materialized
once by the indexed Maven or Gradle module that directly encloses the file;
other modules may reference it but do not list it again. This attribution is
resolved by matching the repository-relative evidence path's segments against
the enclosing module's own directory segments (not merely its last path
component), so it stays correct for modules nested two or more levels below
the repository root and for a publishing module (a Strategy1 declaration)
whose contract physically lives in a different, shared module: the export
looks up the parsed spec and materializes the contract exactly once, keyed by
the module that truly encloses the file. A DTO definition retains
its qualified name, owning module, module-relative Java source path, declared
fields, enum values, and conservative nested-type references. The HTML export
uses those stored facts and only derives its VS Code URI at render time; it
does not reopen a Java or OpenAPI source file.

For Strategy1 OpenAPI publication, `xxx.rest` selects both same-named contract
files anywhere in the repository and all YAML or JSON candidates under a
`model-xxx/src/main/resources/openapi/` module. Each candidate is validated as
an OpenAPI document before endpoint facts are persisted; endpoints remain
attributed to the module containing the declaration while the contract source
path stays evidence.

## Export snapshot contract

HTML and JSON graph exports only consume the loaded architecture snapshot.
They do not reopen OpenAPI documents or recursively parse Java DTO sources at
render time. The export can show indexed Kafka payload-type identities and
OpenAPI evidence paths; richer DTO/OpenAPI content requires an explicit future
indexed contract rather than a live source read. This keeps an export
reproducible when repository files change after `systemlens index`.

The HTML renderer keeps its floating graph-detail panel in an explicit empty
state until a resource or itinerary is selected. The empty state is visual-only
and ignores pointer events, preventing it from obscuring a control in a narrow
viewport; any populated detail panel restores pointer interaction. Path parsing
filters same-name candidates by the grammar before accepting an itinerary
endpoint: only a microservice can be first or last, while intermediate stops
can be microservices or Kafka topics. Direct resource search continues to
report multiple same-name resources as ambiguous.

The legacy `findings` table and model are retained only to open existing index
databases. `Store.clear_findings_once("ast_only_analysis_v1")` removes stale
external-analyzer data on the first AST-only index run.

## Indexing

`index_repo` performs these steps:

1. Clear parser and discovery caches for long-lived MCP processes.
2. Discover modules unless disabled.
3. Build the eligible-file hash inventory, respecting include/exclude rules,
   test-source exclusion and nested-build boundaries.
4. Compare hashes with the stored inventory and purge removed files. A changed
   or deleted Spring configuration file or Maven/Gradle descriptor promotes the
   delta to a full endpoint refresh because these files are dependencies of
   otherwise unchanged Java facts.
5. Force a full refresh when extractor/configuration/strategy signatures differ.
6. Run AST extractors for the changed files and atomically replace their
   endpoints.
7. Persist hashes, modules, dependencies and derived relations.

Steps 1–7 execute inside `Store.transaction()`. The writable connection uses
`BEGIN IMMEDIATE`, then commits the complete snapshot only after relation
materialization succeeds; any exception rolls back files, endpoints, modules,
dependencies, relations and index signatures together. Schema creation and
compatible migrations happen when a writable store opens, before an index
transaction. Read-only stores open SQLite in `mode=ro` and never migrate or
commit. A concurrent reader sees the last committed snapshot until the writer
commits the next one.

No subprocess is used to analyze source code. A failed index leaves the whole
previous successful snapshot intact; isolated extraction failures are reported
as diagnostics when that capability is enabled.

## Extractors

`infer_framework_endpoints` walks Java declarations, annotations and method
invocations to discover Spring MVC/WebFlux routes, Feign clients, RestTemplate,
WebClient, Spring Data REST and gateway routes. It resolves literals and known
Spring property expressions conservatively.

REST graph construction first resolves an explicit target identity from an HTTP
host, `lb://` URI, configured client domain, or an opt-in Strategy1 convention.
The normalized alias must match exactly one indexed service; prefix, suffix and
substring matching are not used. Route compatibility is evaluated only within
that service. A targetless or ambiguous call remains an endpoint fact and is
reported as unresolved rather than creating an internal edge.

`infer_kafka_endpoints` recognises Spring Kafka listeners and send sites,
KafkaTemplate/ProducerRecord usage and Spring Cloud Stream StreamBridge calls.
It preserves dynamic topic expressions and derives a payload type only from an
explicit listener parameter or client generic signature.

With `--topic-strategy strategy1`, every method whose name starts with
`envoyerMessageKafka` is an additional producer convention, including
`envoyerMessageKafkaRequest(topic, payload)` and
`envoyerMessageKafkaReply(topic, payload)`. A first argument shaped as
`kafkaProperties.getTopics().getXxx()` resolves to the normalized Strategy1
topic name; other values use the conservative topic resolver. The second
argument is used to derive the payload type from its method parameter, local
variable declaration or enclosing class field.

Strategy1 also enables the `getXxxServiceUrl()` REST target-name convention;
without it, SystemLens uses only an explicit URL or `lb://` service target.

The graph export exposes only the indexed Java payload-type identities linked
to Kafka endpoints. It does not resolve Java DTO fields or enums from source
roots at render time; recursive DTO inspection requires a future persisted
schema contract.

The graph export keeps an exact-name index of its visual nodes. Its client-side
itinerary algorithm performs directed breadth-first searches for each pair of
user-supplied stops and concatenates those shortest segments. It uses only
Kafka graph relations, independently of temporary display filters, so an
itinerary always alternates between microservices and Kafka topics. Kafka
links carry relation-specific published or consumed Java message types; the
selected-path detail uses only these adjacent links and explicitly reports
missing type information.

The manifest extractors add explicitly declared Kafka facts from Markdown and
JSON. Strategy1 is separate and opt-in because it embeds repository-specific
naming conventions.

`systemlens analyze indexing-issues --json` exposes unresolved facts as a structured
remediation review payload. Each endpoint-backed issue has a stable code,
severity, service, framework, topic/API, extracted message type and its source
path, line range and snippet. The command does not infer or apply a heuristic;
its evidence is intended for a human or an AI to assess a conservative rule.

## Persistence and compatibility

SQLite schema migration is additive where possible. `files` stores hash state,
`endpoints` stores source facts, and normalized tables store modules,
dependencies and relations. Each module has a collision-safe identity used by
endpoints and relations; its artifact/project name remains a display alias.
The database filename remains `findings.db` for backward compatibility; new
AST-only behavior must not infer that it contains security findings.

SystemLens stores this database under `.systemlens/`. It intentionally does not
load the former `.cccr/`, `.archlens/`, or `.codeatlas/` state directory: the
product rename requires a fresh `systemlens init` and `systemlens index` so the
configuration and index namespace remain unambiguous.

The endpoint-inventory signature in `meta` is bumped whenever extractor
behaviour changes. This forces a complete refresh before new facts are served.

## Verification

Unit tests use fixture repositories with real Java source and assert source
locations, roles, dynamic flags and derived relations. Static checks are Ruff
and mypy. The project does not require an external scanner in development or
at runtime.
