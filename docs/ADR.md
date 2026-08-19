# Architecture Decision Records — systemlens (`systemlens`)

## ADR-1 — Local AST extraction is the sole static architecture analysis source

**Status:** Accepted.

**Context:** Architecture facts need to be reproducible offline and traceable
to source locations without a separate analysis runtime.

**Decision:** Parse Java source locally with Tree-sitter and derive static Spring
REST, Kafka and module facts from AST nodes and deterministic local
configuration.

**Consequences:** The project has no external analyzer, rule-pack or source-code
search dependency. Dynamic values are surfaced as unresolved facts instead of
being guessed.

## ADR-2 — SQLite is the local fact store

**Status:** Accepted.

**Context:** Incremental inventory, MCP queries and graph rendering require a
portable local state.

**Decision:** Persist files, endpoints, modules, dependencies and architecture
relations in `.systemlens/findings.db`. The filename is retained for compatibility
with existing installations.

**Consequences:** The database is private implementation state; clients use CLI
and MCP contracts rather than writing SQL. Legacy external-analyzer results are
cleared during the first AST-only index run.

## ADR-3 — Conservative static resolution

**Status:** Accepted.

**Context:** HTTP paths and Kafka topics are often composed from properties,
constants or runtime values.

**Decision:** Resolve only literals and supported local Spring expressions. Mark
the rest as dynamic and preserve source evidence.

**Consequences:** The inventory favours trustworthy partial results over
plausible but unsupported dependencies.

## ADR-4 — Workspace federation is read-only

**Status:** Accepted.

**Context:** A parent workspace may contain independently indexed services.

**Decision:** Load service indexes read-only and normalize them before graph and
audit queries.

**Consequences:** Federated exploration never rewrites another repository’s
index and reports incomplete or stale sources as warnings.

## ADR-5 — Strategy1 conventions are opt-in

**Status:** Accepted.

**Context:** Some repositories encode Kafka and REST dependencies through
project-specific naming conventions.

**Decision:** Keep these in `--topic-strategy strategy1`, separate from the
default AST extractor.

**Consequences:** Default indexing remains framework-oriented and portable;
Strategy1 facts are explicitly identified as convention-derived.

## ADR-6 — SystemLens is the public product and state namespace

**Status:** Accepted.

**Context:** The former product names and commands (`cccr`, `archlens`, and
`codeatlas`) were
not suitable for users, while the tool's purpose is to make a local
architecture view easier to read.

**Decision:** Rename the distribution, Python package, CLI command, MCP server
name, generated-export labels and state directory to `SystemLens` / `systemlens`.
The project state is now stored in `.systemlens/`.

**Consequences:** This is a breaking rename. Existing `.cccr/`, `.archlens/`,
and `.codeatlas/` configuration and index data are not read by SystemLens: run
`systemlens init` and `systemlens index` in each repository to create a fresh
`.systemlens/` inventory. Trace environment variables use the `SYSTEMLENS_`
prefix.

## ADR-7 — Publish each index as an atomic SQLite snapshot

**Status:** Accepted.

**Context:** An index refresh replaces file hashes, endpoints, modules,
dependencies, relation facts and their signatures. Publishing only part of
that sequence would make all read adapters report an incoherent architecture.

**Decision:** `index_repo` executes its complete refresh inside one explicit
`Store.transaction()` using SQLite `BEGIN IMMEDIATE`. The store commits only
after the complete relation projection is materialized, and rolls back on any
exception. Schema setup remains a separate opening-time concern; read-only
connections never mutate the database.

**Consequences:** Concurrent readers observe the previous complete snapshot
while a refresh is in progress, then the next complete snapshot after commit.
An index holds the single-writer SQLite lock for its run; this favours a
trustworthy local inventory over concurrent writers.

## ADR-8 — Persisted relations are the canonical architecture projection

**Status:** Accepted.

**Context:** Endpoints, request-time graph edges, dependency dictionaries and
renderer-specific links previously represented overlapping architecture facts
with independently implemented rules.

**Decision:** `architecture_relations` is the canonical persisted relation
projection. Indexing materializes evidenced endpoint, module, data-store and
resolved inter-service topology relations into it. Read adapters receive the
immutable `ArchitectureSnapshot`; the legacy `ArchitectureCatalog` name is a
compatibility alias for that projection.

**Consequences:** Relation identity, provenance and confidence are stable
across local catalog and coverage adapters. Graph-shaped renderers can still
adapt endpoint evidence for route and topic labels, but must not independently
infer service topology or rescan repository sources.

## ADR-9 — Exports never enrich a snapshot from live source files

**Status:** Accepted.

**Context:** HTML export previously reopened OpenAPI documents and recursively
parsed Java DTO files, allowing a single export to mix indexed topology with a
later working-tree revision.

**Decision:** Renderers only consume loaded snapshot facts. They display
indexed Kafka payload identities and OpenAPI evidence paths, but do not parse
contract or Java source files. A later detailed-contract feature must add an
explicit indexed data model first.

**Consequences:** Exports are reproducible and work without source trees. DTO
field and enum inspection is deliberately unavailable until its facts are
persisted at index time.

## ADR-10 — Persist the analysis profile with each snapshot

**Status:** Accepted.

**Context:** Strategy-dependent extraction facts were persisted, but delivery
adapters could silently select a different default while reading or refreshing
the same index.

**Decision:** Load the persisted topic strategy as an immutable
`AnalysisProfile` with every architecture inventory. CLI, MCP, graph, audit,
and export adapters consume that profile. A federation rejects source indexes
whose profiles are incompatible.

**Consequences:** MCP reindexing preserves the existing strategy, and a
Strategy1 convention cannot appear in a default inventory or disappear from a
Strategy1 one because of the delivery path.

## ADR-11 — Separate module identity from its display alias

**Status:** Accepted.

**Context:** Maven artifact IDs and Gradle project names are not unique within
all workspaces or across independently indexed federated repositories.

**Decision:** Persist a collision-safe module identity. It equals the build
name when unique and is qualified with the relative module path when a
collision exists. Endpoint, relation, dependency, and federation keys use the
identity; the build name remains a display alias. Direct service indexes are
namespaced at the federation boundary.

**Consequences:** Ambiguous aliases are not resolved implicitly, and two
services with the same display name coexist without data loss. Existing SQLite
indexes receive the additive `modules.identity` migration on their next
writable open.

## ADR-12 — Kubernetes discovery is explicit and snapshot-based

**Status:** Accepted.

**Context:** CPU and memory dimensions are runtime deployment facts, but a
normal source index must remain usable offline and without cluster credentials.

**Decision:** `systemlens index --kubernetes` invokes the local `kubectl` CLI
against its active context and records Deployments and StatefulSets. It attaches
a workload only when its Kubernetes name exactly matches an indexed module
name, and aggregates requests and limits across regular containers. Init
containers are excluded because their scheduling resources are not steady-state
service capacity.

**Consequences:** Kubernetes access is opt-in and may fail if `kubectl`,
credentials, or API connectivity are unavailable. The resulting dimensions are
persisted in the SQLite snapshot, so catalog and HTML export do not re-query a
cluster after indexing.

## ADR-13 — Export Elastic APM as an explicit bounded aggregate

**Status:** Accepted.

**Context:** Distributed traces can reveal deployed behaviour that static source
analysis cannot prove, but sending raw traces or large log volumes to an
external analysis model is costly and risks exposing request-level data.

**Decision:** Add a stateless, opt-in `systemlens apm` adapter that uses a
read-only Elasticsearch API key to aggregate `service_destination` metrics.
It emits a compact versioned JSON digest limited by relation count, byte budget,
and upstream aggregation buckets. It does not ingest, persist, or merge APM
data with the source architecture snapshot.

**Consequences:** APM export requires network access and a separately managed
read-only credential, but normal indexing and catalog/MCP queries remain offline.
The digest intentionally cannot reconstruct individual traces, request paths, or
trace exemplars. Mapping observed APM service names to static module identities,
historical comparisons, and visual overlays remain future work.

## ADR-14 — Use metric histograms for service and transaction P95, not dependency P95

**Status:** Accepted.

**Context:** A quick runtime investigation needs a tail-latency signal for
services and transactions, while the bounded APM `service_destination` metrics
contain only response-time count and sum. Deriving a dependency P95 from an
average would present false precision.

**Decision:** `systemlens apm report` queries aggregate-only APM metricsets.
It shows average and P95 latency for `service_transaction` and `transaction`
using `transaction.duration.histogram`. It shows average latency, volume, and
aggregate failure rate for `service_destination`. Dependency P95 is deferred to
a separately approved, sampled-span aggregate adapter. The report is an
explicit self-contained HTML output, never part of the SQLite source snapshot.

**Consequences:** Operators can quickly distinguish service or route tail
latency from slow outbound exchanges without raw-event export. P95 remains an
approximate histogram percentile, each ranking carries coverage/truncation,
and the report does not claim Kafka, MongoDB, S3, or Kubernetes signals that its
three metricsets cannot provide.

## ADR-15 — Show transaction ownership without inventing transaction-to-dependency calls

**Status:** Accepted.

**Context:** The runtime report has separate aggregate transaction and
service-destination metrics. A visual connection between a transaction and a
destination would look like trace evidence even though these independent
aggregations cannot establish it.

**Decision:** The runtime report may graph a service and its observed
transactions, using P95, call volume, and aggregate error rate on transaction
nodes. It may show dependencies in a separate service-level flow, but it must
not draw a transaction-to-dependency edge. Such an edge requires a future,
explicitly approved sampled-span aggregate with coverage reporting.

**Consequences:** The graph remains useful for finding slow or failing routes
without turning correlation into causation. The visual model stays compatible
with the aggregate-only, no-raw-event boundary of `apm report`.

## ADR-16 — Overlay APM aggregates on the static microservice graph as an opt-in, unpersisted layer

**Status:** Accepted.

**Context:** ADR-13 deferred visual overlays of observed APM data on the static
topology. Operators reviewing `export microservices --html` want to see which
indexed services and dependencies are actually busy, slow, or failing, without
waiting for the full SL-009/SL-010 ranking and alias-mapping adapters.

**Decision:** `systemlens export microservices --html FILE` accepts an opt-in
`--apm-overlay` flag (plus the same `--since`, `--environment`, `--endpoint`,
`--api-key`, `--max-relations`, and `--max-buckets` connection/window flags as
`apm export`, though `--max-buckets` defaults to 2,000 for the overlay versus
5,000 for `apm export`, because the overlay reads two aggregate queries per
export). When set, the export additionally queries `service_destination`
(edge call volume and error rate) and `service_transaction` (node average
latency) aggregates, using the same bounded, read-only adapter as `apm export`
and `apm report`. The result is joined to indexed microservice names through a
best-effort name correlation:

- An exact, case/separator-normalized name match is a **matched** observation.
- Otherwise, a normalized substring containment (either direction) is accepted
  as a **heuristic** match, scored by containment tightness. The single
  best-scoring indexed candidate receives the overlay and is visually flagged
  as heuristic; a genuine tie between equally-scored candidates is **ambiguous**
  and is not drawn on any node or edge.
- An observation with no matching indexed candidate is **unmapped**.

Unmapped and ambiguous observations never touch the graph; they are listed in a
separate side panel with their candidates (if any) so their absence from the
graph is never confused with zero runtime activity. The overlay is computed
only at export time, in memory; it is never written to SQLite, the persisted
snapshot, `architecture_relations`, or MCP.

**Consequences:** This intentionally trades some precision for earlier value:
unlike the conservative exact-alias policy already committed for the future
SL-010 correlation adapter, this HTML-only overlay may attach a heuristic,
possibly wrong, single-candidate match instead of leaving every non-exact name
unmapped. This is acceptable only because the overlay is visual, opt-in,
unpersisted, and explicitly labelled; it must not be reused as a source of
truth for `graph`, `dependency_graph`, `audit_dependency_graph`, impact/path
analysis, or any other static or persisted view, which remain governed by the
stricter exact-alias policy of a future SL-010. When SL-010 ships a configured
alias mechanism, an exact alias must take precedence over the heuristic
containment match for the same observed name.

## ADR-17 — Read compatible Elastic APM and OpenTelemetry runtime streams

**Status:** Accepted.

**Context:** Elastic APM Server/Elastic Agent writes APM metric and trace data
to `metrics-apm*` and `traces-apm*`, while the Elasticsearch OpenTelemetry
exporter writes compatible runtime aggregates and transaction events to
dedicated `.otel` data streams. Querying only the APM patterns yields an empty
report for an OpenTelemetry-only deployment.

**Decision:** The read-only SystemLens APM adapter queries the existing APM
patterns together with the narrowly targeted OpenTelemetry service-destination,
service-transaction, transaction, and generic-trace patterns. It keeps the
same bounded aggregate and field-projection contracts; it neither writes data
nor reads raw trace identifiers. API keys supplied as raw Elasticsearch
`id:secret` values are encoded to the `Authorization: ApiKey` header form, while
already encoded header values remain supported.

**Consequences:** One report can cover either ingestion path without a
configuration switch. If the same runtime signal is intentionally sent through
both paths, Elasticsearch may contain duplicate aggregates; SystemLens reports
the observed documents and does not infer cross-stream identity from forbidden
raw trace identifiers.

## ADR-18 — Restrict transaction workloads to distributed trace exemplars

**Status:** Accepted.

**Context:** Aggregate transaction metrics include health checks and local
operations. The runtime report's Transactions view must focus on workloads
that actually crossed a service boundary, without exporting raw traces.

**Decision:** SystemLens first performs a bounded Elasticsearch aggregation on
`trace.id`, retaining only trace buckets with spans from more than one
`service.name`. It uses those identifiers only in memory to constrain the
bounded transaction-event query, then retains aggregate transaction workloads
that have at least one selected event. Trace identifiers are never emitted,
persisted, or embedded in HTML.

**Consequences:** The Transactions view can be empty when no distributed trace
is present even though service metrics exist. Candidate traces are capped and
the report exposes any candidate or timeline limit in coverage; the aggregate
service and dependency views remain unchanged.

## ADR-19 — Export bounded, identifier-free distributed trace waterfalls

**Status:** Accepted.

**Context:** Aggregate transaction rows identify distributed workloads but do
not show the cross-service and database work that makes a trace actionable.
Operators need a compact view comparable to an APM waterfall, sorted by the
HTTP source service or the Kafka source topic.

**Decision:** After selecting multi-service trace candidates, SystemLens reads
at most 20 recent traces with `_source: false` and a bounded span count. It may
read `trace.id`, `span.id`, and `parent.id` only in memory to rebuild tree depth
and relative timings. The HTML projection excludes those identifiers and keeps
only safe operational fields. It groups first by an HTTP root service, then by
a Kafka topic inferred only from a producer span named `<topic> publish`; any
other trace is grouped by its root service.

**Consequences:** The report gains useful, bounded trace exemplars without
becoming a raw-trace export or a persistent store. A topic label is explicitly
an instrumentation-based inference, so deployments with different producer
span naming fall back to the root service instead of guessing a destination.
