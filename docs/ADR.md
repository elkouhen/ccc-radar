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

## ADR-23 — MCP control in two phases with a graph enrichment layer

The MCP exposes `index_repository`, followed by dedicated operations to add,
inspect and remove graph facts. Added facts are persisted in `graph_facts`,
separately from relations derived from source code.

This separation lets an AI complete conventions that cannot be extracted
deterministically, while ensuring reindexing preserves enrichment and MCP
deletion never destroys source evidence. `architecture_graph` merges both
layers for reading. Added evidence paths remain relative to the repository.
