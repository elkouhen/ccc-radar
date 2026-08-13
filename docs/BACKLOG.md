# Architecture review and improvement backlog

This document replaces the previous product backlog. It records the findings
from a repository-wide architecture review performed on 2026-08-13 against
commit `eda7a73`.

The review covered the product and technical specifications, ADRs, CLI and MCP
adapters, indexing, persistence, extraction, graph construction, generated
HTML, tests, packaging and repository hygiene. It did not reassess CDN
delivery, ARIA semantics or keyboard navigation because those concerns are
explicitly deferred in `AGENTS.md`.

## Executive assessment

SystemLens has a sound product direction: local AST analysis, explicit source
evidence, conservative handling of dynamic values, an incremental SQLite
snapshot and read-only workspace federation are appropriate architectural
choices. The codebase also has a useful test corpus and currently passes its
default quality gates.

The main architectural risk is no longer extraction coverage but divergence
between several overlapping representations of the same architecture. Raw
endpoints, materialized `ArchitectureRelation` rows, `GraphEdge`, dependency
graph dictionaries and renderer-specific dictionaries each re-derive part of
the topology. CLI and MCP adapters then compose those projections through
different paths. This makes consistency expensive and has already produced a
critical contradiction in REST resolution.

Current verification baseline:

- `pytest -q`: 110 passed, 1 deselected;
- Ruff default checks: passed;
- mypy: passed for 27 source files;
- Ruff C901 audit: 32 functions above complexity 10, including
  `_extract_java_architecture` at 40 and both main graph builders at 25;
- largest modules: `render.py` 4,552 lines, `scanner.py` 3,105 lines,
  `cli.py` 1,796 lines and `store.py` 993 lines.

## Priority model

- **P0:** can produce materially false architecture data or corrupt the
  persisted snapshot.
- **P1:** creates contract divergence, hides incomplete analysis or makes a
  central subsystem unsafe to evolve.
- **P2:** significant maintainability, performance, test or product-coherence
  debt with a contained current impact.
- **P3:** repository hygiene or optional simplification.

## Recommended execution order

1. Make REST dependency resolution evidence-based and fail closed.
2. Establish one canonical architecture projection shared by CLI, MCP, audits
   and exports.
3. Make indexing atomic and expose extraction failures.
4. Move DTO/OpenAPI enrichment into the indexed snapshot.
5. Split the scanner, renderers and legacy persistence surface behind stable
   contracts.

---

## ARCH-001 — Stop fabricating REST dependencies from route coincidence

**Priority:** P0
**Status:** Implemented (2026-08-13)
**Area:** Data quality, graph construction

### Evidence

Before this task was implemented, `graph.build_graph` matched every REST call
against every served route. When a call had no target hint,
`_service_matches_hint(..., None)` accepted every service and a matching
method/path was enough to emit an inter-service edge. The previous test
`test_build_graph_links_compatible_rest_routes_without_a_service_target`
codified this permissive behaviour. When a hint existed, service matching also
accepted suffixes and arbitrary substring containment.

This contradicts the product requirement that a dependency must not be
invented and the documented rule that an HTTP edge requires a resolved caller
and target. Two unrelated services exposing `GET /health`, or names such as
`order-service` and `order-history-service`, can therefore create false edges.

### Improvement

Introduce an explicit REST target-resolution result with evidence and a closed
set of outcomes: `resolved`, `ambiguous`, `external` or `unresolved`. Only an
exact normalized service alias, a load-balanced URI, an explicit configured
domain or another documented high-confidence alias may create an internal
edge. Route compatibility should select a resource only after the service has
been resolved; it must never resolve the service by itself.

Keep unresolved calls as endpoint facts and expose them through coverage and
indexing issues. Do not silently drop their evidence.

### Acceptance criteria

- A targetless `GET /health` call does not link to services merely because
  they expose `GET /health`.
- A target hint maps only through explicit, normalized aliases; substring
  matching is removed.
- One exact service with several compatible routes produces evidenced resource
  edges; several possible services produce an ambiguity issue and no edge.
- CLI graph, MCP graph, dependency audit and HTML export return the same REST
  resolution outcome.
- Existing conservative Kafka and dynamic-value behaviour is unchanged.

### Likely files

`graph.py`, `relations.py`, `dependency_analysis.py`, `architecture.py`,
`tests/test_graph.py`, `tests/test_dependency_analysis.py`, `SPEC-FONC.md` and
`SPEC-TECH.md`.

### Completion

Internal REST edges now require exactly one case-normalized explicit service
alias. Route equality no longer identifies a service, and partial name matches
are rejected. Calls without a unique target remain visible as unresolved HTTP
facts in indexing issues and as external API calls in the dependency graph.

---

## ARCH-002 — Define one canonical architecture graph projection

**Priority:** P1
**Status:** Proposed
**Area:** Architecture, contract consistency

### Evidence

The same domain is represented independently by:

- persisted `ArchitectureRelation` objects built by `relations.py`;
- request-time `GraphEdge` objects built by `graph.py`;
- dictionary-based dependency nodes and edges in `dependency_analysis.py`;
- a separate `ArchitectureCatalog` projection in `architecture.py`;
- renderer-specific nodes and links in `render.py`.

The materialized relation table is currently consumed only by inventory
coverage, while the main graph and dependency views re-derive relations from
endpoints. Kafka manifest precedence is duplicated between `graph.py` and
`dependency_analysis.py`. REST resolution rules are consequently not enforced
from one place.

### Improvement

Define a typed, immutable `ArchitectureSnapshot` and one canonical relation
projection containing identity, source, target, kind, confidence, provenance
and evidence. Build it once from persisted facts. Catalog, graph, audit, CLI,
MCP and renderers should query or adapt this projection rather than rebuilding
it.

Decide explicitly whether relations are persisted or derived. If persisted,
they are the canonical snapshot and must be invalidated transactionally. If
derived, remove the materialized table and compute through one domain service.

### Acceptance criteria

- A relation has one identity and one confidence/provenance vocabulary across
  all public surfaces.
- CLI JSON and equivalent MCP tools are contract-tested against the same
  application service.
- Manifest precedence, dynamic identities, external services and MongoDB
  scoping are implemented once.
- `architecture_relations` is either the documented source of truth or removed
  after a compatibility migration.
- An ADR records the selected projection and persistence strategy.

### Likely files

`models.py`, `architecture_inventory.py`, `relations.py`, `graph.py`,
`dependency_analysis.py`, `architecture.py`, `store.py`, `cli.py`,
`mcp_server.py`, `SPEC-TECH.md` and `ADR.md`.

---

## ARCH-003 — Make one index run an explicit atomic snapshot transaction

**Priority:** P1
**Status:** Proposed
**Area:** Persistence, reliability

### Evidence

`index_repo` removes deleted and changed endpoint rows, writes hashes and meta
signatures, replaces modules, replaces module dependencies and finally replaces
materialized relations through many public `Store` calls. The context manager
commits only on a clean exit, which provides implicit rollback today, but this
invariant is neither represented as a transaction API nor protected by a
failure-injection test. `_create_schema` also performs and commits migrations
when any writable `Store` is opened, independently of an index transaction.

The implementation comment claims a transactional inventory, but callers can
invoke the same mutation methods in a partially ordered way. Future commits or
new adapters could expose an inconsistent snapshot without violating a type or
API boundary.

### Improvement

Add a `Store.transaction()` or `replace_architecture_snapshot(...)` boundary
that owns the full index mutation. Separate schema migration from domain
writes. Make rollback explicit and document the isolation/concurrency model for
the long-lived MCP process.

### Acceptance criteria

- A forced exception after endpoint replacement and before relation
  replacement leaves the previous files, endpoints, modules and relations
  intact.
- Meta signatures become visible only with the corresponding facts.
- Writable and read-only connection lifecycles have explicit tests.
- Concurrent read behaviour during an index refresh is documented and tested
  at the supported level.

### Likely files

`store.py`, `indexer.py`, `mcp_server.py`, `tests/` and `SPEC-TECH.md`.

---

## ARCH-004 — Persist extraction diagnostics instead of silently returning no facts

**Priority:** P1
**Status:** Proposed
**Area:** Observability, data quality

### Evidence

`java_parser.parse_java` returns `None` for both read failures and any syntax
error. Many scanner, module, YAML, JSON and build-parser paths then return an
empty list or continue. The index report exposes counts but not failed files or
extractors. `doctor` reports AST extraction as ready without parsing a probe,
opening the database or checking schema compatibility. Consequently, “zero
facts” can mean either “nothing found” or “analysis failed”.

This weakens the product promise that uncertainty is visible.

### Improvement

Introduce structured `ExtractionDiagnostic` facts with extractor, file,
category, severity and safe error detail. Persist them with the file snapshot
and merge them into coverage/indexing issues. Reserve exceptions for failures
that invalidate the whole run; retain best-effort extraction for isolated
files, but report it.

Upgrade `doctor` to verify Tree-sitter initialization, SQLite/sqlite-vec
availability, schema compatibility and snapshot freshness without mutation.

### Acceptance criteria

- Invalid Java, malformed OpenAPI, unreadable files and invalid build files are
  distinguishable from an empty repository.
- `systemlens index`, `doctor`, coverage, MCP and HTML inventory status expose
  the same diagnostic counts.
- Diagnostics do not leak source contents or secrets beyond the documented
  evidence policy.
- Tests cover partial extraction and a fatal initialization failure.

### Likely files

`java_parser.py`, `scanner.py`, `modules.py`, `maven.py`, `gradle.py`,
`models.py`, `store.py`, `indexer.py`, `doctor.py`, `architecture.py` and
`render.py`.

---

## ARCH-005 — Keep exports faithful to the persisted snapshot

**Priority:** P1
**Status:** Proposed
**Area:** Snapshot consistency, export architecture

### Evidence

The specifications describe `render.py` as an output adapter and module facts
as the exact audited repository snapshot. In contrast, `render_graph_html`
reads local OpenAPI files and `_kafka_dto_views` recursively scans and parses
every Java file under supplied source roots at export time. If source changes
after indexing, the graph topology and its DTO/OpenAPI details can represent
different revisions. Rendering also depends on filesystem availability, which
is problematic for federation and reproducibility.

### Improvement

Move DTO declarations, enum values and normalized OpenAPI summaries into the
indexing pipeline with source evidence and hashes. Renderers should accept only
typed snapshot data. If live enrichment remains temporarily supported, mark it
as such in the export and reject mixed revisions.

### Acceptance criteria

- Changing a Java DTO or OpenAPI file after indexing does not alter an export
  until reindexing.
- Federated and local exports use the same DTO/OpenAPI data contract.
- `render.py` performs no repository discovery or source parsing.
- DTO ambiguity and missing source are stored as explicit diagnostics.
- `SPEC-TECH.md` and an ADR define snapshot consistency.

### Likely files

`models.py`, `scanner.py`, `indexer.py`, `store.py`, `architecture_inventory.py`,
`render.py`, `tests/test_render.py`, `SPEC-TECH.md` and `ADR.md`.

---

## ARCH-006 — Split the extraction engine by framework and concern

**Priority:** P1
**Status:** Proposed
**Area:** Maintainability, extraction correctness

### Evidence

`scanner.py` contains 3,105 lines and 123 top-level functions. It combines
Spring property loading, Java type inference, REST server extraction, several
HTTP clients, gateway parsing, Kafka APIs, Strategy1 conventions and two
manifest formats. `modules.py` contains another Java architecture extraction
pipeline for MongoDB, Kafka method inventory and blocking points, importing
helpers from Maven while `scanner.py` imports module-discovery helpers.

The C901 audit flags central extraction functions, including
`infer_kafka_endpoints` at 21 and `_extract_java_architecture` at 40. This makes
framework additions risky and encourages regex/AST logic to be duplicated.

### Improvement

Introduce an extractor protocol returning facts plus diagnostics. Split
framework extractors into focused modules such as `extractors/rest_server.py`,
`rest_clients.py`, `kafka.py`, `mongodb.py`, `openapi.py` and `manifests.py`.
Keep shared AST traversal and value resolution in small infrastructure modules.
Remove cross-imports between scanner and module discovery.

### Acceptance criteria

- Each extractor has a typed input/output contract and focused fixtures.
- No extractor imports CLI, MCP, renderer, store or another orchestration
  layer.
- Main extractor entry points remain below an agreed complexity budget.
- Adding one framework does not require editing a central multi-thousand-line
  dispatch function.
- Endpoint inventory signatures are derived from extractor versions rather
  than one manually edited opaque string.

### Likely files

`scanner.py`, `modules.py`, `java_parser.py`, new `extractors/` modules,
`indexer.py`, tests, `ARCHITECTURE.md` and `SPEC-TECH.md`.

---

## ARCH-007 — Split renderers and remove domain derivation from presentation code

**Priority:** P1
**Status:** Proposed
**Area:** Maintainability, UX consistency

### Evidence

`render.py` contains 4,552 lines and mixes legacy findings text/JSON,
architecture projection, DTO parsing, LikeC4 generation, three complete HTML
applications, WebGL shaders, graph layout algorithms, VS Code URI generation
and flow/module/workspace presentation. Its main HTML template alone contains
large embedded CSS and JavaScript programs that are validated mostly through
string assertions and one browser scenario.

This structure makes visual consistency between microservice, topic, MongoDB
and module views expensive and couples Python changes to client-side behaviour.

### Improvement

Create a renderer package with one typed view-model builder and separate
adapters for JSON, text, LikeC4 and HTML. Move HTML/CSS/JavaScript to package
resources or small composable templates. Share design tokens and graph-node
semantics between the microservice and module exports. Keep connectivity,
confidence and relation aggregation in application/domain services.

### Acceptance criteria

- Presentation modules receive view models and do not derive architecture
  relations or parse source.
- Microservice, topic, MongoDB and module views share documented visual tokens
  and interaction primitives.
- JavaScript can be linted and tested independently of a Python string.
- Browser tests cover empty, isolated, ambiguous, large and degraded-data
  states.
- No generated-output contract changes without `SPEC-FONC.md` updates.

### Likely files

`render.py`, new `renderers/` and template resource files,
`tests/test_render.py`, `tests/test_browser_export.py`, `ARCHITECTURE.md` and
`SPEC-TECH.md`.

---

## ARCH-008 — Replace overloaded strings with typed domain identities

**Priority:** P2
**Status:** Proposed
**Area:** Domain model, type safety

### Evidence

`MessageEndpoint` stores either a Kafka topic or a REST `METHOD /path` value in
the field named `topic`. `role`, `system`, `source`, relation kind, confidence,
module kind and node kind are unrestricted strings. Several modules then parse
those values using string partitioning and repeat mapping dictionaries. Invalid
combinations such as `system="rest"` with `role="consume"` are representable and
can reach persistence.

### Improvement

Introduce discriminated REST and Kafka fact types, or a common protocol with
typed `Literal`/enum fields and validated constructors. Define explicit
`ServiceId`, `TopicId`, `ApiId` and scoped `CollectionId` identities. Preserve
the public JSON compatibility layer during migration.

### Acceptance criteria

- Invalid system/role combinations cannot be constructed through public
  domain APIs.
- REST methods and paths are modeled separately and normalized once.
- Kafka topics and dynamic expressions have distinct identity semantics.
- JSON and SQLite migration compatibility is covered by tests.

### Likely files

`models.py`, `scanner.py`, `store.py`, `relations.py`, `graph.py`,
`architecture.py`, `render.py`, `SPEC-TECH.md` and `ADR.md`.

---

## ARCH-009 — Centralize CLI and MCP use cases and error contracts

**Priority:** P2
**Status:** Proposed
**Area:** Delivery adapters, public contracts

### Evidence

`cli.py` is 1,796 lines with 107 top-level functions and supports overlapping
legacy callback syntax plus explicit subcommands. `mcp_server.py` independently
loads stores and inventories and contains its own action router. Several MCP
read tools open a writable `Store` even though they only query. Workspace
discovery is also invoked through different functions. There are no direct MCP
contract tests, while CLI coverage focuses on a subset of module and indexing
flows.

### Improvement

Create application-level query services with typed request/response objects and
domain errors. Keep Typer and MCP functions limited to validation,
serialization and exit/tool-error mapping. Decide which compatibility aliases
remain public, deprecate the rest and publish one command grammar.

### Acceptance criteria

- Each documented CLI operation and MCP equivalent calls the same use case.
- Read-only operations always use read-only inventory/store access.
- Unknown object, ambiguity, stale index and incompatible schema have stable
  cross-surface error codes.
- Contract tests cover every documented MCP tool and representative CLI alias.
- `SPEC-FONC.md` is generated from or checked against the registered commands
  and tools where practical.

### Likely files

`cli.py`, `mcp_server.py`, `architecture.py`, `architecture_inventory.py`, new
application service modules and contract tests.

---

## ARCH-010 — Remove retired findings, vector search and mandatory sqlite-vec

**Priority:** P2
**Status:** Proposed
**Area:** Dependency footprint, legacy architecture

### Evidence

The PRD explicitly excludes security/quality scans, and the technical
specification calls `Finding` and the `findings` table legacy compatibility
state. Nevertheless, `Finding`, `search.py`, code chunks, three vector-table
families, NumPy and sqlite-vec remain in the runtime package. Every `Store`,
including read-only architecture queries, loads the sqlite-vec extension.
Renderer, federation and inventory contracts still carry findings despite the
indexer permanently adding none and clearing old ones.

This increases install and native-extension risk for features outside the
current product scope and obscures the architecture model.

### Improvement

Define a time-bounded compatibility policy. Migrate or archive old findings if
required, then remove the dead model, search/render functions, embedding tables
and mandatory NumPy/sqlite-vec dependencies. If legacy inspection must remain,
isolate it in an optional compatibility package that is not loaded by normal
architecture operations.

### Acceptance criteria

- Normal indexing, CLI, MCP and export paths do not import NumPy or load a
  SQLite extension.
- The active schema contains only current product facts.
- Opening an old database yields a clear migration/export path.
- PRD, specifications, package dependencies and code describe the same scope.

### Likely files

`models.py`, `search.py`, `store.py`, `render.py`, `workspace.py`,
`architecture_inventory.py`, `pyproject.toml`, `SPEC-TECH.md` and `ADR.md`.

---

## ARCH-011 — Add enforceable complexity and architectural dependency gates

**Priority:** P2
**Status:** Proposed
**Area:** Quality engineering

### Evidence

Default Ruff and mypy checks pass, but C901 is not enabled. A dedicated C901
run reports 32 functions above complexity 10. The documented dependency rules
are prose only. There is no repository CI configuration, no import-boundary
test and no coverage threshold. The default pytest configuration deselects the
only browser integration test.

### Improvement

Adopt a staged quality budget rather than enabling a failing global threshold
at once. Freeze the current high-complexity list, prevent regressions and lower
the budget as ARCH-006/007/009 land. Add import-boundary tests for the runtime
layers and a CI workflow covering supported Python versions, Ruff, mypy, unit
tests and a separately reported browser job.

### Acceptance criteria

- New or modified functions cannot exceed the agreed complexity budget without
  an explicit exception.
- Tests fail when discovery imports delivery/rendering or when renderers import
  persistence/indexing.
- CI runs on every proposed change and publishes unit and browser outcomes
  separately.
- Supported Python versions match mypy and CI configuration.

### Likely files

`pyproject.toml`, CI configuration, architecture tests and the modules targeted
by ARCH-006/007/009.

---

## ARCH-012 — Define performance budgets and protect large-repository paths

**Priority:** P2
**Status:** Proposed
**Area:** Performance, scalability

### Evidence

Indexing recursively walks and hashes every eligible file before extraction.
Module and Gradle discovery perform additional recursive scans. Graph
construction compares calls to served routes and Kafka producers to consumers
with nested loops. The default HTML layout applies pairwise node repulsion for
many iterations in the browser. DTO export currently scans Java sources again.

There are limits for simple-path exploration, but no documented repository,
file, graph or export-size budgets and no benchmark suite. These algorithms can
be acceptable for small examples while degrading abruptly on a large
multi-service workspace.

### Improvement

Measure before optimizing. Add benchmark fixtures and trace metrics for file
walks, parse cache hits, relation joins, snapshot size, HTML payload size and
browser layout time. Index facts by normalized service/topic/route keys instead
of Cartesian matching. Add configurable safety limits with explicit diagnostics
rather than silent truncation.

### Acceptance criteria

- A documented reference workspace has indexing and export budgets in CI or a
  repeatable benchmark command.
- REST/Kafka joins scale by keyed candidate sets rather than all-pairs scans.
- Large files and graphs produce explicit warnings or controlled degraded
  modes.
- Export generation does not repeat source discovery after ARCH-005.

### Likely files

`indexer.py`, `modules.py`, `gradle.py`, `graph.py`, `relations.py`, `render.py`,
benchmark fixtures and `SPEC-TECH.md`.

---

## ARCH-013 — Validate configuration types and remove inert compatibility options

**Priority:** P2
**Status:** Proposed
**Area:** Configuration, product coherence

### Evidence

`load_config` converts `include` and `exclude` with `list(...)` without checking
that the YAML value is a list of strings; a scalar becomes a list of characters
and a mapping is accepted as keys. `min_severity` remains in every newly
generated configuration even though security findings and severity filtering
are outside scope and it has no effect on endpoint extraction. Unknown keys are
silently ignored.

### Improvement

Version the configuration schema, validate all values strictly and reject or
warn on unknown keys. Remove `min_severity` from new configurations and retain
it only in a documented migration reader if old configurations must remain
loadable.

### Acceptance criteria

- Invalid scalar/mapping include and exclude values fail with an actionable
  path-specific message.
- Unknown keys are handled by a documented strictness policy.
- Fresh `systemlens init` output contains only active settings.
- A migration test covers the selected legacy behaviour.

### Likely files

`config.py`, `doctor.py`, `indexer.py`, configuration tests, `README.md`,
`SPEC-FONC.md` and `ADR.md` if compatibility changes.

---

## ARCH-014 — Adopt one language policy for generated user interfaces and messages

**Priority:** P3
**Status:** Proposed
**Area:** Product consistency, documentation

### Evidence

Repository documentation rules require English for user-facing documentation
and generated-document templates. The specifications and README are English,
while CLI errors, MCP docstrings, doctor output and most generated HTML labels
are French; some HTML titles and LikeC4 labels are English. Several French
labels omit accents, creating a third style. There is no locale setting or
documented language policy.

### Improvement

Choose either one product language or explicit localization. Keep stable
machine-readable error codes separate from translated messages. Consolidate UI
strings so all export variants use the same terminology.

### Acceptance criteria

- The README, specifications, CLI, MCP descriptions and generated exports
  follow a documented language policy.
- Public JSON field names and error codes remain stable.
- Terms such as integration, endpoint, relation, connectivity and inventory
  have one glossary across surfaces.

### Likely files

`cli.py`, `mcp_server.py`, `doctor.py`, `render.py`, documentation and message
contract tests.

---

## ARCH-015 — Clean stale project artifacts and make the release metadata truthful

**Priority:** P3
**Status:** Proposed
**Area:** Repository hygiene, release engineering

### Evidence

Tracked root documents `IMPLEMENTATION_SUMMARY.md` and `improve.md` describe
historical work using obsolete concepts such as Semgrep and backlog/ADR numbers
that are absent from the current authoritative documentation. Source comments
also cite retired `BACKLOG-*` and `ADR-30+` identifiers while the current ADR
log stops at ADR-6. The tracked `dist/` directory contains artifacts named
`archlens-0.1.0`, while the package is SystemLens. `pyproject.toml` still has a
French, findings-oriented description and lacks standard project/readme URL
metadata. The product has undergone several breaking renames while remaining
at version `0.1.0` with no changelog or tags.

### Improvement

Remove or archive stale implementation notes, obsolete comments and old build
artifacts. Build releases in CI from a clean tree. Complete package metadata,
define versioning/release policy and add a changelog once external distribution
is intended.

### Acceptance criteria

- No tracked artifact uses a retired product name unless explicitly archived.
- Source comments link only to existing ADRs/backlog items or explain the
  decision directly.
- Package name, description, README, version and built artifact metadata agree.
- A clean build/test/install smoke check is reproducible.

### Likely files

`IMPLEMENTATION_SUMMARY.md`, `improve.md`, `dist/`, source comments,
`pyproject.toml`, `README.md`, release automation and `ADR.md`.

---

## Previously completed product work

The former backlog contained two completed items: route/itinerary discovery
from the Explore view and an ordered DTO-aware itinerary explanation. Their
observable behaviour is now part of `SPEC-FONC.md` and their tests. They are not
carried as open backlog tasks because this file is an improvement backlog, not
an implementation history.

## Explicitly deferred concerns

Per `AGENTS.md`, the following generated-HTML concerns are intentionally not
prioritized here until the user reopens them:

- CDN dependencies and offline bundling of browser libraries;
- ARIA semantics;
- keyboard navigation.
