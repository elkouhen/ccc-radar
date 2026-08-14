# SystemLens backlog

This backlog consolidates the architecture-review findings that were reproduced
against the current implementation. It is ordered by user-visible correctness
risk rather than estimated implementation effort.

## Priority and status conventions

- **P0** — indexing can publish stale or unusable architecture facts.
- **P1** — a supported HTTP/Kafka workflow can return a false positive, false
  negative, or a materially inconsistent result.
- **P2** — delivery confidence, portability, maintainability, or documentation
  debt without a known incorrect default topology.
- **Proposed** — accepted into the backlog but not started.
- **In progress** — implementation is underway.
- **Done** — acceptance criteria and verification are complete.

## Delivery order

| ID | Priority | Status | Outcome | Depends on |
|---|---|---|---|---|
| SL-001 | P0 | Done | Dependency-aware incremental indexing | — |
| SL-002 | P1 | Done | Preserve the analysis profile across every adapter | — |
| SL-003 | P1 | Done | Make persisted relations the only topology source of truth | SL-002 |
| SL-004 | P1 | Done | Remove the unsupported recursive DTO contract from current documentation | — |
| SL-005 | P1 | Done | Introduce collision-safe module and service identities | SL-003 |
| SL-006 | P2 | Proposed | Restore a reliable browser acceptance gate | — |
| SL-007 | P2 | Proposed | Add a portable HTML export without absolute local paths | — |
| SL-008 | P2 | Done | Retire the legacy vector runtime | — |

## P0 — Index correctness

### SL-001 — Dependency-aware incremental indexing

**Problem**

Endpoint extraction is scheduled by the hash of the endpoint's own file, but
some extracted facts depend on other files. A Spring configuration change does
not reanalyse Java endpoints that resolve its properties. A Maven or Gradle
identity change does not reattribute endpoints in unchanged source files.
Consequently, one successful incremental transaction can publish a
semantically mixed snapshot.

**Reproduced evidence**

- Changing a Kafka property from `payments.received` to `payments.changed`
  scanned only the YAML file and left Java endpoints on the old topic.
- Renaming the Maven artifact `order-service` to `orders-renamed` updated the
  module inventory while endpoints and relations remained attached to
  `order-service`.

**Implementation direction**

Start with conservative module-level invalidation:

- a Spring configuration change reanalyses Java sources in the affected
  service, including relevant Spring Cloud configuration consumers;
- a Maven/Gradle descriptor change reanalyses files whose module attribution or
  build-derived facts may have changed;
- deleted dependency inputs invalidate their dependants as well as their own
  persisted facts;
- retain the zero-scan fast path for a repository with no relevant changes.

A persisted file-dependency graph can replace this coarse invalidation later,
but is not required for the first correct implementation.

**Likely files**

- `src/systemlens/indexer.py`
- `src/systemlens/scanner.py`
- `src/systemlens/maven.py`
- `src/systemlens/gradle.py`
- `tests/test_ast_only.py`

**Acceptance criteria**

- An incremental Spring property edit updates all dependent REST/Kafka facts in
  the same run.
- Removing a resolved property makes the dependent endpoint dynamic or applies
  its declared default in the same run.
- Renaming a Maven `artifactId` or Gradle project name leaves no endpoint or
  architecture relation under the former module identity.
- A failed dependent reindex rolls back files, endpoints, modules,
  dependencies, relations, diagnostics, and signatures together.
- A second run without changes reports `scanned=0`.
- Regression tests cover configuration edits, configuration deletion, build
  identity changes, and rollback.

## P1 — Contract consistency

### SL-002 — Preserve the analysis profile across every adapter

**Problem**

The selected topic strategy is persisted as metadata but is not consistently
carried through CLI, MCP, HTML export, dependency analysis, or workspace
federation. In particular, MCP reindexing defaults to `default`, some analyses
ignore Strategy1, federation forces it off, and the HTML export always enables
the Strategy1 request/reply convention.

**Implementation direction**

Introduce an immutable `AnalysisProfile` or equivalent snapshot metadata that
contains at least the selected topic strategy and relevant extractor
signatures. Load it once with the inventory and require every query and adapter
to consume it. A federated inventory must retain the profile of each source
index or explicitly reject incompatible combinations.

**Likely files**

- `src/systemlens/architecture_inventory.py`
- `src/systemlens/architecture.py`
- `src/systemlens/mcp_server.py`
- `src/systemlens/cli.py`
- `src/systemlens/dependency_analysis.py`
- `src/systemlens/store.py`

**Acceptance criteria**

- `reindex_architecture` preserves the currently indexed strategy unless an
  explicit supported parameter changes it.
- CLI and MCP graph, audit, coverage, and indexing-issue views agree for the
  same index.
- Dependency analysis resolves Strategy1 REST targets when, and only when, the
  indexed profile enables Strategy1.
- HTML request/reply links are emitted only for a Strategy1 inventory.
- Federation neither silently disables nor globally invents Strategy1 facts.
- Focused MCP contract tests cover both `default` and `strategy1` indexes.

### SL-003 — Make persisted relations the only topology source of truth

**Problem**

ADR-8 defines `architecture_relations` as canonical, but several read paths
rebuild service topology from endpoints. `ArchitectureSnapshot` contains both
persisted relations and independently derived graph edges, while export,
federation, dependency analysis, and audits can each perform another
derivation. This permits drift in resolution rules, confidence, and profile
handling.

**Implementation direction**

- Use persisted relations for every service-to-service and service-to-resource
  topology decision.
- Use endpoints only to decorate relations with route/topic labels and source
  evidence.
- Load and normalize persisted relations during read-only federation.
- Remove or strictly derive the duplicate `edges` projection from relations;
  do not call the topology resolver from read adapters.

**Likely files**

- `src/systemlens/architecture.py`
- `src/systemlens/architecture_inventory.py`
- `src/systemlens/workspace.py`
- `src/systemlens/graph.py`
- `src/systemlens/dependency_analysis.py`
- `src/systemlens/render.py`
- `src/systemlens/mcp_server.py`

**Acceptance criteria**

- CLI, MCP, HTML, JSON, audit, path, and dependency views consume the same
  persisted topology relations.
- No read adapter independently infers an internal service target.
- Relation origin, confidence, source path, and line remain stable across all
  output formats.
- A federated view preserves relations and evidence from each read-only source
  index.
- Tests deliberately provide endpoints that could be re-matched differently
  and verify that persisted relations remain authoritative.

### SL-004 — Align the DTO documentation with the snapshot-only contract

**Problem**

The functional and technical specifications currently promise recursive DTO
field and enum navigation. ADR-9, the export snapshot contract, implementation,
and tests instead expose only indexed Kafka payload identities; DTO fields and
project definitions are empty.

**Decision for the current release**

Keep ADR-9 authoritative. Do not restore render-time source parsing. Recursive
DTO inspection remains unavailable until a dedicated indexed DTO contract is
designed and persisted.

**Likely files**

- `docs/SPEC-FONC.md`
- `docs/SPEC-TECH.md`
- `README.md` if the feature is mentioned during the edit

**Acceptance criteria**

- Functional and technical specifications describe only payload identities,
  producers, consumers, topics, and indexed evidence currently available.
- No authoritative document claims recursive DTO fields or enums are present.
- ADR-9 and the export snapshot contract remain consistent with each other.
- A separately approved future ticket is required before adding an indexed DTO
  schema or restoring a deep inspector.

### SL-005 — Introduce collision-safe module and service identities

**Problem**

Persistence identifies modules by path, but application projections and
federation key them by display name. Duplicate Maven artifact IDs or Gradle
project names can overwrite modules, services, endpoints, and dependency
relations silently.

**Implementation direction**

Define a stable identity containing the index origin and module coordinates or
relative build path. Keep the current name as a user-facing alias. Resolution
by alias must return an explicit ambiguity instead of selecting one module.

**Likely files**

- `src/systemlens/models.py`
- `src/systemlens/modules.py`
- `src/systemlens/store.py`
- `src/systemlens/workspace.py`
- `src/systemlens/architecture.py`
- `src/systemlens/relations.py`
- `docs/SPEC-TECH.md`
- `docs/ADR.md`

**Acceptance criteria**

- Two modules or federated services with the same display name coexist without
  data loss.
- Relations reference stable identities while output retains readable aliases.
- CLI and MCP lookups report ambiguity with the candidate paths/origins.
- Database migration is additive or provides an explicit compatibility error.
- Tests cover duplicate names in one multi-module index and across separately
  indexed federated repositories.

## P2 — Delivery, portability, and maintenance

### SL-006 — Restore a reliable browser acceptance gate

**Problem**

The only browser integration test is excluded from the default suite and
currently fails at an 800×450 viewport because it attempts to use a relation
filter inside a closed advanced-controls disclosure. The intended UX and the
test contract have diverged.

**Implementation direction**

Retain progressive disclosure. Update the browser scenario to open the
advanced controls before exercising relation filters, then verify the complete
task flow at the constrained viewport. Run the test in a dedicated CI job with
a controlled Chromium version.

**Likely files**

- `tests/test_browser_export.py`
- `src/systemlens/render.py` only if the verified flow remains unusable
- CI configuration when present

**Acceptance criteria**

- The browser test passes at 800×450 without forced selectors or hidden-element
  interaction.
- Relation filtering, resource search, Kafka itinerary, details, and source
  evidence remain usable in that viewport.
- JavaScript page errors fail the test.
- A documented CI command runs the slow browser suite separately from unit
  tests.

### SL-007 — Add a portable HTML export without absolute local paths

**Problem**

HTML exports embed absolute `vscode://file` links for modules, endpoints,
findings, and OpenAPI contracts. Sharing an export therefore discloses local
usernames and workspace layout.

**Implementation direction**

Add a documented portable mode such as `--portable` or `--no-local-links`.
Portable output should retain repository-relative evidence but omit absolute
filesystem and WSL paths. Local deep links can remain available in the default
developer-oriented mode if the trade-off is documented.

**Likely files**

- `src/systemlens/cli.py`
- `src/systemlens/render.py`
- `docs/SPEC-FONC.md`
- `README.md`
- `tests/test_render.py`

**Acceptance criteria**

- Portable HTML contains no absolute POSIX, Windows, WSL, or `vscode://file`
  path.
- Relative file and line evidence remains visible.
- Existing local-link behavior is either preserved by default or changed
  through an explicit documented product decision.
- Tests inspect the complete embedded JSON payload and rendered links for path
  disclosure.

### SL-008 — Retire the legacy vector runtime

**Resolution**

The unused vector runtime, its native extension loading, KNN store APIs and
NumPy/sqlite-vec dependencies were removed. Findings search remains a purely
lexical compatibility path. Existing indexes may retain unreachable legacy
vector tables; normal reads no longer load or access their SQLite extension.

**Likely files**

- `src/systemlens/store.py`
- `src/systemlens/models.py`
- `src/systemlens/search.py`
- `src/systemlens/render.py`
- `pyproject.toml`
- `docs/SPEC-TECH.md`
- `IMPLEMENTATION_SUMMARY.md`

**Acceptance criteria**

- A fresh architecture index can be created and queried without native vector
  dependencies in the normal runtime dependency set.
- The supported behavior for an existing legacy database is documented and
  tested: migrate, reject clearly, or provide an explicit conversion command.
- No public CLI or MCP contract references the retired vector behavior.
- Package metadata describes the current SystemLens architecture product.
- Unit tests, static checks, package build, and a fresh-install smoke test pass.

## Required verification before closing P0/P1 tickets

Run, at minimum:

```bash
uv run pytest
uv run ruff check src tests
uv run mypy src/systemlens
uv build
```

For changes affecting the HTML export, also run:

```bash
uv run pytest -m slow tests/test_browser_export.py
```

Each completed ticket must update `docs/SPEC-FONC.md`, `docs/SPEC-TECH.md`, or
`docs/ADR.md` when it changes the corresponding public behavior, data model, or
durable design decision.
