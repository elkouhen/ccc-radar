# Architecture Decision Records — archlens (`archlens`)

## ADR-1 — Local AST extraction is the sole analysis source

**Status:** Accepted.

**Context:** Architecture facts need to be reproducible offline and traceable
to source locations without a separate analysis runtime.

**Decision:** Parse Java source locally with Tree-sitter and derive Spring REST,
Kafka and module facts from AST nodes and deterministic local configuration.

**Consequences:** The project has no external analyzer, rule-pack or source-code
search dependency. Dynamic values are surfaced as unresolved facts instead of
being guessed.

## ADR-2 — SQLite is the local fact store

**Status:** Accepted.

**Context:** Incremental inventory, MCP queries and graph rendering require a
portable local state.

**Decision:** Persist files, endpoints, modules, dependencies and architecture
relations in `.archlens/findings.db`. The filename is retained for compatibility
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

## ADR-6 — ArchLens is the public product and state namespace

**Status:** Accepted.

**Context:** The former product name and `cccr` command were opaque to users,
while the tool's purpose is to make a local architecture view easier to read.

**Decision:** Rename the distribution, Python package, CLI command, MCP server
name, generated-export labels and state directory to `ArchLens` / `archlens`.
The project state is now stored in `.archlens/`.

**Consequences:** This is a breaking rename. Existing `.cccr/` configuration
and index data are not read by ArchLens: run `archlens init` and `archlens
index` in each repository to create a fresh `.archlens/` inventory. Trace
environment variables use the `ARCHLENS_` prefix.
