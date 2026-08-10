# Architecture Decision Records — ccc-radar (`cccr`)

> One entry per structuring decision: context, decision, consequences.
> ADR-1 to ADR-6 capture the initial framing. ADR-7 onward records decisions
> taken during implementation in response to tool behavior, architecture
> pressure, or execution-environment constraints.

---

## ADR-1 — Companion Python package, not a fork of `cocoindex-code`

**Status**: Accepted.

**Context**: the PRD (§13, open question 1) hesitated between contributing
upstream to `cocoindex-code` or shipping a separate package.

**Decision**: `ccc-radar` (CLI `cccr`) is an independent Python package, with no
dependency on `ccc` internal APIs. The join with `ccc` happens at query time,
through a subprocess (`ccc search ...`) and file + line-range overlap — never
by importing internal `ccc` code.

**Consequences**: zero breakage risk if `ccc` changes its internal APIs; in
return, the join depends on `ccc`'s **text** output format (see ADR-10) rather
than on a stable API.

---

## ADR-2 — Single SQLite store, brute-force cosine

**Status**: Superseded by ADR-17 (storage remains a single SQLite file, but
similarity is no longer computed with NumPy brute force).

**Context**: a repo contains at most a few thousand findings.

**Decision**: a single `.cccr/findings.db` file (SQLite), embeddings stored as
`BLOB` (`float32.tobytes()`), cosine similarity computed in Python/NumPy by
brute force (load all embeddings, dot product).

**Consequences**: latency < 50 ms for a few thousand findings, no dependency on
an external vector index (LMDB/ANN). Would not scale beyond ~50-100k findings —
not addressed, outside the V1 target scale.

---

## ADR-3 — Embeddings via `sentence-transformers`, local Jina code embeddings as the default model

**Status**: Accepted.

**Context**: local-first constraint and network/TLS instability observed during
Hugging Face downloads in enterprise environments.

**Decision**: `sentence-transformers`, default model
`~/models/jina-code-embeddings-1.5b`, configurable through
`config.embedding_model`. The repo documents download via `hf download` into a
dedicated local directory, with `SSL_CERT_FILE` explicitly removed from the
environment to work around TLS failures observed on some workstations.

**Consequences**: first use no longer needs network access if the model has
already been pre-downloaded to `~/models/jina-code-embeddings-1.5b`; a model
change still triggers full re-embedding of the database
(`indexer.index_repo`, comparing `meta.embedding_model` vs
`config.embedding_model`).

---

## ADR-4 — Local Semgrep rules in tests, never registry packs

**Status**: Accepted.

**Context**: determinism and offline test execution.

**Decision**: test fixtures (`tests/fixtures/vuln_repo/rules/rules.yml`) define
local Semgrep rules; no test uses a registry `p/...` pack.

**Consequences**: reproducible tests without network access; in return, they do
not cover behavioral quirks of real registry packs (versions, extra metadata).

---

## ADR-5 — Stable finding identity: hash(rule + path + normalized snippet)

**Status**: Accepted.

**Context**: allow diffs between indexing runs and deduplication without
depending on line numbers (which move).

**Decision**:
`compute_finding_id = sha256(f"{rule_id}|{path}|{normalized_snippet}")[:16]`,
where `normalized_snippet = " ".join(snippet.split())`.

**Consequences**: survives line shifts caused by edits elsewhere in the file.
Accepted trade-off later identified as a real limit in review: two findings of
the same rule/path with identical snippets (duplicated line, or empty snippet
on an unreadable file) still collide.

---

## ADR-6 — Python ≥ 3.10, `uv`, `pytest`

**Status**: Accepted.

**Decision**: align with the `cocoindex-code` ecosystem — project management by
`uv`, tests with `pytest`, lint with `ruff`.

---

## ADR-7 — Root `.semgrepignore` to neutralize the default exclusion of `tests/`

**Status**: Accepted (limited to the `ccc-radar` repo itself — see limit
below).

**Context**: Semgrep (v1.168, installed in the development environment) ships a
default ignore pattern `tests/` — any path containing a directory component
named `tests` is silently excluded from scanning, even when explicitly passed as
a target. But D4 (ADR-4) requires fixtures under `tests/fixtures/vuln_repo/`,
and that repo is itself a git repository — verification command F0.2
(`semgrep scan --config tests/fixtures/vuln_repo/rules/rules.yml tests/fixtures/vuln_repo/app --json`)
returned 0 findings instead of 2, exactly because of that behavior.

**Decision**: add a `.semgrepignore` file at the root of the `ccc-radar` repo,
containing `!tests/`, to explicitly re-include the `tests/` tree in scans of
this project. Decision validated with the user before application (it was
outside the task's declared `Files` scope F0.2).

**Consequences**: fixes the `ccc-radar` repo itself. **Does NOT** fix the
general case — in any target repo used by a `cccr` user, the same Semgrep issue
applies: its `tests/` directories are silently absent from the index, with no
error or warning in user repos.

---

## ADR-8 — Snippet read from the source file, not from Semgrep `extra.lines`

**Status**: Accepted.

**Context**: specification F1.2 planned to map `extra.lines` (Semgrep JSON
field) directly to `Finding.snippet`. In practice, the installed Semgrep
version returns the literal string `"requires login"` for that field as long as
the user is not authenticated on semgrep.dev — a behavior change in the OSS CLI
that gates a feature behind an account.

**Decision**: `scanner._read_snippet` re-reads lines `[start_line, end_line]`
directly from the source file on disk (`repo_root / path`) instead of using
`extra.lines`. Decision made without prior consultation because it was forced by
already-accepted constraints D4/NF4 (offline tests, local-first) — requiring a
`semgrep login` would have violated those two non-negotiable constraints.

**Consequences**: works offline, without an account, and yields a non-truncated
snippet (unlike `extra.lines`, which has line/character limits on the Semgrep
side). Introduces a dependency on file readability at parse time
(`OSError` → empty snippet, see known defect R6).

---

## ADR-9 — `run_semgrep` targets `"."` (not the absolute repo path) for a full scan

**Status**: Accepted.

**Context**: Semgrep prefixes the returned `check_id` with the directory
components of the `--config` argument **exactly as passed on the command line**
(not relative to the actual working directory). With
`config.rules = ["rules/rules.yml"]` and `cwd=repo_root`, that yields
`rules.custom.sql-fstring` rather than `custom.sql-fstring`. Separately,
scanning with an absolute target path makes absolute paths appear in JSON
results, which makes committed test fixtures non-portable across machines.

**Decision**: `run_semgrep` always invokes Semgrep with `cwd=repo_root` and a
relative target (`"."` for a full scan, relative paths for a targeted scan),
never an absolute path argument.

**Consequences**: committed JSON fixtures
(`tests/fixtures/semgrep_output.json`) are portable across machines. The
`rule_id` prefix remains an accepted side effect (documented in
`SPEC-TECH.md` §4) rather than being hidden — the contract does not require
`rule_id` to be strictly identical to the `id` declared in the rule file.

---

## ADR-10 — `ccc_bridge` parses `ccc search` text output, not JSON

**Status**: Accepted.

**Context**: specification F5.2 planned `ccc search "<query>" --json --limit N`.
The version of `ccc` installed in the development environment exposes **no**
`--json` flag on its `search` command (verified through `ccc search --help` and
confirmed by exit code 2 plus “No such option: --json” at runtime).

**Decision**: `ccc_bridge.search_code` invokes `ccc search <query> --limit N`
without `--json` and parses the real text output format (blocks
`--- Result N (score: X) ---` / `File: path:start-end [language]`).

**Consequences**: works with the version of `ccc` actually installed.
Contract is inherently fragile — a display-format change in `ccc` silently
breaks parsing (block ignored, no error). Hardening path identified but not
implemented: detect the
absence of parsed blocks on non-empty output and switch to `CccUnavailable` to
trigger the existing fallback.

---

## ADR-11 — Default exclusion of `@pytest.mark.slow` tests

**Status**: Accepted — to be revisited (see note).

**Context**: the test validating `Embedder.embed_texts` with the real
sentence-transformers model depends on a local model that may be absent on the
machine (`~/models/jina-code-embeddings-1.5b`). Initial download from Hugging
Face also failed by default in some environments
(`CERTIFICATE_VERIFY_FAILED`) and is not guaranteed everywhere (networkless
sandboxes, restricted CI).

**Decision**: `pyproject.toml` declares `addopts = "-m 'not slow'"` —
`uv run pytest` with no argument never runs tests marked `slow`. The slow test
checks the real model only if it is already present locally; otherwise it
explicitly skips itself with an actionable message.

**Consequences**: `uv run pytest` (without arguments) no longer covers that test
on every run — a weakening of the default test surface. Rejected alternative:
`pytest.mark.skipif` conditioned on network presence, which would have kept the
test in the default run while cleanly neutralizing it in isolated
environments.

---

## ADR-12 — The Claude Code skill is distributed outside the `ccc-radar` repo

**Status**: Accepted (at the user's explicit request).

**Context**: `skills/cccr/SKILL.md` had originally shipped inside the
`ccc-radar` package. The skill was later moved to its own repository,
`ccc-radar-skill`, outside this repo, rather than being duplicated in both
places.

**Decision**: `skills/cccr/SKILL.md` is removed from the `ccc-radar` repo; the
skill now lives only in the separate `ccc-radar-skill` repository under
`skills/cccr/SKILL.md`. `docs/SPEC-FONC.md` §4 and `README.md` point to that
companion repository rather than to a path that no longer exists here.

**Consequences**: the `ccc-radar` package (pip/uv) no longer contains the skill
— anyone installing only `ccc-radar` must also fetch `ccc-radar-skill` to
enable the Claude Code workflow. Only living documents (`docs/`, `README.md`)
reflect the current state.

---

## ADR-13 — `cccr init` falls back to a default registry pack

**Status**: Superseded in part by the expanded default registry rulesets.

**Context**: the initial PRD (§12, open question 2) had decided in favor of a
mandatory explicit Semgrep config, to avoid noise from a poorly calibrated
default pack. After using the tool, the user asked to be able to use standard
Semgrep rule libraries without having to define `rules` explicitly.

**Original decision**: when `cccr init` receives neither `--rules` nor detects
a local Semgrep config (`.semgrep.yml`/`semgrep.yml`/`.semgrep`), it falls back
to the registry pack `p/security-audit` instead of failing.

**Current decision**: the fallback is now the registry set
`p/security-audit`, `p/java`, `p/owasp-top-ten` and `p/secrets`. An
informational message (stdout, exit code 0) indicates the rulesets used and how
to customize them via `--rules`. `p/security-audit` remains the security base;
the Java, OWASP and secrets rulesets make the default useful on the supported
Java/Spring scope. Priority order remains explicit `--rules` > detected local
config > copied CCCR packs > registry rulesets.

**Consequences**: removes startup friction (no longer necessary to write custom
rules to try `cccr`) at the cost of potential noise from general-purpose
rulesets. Registry coverage remains outside `cccr`'s control. The registry
rulesets provide findings only; the copied CCCR `rest` and `kafka` packs remain
necessary for architecture inventory.

---

## ADR-14 — `cccr` scope takes precedence over Semgrep ignores

**Status**: Accepted.

**Context**: the architecture review confirmed two silent indexing gaps:
`include: ["**/*"]` did not match root files with `fnmatch`, and Semgrep could
exclude `tests/` directories through its ignore mechanisms before `cccr` even
parsed the results.

**Decision**: `cccr` explicitly treats `**/*` as “every file in the repo”
during the hashing phase, and invokes Semgrep with
`--x-ignore-semgrepignore-files` so that the scope selected by
`.cccr/config.yml` remains the source of truth.

**Consequences**: root files and `tests/` directories are no longer silently
absent from the index. The choice relies on an internal Semgrep flag not
guaranteed as a stable API; if Semgrep removes it, `run_semgrep` will fail
loudly rather than produce an incomplete index with no signal.

---

## ADR-15 — Finding identity includes location

**Status**: Accepted.

**Context**: the historical identity `hash(rule_id|path|normalized_snippet)`
resisted line shifts, but merged two identical occurrences of the same rule in
the same file, and collided even more easily on empty-snippet findings.

**Decision**: the identity computed by `compute_finding_id` now includes the
`start_line:end_line` range in addition to rule, path, and normalized snippet.

**Consequences**: two identical occurrences remain distinct in the database and
no longer overwrite each other through the primary key. In return, a finding
whose code does not change but whose line shifts gets a new identifier; that is
accepted to prioritize the absence of silent under-reporting.

---

## ADR-16 — Embedding signature and dimension stored in the index

**Status**: Accepted.

**Context**: the `CCCR_FAKE_EMBEDDER=1` hook used in tests could create a
 database with 8-dimensional vectors while recording only the real model name,
and a subsequent search with the real model would fail late in NumPy.

**Decision**: each embedder exposes a `signature` encoding its type and model.
`index_repo` stores `embedding_signature` and `embedding_dim` in the `meta`
table, re-embeds everything when the signature changes, and search explicitly
checks vector dimension before the dot product.

**Consequences**: mixed or corrupted indexes now produce an actionable error
asking for a full reindex, instead of a raw traceback or incoherent scores. The
fake embedder remains available for tests, but its distinct signature prevents
confusing it with the production model.

---

## ADR-17 — Vector search via `sqlite-vec` (`vec0`), no more NumPy brute force

**Status**: Accepted. Supersedes ADR-2.

**Context**: `ccc` (cocoindex-code) — whose default embedding model `cccr`
already reuses (ADR-3) — stores its own index in
`.cocoindex_code/target_sqlite.db` through the
`cocoindex.connectors.sqlite` connector, which relies on the `sqlite-vec`
extension (`vec0` virtual tables, SIMD distance) rather than on brute-force
computation. `cccr` stayed on “plain” SQLite with cosine computed in
Python/NumPy (ADR-2): acceptable at target scale, but inconsistent with the
tool whose embedding format it already inherits, and less performant with no
extra simplicity benefit (`sqlite-vec` is already a transitive dependency of the
`ccc` ecosystem).

**Decision**: embeddings are no longer stored as `BLOB` in the `findings`
table, but in a dedicated `vec0` virtual table (`vec_findings`,
column `embedding float[N] distance_metric=cosine`, auxiliary column
`+finding_id TEXT` for the join back). `Store.knn_search` delegates similarity
computation to `sqlite-vec` (`... WHERE embedding MATCH ? AND k = ?`) instead
of iterating in Python. Because `vec0` supports neither `ALTER TABLE` nor an
arbitrary primary key, vector dimension doubles as a table-recreation signal
(`meta.embedding_dim`), and severity/rule/path filtering remains done upstream
in regular SQL on `findings` (post-KNN filtering then happens in Python on the
sorted set returned by `vec0`, with no artificial cap since
`k` = total number of vectors).

**Migration**: on opening a database created by an earlier version of `cccr`
(`schema_version` = 1, `findings.embedding` column present), `Store` drops that
column (`ALTER TABLE ... DROP COLUMN`), clears
`embedding_signature`/`embedding_dim` from `meta`, and sets
`schema_version` to 2. The next `cccr index` detects the missing signature and
automatically re-embeds — no dedicated migration command is needed, but a first
`cccr index` (potentially full) is required after upgrade.

**Consequences**: storage format aligned with `ccc`, similarity computation
accelerated through SIMD rather than a Python loop, but one more dependency
(`sqlite-vec`, already present transitively in the `ccc` ecosystem). The choice
of keeping SQLite as the only backend (rather than Postgres/pgvector or a
dedicated vector store) remains ADR-2's: the V1 target (a few thousand findings
per repo) does not justify an external dependency.

---

## ADR-18 — Structured MCP output (`TypedDict`/dataclass), errors by exception

**Status**: Accepted.

**Context**: the 4 tools in `mcp_server.py` were annotated `-> str` and returned
`json.dumps(...)`. FastMCP nevertheless derives an `outputSchema` from the
return annotation even then (`str` → primitive type, wrapped): the 4 tools were
announcing `{"result": {"type": "string"}}` — a schema that promises a
structure without providing one, verified empirically through
`mcp.list_tools()`. `ccc` (cocoindex-code), for comparison, returns a real
`pydantic.BaseModel` (`SearchResultModel`) for its `search` tool, with a field-
by-field schema. Separately, the 4 tools caught every exception and turned it
into `{"error": "<message>"}` — a result returned as success, with no protocol
signal allowing a client to distinguish failure from a valid response without an
ad hoc convention.

**Decision**: each tool is annotated with its real return type — `TypedDict`
(`FindingHit`, `FindingsSummary`, `CodeSearchResult`, defined in
`render.py`/`ccc_bridge.py`/`mcp_server.py`) or an existing dataclass
(`IndexReport`, reused as-is from `indexer.py`, no duplication). FastMCP derives
an `outputSchema` field by field and returns both the usual JSON text (`content`,
for clients that only read that) and structured content (`structuredContent`) —
additive, no regression for existing clients. The `try/except Exception` blocks
that swallowed errors are removed: an exception now bubbles up as-is, FastMCP
turns it into `ToolError`, exposed to the client as `isError: true`. Since
ADR-22, `CccUnavailable` in `search_code_with_findings` is also a real error,
no longer a success-shaped fallback.

**Consequences**: the 4 tools are now symmetrical with `ccc mcp` in output
shape (rich schema, not a string to re-parse), without adding a direct
dependency (`pydantic` is already transitive through `mcp`, but `TypedDict` is
enough here — no runtime validation needed on the `cccr` side, which already
controls both ends). Positive side effect: `search_findings`,
`findings_summary`, and `search_code_with_findings` no longer manually
duplicate `Finding → dict` serialization; that logic is now shared through
`TypedDict`s in `render.py`/`ccc_bridge.py` rather than inline-built dicts.
The companion skill in `ccc-radar-skill` depended on no strict parsing of an
`"error"` key, so no update was required.

---

## ADR-19 — `search_code_with_findings`: severity-weighted ranking, not just annotation

**Status**: Superseded by the strict `ccc`-order contract.

**Context**: `search_code_with_findings` composed `ccc` semantic search with
`cccr` findings purely as post-processing — findings were attached to each
result but never influenced their order. A chunk with an `ERROR` finding and a
chunk with no finding could appear in any order, driven only by `ccc` semantic
relevance. A second improvement path for `ccc`↔`cccr` coupling (translating a
finding into a `ccc grep` pattern to find structurally similar occurrences) was
evaluated in parallel and rejected for now: empirically tested on the 4 rules in
`tests/fixtures/vuln_repo/rules/rules.yml`, only rules without `...`/composite
patterns (2 of 4) translate correctly — rules mixing ellipsis with a literal
kwarg (`subprocess.run(..., shell=True, ...)`) lose their security constraint
once translated (`ccc grep` then matches *all* calls to the function).

**Historical decision**: `ccc_bridge.rank_by_severity` re-ordered annotated results
by adding an additive boost to `score` depending on `max_severity` (`ERROR`
+0.15, `WARNING` +0.05, `INFO`/none +0.0), without modifying `score` itself
(which still reflects `ccc`'s raw semantic relevance). Because `ccc search`
already truncates to `--limit` before `cccr` sees the results, a result just
outside the top `N` could never benefit from the boost —
`ccc_bridge.overfetch_limit` therefore over-requests `limit × 3` (capped at 50)
before annotation, ranking, and final truncation.

**Supersession**: findings are now annotations only. `cccr search` delegates to
`ccc` with the requested limit and preserves its result set, order and scores;
the severity boost and over-fetching were removed because they changed the
meaning of a `ccc search` response.

**Historical consequences**: boost weights were an initial heuristic choice
(deliberately small relative to the typical spread of `ccc` scores, so only near
cases are re-ordered and a clearly irrelevant result never rises). They may be
adjusted if real usage shows a different need. Over-fetch adds cost (up to 3×
more results requested from `ccc` per call), negligible at target scale
(interactive search, not high-volume traffic). The idea of translating a
finding into `ccc grep` remains open but out of scope, and should only be
revisited for rules that do not rely on ellipsis.

---

## ADR-20 — `cccr search` = superset of `ccc search`; findings search becomes `cccr findings`

**Status**: Accepted.

**Context**: since V1, `cccr search` searched *within findings*
(Semgrep finding embeddings), and the code + findings composition was only
exposed through MCP (`search_code_with_findings`). That did not match the
product intent: `cccr` should **extend** `ccc` — same question, same kind of
answer. Expected: `ccc search "user authentication flow"` describes the flow;
`cccr search "user authentication flow"` describes the same flow **and** brings
back the Semgrep findings on it.

**Decision**: `cccr search` becomes code + findings search — the orchestration
(over-fetching `ccc`, annotation, severity ranking, degraded modes), previously
in `mcp_server.py`, is extracted into `code_search.py` and shared by the CLI and
the MCP tool (guaranteed identical behavior). Text rendering reproduces the
format of `ccc search` **exactly** (`--- Result N (score) --- / File: path:l1-l2 [lang]`),
followed by a findings block under each relevant result — a `ccc` user keeps
familiar landmarks, `cccr` adds the findings layer. The `ccc_bridge` parser now
captures language to reproduce the `File:` line identically. The old findings-
only search moves as-is (same flags, same JSON contract) to `cccr findings`.

**Degraded modes**: missing findings index → raw `ccc` results with warning
(rather than silently empty findings, and without creating `.cccr/` as a side
 effect in an uninitialized repo). Since ADR-22, unavailable or failing `ccc` is
no longer a successful degraded mode: the error bubbles up to CLI/MCP.

**Consequences**: CLI contract break (`cccr search` changes semantics ;
findings-only usages must migrate to `cccr findings`) — accepted, the package
not yet being distributed beyond this workstation. MCP tools are unchanged
(`search_findings` = `cccr findings`, `search_code_with_findings` =
`cccr search`). Along the way, fake `ccc` fixtures are shared in
`tests/conftest.py`.

---

## ADR-21 — Prototype of a native CocoIndex extension without abandoning the companion-package approach

**Status**: Accepted experimentally.

**Context**: review of `../cocoindex/examples` showed that `cccr`'s current
indexing manually reimplements several primitives provided by CocoIndex:
declarative target state (`TargetState = Transform(SourceState)`), incremental
invalidation, automatic orphan deletion, transformation memoization, and live
mode. The current bridge to `ccc` also remains fragile because the locally used
version of `ccc search` provides no stable JSON (ADR-10): `cccr` parses human
output.

**Decision**: `cccr` remains a separate companion package (ADR-1 is not
reverted) but introduces an experimental mode `cccr index --engine cocoindex`.
That mode prepares a native CocoIndex extension by modeling findings and code
chunks as typed target states in the local store. It still depends on no
internal API from `cocoindex-code` and does not make `cocoindex` mandatory at
install time: the stable backend remains `--engine manual`.

When the experimental index exists (`meta.index_engine = "cocoindex-prototype"`),
`cccr search` and the MCP tool `search_code_with_findings` first query the
locally indexed code chunks (`vec_code_chunks`) then annotate those results with
findings. The `ccc search` + text parsing fallback remains available for manual
indexes or repos not yet migrated.

Options rejected for now:
- contributing directly into `cocoindex-code` / `ccc`: better long-term
  alignment, but too tightly coupled for a quick local correction;
- immediately replacing `cccr` with a new unified index: too risky for existing
  MCP/CLI commands;
- making `cocoindex` a mandatory dependency: premature until the prototype
  covers the same guarantees as the manual indexer.

**Consequences**: X2/X4 reduce ADR-10's risk without a breaking change: users
keep current commands, and experimental mode is opt-in. The prototype is not
yet a full CocoIndex flow with `live=True`, nor a backend migration; those steps
remain to be handled (X3/X5/X6). The store moves to `schema_version = 3` to add
`code_chunks` and `vec_code_chunks`.

---

## ADR-22 — A `ccc` failure makes `cccr search` fail

**Status**: Accepted.

**Context**: the historical fallback of `search_code_with_findings` masked a
`ccc` failure (`ccc` absent or non-zero return code) by returning a findings-
only search in `findings_only_fallback`. That behavior made output ambiguous:
the caller could believe they had obtained a valid code + findings search while
underlying code search was actually failing.

**Decision**: when `cccr search` must go through the `ccc` bridge, any
`CccUnavailable` is converted to an error (`RuntimeError`) and bubbles up to the
CLI (exit code 2) or MCP (`ToolError` / `isError: true`). The message keeps the
original cause: `ccc not found in PATH` or
`ccc failed (code N): <stderr>`.

Experimental `--engine cocoindex` mode remains independent: if a local code
index exists, `cccr search` uses it without calling `ccc`.

**Consequences**: `findings_only_fallback` remains present in
`CodeSearchResult` for schema compatibility, but is no longer used to mask a
`ccc` failure. A user who wants findings-only search must call `cccr findings`
or the MCP tool `search_findings` explicitly.

---

## ADR-23 — The MCP code search tool uses the same name and parameters as `ccc search`

**Status**: Accepted.

**Context**: the MCP tool `search_code_with_findings` was already a superset of
`ccc search` (ADR-20/21) but exposed only `query` and `limit`, whereas `ccc search`
also accepts `--offset`, `--lang`, `--path`, and `--refresh` (see
`ccc search --help`). An agent that already knew `ccc` therefore had to guess
that those options did not exist on the `cccr` side, or switch back to `ccc`
for paginated/filtered use cases — breaking the “`cccr search` = `ccc search` +
findings” positioning.

**Decision**:
1. The MCP tool is renamed `search_code_with_findings` → `search`, same name as
   the tool exposed by `ccc mcp`. Since MCP tools are prefixed by server on the
   client side (`mcp__cccr__search` vs `mcp__ccc__search`), there is no real
   collision even when both servers are registered at the same time.
2. `search`/`cccr search` now accept `offset`, `lang`, `path`, `refresh` — same
   names as `ccc search --offset/--lang/--path/--refresh` flags.
3. When the `ccc` bridge is used (no experimental code index), those parameters
   are forwarded as-is to the `ccc` binary (`ccc_bridge.search_code`), with no
   transformation.
4. When the experimental code index (`--engine cocoindex`) is used,
   `lang`/`path` filter and `offset` paginates `Store.knn_search_code_chunks`
   (post-filtering, since `vec0` has no native metadata filter — over-request
   `(offset + top_k) × 3`, capped at 200, same pattern as over-fetch in
   `rank_by_severity`) ; `refresh=True` triggers a local incremental reindex
   (`coco_indexer.index_repo_with_cocoindex`) before searching, but only if the
   repo already uses that engine — `refresh=True` does not silently activate the
   experimental engine on a repo indexed in `manual` mode.

**Consequences**: the Python name of the shared CLI/MCP function,
`code_search.search_code_with_findings`, does not change — only the exposed MCP
tool name changes. Tests and docs that referenced the tool by its old name are
updated (`tests/test_mcp_server.py`, `tests/test_ccc_bridge.py`).

## ADR-24 — Rule packs live in the skill repo, never in `cccr`, and are never referenced by an absolute path

**Status**: Accepted.

**Context**: BACKLOG-10 K8 first delivered an initial pack (Python liveness)
embedded in the `cccr` package itself
(`src/ccc_radar/rules/liveness/rules.yml`) — until then, `rules:` contained only
project paths or registry packs (ADR-4, ADR-13). While experimenting with
direct use through `--config /absolute/path/to/.venv/.../rules/liveness.yml`,
the Semgrep `check_id` output (therefore `Finding.rule_id`, and its identity —
ADR-5/ADR-15) turned out to be prefixed by the path components passed to
`--config` exactly as written: two machines with the package installed under
different paths (or a dev checkout vs a `uv tool` install) get different
`rule_id`s for the same rule. Separately, the `ccc-radar-skill` repo proved to
already be the natural distribution point for that kind of content: it carries
its own Java rule pack (`skills/cccr/rules/plateforme-agree/`, specific to the
target platform being analyzed), with the same rule already stated in
`SKILL.md` — copy the pack into the target repo before declaring it in
`rules:`.

**Decision**:
1. Rule packs are **never embedded in `cccr`**
   (`src/ccc_radar/rules/` does not exist) — `cccr` remains a generic Semgrep
   executor, agnostic of rule content (consistent with ADR-1: companion
   package, not product logic specific to one platform).
2. They live in `ccc-radar-skill` under `skills/cccr/rules/<pack>/`
   (e.g. `liveness/{python,java}.yaml`, `plateforme-agree/*.yaml`), alongside
   usage documentation in `SKILL.md`.
3. They are documented as reference files to **copy into the target repo**
   (e.g. `.cccr/rules/liveness/`) and declare in `rules:` through a path
   **relative to the scanned repo** — never an absolute path to the skill repo
   or to an installed package, exactly like an ordinary local rule (ADR-4).

**Consequences**: `rule_id` remains stable and predictable (`rules.<id>` when
the rule lives in `<repo>/rules/...`), regardless of where `cccr` or the skill
repo are installed. `ccc-radar` keeps a test copy
(`tests/fixtures/liveness_repo/rules/`, `tests/test_liveness_rules.py`) that
validates rule *behavior* (positive/negative on real fixtures) but is no longer
the source of truth — that source is `ccc-radar-skill`, which has no testing
infrastructure of its own ; synchronization between the two copies is manual,
not automatically checked (the two repos are versioned independently). If that
becomes a friction point, cross-repo checking or a sync script may be added.

## ADR-25 — `MessageEndpoint`: identity without snippet in the hash, one endpoint per site rather than per flow

**Status**: Accepted.

**Context**: BACKLOG-10 K1 introduces `MessageEndpoint`, the entity modeling a
static service-interaction site (Kafka production/consumption, REST exposure/
call — K2/K11), so an agent can answer “who produces/consumes this topic?” or
“who calls this route?” without runtime connectivity (guiding principle of
BACKLOG-10).

**Decision**:
1. `compute_endpoint_id(role, topic, path, start_line, end_line)` — no snippet
   in the hash, unlike `compute_finding_id`. A `Finding` distinguishes two
   occurrences of the same problem by their text; an endpoint is distinguished
   by *where* it is (code site or manifest entry), the exact call syntax matters
   little to answer “who talks to whom?”. It also makes identity insensitive to
   a variable rename that changes neither topic/route nor position.
2. An endpoint represents a **site**, not a flow: two calls
   `producer.send("orders.created", ...)` on two different lines in the same
   file are two distinct `MessageEndpoint` objects (same topic, identical
   `path`, different `start_line`/`end_line`) — consistent with
   `replace_endpoints_for_files`, which reasons per file like
   `replace_findings_for_files`.
3. `source: code`/`manifest` (K10) coexist for the same topic without a
   dedicated field in the hash: their `path` naturally differs (code file vs
   `TOPICS.md`), so their identities do too. No need to add `source` to the hash
   function to avoid a collision that cannot occur.
4. No `vec0` table / embedding associated with `endpoints` for now — K1 covers
   only the model and storage ; vectorization (NL search on endpoints, if ever
   useful) would remain to be specified separately, outside K1/K3 scope.
5. `remove_files` also purges `endpoints` (like `findings` and `code_chunks`):
   a file deleted from disk must not leave behind ghost endpoints.

**Consequences**: schema v3 → v4 (`docs/SPEC-TECH.md`), purely additive
migration (`CREATE TABLE IF NOT EXISTS`, no vector table to recreate).
`tests/test_store.py` freezes the contract: round-trip, replacement per file,
identity stability/variation, coexistence of code/manifest, filters
(`system`/`role`/`topic`/`path_glob`), purge by `remove_files`.

## ADR-26 — REST path extraction by regex on the snippet, not by a Semgrep metavariable

**Status**: Accepted.

**Context**: BACKLOG-10 K11 must extract the HTTP method and path of a route or
REST call captured by a Semgrep rule (e.g. `@GetMapping("/orders/{id}")`). The
natural approach would be to read `extra.metavars` from Semgrep JSON output (the
exact value captured by a metavariable such as `$PATH`). In experiments
(semgrep 1.168.0, unauthenticated OSS CLI), `extra.metavars` is **absent** from
JSON output, and `fingerprint`/`lines` are replaced by the literal
`"requires login"` — that behavior does not depend on `--metrics`, apparently
only on an active `semgrep login` session, which would violate NF4
(local-first, no dependency on an external account to index).

**Decision**: each inventory rule fixes the HTTP method in its own metadata
(`metadata.http_method`, one rule = one method, e.g. `@GetMapping`/
`@PostMapping` are two separate rules rather than one rule with a method
metavariable) ; only the **path** varies and must be extracted from the text.
`cccr` already re-reads the snippet from the source file (ADR-8) —
`_extract_rest_path` looks there, by regex, for the first quoted literal of the
first line. If that literal is followed by a concatenation (`+ variable`) or if
no literal exists, the path is marked `topic_dynamic=True` (same policy as
`topic_dynamic` in K2: never silently resolved). A Python f-string
(`f"...{id}..."`) is treated as a **resolved** literal: interpolation braces are
indistinguishable, at extraction time, from a URI template such as `{id}` — and
are naturally read that way.

**Consequences**: best-effort, non-semantic extraction — a concatenation in the
middle of an expression (`base + "/orders/" + id`) may produce a path fragment
that is not the real prefix. Documented and tested as such
(`tests/test_rest_endpoints.py`), consistent with the “best-effort” already
accepted for path matching in K12. If Semgrep one day exposes metavariables
without login (or via a dedicated flag), this decision may be revised for exact
extraction.

## ADR-27 — The interaction graph is derived at query time, never persisted

**Status**: Accepted.

**Context**: BACKLOG-10 K12 must expose inter-service topology from indexed
endpoints (K1/K11). One option would have been to materialize the graph
(edges) in dedicated
SQLite tables, recomputed on each `cccr index` — consistent with the rest of
the store (findings, code_chunks, endpoints).

**Decision**: `src/ccc_radar/graph.py` does not touch the SQLite schema.
`build_graph`/`find_outbound_calls_in_consumers` are pure functions taking
in-memory `MessageEndpoint` objects (read from the store by the caller) and
returning in-memory structures (`GraphEdge`, `OutboundCallInConsumer`) —
never written to the database.
Reasons:
1. The graph depends on **multiple projects** as soon as K7 (multi-repo
   federation) comes into play — it has no natural “owner” among the SQLite
   stores of a single repo.
2. It is a cheap derived view to recompute (a few hundred endpoints per
   project, not millions): no performance benefit justifies the complexity of a
   cache invalidated on every reindex.
3. Consistent with BACKLOG-10's guiding principle: “the distributed view is a
   join at query time”, already applied to `search_code_with_findings`
   (ADR-19) and to `cccr flow`/K10.

**Consequences**: `cccr graph` (CLI/MCP) keeps exposing a derived view built
at query time rather than persisted state. `tests/test_graph.py` verifies that
no `*graph*`/`*cycle*` table exists in the schema (AC5).

## ADR-28 — A Kafka topic given as a Spring property is resolved locally against `application.yml`/`.properties`, never guessed

**Status**: Accepted.

**Context**: BACKLOG-10 K2 must extract the topic name in
`@KafkaListener(topics = "...")` / `KafkaTemplate.send(...)`. In practice
(target = Java + Spring on Maven or Gradle), the topic is almost never a raw literal: it
is externalized into configuration —
`@KafkaListener(topics = "${app.kafka.topics.orders}")`, with the real value in
`application.yml`/`.properties`. The text captured by regex extraction
(ADR-26) is then `${app.kafka.topics.orders}` — a configuration key, not a
topic name. Simply marking it `topic_dynamic=True` like an unresolved case
(variable, concatenation) would have been correct but not very useful: the key
is almost always statically resolvable in the same repo, with no runtime
connection.

**Decision**: `resolve_spring_property(repo_root, property_key)` looks for
`property_key` (syntax `prop` or `prop:default`, like Spring) in the repo's
conventional Spring Boot configuration files (standard Maven/Gradle layout:
`src/main/resources/application.{yml,yaml,properties}`, then the same names at
the root), in that order — first file defining the key wins. Nested YAML is
flattened into dotted keys (`app.kafka.topics.orders`) ; `.properties` is
already flat. If the key is found in no file, the Spring default
(`${prop:default}`) applies; otherwise the placeholder is kept as-is and marked
`topic_dynamic=True` — never guessed, same policy as ADR-26 for non-literal REST
paths.

**Consequences**: a variable that receives a value from `@Value("${...}")` then
gets passed to `.send(topic, ...)` is **not** resolved (no statement-level
dataflow analysis — outside scope, consistent with the lack of metavariables /
taint in ADR-26) : only the case where the placeholder appears **textually** in
the annotation/call is covered. Spring profiles (`application-prod.yml`) are
consulted now alongside the base file set; whichever eligible file is found
first in the configured search order wins. `tests/test_kafka_endpoints.py`
freezes the contract: YAML and `.properties` resolution, default value,
missing key, YAML > `.properties` priority when both exist.

## ADR-29 — `cccr index` performs a single Semgrep scan for findings and endpoints, discriminated by `metadata.category`

**Status**: Accepted.

**Context**: BACKLOG-11 A1 wires endpoint extraction (K2/K11) into
`cccr index`, until then dedicated to findings (`run_semgrep`). The two kinds of
rules (findings on one side, endpoint inventory on the other) may coexist in
`config.rules` (e.g. `default.yaml` + `liveness.yaml` + `rest/java.yaml` +
`kafka/java.yaml`). Running `run_semgrep` then `run_semgrep_endpoints`
separately would have scanned the same files twice with Semgrep — the most
expensive stage of the pipeline (NF2).

**Decision**: `indexer.index_repo` calls `scanner.invoke_semgrep_raw`
(renamed from the former private `_invoke_semgrep`, now shared) **once per
indexing**, then passes the same JSON output to `parse_semgrep_json`
(findings) and `parse_semgrep_endpoints` (endpoints). Each parser ignores what
is not for it through `extra.metadata.category`:
`parse_semgrep_json` now skips `category: endpoint-inventory` results
(otherwise they would become false INFO findings — contrary to K8 AC2,
“handled as endpoints, not as findings filtered by `min_severity`”) ;
`parse_semgrep_endpoints` keeps only those. The `min_severity` filter
(previously in `run_semgrep`) is applied in `index_repo` on the already parsed
findings list, so the logic is not duplicated between
`run_semgrep`/`run_semgrep_endpoints` (preserved as-is for tests and for future
standalone CLI use outside indexing).

**Consequences**: `IndexReport` gains `endpoints_added`/`endpoints_removed`
(defaulting to `0`, compatible with all existing positional construction).
`tests/test_indexer.py` freezes the contract: a scan mixing one finding rule and
two inventory rules produces 1 finding and 2 endpoints, with no leakage either
way ; deleting a file also purges endpoints (already true since K1 through
`Store.remove_files`).

## ADR-30 — Multi-service federation: Maven discovery by `pom.xml`, strictly read-only access to peer databases

**Status**: Accepted.

**Context**: BACKLOG-11 A2 must allow `cccr` to reason over several
microservices at once (inter-service REST/Kafka graph, K12), without depending
on manual “named workspace” configuration (K7's initial plan,
`~/.cccr/workspaces/<name>.yml`) — the real target is a parent directory
containing all microservices and shared Maven modules of one product, not
unrelated standalone repos.

**Decision**:
1. **Discovery** (`workspace.discover_maven_services`): every `pom.xml` found
   under the given directory is a module. Stable logical name comes from the
   pom's `artifactId` (fallback: directory name if the pom is unreadable or has
   no `artifactId` — one broken module never blocks the others). A module is
   classified as `microservice` if its pom references
   `spring-boot-maven-plugin` (it produces an executable jar), `shared-module`
   otherwise — simple textual search in the XML, not full Maven model
   resolution (parent POM inheritance, profiles): sufficient to distinguish a
   deployable service from an internal library in the overwhelming majority of
   real Spring Boot repos, documented as a heuristic and not as a guarantee.
2. **Read-only access** (`Store(path, readonly=True)`): SQLite open via
   `file:...?mode=ro` (URI), without `_create_schema()` (no schema write /
   migration in another project's DB), without `commit()` on exit. Missing DB →
   explicit `StoreError` before even attempting the connection ; incompatible
   schema (`schema_version` differs, or missing table) → explicit `StoreError`
   rather than a silent partial read (K7 AC2/A2 AC8). SQLite itself refuses any
   write on a `mode=ro` connection — double guarantee, not only a code-side
   convention.
3. **Per-service robustness** (`workspace.load_federation`): a module not
   indexed or whose DB is incompatible adds a warning and does not interrupt
   federation of other services — never a global failure because of one bad
   module.
4. **Shared modules vs microservices** (A2 AC5): `load_federation` includes
   findings from a `shared-module`, but never its endpoints — a
   shared module is not a runtime producer/consumer, even if a scan mistakenly
   detected an endpoint-inventory there.

**Consequences**: `src/ccc_radar/workspace.py` (new), no dependency on a
third-party Maven parser (only `xml.etree.ElementTree`, stdlib). CLI
`cccr microservices [--root ROOT]` and MCP tool `list_workspace_services` expose
service discovery + endpoint/finding counts, ahead of K12 which consumes
`load_federation` to build the real graph.
`tests/test_workspace.py` freezes the contract: names/classification, indexing
detection, warning on non-indexed service or incompatible database, no endpoint
leak from a shared module, and effective non-write behavior of a read-only
connection.

## ADR-31 — `metavariable-regex` on a Java literal must account for escaped quotes in the source text

**Status**: Accepted.

**Context**: BACKLOG-10 K8 (security part) must distinguish a
`sasl.jaas.config` carrying a **literal** password (`******`, hard-coded in the
source) from a **constructed** password (`******`, injected from outside).
`metavariable-regex` applies its regex to the raw source text captured by the
metavariable — for a Java literal containing internal quotes, that text carries
those quotes **escaped** (`\"`), not as bare quotes, because that is how they
appear in the `.java` file itself. A regex such as
`password\s*=\s*"[^"]+"` (bare quotes) therefore **never** matches, even on
the very case it is meant to detect — tested and confirmed experimentally (see
also ADR-26: `extra.metavars` does not appear in JSON output without a
`semgrep login` session, which made this trap longer to diagnose because the
captured text could not be inspected directly).

**Decision**: the regex must explicitly look for the escaped quote —
`password\s*=\s*\\"[^"]*\\"` (a literal backslash-quote, no backslash inside
the content). A password concatenated with a variable produces only **one**
escaped quote followed by the variable name (no closing escaped quote in the
same substring), so it does not match — exactly the intended distinction.

**Consequences**: `cccr.kafka-security.sasl-plaintext-credentials`
(`skills/cccr/rules/kafka-security/java.yaml`) encodes that regex ;
`tests/test_kafka_security_rules.py` freezes the contract for both forms
(pure literal vs concatenation) to prevent a silent regression if someone
“simplifies” the regex back to bare quotes. Any future use of
`metavariable-regex` targeting the content of a Java string literal (or a
similar language with escapable quotes) must keep this trap in mind instead of
rediscovering it.

## ADR-32 — Module/class attribution at indexing time, in addition to (not instead of) A2/K7 federation

**Status**: Accepted.

**Context**: multi-service federation (ADR-30, BACKLOG-11 A2) requires
indexing **each** Maven module separately (`cccr init`/`cccr index` run in each
subdirectory), then federating at query time through `--workspace`. Direct user
feedback (2026-07-13): for a monorepo — one Git repo containing several Maven
modules, not separate repos — this feels like artificial overhead: the parent
directory is already indexed in one pass, but `cccr graph`/`cccr flow` remain
blind to inter-module relationships until each module has its own database and
`--workspace` is provided.

**Decision**: indexing the parent directory once must be enough to detect
inter-module topology, without going through federation.
`Finding`/`MessageEndpoint` gain two optional fields (schema v4→v5, purely
additive):
- `module: str | None` — name of the nearest Maven module (artifactId of the
  nearest `pom.xml` found by walking upward from the file to the indexed repo
  root, bounded like `scanner._candidate_spring_roots` — never above
  `repo_root`) ; `None` if the repo has no Maven layout or the file is outside
  the Maven tree.
- `qualified_name: str | None` — Java package + class name
  (`scanner._java_qualified_name`, regex on `package ...;` + file name, never
  AST), `None` for non-Java files.

`graph.group_endpoints_by_module` turns a flat list into the same dict shape
(`dict[str, list[...]]`) that `workspace.load_federation` already produces —
`build_graph` needs **no** change, it consumes equally well a dict coming from
federation or from module grouping. An endpoint without a module is
excluded from that grouping (never a false edge between two unattributed sites
that would arbitrarily land in the same bucket) — deliberately conservative
choice: fewer detected edges rather than an invented one. `cccr flow` needs a
different grouping (`flow.group_endpoints_by_module_for_flow` /
`group_findings_by_module_for_flow`) : unlike the graph, listing all sites of a
topic is the contract, so a site without a module stays present under key
`None` — no false-edge risk here because `trace_flow` never compares keys with
one another.

**What this decision does not replace**: A2/K7 federation remains the only path
for services that live in genuinely separate Git repos (no common parent
indexed in one pass). The two mechanisms coexist:
`cccr graph`/`cccr flow` without `--workspace` first try module grouping (new);
`--workspace` still triggers full federation (unchanged) and takes precedence
when provided.

**Consequences**: `src/ccc_radar/maven.py` (new) factors out shared `pom.xml`
reading (`parse_pom`) between `workspace.py` (federation) and `scanner.py`
(module attribution) — no more `_parse_pom` duplication. `render_graph_json` /
`GraphResult.note` now reflects “no inter-module data available” rather than
“no `--workspace`”: the message stays correct even when missing topology comes
from a non-Maven repo, not only from the absence of the flag.
`cccr endpoints --module` / `EndpointHit.module` expose the new field for
direct inspection. Tested end-to-end in `tests/test_m3_module_graph_e2e.py`:
the same 3-service fixture (`rest_cycle_workspace`) that proves the cycle in
federated mode (`test_k12_graph_workspace_e2e.py`) proves the same cycle when
the parent is indexed once, without `--workspace`.

---

## ADR-33 — Gradle service detection by Spring Boot `main()` class, not by `build.gradle` contents

**Status**: Accepted.

**Context**: ADR-30/ADR-32 attribute a module/service by looking for
`spring-boot-maven-plugin` in a `pom.xml`'s text — reliable for Maven because
that string is a near-universal marker. Direct user feedback (2026-07-13,
audit of `eventuate-tram-examples-customers-and-orders`): that repo is 100%
Gradle (zero `pom.xml`), so neither federation (A2) nor module attribution (M1)
ever finds anything, whatever the index. But Gradle has no universal equivalent:
that repo applies a custom convention plugin (`ServicePlugin`, defined in
`buildSrc/`) rather than `org.springframework.boot` directly — grepping
`build.gradle` text cannot detect that generally. The user explicitly asked to
detect a microservice by the Java class that actually starts it
(`main()` + `SpringApplication.run(...)`), rather than by a build convention.

**Decision**: `gradle.gradle_service_for_path` (new, BACKLOG-15 H1) searches the
whole repo for Java classes carrying a `main()` that calls
`SpringApplication.run(...)` (regex, no AST — same spirit as ADR-26). The
service name is its declared Gradle archive name, then `rootProject.name`, or
Gradle's default project name. The first path segment (top-level directory
under the indexed root) only identifies which files belong to that service — a
Gradle microservice split across several subprojects
(` <service>/<service>-domain`, `-restapi`, ... `-main`) is thus grouped under a
single name, at the same granularity that a single Maven `pom.xml` gives for an
equivalent microservice. `scanner._module_for_path` first tries
`maven.module_name_for_path` (unchanged — the user explicitly chose to keep the
`pom.xml`/`spring-boot-maven-plugin` detection for Maven rather than switch
Maven too to `main()`-class detection) and only falls back to Gradle detection
if no `pom.xml` is found on the path.

**Consequences**: a mixed repo (some Maven modules, some Gradle ones) works
file by file, with no explicit build-tool configuration. The grouping by first
path segment assumes a Gradle microservice split across subprojects lives under
one top-level directory (the convention observed in the repo that
motivated this task) — a Gradle layout that puts all subprojects flat at the
root with no per-service grouping would not be correctly detected ; not handled
here, to revisit if such a layout appears. No microservice/shared-module
distinction on the Gradle side (unlike Maven, ADR-30 AC5): every directory with
a Spring Boot `main()` class becomes a service, with no notion of internal
library excluded from endpoints — accepted for this first support, to refine if
needed.

---

## ADR-34 — Excluding test code from the entire scan (findings and endpoints), breaking with ADR-14/R2

**Status**: Accepted.

**Context**: ADR-14 (BACKLOG-2 R2) had deliberately chosen to **never**
silently exclude test directories from a security scan, so as not to miss
vulnerabilities in test helpers. Direct user feedback (2026-07-13) on the same
Gradle audit: a REST call in a test file (`ApiGatewayComponentTest.java`, a
`WebClient` call from test harness to the API) was appearing as a real
interaction site in the endpoint inventory, polluting the service↔service graph
with calls that do not exist in production. Explicit question asked to the user
before acting: exclude only from endpoint inventory (preserves ADR-14/R2), or
from the entire scan including security findings (revisits ADR-14/R2) — the
user chose the second option with full knowledge of the announced trade-off.

**Decision**: `indexer._is_test_source(rel_path)` (BACKLOG-15 H2) excludes any
file under a `src/<source-set>` directory where `<source-set>` follows the
Maven/Gradle naming convention for test source sets (`test`, `componentTest`,
`contractTest`, `endToEndTest`, etc. — see fix below) — applied in
`_list_repo_files`, before `config.exclude`/`include`, therefore never scanned
at all by Semgrep (findings **and** endpoints). Decision based on path segments
rather than an `fnmatch` glob pattern: `*` does not respect directory boundaries
in this project (see `indexer._matches_any`), which would confuse a real test
source set with a mere package named `testutils` under `src/main`.

**Consequences**: a vulnerability that exists only in a test helper becomes
invisible again with no signal — exactly the risk ADR-14/R2 aimed to eliminate,
now explicitly accepted rather than silently suffered. A file already indexed
that becomes excluded by this change is purged on the next `cccr index` through
the existing mechanism `deleted = previous_paths - current_paths`, with no
dedicated migration.

**Fix (BACKLOG-16 P1, 2026-07-13)**: the initial rule (“any `src/<x>` with
`x != "main"`”) also excluded any Python/JS/Rust `src/<package>` layout —
including `cccr` itself — because `<package>` is never `"main"`.
`_is_test_source` now recognizes a test source set only if its name follows the
Maven/Gradle convention `test`/`<prefix>Test`
(`_is_maven_or_gradle_test_source_set`), preserving ADR-34's intent
(exclude `src/test`, `src/componentTest`, ...) without capturing a generic
`src/<package>` layout.

---

## ADR-35 — Strategy1 models configured REST property entries as external microservices

**Status**: Accepted.

**Context**: Strategy1 already preserves configured HTTP dependencies declared
through uppercase domain constants in a `Rest*Config*` class, but some services
select an HTTP client from `getRest().get("xxx")`. The
target is an explicit logical service name, not a route and not merely an
opaque URL. Reducing it to an `external_api` node loses the fact that the
dependency is service-to-service; requiring an indexed provider would hide
real partner dependencies outside the workspace.

**Decision**: the AST-only Strategy1 extractor recognizes that exact fluent
invocation inside `Rest*Config*` and stores a `MessageEndpoint` call fact with
the `cccr-external-microservice:xxx` evidence marker. `graph.build_graph`
creates an HTTP edge to `xxx` even without indexed endpoints. JSON and visual
exports include `xxx` as a microservice annotated external; the dependency
graph carries the same annotation. The LikeC4 exporter intentionally collapses
all HTTP route matches between one caller/callee pair into one `HTTP` relation,
so resource-level endpoint multiplicity does not clutter a service topology.

**Consequences**: a literal key is required; dynamic map keys, arbitrary
`get(...)` chains, comments, and non-`Rest*Config*` classes are ignored. The
edge is static evidence of configuration, never a runtime reachability claim.
An external name that collides with an indexed service retains the external
annotation, because the explicit configuration convention is authoritative for
that call site.

---

## ADR-36 — Static type checking with `mypy`

**Status**: Accepted.

**Context**: `src/ccc_radar/` had no static type-checking gate. Several
functions and TypedDicts carried implicit or inconsistent typing, and at least
two latent bugs existed that plain lint/tests never caught: an unconditional
2-value unpack of a single string literal in `modules.py`'s blocking-point
detection (`blocking_mechanism, detail = "thread-or-future-join"`, never
exercised by any test), and two TypedDicts (`ModuleSummary`, `ModuleDetail` in
`render.py`) missing fields (`starts_application`,
`application_entrypoint`) that were nonetheless constructed and read
elsewhere — invisible at runtime because Python TypedDicts do not enforce keys.

**Decision**: add `mypy` as a dev dependency and a `[tool.mypy]` configuration
in `pyproject.toml` covering `src/ccc_radar`. `python_version` is pinned to
`"3.13"` in that config only to satisfy mypy's own stub parser (numpy's
bundled stubs use PEP 695 `type` syntax requiring 3.12+ to parse) — it does not
change the project's actual `requires-python = ">=3.10"` support target.
`disallow_untyped_defs` is left off via a `ccc_radar.*` override so the
codebase is not forced into full annotation coverage in one pass; all 85
type errors were fixed properly (correct annotations, `TypedDict`/`Protocol`
definitions, `cast()`/`assert` narrowing) rather than suppressed with blanket
`# type: ignore` comments, and the two latent bugs above were fixed as part of
this pass.

**Consequences**: `uv run mypy src/ccc_radar` is now part of the documented
development loop (README) and should be run alongside `ruff check .` and
`pytest` before merging. No CI workflow enforces this yet (tracked as a
separate improvement).
