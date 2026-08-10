# Technical specification — ccc-radar (`cccr`)

> Describes the internal architecture actually delivered: modules, data model,
> algorithms, SQLite schema, internal contracts. For user-visible behavior, see
> [`SPEC-FONC.md`](./SPEC-FONC.md). For the reasoning behind the choices, see
> [`ADR.md`](./ADR.md).

## 1. Module map (`src/ccc_radar/`)

| Module | Role | Depends on |
|---|---|---|
| `models.py` | `Finding`, `MessageEndpoint` and evidenced `ArchitectureRelation` frozen facts | — |
| `config.py` | `Config`, `load_config`, `init_config`, `ConfigError` | — |
| `semgrep.py` | Optional Semgrep execution (subprocess) + JSON parsing → `Finding` and rule-based REST endpoint inventory | `config`, `models`, `scanner` |
| `scanner.py` | Local Java/Spring/Kafka endpoint inference; `resolve_spring_property` (K2, ADR-28); `_module_for_path` (Maven then Gradle fallback, BACKLOG-15 H1) | `models`, `maven`, `gradle` |
| `gradle.py` | Gradle service detection by Spring Boot `main()` class, complementing `maven.py` when no `pom.xml` exists (BACKLOG-15 H1, ADR-33): `gradle_service_for_path` | — |
| `store.py` | `Store`: SQLite persistence (findings, endpoints, normalized architecture relations, module dependencies, experimental code chunks, file hashes, meta, embeddings) | `models` |
| `relations.py` | Materializes typed, evidenced relations between modules/services, APIs, topics, DTOs, collections, classes, methods and Spring properties | `models`, `modules` |
| `indexer.py` | `index_repo`: incremental orchestration (file diff → targeted scan → findings + endpoints (A1) → embedding ; can also index code chunks) | `config`, `scanner`, `store`, `embedder` |
| `coco_indexer.py` | Experimental `--engine cocoindex` adapter: findings + code chunks as typed target states | `config`, `indexer`, `store` |
| `embedder.py` | `Embedder` (sentence-transformers), `finding_to_text` | `models` |
| `search.py` | `search_findings` (precision-first lexical), `summary`, `get_context` | `store`, `models` |
| `graph.py` | Interaction graph derived at query time (BACKLOG-10 K12): `build_graph`, `find_outbound_calls_in_consumers`, `group_endpoints_by_module`, `paths_match` | `models` |
| `dependency_analysis.py` | Typed runtime dependency topology and static graph audit: Kafka/HTTP/MongoDB relations, event cycles and blocking calls | `audit`, `graph`, `models`, `modules` |
| `workspace.py` | Read-only federation of a multi-service Maven/Gradle directory (BACKLOG-11 A2, ADR-30/33): `discover_workspace_services`, `load_federation` | `models`, `store` |
| `render.py` | Text/JSON serialization of findings, architecture objects, graphs and workspace discovery; Sigma.js HTML and LikeC4 graph exports | `search`, `graph`, `workspace` |
| `configuration.py` | Extracts Spring property keys from production code and constructs a synthetic typed YAML template (`<secret>` for sensitive keys) | — |
| `modules.py` | Discovers every Maven/Gradle module and creates its persisted audit snapshot for `cccr modules` | `configuration`, `maven`, `gradle` |
| `cli.py` | Typer application for setup, findings and architecture exploration (`microservices`, `topics`, `apis`, `mongodb`, `modules`, `analyze`, `export`) | all modules above |
| `mcp_server.py` | `FastMCP` stdio server, tools | `config`, `dependency_analysis`, `embedder`, `graph`, `indexer`, `render`, `search`, `store`, `workspace` |
| `architecture.py` | Catalog queries for microservices, topics, APIs, MongoDB and DTOs | `models`, `modules` |
| `architecture_inventory.py` | Single read-only loader normalizing current-index or workspace-federation facts for graph, audit, CLI and MCP queries | `graph`, `inventory_freshness`, `store`, `workspace` |
| `audit.py` | Architecture-risk assessment over the catalog | `architecture`, `models` |
| `flow.py` | Topic/route tracing with finding overlap and optional vector fallback | `models`, `store` |
| `doctor.py` | Validates architecture rule-pack configuration | `config` |
| `java_parser.py` | Shared cached Tree-sitter Java parsing and syntax helpers | — |
| `topic_expressions.py` | Parses Spring topic-property expressions | — |
| `inventory_freshness.py` | Endpoint inventory signature and stale-index warning | — |
| `paths.py` | Central `.cccr` state paths | — |

For the maintainer-oriented ownership map and dependency rules, see
[`ARCHITECTURE.md`](./ARCHITECTURE.md). Public cross-module dependencies use a
named non-private function; helpers whose name starts with `_` stay local to
their module.

The overall dependency direction is broadly `cli.py`/`mcp_server.py` → business
logic → `store.py`. The public embedder factory lives in `embedder.py` and is
used by both the CLI and the MCP server.

## 2. Data model

### `Finding` (`models.py`)
```python
@dataclass(frozen=True)
class Finding:
    id: str            # sha256(rule_id|path|start:end|normalized_snippet)[:16]
    rule_id: str       # Semgrep check_id (may be prefixed, see §4)
    severity: str      # INFO | WARNING | ERROR (normalized)
    message: str
    path: str          # relative to repo_root, '/' separators
    start_line: int
    end_line: int
    snippet: str       # read from the source file, not from Semgrep (see ADR-8)
    fix: str | None
    cwe: list[str]
    owasp: list[str]
    module: str | None = None          # Maven or Gradle artifact name (BACKLOG-13/15)
    qualified_name: str | None = None  # Java package + class (BACKLOG-13 M1)
```

`compute_finding_id(rule_id, path, snippet, start_line, end_line)`: normalizes
the snippet (`" ".join(snippet.split())` — whitespace/indentation reduced),
then
`sha256(f"{rule_id}|{path}|{start_line}:{end_line}|{normalized_snippet}")[:16]`.
The location keeps two identical occurrences of the same rule in the same file
distinct; the trade-off is that the identity changes if the finding shifts in
the file.

### `MessageEndpoint` (`models.py`, BACKLOG-10 K1)

```python
@dataclass(frozen=True)
class MessageEndpoint:
    id: str               # sha256(role|topic|path[|start:end])[:16]
    role: str             # produce | consume (kafka) ; serve | call (rest)
    system: str           # kafka | rest
    topic: str            # Kafka topic name, or "METHOD /path" (rest)
    topic_dynamic: bool   # name not statically resolvable (K2/K11)
    source: str           # code | manifest (K10)
    framework: str | None
    path: str             # code file, or TOPICS.md for source=manifest
    start_line: int
    end_line: int
    snippet: str
    module: str | None = None          # Maven or Gradle artifact name (BACKLOG-13/15)
    qualified_name: str | None = None  # Java package + class (BACKLOG-13 M1)
```

`compute_endpoint_id(role, topic, path, start_line, end_line)`:
`sha256(f"{role}|{topic}|{path}|{start_line}:{end_line}")[:16]` — no snippet in
the hash (unlike `Finding`): an endpoint is identified by *where* it is, not by
the exact text of the call site. A `source: manifest` endpoint (K10) and a
`source: code` endpoint (K2/K11) for the same topic have different identities
because their `path` differs (`TOPICS.md` vs the code file) — coexistence
without collision by construction, not via a dedicated field in the hash.
`module`/`qualified_name` do not enter the hash (like `snippet` for `Finding`):
they are metadata derived from `path`, not part of site identity.

**`module`/`qualified_name` (BACKLOG-13 M1)** — computed in `scanner.py` when
constructing each `Finding`/`MessageEndpoint` (`parse_semgrep_json` /
`parse_semgrep_endpoints`), not by `Store`:
- `scanner._module_for_path(repo_root, rel_path) -> str | None`
  (BACKLOG-15 H1, ADR-33) — first tries
  `maven.module_name_for_path` (below), then falls back to
  `gradle.gradle_service_for_path` if no `pom.xml` is found. A mixed repo works
  file by file; a purely Maven or purely Gradle repo never needs the second
  mechanism.
- `maven.module_name_for_path(repo_root, rel_path) -> str | None` — module name
  (artifactId, falling back to directory name) of the nearest `pom.xml` found
  by walking upward from `rel_path` to `repo_root` inclusive, same boundary as
  `scanner._candidate_spring_roots` (never beyond `repo_root`). `None` if no
  `pom.xml` exists on that path. Result cached per `pom.xml` (`lru_cache`, a
  pom read only once per process). `parse_pom` (minimal XML read: `artifactId`,
  presence of `spring-boot-maven-plugin`) is shared with `workspace.py`
  (`discover_maven_services`) — no more duplication since this task.
- `maven.is_runtime_service(packaging, is_spring_boot_app) -> bool` — explicitly
  filters out `packaging=pom` parents/aggregators, never treated as runtime
  services even if they centralize the Spring Boot plugin for their children.
- `gradle.gradle_service_for_path(repo_root, rel_path) -> str | None`
  (BACKLOG-15 H1, ADR-33) — a `build.gradle` has no universal marker equivalent
  to `spring-boot-maven-plugin` (custom convention plugins through `buildSrc`).
  Signal used instead: `gradle._service_root_artifacts(repo_root)` walks the whole repo
  (`rglob("*.java")`, cached by `repo_root`) to find classes bearing a `main()`
  that calls `SpringApplication.run(...)` (regex, no AST). The service name is
  the declared archive name, then `rootProject.name`, or Gradle's default
  project name. Its first path segment is used solely to attach all subprojects
  of the same service (`<service>/<service>-domain`, `-restapi`, ... `-main`).
  `None` if the path matches no detected service.
- `scanner._java_qualified_name(repo_root_str, rel_path) -> str | None` —
  `None` for a non-`.java` file; otherwise `package + "." + file_name` if a
  `package ...;` declaration is found by regex (no AST), otherwise just the file
  name. Cached per file (`lru_cache`).

### SQLite schema (`.cccr/findings.db`, managed by `Store`)

```sql
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
-- keys used: schema_version ("5"), embedding_model,
-- embedding_signature, embedding_dim, index_engine,
-- code_embedding_signature, code_embedding_dim, endpoint_embedding_dim,
-- endpoint_inventory_signature

CREATE TABLE files (
    path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, indexed_at TEXT NOT NULL
);

CREATE TABLE findings (
    id TEXT PRIMARY KEY, rule_id TEXT, severity TEXT, message TEXT,
    path TEXT, start_line INTEGER, end_line INTEGER, snippet TEXT,
    fix TEXT, cwe TEXT,      -- JSON-serialized
    owasp TEXT,              -- JSON-serialized
    module TEXT, qualified_name TEXT   -- BACKLOG-13 M1
);
CREATE INDEX idx_findings_path ON findings(path);
CREATE INDEX idx_findings_severity ON findings(severity);
CREATE INDEX idx_findings_module ON findings(module);

CREATE TABLE code_chunks (
    id TEXT PRIMARY KEY, path TEXT, start_line INTEGER, end_line INTEGER,
    language TEXT, content TEXT
);
CREATE INDEX idx_code_chunks_path ON code_chunks(path);

CREATE TABLE endpoints (
    id TEXT PRIMARY KEY, role TEXT NOT NULL, system TEXT NOT NULL,
    topic TEXT NOT NULL, topic_dynamic INTEGER NOT NULL, source TEXT NOT NULL,
    framework TEXT, path TEXT NOT NULL, start_line INTEGER NOT NULL,
    end_line INTEGER NOT NULL, snippet TEXT NOT NULL,
    module TEXT, qualified_name TEXT   -- BACKLOG-13 M1
);
CREATE INDEX idx_endpoints_path ON endpoints(path);
CREATE INDEX idx_endpoints_module ON endpoints(module);
CREATE INDEX idx_endpoints_topic ON endpoints(topic);

-- vec0 virtual table (sqlite-vec extension), created lazily on the
-- first set_embedding() once the dimension is known; recreated if the
-- dimension changes (model change, ADR-16/ADR-17). meta.embedding_dim
-- doubles as current dimension AND “the table exists”.
CREATE VIRTUAL TABLE vec_findings USING vec0(
    embedding float[N] distance_metric=cosine,
    +finding_id TEXT   -- auxiliary column, not indexed by KNN
);

CREATE VIRTUAL TABLE vec_code_chunks USING vec0(
    embedding float[N] distance_metric=cosine,
    +chunk_id TEXT
);

-- BACKLOG-10 K3: same lazy mechanism, gated by meta.endpoint_embedding_dim.
CREATE VIRTUAL TABLE vec_endpoints USING vec0(
    embedding float[N] distance_metric=cosine,
    +endpoint_id TEXT
);
```

`Store` is a context manager: entering = connection + loading the
`sqlite-vec` extension (`sqlite_vec.load`) + schema creation if missing;
exiting = `commit()` if no exception was raised in the block, otherwise the
connection is closed with no commit (implicit SQLite rollback — the mechanism
that guarantees NF5: a `SemgrepError` during indexing leaves the database in
its pre-call state).

**Schema migration v1 → v2** (ADR-17): on open, if `findings.embedding`
(`BLOB` column, old format) still exists, `Store` drops it
(`ALTER TABLE ... DROP COLUMN`) and deletes `embedding_signature`/
`embedding_dim` from `meta`.

**Schema migration v2 → v3** (ADR-21): `Store` lazily creates `code_chunks`
and `vec_code_chunks`, then sets `schema_version` to `3`. Already indexed repos
remain usable: the experimental code index stays empty until a
`cccr index --engine cocoindex` has been executed. The next manual
`cccr index` still works without filling `code_chunks`; no separate migration
command is required.

**Schema migration v3 → v4** (ADR-25): `Store` creates `endpoints`
(`CREATE TABLE IF NOT EXISTS`), purely additive — no existing data touched.
`vec_endpoints` (BACKLOG-10 K3) comes later, without bumping
`SCHEMA_VERSION`: like `vec_findings`/`vec_code_chunks`, it is created lazily on
first `set_endpoint_embedding()`, gated by `meta.endpoint_embedding_dim` — same
reasoning as adding `vec_code_chunks` in v2→v3 (ADR-21), which did not require
a separate bump for its own vector table either.

**Schema migration v4 → v5** (ADR-32, BACKLOG-13 M1):
`Store._migrate_module_columns` adds `module`/`qualified_name` to
`findings`/`endpoints` via `ALTER TABLE ... ADD COLUMN` (guarded by
`PRAGMA table_info`, idempotent) then creates the associated indexes — purely
additive, `NULL` for existing rows until the next `cccr index` recalculates
them. Important ordering constraint: `CREATE INDEX ... ON findings(module)` /
`endpoints(module)` cannot live in the same `executescript` as the
`CREATE TABLE IF NOT EXISTS` — on an existing v4 database, the `module` column
does not exist yet at that point (`CREATE TABLE IF NOT EXISTS` does not add a
column to an already existing table); the two `CREATE INDEX` statements
therefore live in `_migrate_module_columns`, after the `ALTER TABLE`, never in
the initial script.

**Schema migration v11 → v12**: `Store` creates `module_dependencies`
(`source`, `target`, unique pair). The table is replaced on every enabled
module-inventory phase of `cccr index`; it stores only declared Maven/Gradle
dependencies whose two modules are present locally. It is consumed solely by
`cccr modules graph` and `cccr export modules`, never by the REST/Kafka interaction graph.

## 3. Indexing pipeline (`indexer.index_repo`)

```
1. List repo files (`rglob`), first excluding any file under a
   src/<source-set> directory where <source-set> follows the Maven/Gradle
   naming convention for test source sets ("test" or ending in "Test" —
   indexer._is_test_source, BACKLOG-15 H2, ADR-34 — revisits ADR-14/R2,
   explicit decision; rule tightened in BACKLOG-16 P1 so it does not capture a
   generic src/<package> Python/JS/Rust layout), then matching include/exclude
   (`fnmatch`), compute their sha256.
2. If `meta.endpoint_inventory_signature` differs from
   `inventory_freshness.current_endpoint_inventory_signature()`, force
   `full=True` before computing `changed`: a changed REST/Kafka extractor makes
   the existing inventory potentially stale even without any modified file.
3. Compare with stored hashes (table `files`) → deleted / changed / unchanged.
   A file excluded by _is_test_source that had been indexed before this
   decision ends up in deleted (absent from current_hashes) and is purged on the
   next index, with no dedicated mechanism.
   If full=True: changed = all current files.
4. `store.remove_files(deleted)` — purges files + associated findings +
   endpoints (K1).
5. If changed is non-empty:
     raw = invoke_semgrep_raw(repo_root, config, files=changed)  — A SINGLE
       Semgrep scan, whether config.rules mixes findings rules and endpoint
       inventory rules (BACKLOG-11 A1) or not.
     when `include_semgrep_findings` is enabled (CLI: `--semgrep`), findings =
       parse_semgrep_json(raw, repo_root), filtered by min_severity (the filter
       used to live in run_semgrep, now applied here so `raw` can be shared with
       endpoints without rescanning). Otherwise findings already stored for
       changed files are kept.
     endpoints = parse_semgrep_endpoints(raw, repo_root) + infer_framework_endpoints(...)
       — no min_severity filter (K8 AC2: those are not findings).
     when `include_semgrep_findings` is enabled,
       store.replace_findings_for_files(changed, findings) — DELETE then INSERT,
       the only update mechanism (natively handles fixed findings).
     store.replace_endpoints_for_files(changed, endpoints)  — same mechanism.
     set_file_hash for each file in changed.
6. Write `meta.endpoint_inventory_signature` with the current signature once the
   endpoint inventory is refreshed.
7. Embedding (see §5):
       if meta.embedding_signature != current embedder signature:
         re-embed ALL store.all_findings() and update meta.
       otherwise: only embed findings from `changed` whose id does not already
         have an embedding in the DB (iter_embeddings()).
     Endpoints are not embedded (ADR-25: outside K1 scope).
8. Return IndexReport(scanned, skipped, findings_added, findings_removed,
   deleted_files, endpoints_added, endpoints_removed).
```

With `index_code_chunks=True` (used by
`coco_indexer.index_repo_with_cocoindex`): after scanning changed files, each
file is split into chunks of at most 80 lines, typed by extension (`.py` →
`python`, `.ts` → `typescript`, fallback `text`), and stored in `code_chunks`.
Re-embedding follows the same principle as for findings (BACKLOG-16 P5, filling
a gap left by X3/BACKLOG-8): if `meta.code_embedding_signature` differs from
the current embedder signature, **all** existing chunks
(`store.all_code_chunks()`) are re-embedded, not only those of `changed` files
— otherwise a same-dimension model change (the only condition that recreates
`vec_code_chunks`, see §5) would silently leave vectors from different models
coexisting. Otherwise, only chunks from `changed` files are (re-)embedded.
Deleted files go through `Store.remove_files`, which purges findings, chunks,
and associated embeddings.

`cccr index --engine cocoindex` calls that experimental adapter and writes
`meta.index_engine = "cocoindex-prototype"`. The manual engine remains the
default and writes `meta.index_engine = "manual"` when used through the CLI.

The embedder is built through `embedder.resolve_embedding_model()`: an
`embedding_model` of the form `org/model` is treated as a remote identifier only
if no explicit local path is requested. If the default local model
(`~/models/jina-code-embeddings-1.5b`) exists, `cccr` reuses it instead and
`meta.embedding_model` stores that effective path, not the historical value from
the config file.

The MCP tool `reindex_findings` (BACKLOG-16 P3) honors that same choice:
it reads `meta.index_engine` and dispatches to `index_repo_with_cocoindex`
if its value is `"cocoindex-prototype"`, otherwise to `index_repo` (and then
writes `"manual"`, parity with the CLI) — without that, a repo indexed with
`--engine cocoindex` would stop ever refreshing its code chunks as soon as an
agent reindexed through MCP rather than the CLI.

`findings_removed` / `endpoints_removed` are computed by counting, **before**
deletion, the rows already present for `deleted` and `changed` paths via
batched SQL `COUNT(*) WHERE path IN (...)` queries below the SQLite bind limit.

## 3bis. Rule initialization by `cccr init`

Without explicit `--rules` and without an existing Semgrep config in the target
repo, `cli.init()` tries the following sources in order:

1. `~/ccc-radar-skill/skills/cccr/rules/`
2. `~/cocoindex-ext-skill/skills/cccr/rules/` (legacy location, still checked
   for compatibility)

Each source must contain the packs `default`, `liveness`, `rest`, `kafka`,
`kafka-security`. If all are present, they are copied recursively into
`<repo>/.cccr/rules/<pack>/` via `shutil.copytree(..., dirs_exist_ok=True)` and
the generated config references those **paths relative to the target repo**
(never an absolute path to the skill repo, consistent with ADR-24). If a pack
is missing or no source exists, `cccr init` falls back to `p/security-audit`,
`p/java`, `p/owasp-top-ten` and `p/secrets`.

## 4. Semgrep execution (`scanner.py`)

Constructed command:
```
semgrep scan --json --quiet --x-ignore-semgrepignore-files --timeout <semgrep_timeout_s>
  --config <r1> --config <r2> ...   # one per config.rules entry
  <files from `files`>  or  "."     # targeted or full scan
```
Executed with `cwd=repo_root`. Return codes 0 and 1 are normal (1 = “findings
were found”); any other code raises `SemgrepError(stderr)`.
`--x-ignore-semgrepignore-files` is used so that the scope driven by
`.cccr/config.yml` is not silently reduced by `.semgrepignore` files or
Semgrep default ignores, notably on `tests/` directories.

**Notable side effect**: when an entry in `config.rules` contains a path with a
subdirectory (e.g. `rules/rules.yml`), Semgrep prefixes the returned `check_id`
with the path components (`rules.custom.sql-fstring` instead of
`custom.sql-fstring`). That real value is what gets stored in
`Finding.rule_id` — see ADR-9.

`parse_semgrep_json(raw, repo_root)` maps:
- `check_id` → `rule_id`
- `extra.severity`, normalized via a table that includes the older
  `LOW/MEDIUM/HIGH/CRITICAL` format → `INFO/WARNING/ERROR/ERROR`
- `path` relativized to `repo_root` (handles absolute or relative paths)
- `start.line` / `end.line`
- **snippet**: re-read from the source file (`repo_root/path`, lines
  `start_line`..`end_line`) rather than from `extra.lines` — see ADR-8.
  Returns `""` if the file is not readable; decoding uses
  `encoding="utf-8", errors="replace"` so that one legacy non-UTF-8 file does
  not make the whole indexing fail.
- `extra.fix` → `fix`
- `extra.metadata.cwe` / `.owasp`: string or list accepted, normalized to a
  list.

Filtering by `min_severity` is applied in `run_semgrep` (after
`parse_semgrep_json`, which returns everything unfiltered) — applied **at scan
 time only**; tightening `min_severity` in config does not affect already
indexed findings until their file is re-scanned (known defect R10).

### 4bis. REST + Kafka endpoint extraction (`run_semgrep_endpoints`,
BACKLOG-10 K11/K2)

Same Semgrep execution as `run_semgrep` (factored into `_invoke_semgrep`), but
without `min_severity` filtering: inventory rules have no meaningful severity.
`parse_semgrep_endpoints(raw, repo_root)` keeps only results where
`extra.metadata.category == "endpoint-inventory"` (other results — security
findings from a pack launched in the same `cccr index` — are silently ignored,
not an error) and `extra.metadata.system` (`"rest"` by default, or `"kafka"`);
any other system is ignored. Common to both systems:

- `role` comes directly from `extra.metadata` (`serve`/`call` in REST,
  `consume`/`produce` in Kafka).
- `framework` (optional) also comes from `extra.metadata`.
- `source = "code"` always here (no manifest — K10).
- Missing field in the metadata of an inventory rule (`role`, and `http_method`
  in REST) → explicit `SemgrepError`, same as malformed JSON.
- `module`/`qualified_name` (BACKLOG-13 M1) are computed systematically at
  construction time for both `Finding` (`parse_semgrep_json`) and
  `MessageEndpoint`: `maven.module_name_for_path(repo_root, path)` and
  `scanner._java_qualified_name(str(repo_root), path)` — see §2.

**REST** (`system: rest`, or missing) — `_extract_rest_path(snippet, repo_root,
source_path, start_line)`: first quoted literal from the snippet (re-read from
the source file, like `parse_semgrep_json`), searched line by line in order —
not only on the first line (BACKLOG-10 K13: a fluent `WebClient` or `RestClient` chain can
split `.get()` and `.uri(...)` across two lines; the snippet still remains
exactly bounded by the match's `start_line`/`end_line`, never reading code
outside the call). Missing, or followed by a concatenation (`+`) on the same
line → `topic_dynamic=True`, path kept as a usable literal prefix (or
`"<dynamic>"` if there is no literal) — never silently resolved (ADR-26).
Absolute/scheme-relative URLs are normalized into a canonical route
(`http://order-service/orders?x=1` → `/orders`): host, query string, and
fragment are dropped, initial slash enforced, repeated slashes compacted.
`topic = f"{http_method} {path}"` (e.g. `"GET /orders/{id}"`),
`http_method` fixed by the rule (one rule = one method).

**Class-level `@RequestMapping` prefix (BACKLOG Q24)**: a REST
`endpoint-inventory` rule is bounded to the annotated method — it never sees a
`@RequestMapping` carried by the enclosing class, while Spring MVC silently
prefixes the method path with it. `_class_base_path(repo_root, source_path,
start_line)` finds the nearest class/interface above `start_line` (best-effort,
line by line, ADR-26 — no AST) and its possible class-level `@RequestMapping`.
Two cases where the absence of a method-level literal does *not* mean
`<dynamic>`: an empty annotation (`@GetMapping`) or one carrying only non-path
attributes (`method=`/`produces=`/`consumes=`/`headers=`/`params=`/`name=`)
silently inherits the class path on the Spring side —
`_mapping_args_have_only_non_path_attrs` distinguishes that case from a truly
unknown value (constant reference, expression). Prefix and method path are each
normalized separately then joined by segment (`_join_rest_paths`) rather than
concatenated then renormalized: a naive concatenation `"" + "/" + "/orders/{id}"`
produces `"//orders/{id}"`, which `_normalize_rest_path` would wrongly interpret
as a protocol-relative URL (`http://orders/{id}`, `orders` swallowed as host).
A class-level `@RequestMapping` present but with no literal value makes the
whole path `<dynamic>`, even if the method itself has a literal path (the real
prefix remains unknown).

**`@FeignClient` base URL + `RestTemplate` calls (BACKLOG-9 N1)**:
`parse_semgrep_endpoints` now specializes `framework=resttemplate` calls and
Feign declarative bases.
- For Feign, `url=` and `path=` are read through `_FEIGN_CLIENT_RE` and
  `_named_string_arg`, resolved by `_resolve_rest_path_expression` (literal,
  Spring placeholder, or same-file `@Value` field), then merged with the
  method-level route.
- For `RestTemplate`, `parse_semgrep_endpoints` only keeps a Semgrep match if
  `_file_uses_resttemplate(...)` confirms a real client footprint in the file
  (import, instantiation, or `RestTemplate` type), which removes `Map.put(...)`
  false positives.
- `_extract_resttemplate_path` explicitly reads the first-argument expression
  for `getForObject`/`getForEntity`/`postForObject`/`postForEntity`/`put`/
  `delete`/`exchange`, instead of relying on the generic “first literal in the
  snippet” heuristic.
- `_resolve_rest_path_expression` splits a simple Java concatenation
  (`baseUrl + "/purchase" + id`) into segments, resolves the ones it knows
  (`"${...}"`, same-file `@Value`, literal), concatenates only the known
  pieces, then normalizes the result into a route; unknown segments keep the
  best available partial route but force `topic_dynamic=True`.
- `resolve_spring_property` also extends the standard module-local
  `application.*`/`bootstrap.*` search with Spring Cloud Config Server layouts
  `**/src/main/resources/configurations/<spring.application.name>.*` and
  `**/configurations/<spring.application.name>.*`, covering repos where client
  base URLs live in a dedicated config service.

**Framework inferences outside Semgrep**: `infer_framework_endpoints(repo_root,
files)` complements Semgrep matches. Java declaration-based inferences use the
shared Tree-sitter parser, so annotations remain attached to their actual
declarations across line breaks and comment text cannot be mistaken for code.
Covered cases:
- Spring mapping annotations on Java methods (`@GetMapping`, `@PostMapping`,
  `@PutMapping`, `@DeleteMapping`, `@PatchMapping`, and `@RequestMapping`):
  create `serve/rest` routes, or `call/rest` routes when their enclosing type
  bears `@FeignClient`. `@RequestMapping` without `method=` becomes
  `ANY /path`; its class prefix and `@RequestParam` query names are read from
  the AST. Feign `url`/`path` bases are resolved through the same property
  resolver before the method route is joined;
- `@RepositoryRestResource(path = "...")`: Spring Data REST family
  `GET/POST /path` and `GET/PUT/PATCH/DELETE /path/{id}`;
- `@EnableSwagger2`: endpoint `GET /swagger-ui.html`. Repository inheritance,
  `exported=false`, and Swagger annotations are likewise read from the AST;
- `RestTemplate` convenience calls (`getForObject`, `getForEntity`,
  `postForObject`, `postForEntity`, `put`, `delete`) and
  `exchange(urlExpr, HttpMethod.X, ...)`: `call/rest` endpoints inferred from
  Tree-sitter invocation nodes. The URL expression is the first AST argument,
  rather than a capture from a Semgrep snippet;
- Fluent `WebClient` calls: the verb invocation (`.get()`, `.post()`, etc.)
  and the following `.uri(...)` are connected through their nested AST
  receiver chain, including when the chain spans several lines;
- Spring Cloud Gateway Java `RouteLocatorBuilder.route(...).path(...).method(...).uri(...)`
  is read from the route lambda AST; YAML `spring.cloud.gateway.routes` /
  `spring.cloud.gateway.server.webflux.routes` is parsed as YAML:
  infer both the exposed `serve/rest` route and the proxy `call/rest` route;
  YAML `StripPrefix` filters are applied to the outbound path and `lb://` URI
  targets constrain the graph edge to the intended service;
- WebFlux `RouterFunctions.route(GET("/path"), ...)` / `.andRoute(...)`:
  infer exposed `serve/rest` routes from the nested predicate invocation AST;
- Configured generated API clients: with `--topic-strategy strategy1`, each
  uppercase constant containing an underscore in a class named `Rest*Config*`
  produces a `configured-api-client-configuration` call fact to its kebab-case
  form. The
  factory, bean declaration and generated-interface invocation are deliberately
  ignored. This fact preserves the A→B dependency without resolving a route or
  client type;
- External configured REST clients: with `--topic-strategy strategy1`, a
  `getRest().get("xxx")` invocation inside a
  `Rest*Config*` class produces a
  `configured-external-rest-api-properties` call fact. Its AST evidence carries
  `cccr-external-microservice:xxx`; `graph.build_graph` creates the HTTP edge
  even though `xxx` has no indexed endpoint, and graph renderers expose it as
  an external microservice rather than as an untyped external API;
- Maven OpenAPI model modules: with `--topic-strategy strategy1`, a service
  declaration `src/main/resources/openapi/<api>.rest` resolves the contract
  named `<api>` and declared by `inputSpec` or `inputSpecRootDirectory` in a
  `model-*` module. Its operations become `serve/rest` endpoints of the
  declaring service; no generated `XxxApi` Java interface is inspected;
- `management.endpoints.web.exposure.include=*` in `.properties`/`.yml`/
  `.yaml`: endpoint `GET /actuator/**`.
These endpoints reuse the same `MessageEndpoint` model as Semgrep matches, but
with dedicated `framework` values (`restclient`, `spring-data-rest`,
`spring-cloud-gateway`, `spring-webflux`, `swagger-ui`, `spring-actuator`) so they remain
distinguishable in rendering and graph logic.

**Kafka** (`system: kafka`) — `_extract_kafka_topic(snippet, repo_root,
source_path)`: same literal extraction as REST (`_find_first_literal`,
factored), then one extra case: a literal of the form `${property}` (Spring
property placeholder — e.g. `@KafkaListener(topics = "${app.kafka.topics.
orders}")`) is resolved through `resolve_spring_property(repo_root, property,
source_path)` rather than treated as a literal topic name (ADR-28). Resolved →
`topic_dynamic=False`, `topic` = resolved value; unresolved → placeholder kept
as-is, `topic_dynamic=True`.

**Kafka Streams DSL (BACKLOG Q25)**: second Kafka integration style,
distinct from the imperative `@KafkaListener`/`KafkaTemplate.send`
idiom (`StreamsBuilder.stream(...)` = consume, `KStream.to(...)` = produce).
Rules are intentionally restricted to forms carrying an unambiguous Kafka
Streams marker (`Consumed.with(...)`/`Produced.with(...)`, or nested inside a
`.join(...)`/`.peek(...)`) — a bare `$X.stream($TOPIC)` / `$X.to($TOPIC)`
would collide with `Arrays.stream(x)` / `Collection.stream()` / `Mono.to(...)`
(Reactor) / mappers `.to(Class)`, same logic as `cccr.kafka.java.consume-raw`.
`.to("topic")` is often chained after a `.peek(...)` whose lambda may contain a
literal (log message) before the topic in the snippet text —
`_KAFKA_STREAMS_TO_RE` specifically looks for the literal directly following
`.to(`, taking precedence over the generic “first literal” search (which would
otherwise wrongly take the log message). A `.join(...)` and the following
`.peek(...).to(...)` in the same chained expression may produce two endpoints
with the same `start_line` (the second encloses the first as a prefix of its
own expression) — `end_line` always differs, so there is no id collision
(`compute_endpoint_id` includes both).

**Variable fed by `@Value` (BACKLOG-10 K2, remaining part)**: when
`_find_first_literal` finds no literal at all (e.g.
`@KafkaListener(topics = ordersTopic)`, `kafkaTemplate.send(ordersTopic,
...)`), `_extract_kafka_topic` tries `_BARE_TOPIC_VAR_RE` on the first line of
the snippet to isolate the variable name (after `topics = `, `.send(`, or
`ProducerRecord(`), then `_resolve_value_annotated_variable(repo_root,
source_path, var_name)`: search in the **same source file** for a declaration
`@Value("${key}") ... var_name;` (Tree-sitter field declarations — no
cross-file tracking, no inheritance resolution) via
`_load_value_annotated_fields` (cached per file, `lru_cache`), then resolve the
found key like a normal placeholder (`resolve_spring_property`). Variable absent
from the file's `@Value` fields → `<dynamic>`, as before this task.

**Low-level `kafka-clients` API (BACKLOG-10 K2, remaining part)**: producing
through `new ProducerRecord(...)` was already covered before this task (same
class `org.apache.kafka.clients.producer.ProducerRecord`, Spring or not).
Consuming through the low-level API was not: `cccr.kafka.java.consume-raw`
(skill side) catches
`$CONSUMER.subscribe(Collections.singletonList(...))` /
`Arrays.asList(...)` / `List.of(...)` — restricted to those three forms so it
never confuses a non-Kafka `.subscribe(...)` (RxJava/Reactor take a lambda/
`Observer`, never a `Collection<String>` built by those helpers).
`subscribe(Pattern.compile(...))` (subscription by name pattern) is not covered
— documented, not silently handled.

`resolve_spring_property(repo_root, property_key, source_path=None)`:
`property_key` accepts Spring syntax `prop` or `prop:default`. It looks for the
key (flattened to dotted notation for nested YAML) in Spring configs discovered
around the source file: first the module containing `source_path` (ancestors of
the file + `src/main/resources` / module root), then the parent repo. Supported
names: `application.{yml,yaml,properties}`,
`bootstrap.{yml,yaml,properties}`, and profiled variants
`application-*.{yml,yaml,properties}` / `bootstrap-*.{...}`. Files are parsed
only once per process via `lru_cache`. Missing file or invalid YAML → skip to
next, never an error. Key not found anywhere → Spring default applies if
present, otherwise `None` (never guessed).

**Spring producer `send(Message<?>)` (BACKLOG-6 N1)**: `KafkaTemplate.send`
may receive a `Message<?>` whose topic is stored in the header
`TOPIC`/`KafkaHeaders.TOPIC`, therefore absent from the `.send(...)` call. The
AST-based Kafka detector follows the local `MessageBuilder...setHeader(TOPIC |
KafkaHeaders.TOPIC, expr)...build()` assignment in the enclosing method, then
emits a `produce` endpoint for the later `.send(variable)`. Topic resolution
uses the same hierarchy as the rest of Kafka scanning (literal, Spring
placeholder, `@Value` field in the same file, otherwise `<dynamic>`).

`scanner.clear_analysis_caches()` (BACKLOG-16 P2) clears all path-based
best-effort analysis `lru_cache`s (`_java_qualified_name`,
`_load_flat_spring_properties`, `_load_value_annotated_fields`,
`maven._cached_module_name`, `gradle._service_roots`) — called at the start of
`indexer.index_repo`. These caches speed up one indexing run (same
`application.yml`/`pom.xml` read several times), but an MCP server is a
long-lived process: without clearing on every indexing, a Spring property, an
artifactId, or a Java package modified between two `cccr index` runs would keep
being resolved with its old value.

## 5. Embedding and search

`embedder.finding_to_text(f)` — exact format (frozen contract, used for the
index **and** to verify relevance through `eval/run_eval.py`):
```
f"{f.rule_id} | {f.severity} | {f.message} | {' '.join(f.cwe + f.owasp)} | {f.path} | {' '.join(f.snippet.split())[:500]}"
```

`Embedder` (sentence-transformers, default model
`~/models/jina-code-embeddings-1.5b`) resolves `~` with `expanduser()`, loads
the model lazily on first call, encodes in batches, L2-normalizes, and returns
`float32`.
`embed_query` reuses `embed_texts` on a one-item list. The public factory
`make_embedder(model_name)` is cached by model and fake mode in the process,
which avoids reloading the model on every MCP call. Each embedder exposes a
`signature` stored in `meta.embedding_signature`; vector dimension is stored in
`meta.embedding_dim`.

`search.search_findings` uses a precision-first lexical ranking:
1. First filter in SQL (`store.all_findings(severity_at_least, rule_id,
   path_glob)`) → candidate set.
2. Tokenize the query and each candidate's `rule_id`, message, path,
   `CWE`/`OWASP`, snippet and severity.
3. Keep a candidate only when it contains **every** query token; this prevents
   generic words from returning loosely related findings.
4. Score exact/full-field and token matches, sort deterministically by score
   then severity/path/line, and paginate (`offset`, `limit`).

The `embedder` parameter is retained for CLI/MCP compatibility but this query
does not read `vec_findings` or embedding metadata; a model mismatch cannot
make findings search fail.

`search.summary`: `by_severity`/`top_rules` via `Store.counts_by` (SQL
`GROUP BY`), `by_top_level_dir` computed in Python from
`finding.path.split("/", 1)[0]`.

`search.get_context(repo_root, finding, before=5, after=5)`: re-reads the
source file, returns lines `[start_line-before, end_line+after]` bounded to
`[1, len(lines)]`, prefixed with `f"{n:>5}| {line}"`. Renderers catch read
errors per finding: JSON exposes `context: null` and `context_error`, text
rendering displays an unavailable context note.

## 6. Interaction graph (`graph.py`, BACKLOG-10 K12)

Pure functions, no SQLite write (ADR-27):

- `build_graph(endpoints_by_service: dict[str, list[MessageEndpoint]]) -> list[GraphEdge]`
  — `"rest"` edge when a `role=call` endpoint from one service matches
  (`paths_match`) a `role=serve` endpoint from a **different** service ;
  `"kafka"` edge when a `role=produce` and a `role=consume` from **different**
  services share the same `topic` (strict equality). No self-edge (same service
  on both sides, ignored).
- `paths_match(call_topic, serve_topic) -> bool` — `topic` has the form
  `"METHOD /path"` (K11). Same method required; caller-side `<dynamic>` never
  matches; otherwise path segments (`/`-separated) are compared one by one, a
  `{...}` segment on either side accepts anything, and the caller may have
  **fewer** segments than the exposed route (literal prefix before
  concatenation, ADR-26) but never more. Best-effort by design (K12 AC4):
  no match → no edge, never an exception.
- `find_outbound_calls_in_consumers(endpoints) -> list[OutboundCallInConsumer]`
  — for a **single** service (file/lines not comparable across repos): a `call`
  whose `start_line` falls inside `[consume.start_line, consume.end_line]` of
  the same file. Does not depend on the multi-service graph — works as soon as a
  single project is indexed (K1/K11 are enough).

REST routes keep their raw `METHOD /path` form in `MessageEndpoint.topic` so
route matching remains possible. Every graph, dependency, and architecture-path
view qualifies a published REST resource with its provider —
`<microservice>: METHOD /path` — so identical routes exposed by distinct
services remain distinguishable.

For a federated workspace, runtime dependencies (both REST and Kafka) are
derived from every indexed runtime microservice. If one is missing or
unreadable, the graph remains partial rather than discarding the available
relations, and explicitly reports the missing services; topology never hides
facts that were actually indexed.

`endpoints_by_service`: two ways to produce that multi-key dict, consumed by
`build_graph` — topology depends only on the *shape* of the dict, never on
where it came from:
- `workspace.load_federation` (§6ter) — several services indexed
  **separately**, federated at query time (BACKLOG-11 A2).
- `group_endpoints_by_module(endpoints) -> dict[str, list[MessageEndpoint]]`
  (BACKLOG-13 M2) — a **single** index covering several Maven modules or
  Gradle services (`endpoint.module`, M1/H1), without federation. An endpoint without a module
  (`None`) is excluded from grouping: without a stable
  name, it can never form a reliable inter-service edge — deliberately
  conservative choice (fewer detected edges rather than an invented edge
  between two unattributed sites that would arbitrarily fall into the same
  `None` bucket).

CLI `cccr export microservices --json` / MCP tool `graph` (§2/§3): without `--workspace`/
`workspace_root`, they first try `group_endpoints_by_module` on endpoints from
the current project; if the result is non-empty, they build the graph directly
(no federation). Otherwise (no Maven module or Gradle service detected), `services`/`nodes`/
`edges` remain empty with an explicit note — same behavior as before
BACKLOG-13.
Providing `--workspace`/`workspace_root` always triggers full federation,
unchanged.

`render_graph_json` exposes the topology for `cccr export microservices --json`;
`render_graph_text` remains available to programmatic callers. The result has
`services` (the indexed service keys plus any explicitly inferred external
service), `nodes` (services + Kafka
topics), `edges` (every `GraphEdge` returned by `build_graph`, REST or Kafka,
with both endpoint sites), plus `outbound_calls_in_consumers`.

### 6ter. Multi-service federation (`workspace.py`, BACKLOG-11 A2, ADR-30)

`maven.py` (new, BACKLOG-13 M1, ADR-32) factors out the minimal `pom.xml`
reading shared between `workspace.py` and `scanner.py`:
- `parse_pom(pom_path) -> tuple[str | None, bool, str | None]` —
  `(artifactId, is_spring_boot_app, packaging)`, `(None, False, None)` if the
  pom is unreadable/malformed (one broken module never blocks the others).
  `is_spring_boot_app` combines two signals: textual presence of
  `spring-boot-maven-plugin` in the pom, or presence under
  `src/main/java/**/*.java` of a class with `main()` calling
  `SpringApplication.run(...)`.
- `module_name_for_path(repo_root, rel_path) -> str | None` — used by
  `scanner.py` (§4bis), not by `workspace.py` (see §6bis).

- `discover_maven_services(root: Path) -> list[DiscoveredService]` —
  compatibility wrapper now delegating to workspace discovery. It still
  returns Maven modules exactly as before, but also adds Gradle microservices
  detected from top-level directories containing a Spring Boot `main()`
  somewhere in their subtree (`gradle.discover_gradle_service_roots`).
  Maven entries still come from `root.rglob("pom.xml")`, sorted by path, with
  `artifactId` (fallback: directory name) and `kind =
  "microservice"|"shared-module"` according to `maven.parse_pom`; Gradle
  entries are always `kind="microservice"`. `indexed` is true if
  `<service>/.cccr/findings.db` exists, or if `<root>/.cccr/findings.db`
  exists (mono-indexed parent workspace).
- `load_federation(services) -> FederationResult` — for each indexed service,
  open `Store(service.index_root, readonly=True)`: either the module database,
  or the parent mono-indexed one. In the parent-database case, findings and
  endpoints are filtered on `endpoint.module`/`finding.module == service.name`.
  `findings_by_service` is always populated ; `endpoints_by_service` only for
  `kind="microservice"` (A2 AC5 — a shared module is never a source of
  endpoints). Non-indexed service, missing DB, or incompatible schema
  (`StoreError`) → message added to `warnings`, federation continues with the
  others (K7 AC2).
- `Store(path, readonly=True)` (`store.py`) — SQLite connection
  `file:...?mode=ro` (URI), no `_create_schema()`/migration, no `commit()` on
  exit — see ADR-30 for the no-write guarantee. `StoreError` if the database is
  missing (before even attempting the connection) or if `schema_version` does
  not match the current schema.

`FederationResult.endpoints_by_service`/`.findings_by_service` are directly the
multi-key dicts expected by `graph.build_graph` and `trace_flow` (§6bis/§6quater)
— `workspace.py` knows nothing about the graph, `graph.py` knows nothing about
Maven or SQLite: the coupling is only through dict shape.

`tests/test_k7_federation_e2e.py` (BACKLOG-10 K7) chains the three layers on
real fixtures: two Maven microservices indexed separately through the CLI
(`cccr init`/`cccr index`, each ignoring the other), federated by
`discover_maven_services`/`load_federation`, then `graph.build_graph` detects
the Kafka edge between producer and consumer — the only end-to-end proof, beyond
K1/K2/K11 each tested in isolation, that the full chain works.

### 6quater. Flow tracing (`flow.py`, BACKLOG-10 K5)

Pure functions, no SQLite write:

- `resolve_topic(query, all_topics) -> str | None` — exact name first;
  otherwise case-insensitive substring, only if it designates a **unique**
  topic/route among `all_topics` (ambiguous → `None`, never an arbitrary choice).
- `resolve_topic_by_similarity(store, embedder, query, endpoints,
  min_score=0.35) -> str | None` (BACKLOG-10 K3) — last resort when
  `resolve_topic` fails: nearest neighbor among endpoints already embedded in
  `store` (`Store.knn_search_endpoints`, §2 `vec_endpoints`), but only if its
  score exceeds `min_score` — below that threshold, `None` rather than an
  irrelevant candidate (same philosophy as `topic_dynamic`: never guessed).
  Unlike `resolve_topic`/`trace_flow`, this function touches SQLite and the
  embedder: it lives in `flow.py` (thematic consistency) but is not pure — CLI/
  MCP callers invoke it explicitly when falling back from a `FlowError` raised
  by `trace_flow`, never from inside `trace_flow` itself. Threshold not
  empirically calibrated against a real model yet; `0.35` is the documented
  starting point.
- `trace_flow(query, endpoints_by_service, findings_by_service, warnings=None)
  -> FlowResult` — resolves `query` through `resolve_topic` (failure →
  `FlowError`), then for each endpoint whose `topic == resolved_topic` in any
  service builds a `FlowSite` (`service`, `endpoint`, overlapping `findings` —
  same file+line join as the rest of the project, spirit of ADR-19).
  `endpoints_by_service`/`findings_by_service` have the same shape as in
  `workspace.py` (§6ter) but with possible `None` keys (current-project mode,
  outside federation) — `flow.py` knows nothing about Maven, `workspace.py`
  knows nothing about `flow.py`: coupling only through dict shape. `warnings`
  (K7 AC2 federation warnings, already emitted by `load_federation`) are passed
  through unchanged to `FlowResult.warnings` — never silently swallowed: a site
  missing because a service was not federated must remain visible, distinct from
  a real absence of producer/consumer.

- `group_endpoints_by_module_for_flow(endpoints) -> dict[str | None,
  list[MessageEndpoint]]` / `group_findings_by_module_for_flow(findings)`
  (BACKLOG-13 M3) — group by `endpoint.module`/`finding.module`, but **without
  excluding** entries without a module (`None` key kept), unlike
  `graph.group_endpoints_by_module`: listing all sites of a topic is `flow`'s
  contract, and `trace_flow` never compares keys with each other (no false edge
  risk to avoid here, unlike the graph).

The MCP tool `trace_message_flow(query, workspace_root=None)` uses
`group_endpoints_by_module_for_flow(store.all_endpoints())` on the current
project, or `discover_maven_services`/`load_federation` (§6ter) for a
workspace. `render_flow_json` (`render.py`) defines its response contract.

**Similarity fallback (BACKLOG-10 K3)**, current-project mode only
(no federation — each federated service would need its own KNN query on its own
store, not wired): `topics search` and `apis search` retry through
`resolve_topic_by_similarity` after textual resolution fails. The MCP trace
keeps its own explicit flow handling. `ConfigError`/`EmbeddingError` during
the CLI attempt (missing config, unavailable model) are silently absorbed
**only to preserve the original textual error**, never to hide another
problem — tested in `tests/test_flow.py` (threshold, with directly built
vectors) and `tests/test_k5_flow_e2e.py`/`tests/test_mcp_server.py`
(CLI/MCP wiring, by substituting `resolve_topic_by_similarity` rather than
relying on a real/fake embedder over arbitrary text — threshold not
calibrated on a production corpus yet).

## 7. JSON contract (F4.2 — frozen)

Consumed by `cccr search --json`, the MCP tool `search_findings`, and (without
`score`) by `search_code_with_findings`:
```json
{
  "id": "str", "rule_id": "str", "severity": "INFO|WARNING|ERROR",
  "message": "str", "path": "str", "start_line": 0, "end_line": 0,
  "score": 0.0, "fix": "str|null", "cwe": ["str"], "owasp": ["str"],
  "context": "str (optional)"
}
```
This schema must not be modified without updating the serialization points in
`render.py`.

## 8. Tests and fixtures

- `tests/fixtures/vuln_repo/`: mini-repo with 4 vulnerable files (SQL
  injection through f-string, `subprocess.run(shell=True)`, `yaml.load` without
  Loader, `random.random` for a token) and `rules/rules.yml` (4 local Semgrep
  rules, never a registry pack — deterministic offline tests).
- Tests marked `@pytest.mark.integration`: execute the real Semgrep binary
  (required, installed in the CI/dev environment).
- Tests marked `@pytest.mark.slow`: download the real sentence-transformers
  model — **excluded by default** (`addopts = "-m 'not slow'"` in
  `pyproject.toml`, see ADR-11); run explicitly with `uv run pytest -m slow`.
- `CCCR_FAKE_EMBEDDER=1`: switches `embedder.make_embedder` to a deterministic
  embedder (SHA-256 hash, 8 dimensions, signature `fake:<model>:8`) for
  integration tests that do not need real semantics. An index created with that
  fake is distinguished from a production index through `embedding_signature`.
- `eval/run_eval.py`: indexes a temporary copy of `vuln_repo` with the real
  embedder, computes top-3 hit rate on `eval/queries.yml` (8 FR/EN queries).
  Passing threshold: ≥ 0.75 (measured: 1.00 on the latest run).
