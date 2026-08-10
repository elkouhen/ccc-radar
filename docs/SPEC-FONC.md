# Functional specification — ccc-radar (`cccr`)

> Describes the observable behavior of the three delivered surfaces: CLI, MCP
> server, and Claude Code skill. The scope is intentionally read at two levels:
> a **core product** (indexed Semgrep findings that can be searched and joined
> to code) and a **Java/Spring microservices audit extension** (REST/Kafka
> endpoints, graph, flow, workspace) built on top of the same index. For the
> internal architecture (schemas, algorithms), see [`SPEC-TECH.md`](./SPEC-TECH.md).
> For the reasoning behind the choices, see [`ADR.md`](./ADR.md).

## 1. Project configuration

File `.cccr/config.yml`, at the root of the target repo:

```yaml
rules:                  # required — paths or Semgrep config identifiers
  - rules/rules.yml
include:                # default: ["**/*"]
  - "**/*"
exclude:                # default: [".git/**", ".venv/**", "node_modules/**", ".cccr/**"]
  - ".git/**"
  - ".venv/**"
  - "node_modules/**"
  - ".cccr/**"
min_severity: INFO      # INFO | WARNING | ERROR
embedding_model: ~/models/jina-code-embeddings-1.5b
semgrep_timeout_s: 120
semgrep_enabled: true       # set false to disable all Semgrep execution
```

- `rules` is required when `semgrep_enabled` is `true`; with
  `semgrep_enabled: false`, it may be omitted and no Semgrep process is run.
- An invalid `min_severity` (outside `INFO`/`WARNING`/`ERROR`) is a blocking
  error.
- All other fields have a default value applied silently when absent from the
  file.
- The default `embedding_model` targets a model previously downloaded under
  `~/models/jina-code-embeddings-1.5b`; `~` is resolved at runtime.
- If `embedding_model` looks like a remote Hugging Face identifier
  (`org/model`) but the default local model already exists, `cccr`
  automatically reuses that local model to avoid fragile network/TLS downloads;
  `cccr index` explicitly reports that fallback and the database stores the
  model actually used.
- Independently from `include`/`exclude`: any file under a
  `src/<source-set>` directory where `<source-set>` follows the Maven/Gradle
  naming convention for test source sets (`test`, `componentTest`,
  `contractTest`, `endToEndTest`, ... — name equal to `test` or ending in
  `Test`) is **always** excluded from scanning, for both findings and
  endpoints (ADR-34) — neither configurable nor bypassable through `include`.
  A generic `src/<package>` layout (Python, JS, Rust, ...) is **not**
  concerned: `<package>` does not follow that convention. A file that was
  already indexed and becomes excluded by this rule is purged on the next
  `cccr index`, just like a file deleted from disk.

## 2. `cccr` CLI

### `cccr version`
Displays the package version (`0.1.0`).

### `cccr init [--rules PATH]... [--rules-root DIR]`
Creates `.cccr/config.yml`.

- Repeatable `--rules`: paths or Semgrep config identifiers (e.g.
  `rules/rules.yml`, `p/security-audit`).
- `--rules-root`: directory containing the five bundled packs; takes precedence
  over automatic skill discovery. `CCCR_RULES_ROOT` supplies the same explicit,
  portable location.
- Without `--rules`: automatic detection in the order `.semgrep.yml` →
  `semgrep.yml` → `.semgrep`. If nothing is found, `cccr init` first looks for a
  local `ccc-radar-skill` repo (supported candidates:
  `~/ccc-radar-skill/skills/cccr/rules/` then
  `~/cocoindex-ext-skill/skills/cccr/rules/`, plus common agent skill roots)
  and, if it finds the five
  expected packs `default`, `liveness`, `rest`, `kafka`, `kafka-security`, it
  copies them into `.cccr/rules/` in the target repo and writes `rules:` with
  those **relative** paths (`.cccr/rules/<pack>`). If the skill is missing or
  incomplete, it falls back to the default Semgrep registry rulesets
  `p/security-audit`, `p/java`, `p/owasp-top-ten` and `p/secrets` (no error):
  informational stdout message explaining the
  fallback and how to customize it, exit code 0. This fallback keeps the
  **core product** usable, but does not by itself activate the microservices
  extension (`microservices` / `topics` / `dtos` / `apis` / `mongodb`). Priority order:
  explicit `--rules` > detected local config > copied skill packs > default
  registry pack.
- Each automatic copy writes `.cccr/rules/manifest.json`, recording the source
  and SHA-256 of every pack file. It makes the copied rule set auditable and
  lets a future update command compare it without relying on an absolute path
  in `config.yml`.
- If `.cccr/config.yml` already exists: explicit error, exit code 1, and the
  existing file is never overwritten.

### `cccr doctor [--json]`

Read-only preflight for an architecture audit. It reports the availability of
Semgrep (blocking), the configuration, the active `liveness`/`rest`/`kafka`/`kafka-security` packs,
the local embedding model and index state. Any blocking check yields exit code
2. A graph must not be interpreted as a full REST/Kafka topology while the
architecture-pack check fails.

### `cccr index [--full] [--engine manual|cocoindex] [--topic-strategy default|strategy1]`
Indexes the project (Semgrep findings **and** REST/Kafka endpoints). Kafka
endpoints also retain a source-level Java payload type when it is explicit in a
listener parameter or Kafka client generic signature. An unavailable type stays
empty; it is never inferred from a topic name or serializer configuration.

- Default: incremental — only re-scans files added or modified since the last
  indexing (SHA-256 hash per file); files deleted from disk have their findings
  and endpoints purged.
- `--full`: forces a full scan, as if all files were modified (files deleted
  from disk are still purged).
- `--engine manual` (default): indexes findings and endpoints with the
  historical incremental engine.
- `--engine cocoindex`: experimental mode inspired by CocoIndex. It indexes the
  same findings and endpoints, and adds a local code chunk index
  (`code_chunks` + embeddings) for local code-index experiments.
- If the endpoint extractor signature stored in the index is missing or stale,
  `cccr index` implicitly forces a full repo re-scan even without modified
  files, to refresh the REST/Kafka inventory before rewriting
  `meta.endpoint_inventory_signature`.
- `--topic-strategy strategy1` is an opt-in convention extractor for the
  manual engine. It maps `getTopics().getAbcDefGhiJkl()` and
  `${kafka.topics.abc_def_ghi_jkl.name}` to physical topic `ABC_DEF_GHI_JKL`.
  It also materializes a high-confidence `request_reply` relation from a
  request topic to an existing reply topic named `retour_<request-topic>`.
  It also activates configured HTTP-client dependencies: every uppercase
  constant containing an underscore found in a `Rest*Config*` class declares a
  dependency to its kebab-case form. In the same class,
  `getRest().get("xxx")` declares an HTTP call to a new
  `xxx` microservice annotated as external. A service declaration
  `src/main/resources/openapi/<api>.rest` attributes publication to that
  service: the contract named `<api>` and configured by `inputSpec` or
  `inputSpecRootDirectory` in a `model-*` module is parsed; generated `XxxApi`
  interfaces are not used to infer the API.
  It replaces the standard Kafka extraction at the same source location and a
  strategy change forces a full inventory refresh. It is rejected with
  `--engine cocoindex`.
- Exactly one Semgrep scan per indexing: `config.rules` may mix findings rules
  (`default`, `liveness`) and endpoint inventory rules (`rest`, `kafka`,
  `metadata.category: endpoint-inventory`) — each ends up in the proper table
  without colliding (see `docs/SPEC-TECH.md`, §3). Endpoint inventory rules are
  not filtered by `min_severity`.
- The CLI prints stage progress during indexing: repository file inventory,
  delta computation, Semgrep scan, persistence of findings/endpoints, and
  embedding passes (plus code chunks in `cocoindex` mode).
- One-line output:
  `scanned=<N> skipped=<N> +findings=<N> -findings=<N> +endpoints=<N> -endpoints=<N>`
  - `scanned`: number of files (re)scanned.
  - `skipped`: number of unchanged files not re-scanned.
  - `+findings`/`-findings`: findings (re)inserted / removed for scanned files
    or files deleted from disk.
  - `+endpoints`/`-endpoints`: endpoints (re)inserted / removed, same logic.
- Exit code 0 on success.
- Semgrep failure (timeout, crash, unexpected return code): error message on
  stderr, **exit code 2**, `.cccr/findings.db` remains unchanged (no partial
  write, including findings and endpoints).
- Missing or invalid `.cccr/config.yml`: error message on stderr, exit code 1.

### `cccr findings ["<query>"] [options]`
Without a query, lists indexed findings in deterministic severity/location
order, with the same filters and pagination. With a query, it performs a
precision-first lexical search in indexed findings only.
Every query token must match a finding's `rule_id`, message, path,
`CWE`/`OWASP`, snippet or severity. Exact and full-field matches rank first.
This intentionally favours a short, verifiable result set over broad
natural-language recall; `custom.subprocess-shell-true`, `CWE-89`, a path
fragment, or `sql injection` surface only findings that contain all terms.

| Option | Effect |
|---|---|
| `--severity S` | keeps only findings with severity ≥ S (S ∈ INFO/WARNING/ERROR ; a value outside this set — e.g. raw Semgrep severity `HIGH` — is a blocking error, exit code 2) |
| `--rule R` | keeps only findings from rule `R` (exact equality on `rule_id`) |
| `--path GLOB` | keeps only findings whose path matches the glob (style `fnmatch`) |
| `--limit N` | maximum number of results (default 5) |
| `--offset N` | pagination (default 0) |
| `--context` | adds code context (5 lines before/after, bounded to the file) |
| `--json` | structured JSON output instead of text rendering |

Text rendering, one block per result:
```
1. [ERROR] custom.sql-fstring  app/db.py:12-14  (0.83)
   An SQL query built with an f-string allows SQL injection.
```
With `--context`, the numbered code block is added afterwards (format
`{n:>5}| {line}`). If the source file disappeared or is no longer readable
since the last indexing, the finding is still displayed and the context is
reported as unavailable for that result only.

`--json` rendering of `cccr findings`: list of objects — **stable contract**
(`FindingHit`, `render.py`), also consumed by the MCP server
(`search_findings`):
```json
{
  "id": "...", "rule_id": "...", "severity": "...", "message": "...",
  "path": "...", "start_line": 0, "end_line": 0, "score": 0.0,
  "fix": null, "cwe": [], "owasp": [],
  "context": null,        // always present; string if --context succeeded
  "context_error": null   // always present; string if --context failed
}
```
`context`/`context_error` are always present (default value `null`) — stable
schema rather than keys appearing/disappearing depending on `--context`
(necessary for a correct MCP `outputSchema`, see §3).

If the index does not exist (`.cccr/findings.db` absent): exact stderr message
`Index absent. Run first: cccr index`, exit code 2.

### `cccr summary [--json]`
Aggregated view of findings.

Text rendering, 3 lines: totals by severity, top 10 rules with count, count by
first-level directory.

`--json` rendering:
```json
{
  "by_severity": {"ERROR": 2, "WARNING": 2},
  "top_rules": [{"rule_id": "...", "count": 2}, ...],
  "by_top_level_dir": {"app": 4}
}
```

Same “index absent” rules as `findings` (same message, code 2).

### `cccr export microservices [--workspace ROOT] [--vscode-wsl-distro DISTRO] (--html FILE | --c4 DIR | --json)`

`--vscode-wsl-distro DISTRO` makes finding links in the HTML export use the
Windows WSL UNC path (`vscode://file//wsl.localhost/DISTRO/...`). This lets an
HTML file opened on Windows navigate to source files indexed inside WSL.
The interactive HTML inspector exposes VS Code links for findings, OpenAPI
contracts and discovered Kafka DTO source files. Microservice node labels also
include their finding count.
Microservices whose name contains `test`, or is an unresolved Maven-style
placeholder such as `${artifactId}`, are excluded from microservice exports.
*Java/Spring microservices extension — beta.*

Inter-service graph built from indexed endpoints: microservices linked by
HTTP endpoints (`call` -> `serve`) and Kafka topics (`produce` -> `consume`).
Always
included: synchronous REST calls detected inside a Kafka consumer handler
**of the current project** (same file, call site inside the handler's line
range).

`cccr export microservices` is the only command for the microservice dependency
graph. `--json` writes the structured graph to standard output; the visual and
LikeC4 formats always include the indexed MongoDB collections. Collections are detected from `@Document`,
repository entity mappings, and a literal trailing collection argument on
indexed `MongoTemplate`/`MongoOperations` calls. Dynamic expressions are not
guessed.

For the inter-service topology, two sources are possible, tried in this order:
1. **Without `--workspace`**: if the index covers a multi-module Maven or
  Gradle directory (`cccr index` run at the parent directory, with endpoints
  assigned to a module during indexing), endpoints are grouped by module and
  the graph is built directly from that single index —
  no federation needed for a monorepo.
2. **With `--workspace ROOT`**: also federates Maven/Gradle microservices
  under `ROOT`, indexed **separately** (read-only —
  `discover_maven_services`/`load_federation`) — the path for services that
  live in genuinely separate repos.

Both sources feed the same algorithm (`graph.build_graph`) and report:
- **services**: service/module names participating in the inter-service graph;
- **nodes**: microservices plus Kafka topic nodes used in the rendered topology;
  an inferred external microservice has `"external": true`;
- **edges**: REST and Kafka edges with both sites (`from_site` / `to_site`)
  and their topic/route labels;
- **outbound_calls_in_consumers**: synchronous REST calls detected inside a
  Kafka consumer handler of the current project.

If neither is available (repo without module attribution and without
`--workspace`, or no federable service detected), `services`/`nodes`/`edges`
remain empty, with a `note`
explicitly saying so (see ADR-27) rather than making the absence of a result
ambiguous.

`--json` output:
```json
{
  "services": ["service-x", "service-y", "service-z"],
  "nodes": [
    {"name": "service-x", "kind": "microservice"},
    {"name": "service-y", "kind": "microservice"},
    {"name": "service-z", "kind": "microservice"}
  ],
  "edges": [
    {"kind": "rest", "from_node": "service-x", "to_node": "service-y",
     "from_site": {"path": "...", "start_line": 13, "end_line": 13, "topic": "GET /y-status"},
     "to_site": {"path": "...", "start_line": 9, "end_line": 11, "topic": "GET /y-status"}}
  ],
  "outbound_calls_in_consumers": [
    {"consumer": {"path": "...", "start_line": 15, "end_line": 25, "topic": "orders.created"},
     "call": {"path": "...", "start_line": 20, "end_line": 20, "topic": "POST /payments"}}
  ],
  "note": ""
}
```
`note` is empty as soon as an inter-module data source (Maven module or
`--workspace`) produced a result without warning; otherwise it concatenates the
applicable warnings, whether they come from federation (`service` not indexed,
incompatible database) or from a stale endpoint inventory on the current
project. Without inter-module data, `services`, `nodes`, and `edges` stay
empty.

Same “index absent” rules as `findings`/`summary` (same message, code 2) —
`endpoints` lives in the same database as `findings` (`.cccr/findings.db`).
`--workspace` never makes the command fail: a missing or incompatible federated
service is reported in `note`, not as an error.

When a service has Kafka entries from an indexed Markdown manifest, those
entries are authoritative for that service and replace its Kafka endpoints
detected from code. Services absent from the manifest keep code detection.

`--html FILE` writes an interactive Sigma.js graph backed by Graphology. Nodes
can be zoomed, panned, searched, or selected; selecting one mutes unrelated
nodes and edges so dense hubs remain inspectable. The details panel lists the
APIs exposed by the selected microservice, and the published and consumed Java
message types of a selected Kafka topic when statically known, as well as direct relations. Each
relation uses a stable `source -> target : action` wording rather than a
selection-relative direction. The `HTTP`, `Kafka` and `MongoDB` toggles independently
show or hide those relations. Separate resource toggles control internal
microservices, external microservices, Kafka topics and MongoDB collections;
relations attached to a hidden resource are hidden too. The path search is kept in the
expandable `Explorer un chemin` control so that the primary exploration controls remain compact. A text field accepts a path such as
`service-a -> topic-1 -> service-b` and highlights the shortest directed path
between every pair of consecutive entries, preserving REST call direction and
Kafka `producer -> topic -> consumer` steps; producer-to-topic path relations include
their statically inferred published Java message types, also grouped in a
dedicated path-details section and rendered beside the Kafka topic in the path
description (`producer -> TOPIC (JavaType) -> consumer`). The `Verrouiller`
toggle preserves the path form while a node is selected for inspection, so the
path can be shown again without entering its stops anew. An unknown type is
rendered explicitly rather than silently omitted; when publication has no
known type, the topic falls back to its statically known consumer type. The first
and last entries must be microservices; intermediate entries can be
microservices or Kafka topics. The
generated document persists the current node selection or path in its URL
fragment, so a browser refresh restores it, embeds graph data locally and loads Sigma.js from its CDN
when opened. Its legend is also collapsible. Internal microservices are hexagons,
external microservices triangles, Kafka topics circles and MongoDB collections
squares; a blue, orange or red border reflects a microservice's
relative complexity. Microservices are ranked by their number of direct HTTP,
Kafka and MongoDB relations, then split into three groups of equal size as far
as possible (lowest, middle and highest third). Topics and MongoDB collections
are not assigned a complexity level.

The HTML legend distinguishes relationship protocols: HTTP calls are purple,
Kafka publications green, Kafka consumptions orange and MongoDB accesses blue.

`--c4 DIR` writes a runnable LikeC4 project in `DIR`: `architecture.c4`, the
LikeC4 configuration, `package.json`, `.gitignore` and a README. It declares
custom `microservice`, `external_microservice`, `kafka_topic`,
`mongodb_collection` and `external_api` elements, plus `http`, `publishes`,
`consumes`, and data-access relations. It also renders
unmatched HTTP calls as external HTTP API elements, appends statically inferred
Java payload types to Kafka relation labels, distinguishes indexed MongoDB reads
from writes, and lists detected OpenAPI contracts in microservice descriptions.
MongoDB collection nodes are scoped by microservice because a collection name
alone does not establish a shared database. HTTP relations are consolidated to
one link per caller/callee microservice pair, independent of the number of
published resources matched between them. The model is an inferred static
topology, never a runtime trace. No equivalent MCP tool — generated files are
not agent-consumable results, unlike the JSON returned by `graph`.

### `cccr microservices [list|show|topics|apis|mongodb|neighbors|implementation|properties|openapi]`
*Java/Spring microservices extension — beta.*

Discovers Maven modules and Gradle Spring Boot services under `root` (default:
current directory, ADR-30/ADR-33). A Maven module is created for each found
`pom.xml`, named after its `artifactId`, and classified as
`microservice`
(the module carries a `main()` class that runs `SpringApplication.run(...)`, or
its `pom.xml` declares Spring Boot on a runtime packaging) or `shared-module`
otherwise. Aggregator poms with `packaging=pom` are ignored unless they are a
runtime Spring Boot service. A Gradle service is detected from a Java `main()`
calling `SpringApplication.run(...)`, directly or in a subproject; its name is
the configured Gradle archive name, then `rootProject.name`, or the default
Gradle project name if neither is explicit. It is always classified as
`microservice`. For each discovered service
already indexed (`cccr index` run either inside the module itself or once at the
multi-module parent), it reads its database **read-only** (never writes into
another project's database) to count its endpoints and findings.

`--json` rendering:
```json
{
  "services": [
    {"name": "order-service", "kind": "microservice",
     "indexed": true, "integration_count": 4, "finding_count": 2,
     "exposes_http_api": true,
     "http_apis_exposed": ["POST /orders"],
     "http_apis_consumed": [],
     "kafka_topics_published": ["orders.created"],
     "kafka_topics_consumed": ["payments.received"],
     "mongo_collections": ["orders"]}
  ],
  "warnings": ["payment-service: not indexed, ignored (run cccr index in this project)."]
}
```

Les bibliothèques et modules partagés ne figurent pas dans cette commande ;
ils sont listés par `cccr modules`.

La vue texte affiche les mêmes informations principales par microservice :
ressources HTTP exposées et consommées, topics Kafka publiés et consommés, et
noms des collections MongoDB indexées. Ces champs restent vides pour un
microservice non indexé ou lorsque l'index ne contient pas le signal concerné.

The discovery command never includes configuration content. The synthetic YAML
template is available only after an explicit request through
`cccr microservices properties <service> --root <root>`; it is constructed
from Spring property keys referenced by production code (`${...}`, `@Value`,
`Environment.getProperty`, `@ConditionalOnProperty`), never from existing
`application*` files. **No value from the repository is rendered**: key-name
heuristics produce generic values such as `<string>`, `0`, `false` or
`<secret>`; test code is excluded.

The same command is the architecture explorer. Its navigation remains on
business objects and never includes source paths or snippets by default:

- `cccr microservices show <service>` returns the summary of one microservice:
  build tool, language, APIs, Kafka topics and their published/consumed Java
  payload types, MongoDB collections, technologies, OpenAPI presence and direct
  dependencies.
- `cccr microservices topics <service>` lists published and consumed Kafka
  topics, grouped with their statically inferred Java payload types.
- `cccr microservices apis <service>` lists exposed and consumed HTTP APIs.
- `cccr microservices mongodb <service>` lists indexed MongoDB collections
  used by the microservice.
- `cccr microservices neighbors <service>` lists direct relations of that
  microservice.
- `cccr analyze microservices path <source> <target>` returns the shortest
  directed paths between two microservices. Kafka topics remain explicit
  intermediate nodes; REST calls remain direct service-to-service relations.
- `cccr analyze microservices calls|dependencies|impact <service>` answers
  architecture questions centered on one microservice. `external-apis` and
  `orphan-integrations` accept an optional service.
- `cccr microservices implementation integration <id>` is the explicit final
  level that returns location and indexed source evidence.

### `cccr topics [list|show|neighbors|consumers|producers|search|trace] [topic]`

Explores Kafka topic objects from the same indexed graph. With no argument or
with `list`, it returns the discovered topics. The remaining subcommands take
one exact topic name. `consumers`, `producers` and `trace` respectively return
one side of the relation or potential Kafka flows; a trace is never runtime data.

- `show` returns the topic summary, including the published and consumed Java
  payload types when they are statically known;
- `neighbors` returns its producer and consumer microservices;
- `consumers` and `producers` return one side of that relation;
- `search` resolves an exact name or a unique case-insensitive substring; on a
  locally indexed project only, it falls back to endpoint vector similarity.

The command returns business objects only, without source paths or snippets.

### `cccr dtos [list|show|neighbors|consumers|producers|search] [dto]`

Lists the statically inferred Java DTOs used in Kafka producers and consumers.
Each DTO summary includes its Kafka topics and the runtime microservices that
produce or consume it. Unknown payload types are intentionally absent: the
command never guesses a Java type from a topic name.

Examples: `cccr dtos`, `cccr dtos show OrderCreated`, and
`cccr dtos consumers OrderCreated`.

### `cccr mongodb [list|show|neighbors|services|search] [collection]`

Explores indexed MongoDB collection objects from the same architecture graph.
With no argument or `list`, it returns each collection with its indexed modules
and known operation count. `show` returns that summary, `neighbors` returns the
modules using it, and `search` resolves an exact name or a unique
case-insensitive substring. `cccr mongodb services <collection>` restricts
that relation to runtime microservices.
The command does not return source paths or snippets.

### `cccr apis [list|show|neighbors|providers|consumers|search] [api]`

Explores HTTP API objects from the same indexed graph. With no argument or
with `list`, it returns the discovered APIs. The remaining subcommands take
one API name:

- `show` returns the API summary;
- `neighbors` returns its provider and consumer microservices;
- `search` resolves an exact name or a unique case-insensitive substring, then
  uses the same local vector-similarity fallback as `topics search`.

`providers` and `consumers` answer which microservices expose or call one API.

The command returns business objects only, without source paths or snippets.

`integration_count` of a Maven `shared-module` is always `0`: a shared module is never
handled as a runtime producer/consumer, even if integrations were detected there by
mistake. A module not indexed, with a missing database, or with an
incompatible schema does not make the command fail: it appears in `warnings`,
absent from the counts. An indexed module whose
`meta.endpoint_inventory_signature` is missing/old also adds an explicit
warning. No Maven module found → informational message, exit code 0 (not an
error — `root` may legitimately not be a Maven directory).

### `cccr modules [list|show|integrations|properties|openapi|graph]`

Reads the **module inventory materialized by `cccr index`** in the current
directory, including libraries and aggregators, unlike
`microservices` which is focused on runtime services. It never re-reads the
workspace to reconstruct this view; a missing/incompatible index is a blocking
error. Test and mock artifacts, and modules whose declared Maven or Gradle
name contains `archteype` (case-insensitive), are excluded from the inventory
and their sources are not scanned.

- `cccr modules` lists compact module summaries: Maven artifact or Gradle
  project/archive name, declared version (or `null`), build system,
  classification (`microservice`, `library`, `aggregator`) and absolute path.
- `cccr modules show <module>` returns the detailed record for one exact module name.
- `cccr modules integrations|properties|openapi <module>` returns the targeted
  inventory, synthetic configuration example, or local API contracts for that
  module.
- `cccr modules graph` returns the declared Maven/Gradle dependencies whose
  source and target are both indexed modules. It is distinct from `cccr export microservices`:
  it never contains REST/Kafka interactions or topics.
- `cccr export modules --html modules.html` exports the same hierarchical view
  as an interactive Sigma.js graph.

The configuration example is generated during that indexation and follows the
same no-real-values policy as `microservices properties`.

### `cccr analyze audit [--workspace ROOT] [--json]`

Produces conservative architecture risks from the static inventory: Kafka
producer or consumer with no indexed counterpart, dynamic Kafka/HTTP targets,
incompatible known Kafka producer/consumer payload types, and synchronous HTTP
dependency cycles. It also reports non-runtime modules
that expose HTTP APIs, publish or consume Kafka topics, or read/write indexed
MongoDB collections. This last signal is a `WARNING` for architectural review,
not proof of an error. Every result carries evidence and a confidence level;
it is not an execution trace and never claims to prove a runtime path.

### `cccr analyze coverage [--json]`

Summarizes the quality of the persisted architecture inventory: integration and
relation counts, relation confidence, dynamic Kafka topics, Kafka integrations
without a statically inferred Java payload type, and HTTP calls that could not
be matched to an indexed provider. Details are capped at 20 entries per
unresolved category, so the default text view stays concise.

Relations are materialized during indexing with source/target kinds, relation
type, provenance, confidence, module, location and Java class when known. The
current model covers module/microservice, API, Kafka topic, DTO, MongoDB
collection, Java class, Java method and Spring property objects.

### `cccr analyze request-reply [--json]`

Lists Kafka request/reply candidates using the Strategy1 naming convention
`retour_<request-topic>`.
Each pair includes its request and reply topics plus their indexed producers and
consumers. Both topics must be present in the index. This is a conventional
architecture view, not proof of a runtime request/reply exchange.

### `cccr export request-reply --html FILE`

Writes a dedicated, standalone HTML view of the same convention-based Kafka
request/reply candidates, including request/reply producers and consumers.

```json
[
  {"name": "orders-api", "build_system": "maven", "version": "3.1.0",
   "kind": "microservice", "path": "/repo/orders"}
]
```

### `cccr mcp`
Starts the MCP server (stdio) on the current repo (execution directory).
The client must run from an initialized and indexed repository, because the
server resolves `.cccr/config.yml` and `.cccr/findings.db` from its working
directory.

Codex registration:
```bash
codex mcp add cccr -- cccr mcp
codex mcp get cccr
```

Claude Code registration:
```json
{"mcpServers": {"cccr": {"command": "cccr", "args": ["mcp"]}}}
```

Pi does not provide MCP support by default. After installing the
`pi-mcp-adapter` extension, create `.mcp.json` in the indexed repository:
```bash
pi install npm:pi-mcp-adapter
```
```json
{"mcpServers": {"cccr": {"command": "cccr", "args": ["mcp"]}}}
```
Start Pi from that repository and use `/mcp` to inspect the connection.

Restart the client after registering the server.

## 3. MCP server

Fourteen tools, each annotated with a concrete return type (`TypedDict` or
dataclass, never `str`) — FastMCP derives an `outputSchema` from it field by
field, exposed to MCP clients in addition to the usual JSON text
(`structuredContent` *and* text `content`, both in the same response; a client
that ignores the former falls back to the latter, so there is no regression for
existing clients). An exception raised inside a tool **is no longer caught**:
it bubbles up as-is, FastMCP turns it into `ToolError` and then `isError: true`
on the protocol side — the standard signal an MCP client can detect without
parsing the response text (before: `{"error": "<message>"}` returned as a
normal result, indistinguishable from success without a client-side convention).

The first four tools below form the **core product**; the next seven belong to
the **Java/Spring microservices extension**.

| Tool | Return type | Role | Notes |
|---|---|---|---|
| `search_findings(query, severity=None, rule=None, path_glob=None, limit=5, include_context=False)` | `list[FindingHit]` | Precision-first lexical findings search — same contract as `cccr findings --json` | No pagination (`offset`) on the MCP side |
| `findings_summary()` | `FindingsSummary` | Low-cost aggregated view | Same structure as `cccr summary --json` |
| `reindex_findings()` | `IndexReport` (dataclass from `indexer.py`, reused as-is) | Incremental reindexing | Fields `scanned, skipped, findings_added, findings_removed, deleted_files` |
| `list_endpoints(system=None, role=None, topic=None, path_glob=None)` | `list[EndpointHit]` | Filterable raw HTTP/Kafka endpoint inventory | Use `cccr apis` or `cccr topics` for CLI architecture exploration |
| `graph(workspace_root=None)` | `GraphResult` | Inter-service topology + outbound REST calls in Kafka consumers — equivalent to CLI `cccr export microservices --json` | Without inter-module data, `services`/`nodes`/`edges` are empty and `note` explains why |
| `dependency_graph(workspace_root=None)` | `DependencyGraphResult` | Typed topology of microservices, Kafka topics, scoped MongoDB collections and external HTTP APIs | Includes HTTP, Kafka, MongoDB read/write and external-call relations with static confidence |
| `audit_dependency_graph(workspace_root=None)` | `DependencyAuditResult` | Static dependency audit | Combines existing inventory risks with event-flow cycles and synchronous HTTP calls inside Kafka consumers |
| `list_workspace_services(root)` | `WorkspaceResult` | Maven/Gradle workspace discovery + endpoint/finding counts per runtime service — equivalent to CLI `cccr microservices` | Read-only (ADR-30) |
| `list_modules()` | `list[ModuleSummary]` | Indexed module inventory | Includes persisted MongoDB and OpenAPI metadata |
| `trace_message_flow(query, workspace_root=None)` | `FlowResultInfo` | Detailed MCP-only trace of a topic/route and its sites (producers/consumers, or servers/callers), including overlapping findings | No-match or ambiguous query → `ToolError` |
| `architecture_catalog(kind, action="list", name=None, target=None, workspace_root=None, max_depth=12, limit=50)` | `dict` | CLI-aligned catalog navigation for microservices, modules, topics, DTOs, HTTP APIs, MongoDB collections and endpoints | Covers list/show/neighbors plus the subject-specific actions documented by the tool |
| `architecture_audit(workspace_root=None)` | `list[dict]` | Equivalent to `cccr analyze audit --json` | Distinct from `audit_dependency_graph`, which adds dependency-topology checks |
| `architecture_coverage()` | `dict` | Equivalent to `cccr analyze coverage --json` | Current repository only, because it reads its persisted evidenced relations |
| `list_request_reply_patterns()` | `dict` | Equivalent to `cccr analyze request-reply --json` | Current repository only; derives Kafka pairs from the Strategy1 `retour_` convention |

`findings` uses a precision-first lexical match. Every query token must be
present in the indexed rule, message, path, taxonomy, snippet or severity;
filters (`severity`, `rule`, `path`) remain exact store filters. Vector
embeddings are not used for this command, so an embedding-model mismatch does
not affect findings lookup.

`CodeSearchResult` has a **single stable schema** for successful responses
(nominal or missing findings index) — not an alternate shape depending on the
case, so that `outputSchema` remains valid:
```json
{
  "results": [...],                 // without findings if the index is absent
  "findings_only_fallback": [],     // kept empty for schema compatibility
  "warning": null                   // explanatory string in degraded mode, null otherwise
}
```
If `ccc` fails or is absent: exception (`ccc not found...` or
`ccc failed...`) → `isError: true` on the MCP side, exit code 2 on the CLI.

## 4. Claude Code skill (distributed separately in `ccc-radar-skill`)

Triggers: vulnerability, security, semgrep, finding, debt, audit.

UX golden rule: start with the least costly query that answers the question,
then ask for more context only when action is needed. The skill therefore
chooses among:
1. **Overview** — `findings_summary()` for a short state.
2. **Natural-language findings lookup** — `search_findings(...)` to find
   findings from a problem description, a rule/CWE identifier, or a file/path
   clue.
3. **Remediation loop** — `search_findings(..., include_context=true)` →
   patch → fresh Semgrep scan on the file if the official MCP is available →
   `reindex_findings()` → same `search_findings(...)` to confirm disappearance;
   stop and report after 2 unsuccessful attempts.

Explicit anti-patterns: do not scan the whole repo through the official Semgrep
MCP (prefer the `cccr` index), do not fix anything without reading the context,
do not remove an existing `# nosemgrep` comment, do not expose raw JSON to the
user unless explicitly asked, and use existing MCP fallbacks rather than
blocking unnecessarily.

## 5. Error behaviors — summary

| Situation | Surface | Behavior |
|---|---|---|
| `.cccr/config.yml` absent | `cccr index` | stderr + code 1 |
| No Semgrep config detected and no `--rules` | `cccr init` | first copies skill packs if available, otherwise activates `p/security-audit`, `p/java`, `p/owasp-top-ten` and `p/secrets`, informational stdout message + code 0 |
| `.cccr/config.yml` already exists | `cccr init` | stderr + code 1, file unchanged |
| Semgrep fails or exceeds timeout | `cccr index` | stderr + code 2, database unchanged |
| `.cccr/findings.db` absent | `cccr findings` / `cccr summary` | stderr (exact message) + code 2 |
| Embeddings incompatible with the query | `cccr findings` | actionable stderr + code 2 |
| Any exception | MCP tools | bubbles up as-is → FastMCP `ToolError` → `isError: true` on the protocol side; the server remains usable for the next call |

## 6. Liveness rule pack

The rule pack lives in the skill repo, not in this repo: see
[`ccc-radar-skill`](https://github.com/elkouhen/ccc-radar-skill)
`skills/cccr/rules/liveness/java.yaml`, alongside the `default` pack already
distributed by the skill (ADR-24). `cccr` itself no longer ships any rule file
(`src/ccc_radar/rules/` does not exist) — it only runs Semgrep with the paths
declared in `rules:`. This repo keeps a test copy in
`tests/fixtures/liveness_repo/rules/` (`tests/test_liveness_rules.py`), kept
manually in sync with the skill copy.

Analysis target: **Java + Spring** (Maven or Gradle) — scope decision, not a
temporary gap (see “Scope” below).

| Rule | Language | Severity | Detects |
|---|---|---|---|
| `cccr.liveness.java.new-resttemplate-no-timeout` | Java | WARNING | `new RestTemplate()` without timeout configuration (vs `RestTemplateBuilder`) |
| `cccr.liveness.java.blocking-join-no-timeout` | Java | WARNING | `.join()` with no argument (`Thread` or `CompletableFuture`) |
| `cccr.liveness.java.blocking-future-get-no-timeout` | Java | WARNING | `.get()` with no argument on a variable declared as `Future<T>`/`CompletableFuture<T>` |
| `cccr.liveness.java.rest-call-in-kafka-listener` | Java | ERROR | `RestTemplate` call inside a `@KafkaListener` method |
| `cccr.liveness.java.network-call-inside-synchronized` | Java | ERROR | `RestTemplate` call inside a `synchronized` block |
| `cccr.liveness.java.mongo-lock-busy-wait-poll` | Java | ERROR | MongoDB pessimistic lock (`findAndModify`/`findOneAndUpdate`) acquired through blocking polling — `while`/`for` loop also containing `Thread.sleep(...)` |
| `cccr.liveness.java.mongo-lock-inside-synchronized` | Java | ERROR | `findAndModify`/`findOneAndUpdate` call (MongoDB pessimistic lock) inside a `synchronized` block |

**Usage** : like the `default` pack, copy it into the target repo
(e.g. `.cccr/rules/liveness/`) and declare it in `rules:` — never use an
absolute path to the skill repo (ADR-24):

```yaml
rules:
  - .cccr/rules/liveness/java.yaml
```

Scope: Java (`RestTemplate`, Spring Kafka `@KafkaListener`, `synchronized`,
`Future`/`CompletableFuture`, MongoDB pessimistic locks
`findAndModify`/`findOneAndUpdate`) — the target stack is Java + Spring +
Maven or Gradle; Python/JS/TS are not targets. The
security part (cleartext SASL, `PLAINTEXT`, unsafe deserialization) is now
covered separately in the Kafka security pack.

**MongoDB pessimistic locks** — MongoDB has no native pessimistic lock like
`SELECT ... FOR UPDATE`; the pattern observed in this code style is an atomic
write (`findAndModify`/`findOneAndUpdate`) on a “locked” field, combined with a
polling loop or JVM monitor:
- `mongo-lock-busy-wait-poll` flags the Mongo call as soon as it lives in a
  `while`/`for` loop that also contains `Thread.sleep(...)` — structural co-
  occurrence (no dependence on the lock field name), strong signal of polling
  without visible timeout or backoff.
- `mongo-lock-inside-synchronized` flags the same Mongo call inside a
  `synchronized` block — the network round-trip happens while holding a JVM
  monitor, same risk as `network-call-inside-synchronized`.
- Neither rule assumes anything about the “locked” field name (no assumption on
  `locked`/`lockedAt`/etc.): the structure (loop+sleep, or synchronized) around
  the atomic write is what signals the lock usage, not a naming convention.

## 7. REST inventory rule pack

Like the liveness pack, it lives in `ccc-radar-skill`
(`skills/cccr/rules/rest/java.yaml`, ADR-24) — test copy in
`tests/fixtures/rest_repo/`. Unlike the liveness/`default` packs, this pack is
**not a findings pack**: `metadata.severity` (`INFO`) has no meaningful
thresholding use. It is nevertheless run during `cccr index` whenever it appears
in `rules:` (microservices audit workflow of the skill), and feeds
`cccr apis` / `cccr topics` / `cccr export microservices`.

| Rule | Language | Role | Detects |
|---|---|---|---|
| `cccr.rest.java.serve-{get,post,put,delete,patch}` | Java | `serve` | Exposed Spring route (`@GetMapping`/`@PostMapping`/`@PutMapping`/`@DeleteMapping`/`@PatchMapping`, or `@RequestMapping(method=...)` for any verb) |
| `cccr.rest.java.call-{get,post,put,delete}` | Java | `call` | `RestTemplate` call (`getForObject`/`getForEntity`, `postForObject`/`postForEntity`, `put`, `delete`) |
| `cccr.rest.java.feign-{get,post,put,delete,patch}` | Java | `call` | Method of a `@FeignClient` interface annotated with `@GetMapping`/.../`@RequestMapping(method=...)` (signature with no body — declarative client, not exposed route) |
| `cccr.rest.java.webclient-{get,post,put,delete,patch}` | Java | `call` | Fluent `WebClient` or Spring `RestClient` call (`.get().uri(...)`, `.post().uri(...)`, ...); `RestClient` is reported as framework `restclient` |

Each result carries `metadata.category: endpoint-inventory`,
`metadata.role`, `metadata.http_method`, `metadata.framework` — the contract
read by `parse_semgrep_endpoints` (see `docs/SPEC-TECH.md`, §4bis). The path is
extracted from the site text (regex on the snippet, not a Semgrep metavariable
— ADR-26): a non-literal path, or one concatenated with a variable, is marked
`topic_dynamic=True` rather than guessed. An absolute caller URL is normalized
into a canonical route (`GET http://svc/orders` → `GET /orders`) so it remains
comparable to exposed routes. A `@FeignClient` interface is never classified as
`serve`: the `serve-*` rules require a method body (`{ ... }`), absent from
Feign declarative signatures — no explicit exclusion needed. In addition,
best-effort scanner logic now:
1. resolves the base path of `@FeignClient(url = "${...}")` or
   `@FeignClient(path = "...")` and merges it into the method route, so a call
   can surface as `/api/v1/customers/{id}` instead of just `/{id}`;
2. inventories `RestTemplate.exchange(urlExpr, HttpMethod.X, ...)` even
   without a dedicated Semgrep rule, with best-effort resolution of local
   `@Value` fields, concatenated literal suffixes, and Spring Cloud Config
   Server files such as `configurations/order-service.yml`;
3. infers Spring Cloud Gateway proxy routes from Java builders and the standard
   YAML route lists as both exposed `serve` endpoints and outbound `call`
   endpoints (applying `StripPrefix` when present); YAML `lb://service` targets
   are used to disambiguate the graph edge. It also infers WebFlux
   `RouterFunctions.route(...)` declarations as exposed `serve` routes;
4. keeps a `.put(...)` match as a REST call only when the file actually shows a
   `RestTemplate` footprint, which removes `Map.put(...)` false positives.
Scope: Java only — target stack is Java + Spring (Maven or Gradle). A fluent
`WebClient` or `RestClient` chain split across several
lines (`.get()` and `.uri(...)` not on the same line in the snippet —
`_find_first_literal` only searched the first line before its later
improvements).

## 8. Kafka inventory rule pack

Like the REST pack, it lives in `ccc-radar-skill`
(`skills/cccr/rules/kafka/java.yaml`, ADR-24) — test copy in
`tests/fixtures/kafka_repo/`. Not a findings pack, but it is run during
`cccr index` whenever it appears in `rules:` (microservices audit workflow of
the skill).

| Rule | Role | Detects |
|---|---|---|
| `cccr.kafka.java.consume` | `consume` | `@KafkaListener(topics = "...")` method |
| `cccr.kafka.java.produce-template` | `produce` | `KafkaTemplate.send(topic, value, ...)` (at least 2 arguments — excludes `send(ProducerRecord)` and `send(message)`, already covered elsewhere) or `KafkaTemplate.sendDefault(...)` (implicit topic, always dynamic) |
| `cccr.kafka.java.produce-record` | `produce` | `new ProducerRecord(topic, ...)` (low-level `kafka-clients` API **and** Spring, same classes) |
| `cccr.kafka.java.consume-raw` | `consume` | `KafkaConsumer.subscribe(Collections.singletonList(...))`/`Arrays.asList(...)`/`List.of(...)` — low-level API (`confluent-kafka`), outside `@KafkaListener` |

The topic is extracted like REST (`extra.metadata.role`, no `http_method`
here), with one extra Kafka/Spring-specific case: a literal of the form
`${nested.property}` — a topic externalized into configuration
(`@KafkaListener(topics = "${app.kafka.topics.orders}")`) — is **not** treated
as a literal topic name. `cccr` tries to resolve it against repo
`application.yml`/`.yaml`/`.properties`
(`src/main/resources/` then repo root, standard Maven/Gradle layout,
supporting Spring default syntax `${prop:default}`) via
`resolve_spring_property` — see ADR-28. Resolved → `topic_dynamic=False`,
topic = found value (or default); missing and no default →
`topic_dynamic=True`, the placeholder is kept as-is (never guessed).

A variable fed by `@Value("${...}")` elsewhere in the class
(`@KafkaListener(topics = ordersTopic)`, `kafkaTemplate.send(ordersTopic,
...)`) is now followed, best-effort: `_extract_kafka_topic` finds the variable
name in the snippet, then looks for a field declaration
`@Value("${key}") ... ordersTopic;` in the same source file (regex on the text,
no Java AST or dataflow analysis between statements — same spirit as ADR-26);
the found key is resolved like a normal placeholder (`resolve_spring_property`).
A variable not fed by `@Value` in the same file (method parameter, field
initialized differently) still becomes `<dynamic>`, never guessed.
`KafkaConsumer.subscribe(...)` is deliberately restricted to the three usual
collection forms (`Collections.singletonList`/`Arrays.asList`/`List.of`) so it
never confuses an RxJava/Reactor `.subscribe(...)` (lambda/Observer, never a
`Collection<String>`) with a Kafka subscription —
`subscribe(Pattern.compile(...))` (topic-name pattern subscription) remains out
of scope. `cccr` also adds local inference for Spring producers built through
`MessageBuilder.withPayload(...).setHeader(TOPIC, ...)` then sent with
`kafkaTemplate.send(message)`: the topic is read from the `TOPIC` /
`KafkaHeaders.TOPIC` header, then resolved as a literal, Spring placeholder, or
`@Value` field with the same rules; if nothing is resolvable, it remains
`<dynamic>`.

For each Kafka integration, `cccr` also captures the payload type only when a
Java signature makes it explicit: the first non-header parameter of the
`public void consume(...)` method associated with a `@KafkaListener` (then a
compatible listener method), the value generic of `KafkaTemplate`, `ProducerRecord`, or
`KafkaConsumer`, or the value generic of a `KStream`/`KTable` declaration.
For a custom producer wrapper, the type of the payload parameter passed to
`send(topic, payload)` is used when its method signature makes it explicit.
This value is a source-level Java type, may be absent, and is never guessed
from a topic name, serializer, or configuration property.

## 9. Kafka security rule pack

Lives in `ccc-radar-skill` (`skills/cccr/rules/kafka-security/
java.yaml`, ADR-24) — test copy in
`tests/fixtures/kafka_security_repo/`. Unlike the `rest`/
`kafka` packs (inventory), these are real **findings** rules, like
`default`/`liveness` — indexed and searchable through `cccr findings`.

| Rule | Severity | Detects |
|---|---|---|
| `cccr.kafka-security.sasl-plaintext-credentials` | ERROR | `sasl.jaas.config` with a **literal** password (hard-coded `******`, not built from a variable) |
| `cccr.kafka-security.plaintext-protocol` | ERROR | `security.protocol` set to `PLAINTEXT` (literal or constant `CommonClientConfigs.SECURITY_PROTOCOL_CONFIG`) |
| `cccr.kafka-security.json-deserializer-trusts-all-packages` | ERROR | Spring Kafka `JsonDeserializer`/`ErrorHandlingDeserializer` configured with `trusted.packages = "*"` (unsafe deserialization — arbitrary class instantiation from a message) |
| `cccr.kafka-security.unsafe-java-deserialization` | ERROR | `ObjectInputStream(...).readObject()` — native Java deserialization on data potentially coming from an untrusted message |

`cccr.kafka-security.sasl-plaintext-credentials` distinguishes a hard-coded
password from a variable-injected one through `metavariable-regex` on the
source text of the literal — which carries **escaped** quotes (`\"`), not bare
quotes, because it is a Java string literal nested inside another literal (see
ADR-31, a non-obvious trap to know before writing this kind of rule).

**What is deliberately not duplicated here**: non-idempotent producer,
risky `enable.auto.commit`, and handler without DLQ/retry were in K8's initial
scope but are already covered by the `default` pack
(`skills/cccr/rules/default/b-kafka.yaml`, rules R7 and R10) — see
that pack. Risky `max.poll.interval.ms` remains a documented
gap (no rule, since the threshold/intent of “risky” is not unambiguous enough
for reliable detection without false positives).

Scope: Java only.
