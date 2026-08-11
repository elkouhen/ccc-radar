# Technical specification — ccc-radar (`cccr`)

## Architecture

The pipeline is deliberately local:

```text
repository files → file hashes → Tree-sitter Java AST extractors
                 → endpoints/modules/properties → SQLite facts and relations
                 → CLI, MCP, graph and audit views
```

`scanner.py` owns Java/Spring extraction. `java_parser.py` provides cached
Tree-sitter parsing and syntax helpers. `modules.py`, `maven.py` and `gradle.py`
discover build units; `relations.py` derives typed architecture relations from
modules, endpoints and build dependencies. `indexer.py` orchestrates the
incremental transaction. `store.py` owns SQLite persistence.

`cli.py` and `mcp_server.py` are thin delivery layers over the domain modules.

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
module and source location. The graph and audit layers consume relations and
endpoints; they do not rescan source.

The legacy `findings` table and model are retained only to open existing index
databases. `Store.clear_findings_once("ast_only_analysis_v1")` removes stale
external-analyzer data on the first AST-only index run.

## Indexing

`index_repo` performs these steps:

1. Clear parser and discovery caches for long-lived MCP processes.
2. Discover modules unless disabled.
3. Build the eligible-file hash inventory, respecting include/exclude rules,
   test-source exclusion and nested-build boundaries.
4. Compare hashes with the stored inventory and purge removed files.
5. Force a full refresh when extractor/configuration/strategy signatures differ.
6. Run AST extractors for the changed files and atomically replace their
   endpoints.
7. Persist hashes, modules, dependencies and derived relations.

No subprocess is used to analyze source code. Extraction failures leave the
previous successful state intact for files not yet replaced.

## Extractors

`infer_framework_endpoints` walks Java declarations, annotations and method
invocations to discover Spring MVC/WebFlux routes, Feign clients, RestTemplate,
WebClient, Spring Data REST and gateway routes. It resolves literals and known
Spring property expressions conservatively.

`infer_kafka_endpoints` recognises Spring Kafka listeners and send sites,
KafkaTemplate/ProducerRecord usage and Spring Cloud Stream StreamBridge calls.
It preserves dynamic topic expressions and derives a payload type only from an
explicit listener parameter or client generic signature.

With `--topic-strategy strategy1`, `envoyerMessageKafka(topic, payload)` is an
additional producer convention. A first argument shaped as
`kafkaProperties.getTopics().getXxx()` resolves to the normalized Strategy1
topic name; other values use the conservative topic resolver. The second
argument is used to derive the payload type from its method parameter, local
variable declaration or enclosing class field.

`render_graph_html` resolves the Java DTOs and enums rooted at those Kafka
payload types from production source roots. It follows declared field types
recursively only when a project type name resolves unambiguously, and embeds
the resulting definitions and enum constants in the self-contained HTML
payload. DTO navigation is therefore a read-only export concern and does not
add facts to the SQLite index.

The manifest extractors add explicitly declared Kafka facts from Markdown and
JSON. Strategy1 is separate and opt-in because it embeds repository-specific
naming conventions.

## Persistence and compatibility

SQLite schema migration is additive where possible. `files` stores hash state,
`endpoints` stores source facts, and normalized tables store modules,
dependencies and relations. The database filename remains `findings.db` for
backward compatibility; new AST-only behavior must not infer that it contains
security findings.

The endpoint-inventory signature in `meta` is bumped whenever extractor
behaviour changes. This forces a complete refresh before new facts are served.

## Verification

Unit tests use fixture repositories with real Java source and assert source
locations, roles, dynamic flags and derived relations. Static checks are Ruff
and mypy. The project does not require an external scanner in development or
at runtime.
