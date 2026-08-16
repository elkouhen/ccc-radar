# Product requirements — systemlens

## Purpose

`systemlens` gives coding agents trustworthy, local architecture context before
they modify a Java/Spring system. Developers and architects use the same
evidence to inspect and review the result. It derives source facts directly
from local ASTs, without starting an external rule engine or sending source
code to a service.

The product answers questions such as:

- Which services expose or call an HTTP API?
- Which Kafka topics are produced and consumed, and with which payload type?
- What are the dependencies and likely impact paths between services?
- Which Maven/Gradle modules, OpenAPI contracts, MongoDB collections and
  Spring properties belong to a service?
- Which observed service-to-service dependencies are slow, error-prone, or
  incompletely covered during a selected runtime window?
- Which statically evidenced HTTP, Kafka, MongoDB, or S3 integration provides
  context for an observed runtime hotspot?

## Users and primary workflows

| User | Need | Surface |
|---|---|---|
| Coding agent (primary) | Establish proven dependencies, impact and unresolved facts before an edit | MCP tools |
| Developer | Inspect the evidence behind a service, API, topic or module | CLI catalog commands and HTML export |
| Architect | Review topology, uncertainty and static architecture risks across services | `analyze`, graph export |
| Performance engineer / SRE | Identify high-volume, slow, or failing observed dependencies and relate them to the deployed architecture | Explicit bounded APM analysis and runtime overlays |

The primary workflow is `systemlens init`, `systemlens index`, then an agent
uses the MCP catalog, graph, coverage and trace tools before making a bounded
change. Source evidence is available from the catalog and graph; coverage and
indexing-issue tools expose unresolved facts. The agent then reindexes after
the edit. Developers can follow the same workflow through `microservices`,
`topics`, `apis`, `modules`, `analyze`, and HTML export. Indexing is
incremental; `--full` refreshes every eligible source file.

For runtime investigation, a developer or SRE deliberately selects an Elastic
APM window and environment. SystemLens returns bounded aggregate observations
with their coverage, then presents them alongside—never as a replacement for—
the indexed Java/Spring topology. The user can distinguish a static relation
that was not observed from an observed relation that cannot be resolved to a
source fact.

## Scope

Delivered:

- Tree-sitter Java AST extraction for Spring MVC/WebFlux, Feign,
  RestTemplate/WebClient, Spring Cloud Gateway and Spring Data REST endpoints.
- Kafka producers and consumers, dynamic-topic evidence and explicit Java
  payload types.
- Maven/Gradle module discovery, OpenAPI and MongoDB inventory.
- Local SQLite persistence, architecture relations, graph/audit views and
  workspace federation.
- Markdown/JSON Kafka manifests and the opt-in Strategy1 conventions.
- Optional Kubernetes Deployment and StatefulSet resource dimensions from the
  active local `kubectl` context, matched conservatively to indexed modules.
- An explicit, stateless Elastic APM digest command for external-agent analysis.
  It exports bounded service-to-destination metric aggregates only; it does not
  export raw spans or alter the source inventory.

Planned:

- A bounded runtime-analysis view that ranks observed dependency latency,
  failures, and call volume, and reports the selected time window and coverage.
- Conservative presentation of observed HTTP, Kafka, MongoDB, and S3 activity
  beside the static architecture, without inventing a source mapping.
- Explicit Kubernetes capacity context for runtime hotspots where a verified
  workload-to-service match exists. A future matcher must prefer an exact name,
  then accept a unique token-bounded service-name inclusion in a Deployment or
  StatefulSet name; ambiguous matches remain unresolved.

Not delivered:

- Security or quality scans, severity filtering or automated remediation.
- Runtime tracing ingestion, raw-span retention, continuous cluster collection,
  cross-repository source analysis, or a hosted service. The optional APM
  digest and planned runtime analysis are one-shot aggregate reads, not a
  tracing store.
- Guaranteed resolution of dynamic values; unresolved values remain explicitly
  marked as dynamic rather than guessed.

## Product requirements

1. Source facts must be derived from local AST parsing and deterministic local
   configuration only. Optional Kubernetes enrichment is opt-in, retains its
   workload kind, namespace, and name, and never replaces source evidence. The
   optional Elastic APM digest is separate observed data, never a source fact.
2. Each source fact must carry enough evidence to navigate to its file and line
   range. Every non-source enrichment must identify its acquisition origin.
3. A changed or deleted source file must update or remove its facts on the next
   index run.
4. The local inventory and its CLI and MCP queries must operate without network
   access once indexing is complete. Kubernetes discovery and the explicit
   Elastic APM digest export are opt-in network exceptions.
5. Graph, catalog and audit output must make uncertainty visible rather than
   inventing a dependency.
6. An agent must be able to obtain a bounded answer, evidence and unresolved
   facts before it changes a supported Java/Spring integration.
7. Every runtime observation must state its source, time window, aggregation
   coverage, and truncation state. It must remain distinguishable from a
   source-evidenced architecture fact.
8. Runtime analysis must use explicit, read-only, bounded queries. It must not
   export or persist raw spans, request payloads, headers, trace identifiers,
   credentials, or unredacted error values.
9. An observed service, Kafka, MongoDB, or S3 name maps to a static identity
   only through explicit exact evidence. Missing or ambiguous mappings remain
   visible as observations rather than becoming guessed dependencies.
10. Kubernetes workload correlation must prefer an exact service name. When a
    Deployment or StatefulSet name contains a service name, a normalized,
    token-bounded inclusion may be used only if it identifies one service; a
    broad substring or multiple candidates must remain unresolved.

## Success measures

- On each supported reference repository, an MCP client can answer the primary
  pre-edit questions (service dependencies, HTTP APIs, Kafka flow and impact)
  after one local index, with a bounded result and its evidence.
- Every emitted source integration is traceable to a concrete source location
  or an explicitly named manifest entry; optional Kubernetes facts identify the
  matched workload kind, namespace, and name.
- Coverage output distinguishes resolved relations from unresolved or dynamic
  facts, so an agent can decline to assume a missing dependency.
- Incremental indexing touches only changed files unless an extractor signature,
  selected convention, or an analysis dependency (Spring configuration or build
  descriptor) changes.
- For a selected runtime window, a developer can identify the highest-ranked
  latency and error hotspots, determine the completeness of the observation,
  and navigate only to explicitly linked static evidence.

For observable command and MCP contracts, see
[SPEC-FONC.md](./SPEC-FONC.md). For implementation details, see
[SPEC-TECH.md](./SPEC-TECH.md). Historical decisions, including the retired
external-analyzer design, remain in [ADR.md](./ADR.md).
