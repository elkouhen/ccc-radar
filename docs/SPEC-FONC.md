# Functional specification — systemlens (`systemlens`)

`systemlens` builds a local architecture inventory from Java/Spring source ASTs.
The inventory commands operate on the current repository unless an explicit
workspace root is accepted. There is no external code-analysis process in this
workflow. 
## Configuration

`systemlens init` creates `.systemlens/config.yml`:

```yaml
include: ["**/*"]
exclude: [".git/**", ".venv/**", "node_modules/**", ".systemlens/**"]
min_severity: INFO
```

This is a breaking rename from `cccr`, `archlens`, and `codeatlas`: SystemLens
does not read an existing `.cccr/`, `.archlens/`, or `.codeatlas/` directory.
Run `systemlens init` and
`systemlens index` to create a new local inventory in `.systemlens/`.

`include` and `exclude` control source inventory. Maven/Gradle test source
sets (`src/test`, `src/componentTest`, and names ending in `Test`) are always
excluded. The `min_severity` setting remains accepted for database compatibility
but does not alter AST endpoint extraction.

## CLI

| Command | Behaviour |
|---|---|
| `systemlens init` | Creates `.systemlens/config.yml`; it never overwrites an existing file. |
| `systemlens doctor [--json]` | Read-only check of configuration, local AST readiness and index state. |
| `systemlens version` | Prints the installed `systemlens` package version. |
| `systemlens index [MANIFEST]... [--full] [--topic-strategy default\|strategy1] [--manifest FILE]... [--kubernetes] [--kubernetes-namespace NAME]` | Incrementally extracts and persists architecture facts. `--kubernetes` queries the active `kubectl` context for Deployments and StatefulSets; `--kubernetes-namespace` restricts it to one namespace. |
| `systemlens import-facts FILE [--namespace NAME] [--complete]` | Validates and transactionally upserts an AI fact manifest into the separate enrichment layer. `--complete` removes stale facts only within the selected namespace. |
| `systemlens microservices`, `topics`, `apis`, `dtos`, `mongodb`, `modules` | Browse the indexed catalog; `microservices`, `topics` and `mongodb` list the corresponding architecture objects directly, each with a `kind` and `name`, and support the documented list/show/neighbors actions and JSON output where applicable. |
| `systemlens microservices topics\|apis\|mongodb\|properties\|openapi NAME [--root DIR] [--json]` | Follow one linked object kind from a single named microservice. |
| `systemlens microservices implementation KIND ID [--root DIR] [--json]` | Jump to the source implementation of one identified integration. |
| `systemlens modules integrations MODULE [--json]` | Lists the integrations owned by one module. |
| `systemlens modules graph [--json]` | Prints the Maven/Gradle build-dependency graph between modules. |
| `systemlens analyze audit [--workspace DIR]` | Reports static architecture risks; `--workspace` analyzes a parent workspace of independently indexed services instead of the current repository. |
| `systemlens analyze coverage [--root DIR] [--json]` | Reports inventory coverage and unresolved integrations. |
| `systemlens analyze indexing-issues [--root DIR] [--json]` | Lists unresolved indexing facts. JSON includes source evidence suitable for reviewing proposed heuristics. |
| `systemlens analyze microservices calls\|dependencies\|external-apis\|orphan-integrations [NAME] [--root DIR] [--json]` | Lists a service's outgoing calls, dependencies, external APIs, or integrations with no resolved caller/callee, depending on the subcommand. `external-apis` and `orphan-integrations` accept an optional `NAME` to scope the result to one service. |
| `systemlens analyze microservices impact NAME [--root DIR] [--json]` | Lists direct and transitive impact paths. |
| `systemlens analyze microservices path FROM TO [--root DIR] [--json] [--max-depth N] [--limit N]` | Lists bounded paths between services. |
| `systemlens analyze request-reply [--root DIR] [--json]` | Lists Strategy1 Kafka request/reply candidates. |
| `systemlens export microservices (--html FILE | --c4 DIRECTORY | --json) [--graph FILE] [--root-path DIRECTORY]` | Exports the deployable microservice, API, data-schema and message-channel topology. Non-deployable indexed modules (libraries and aggregators without an application entry point) are excluded from this view and remain available to `export modules` and `export layers`. Persisted MCP graph facts are included in the HTML export. `--graph FILE` reads a validated `systemlens-ai-graph-v1` manifest; `--root-path` provides the local source root for HTML source links. |
| `systemlens export modules --html FILE` | Exports the Maven/Gradle build-dependency view. |
| `systemlens export layers --html FILE` | Exports a dedicated software-layer view. With the persisted Strategy1 profile, project namespaces `PORTAIL` and `CYCLE-DE-VIE` are rendered in API/contracts and Orchestration, `DOMAIN-*` modules in Domain, and the documented layer-name prefixes/suffixes in their matching layers; shared libraries and other non-deployable modules are omitted, and without Strategy1 the repository-specific conventions are disabled. |
| `systemlens export namespaces --html FILE` | Exports a namespace view where each parent directory containing projects is shown as a container containing its indexed modules. Kubernetes namespaces remain secondary module metadata and do not define these containers. |
| `systemlens export request-reply --html FILE` | Exports Strategy1 Kafka request/reply candidates. |
| `systemlens web [--host HOST] [--port PORT]` | Starts the local Python web application at `http://127.0.0.1:8765/` by default. Its home page links to Architecture. Architecture renders the persisted snapshot for each request, excluding test-fixture microservices and every relation attached to them; when no index exists, it offers an explicit local button that creates the default configuration when needed and indexes the repository. The default loopback host prevents network exposure unless the user explicitly changes `--host`. |
| `simpleweb [DIRECTORY] [--host HOST] [--port PORT]` | Serves static files from `DIRECTORY`, or from the current directory when omitted, for opening generated HTML files that load adjacent JSON. It binds to `http://127.0.0.1:8000/` by default, has no write routes, and does not create or modify files. The directory must exist. |
| `systemlens mcp` | Starts the stdio MCP server. |

`systemlens index` reports its file delta, AST analysis stage, persisted endpoint
count and materialized relations. It then prints a next-step hint towards the
interactive microservice HTML export. Its result line is:

```text
scanned=<N> skipped=<N> +integrations=<N> -integrations=<N>
```

The first AST-only run removes stale results from the retired analyzer.

`--topic-strategy strategy1` is opt-in. The selected strategy is persisted with
the index and reused by incremental MCP reindexing and all derived views.
`--disable` accepts `properties`,
`module-architecture`, and `module-tree-sitter`.

## Extraction contract

An endpoint has a role (`serve`/`call` for REST, `produce`/`consume` for Kafka),
a system, a topic (`METHOD /path` for REST), source location, framework and
optional module, qualified name and Java message type. A value that cannot be
resolved statically is flagged `topic_dynamic=true`; it is never fabricated.

The Java AST extractor covers Spring MVC/WebFlux, Feign, RestTemplate,
WebClient, Spring Cloud Gateway, Spring Data REST, Spring Kafka and Spring
Cloud Stream. Markdown and JSON Kafka manifests are supported as explicit
sources and are labelled `source=manifest`.

A REST call forms an internal architecture relation only when its target
service is identified by an exact normalized explicit alias, such as an HTTP
host, an `lb://` service name, or a configured client domain. A matching HTTP
method and route only refines a resource within that already identified
service; it never identifies a service by itself. Calls without a unique target
remain indexed as unresolved evidence and are reported by coverage and indexing
issues rather than being linked to a coincidentally similar route.

The HTML microservice export provides an inspector for each statically typed
Kafka message. It shows the indexed payload-type identity, message topic, and
producer and consumer services. When the matching Java type is indexed, its
inspector also shows its source, declared fields, enum values, and conservative
recursive project-type navigation.
The architecture vocabulary is extensible: a `data_schema` node represents a
persisted data resource or contract (MongoDB collection, SQL table, Redis
keyspace or object-store dataset), while a `message_channel` node represents
a messaging channel (Kafka, RabbitMQ, SQS or a webhook stream). The concrete
technology is carried as metadata.
The export uses a responsive workspace layout with eight navigation tabs:
Explorer, Paths, OpenAPI, Kafka, Mongo, Request/reply, Build, and
Quality. It includes compact architecture counters, task-oriented starting
actions, a floating context panel, and a full-size resource inspector. On
narrow viewports the controls and context panel use separate bounded regions so
the graph remains visible while either panel scrolls independently.
The export opens with an Archify-inspired dark blue presentation (or follows
the browser's light preference), and provides a theme toggle in the graph
toolbar. The selected theme is stored only in browser local storage and does
not affect persisted inventory facts or exported architecture data.
Its initial view foregrounds task-oriented entry points (Kafka topic, service
dependencies, service-to-service path and Kafka messages). Relation/resource
filters and graph layouts are available as advanced controls. The graph offers
grouped, airy, balanced, and architectural layouts; the architectural layout
uses ELK.js compound nodes to arrange resources in a deterministic hierarchy:
software layers are stacked vertically, namespaces are nested inside their
layer, and services/resources are placed inside each namespace without
overlap. The internal canonical order is `api`, `application`, `orchestration`,
`infrastructure`, `domain`, then `persistence`;
`persistence` is always the lowest layer. In Strategy1, the `CYCLE-DE-VIE`
project namespace is rendered in the Orchestration layer.
External microservices are rendered in a dedicated `External services` layer
at the bottom, after the internal layer order.
Shared libraries and other non-deployable modules are not rendered as layers.
For the graph export, a namespace is the parent directory containing one or
more projects/modules; Kubernetes namespaces are retained as metadata only.
Projects located directly at the indexed repository root are assigned to the
synthetic `root` namespace.
The namespace-cluster layout is independent of the layer order and uses a
deterministic two-level packing without ELK: fCoSE first computes the local
compound layout of resources inside each namespace, then a deterministic
packing step places namespace rectangles one per row with a fixed separating
margin in graph coordinates; the rectangles are projected only after packing,
so camera zoom does not change their relative separation. The final grid is
the authoritative collision guard. Node identifiers
and namespace names are sorted only to make the result reproducible; there is
no semantic order between clusters. Neither resources nor namespace
rectangles may overlap. If fCoSE is unavailable, the same deterministic grid
is used without the local fCoSE ordering.
ELK is used only for the architectural layer layout, while Sigma.js provides
the interactive rendering for both views. Architecture relations remain
visible even when they are not used as placement edges.
It also provides dedicated OpenAPI, Kafka, Mongo, Request/reply, and Build
views, which keep their domain inventories separate. Changing a relation-type filter rebuilds and relayouts
the graph from only the selected dependency types; excluded relations do not
influence the resulting graph layout.

### Rules for the layered microservice rendering

The HTML architecture view MUST preserve these visual invariants:

- Each software layer is a bounded horizontal band whose width and height are
  calculated from its visible content. Layers MUST NOT be infinite full-width
  backgrounds.
- All visible layer bands MUST share the same left and right bounds. The first
  band starts immediately above its highest visible namespace content, and the
  last band ends immediately below its lowest visible namespace content.
- Each layer band MUST reserve a visible left gutter for its title. The title
  MUST NOT overlap a namespace or project cluster; widening the band is
  preferred to moving or shrinking cluster content.
- The layer-band geometry MUST be calculated from one shared rectangle model:
  all bands use the same left/right bounds, and the title gutter is included
  before the first cluster envelope.
- Layers MUST be stacked vertically in the canonical order above, with the
  Persistence layer at the bottom.
- Each visible project or fact namespace MUST be represented by a bounded rectangle
  fully contained inside its owning layer, including its header and padding.
- A namespace MAY use several rows. The default placement uses at most five
  boxes per row; additional boxes wrap onto subsequent rows.
- Microservices, Kafka topics, message channels, MongoDB collections, data
  schemas and other rendered resources MUST NOT overlap. Placement MUST keep a
  positive horizontal and vertical gap greater than the projected card size.
- Layer and namespace bounds MUST be recomputed after filtering, zooming,
  camera updates and layout changes so containers continue to contain their
  visible children.
- Selecting a layer or namespace MUST rebuild the visible graph without
  turning remaining cards white, losing isolated services, or leaving stale
  containers on screen.
- Changing node-type or relation filters MUST remain valid when no
  microservice layer is visible or when the filtered graph is empty; the
  renderer MUST clear stale layer and cluster containers without producing
  invalid coordinates.
- Changing a node-type filter MUST refresh the main graph renderer and its
  overlays immediately and MUST reapply the active graph layout to the
  filtered network.
- The layered view extends the namespace-cluster packing: each canonical
  software layer is a separate horizontal band ordered from top to bottom,
  and namespaces are packed inside that band. ELK compound-node placement is
  used as a seed when available, while the deterministic layer-aware packing
  is the final collision guard. If ELK is unavailable or fails, the fallback
  MUST retain the same layer order, namespace containment and non-overlap
  guarantees.
- If a namespace cluster becomes too tall and risks crossing a neighbouring
  layer, the renderer MUST add columns to that cluster and recompute the
  layout. The additional horizontal space MUST expand the diagram rather than
  overlap another layer or cluster.
Every layout switch MUST refit the camera to the resulting graph; it MUST NOT
apply an additional automatic zoom-out that makes the layout unnecessarily
small.

The details panel MUST display the resolved software layer and the cluster
path for microservices and resources (topics, collections, and enriched
resources). The cluster path MUST be the slash-separated path of cluster
directories, such as `cluster1/cluster2`, without a structural-group prefix.
For a resource modified in writing, the path MUST be inherited from its
producing or owning microservice.
If several microservices modify the same resource in writing, the renderer
MUST associate the resource with the microservice belonging to the lowest
software layer in the canonical visual order.

### Rules for the namespace-cluster rendering

The cluster view MUST preserve these visual invariants:

- Namespace membership MUST use the same resolver for placement and for the
  visible namespace rectangle.
- A Kafka topic, message channel, collection, or other resource MUST be
  assigned first to the namespace of its producing microservice, using the
  incoming source relation. A consumer namespace MUST NOT move the resource
  into its cluster. Resources without an identifiable producer remain in
  `ROOT`.
- Resources inside one namespace MUST be placed on a grid with a positive
  horizontal and vertical gap greater than the projected card size.
- Within each namespace cluster, microservices MUST occupy the first
  sub-layer and Kafka, MongoDB, and other resources MUST occupy a second
  sub-layer below them. Empty sub-layers are omitted.
- Namespace rectangles MUST be packed with a positive gap based on their full
  rendered envelope, including card, title, and padding margins, and MUST NOT
  overlap each other.
- When a second grouping level is displayed, each parent cluster MUST be the
  union of its visible child cluster rectangles plus its own title/padding
  margin. Parent bounds MUST contain the complete child boxes; the
  non-overlap rule applies between sibling clusters, not between a parent and
  its descendants.
- A project-group parent MUST remain attached to the namespace of its owning
  projects and MUST contain only those owning projects. Resources MUST remain
  in the namespace cluster of their producing microservice; relation targets
  MUST NOT be added as children of the structural parent or enlarge it across
  unrelated namespace clusters.
- Cluster bounds MUST be calculated from graph-coordinate bounds and projected
  after camera changes. Parent bounds MUST be recomputed from the projected
  child bounds, so zooming cannot make a child escape its parent or make the
  hierarchy drift.
- The layout MUST NOT depend on the software-layer order. Narrow viewports MAY
  use additional rows to keep clusters inside the visible graph area.

Microservices with no indexed inter-service relation remain visible in a
separate isolated area of the graph, so their absence of dependencies is not
confused with an absent service.
Microservices, Kafka topics, and MongoDB collections are marked by their
relative connectivity. In the HTML graph, the shape identifies the resource
type. Microservices, Kafka topics, and MongoDB collections use a coloured
outline around a neutral interior; the fill never carries connectivity or risk
meaning. The relation count includes their indexed HTTP, Kafka,
and MongoDB dependencies, while low/medium/high relative tiers are calculated separately
for each resource type. For a microservice, the count is its distinct direct
HTTP clients and targets, Kafka producer/consumer topic relations, and MongoDB
collection relations; multiple HTTP routes between the same client and target
are counted once. The HTML complexity badge exposes the HTTP, Kafka, and
MongoDB breakdown as a tooltip, along with the resource rank and its soft
tercile bounds. The lowest third is blue, the middle third orange, and the
highest third red; the terciles are recalculated separately for each resource
type in every export. The graph label of each coloured resource also displays
its relative connectivity through its coloured outline; the exact details are
available after selecting the node.
The Explore search suggests indexed resource names and accepts either one
exact, unambiguous graph-node name or a Kafka itinerary written with `->`.
An itinerary starts and ends with a microservice and follows only directed Kafka
relations through Kafka topics; it never traverses HTTP or MongoDB dependencies.
When a microservice and another resource have the same display name, a direct
resource search remains ambiguous, while an itinerary endpoint resolves the
unique microservice candidate required by the itinerary grammar.
Invalid, ambiguous, repeated, or unreachable stops leave the current graph
unchanged and produce an actionable message. The itinerary detail is an
ordered, clickable list of node names and types. A Kafka topic lists its
associated DTO names in parentheses. Selecting any path stop reveals its
ordinary detail view, including the indexed Kafka source links where present.
Every graph resource detail starts with indexed and visible relation counts,
then a `Relations` section. For a microservice, that section separates
consumed and published API and Kafka resources, plus MongoDB collections. Each
microservice resolved to an indexed Maven or Gradle module also provides a
visible action that opens the module root directory in VS Code. Kafka topics
list their applicable DTOs. A collapsed `Sources` section lists the indexed
OpenAPI and Kafka files that provide the evidence, avoiding repetition in every
topic.
At constrained viewport sizes, an empty floating context panel remains visible
as guidance but does not intercept pointer interaction with the active tab;
once it contains selected-node or path details, its normal controls remain
interactive.
Indexing persists source evidence only as paths relative to the project root.
HTML export joins those paths to `--root-path` (the current directory by
default) when building VS Code links. No WSL distribution or absolute local
source path is stored in the index.
When several modules reference one OpenAPI/Swagger file, it is listed once for
the module that directly contains the source file.
MongoDB collection details list the services using the collection once, followed
by indexed Java persistence classes resolved from
`@Document`, Mongo repository entity generics, or unambiguous `Type.class`
arguments passed to `MongoTemplate`. They include their qualified name, source
location, and declared fields. The Persistence view provides the same inventory
with filtering by class, package, collection, or service and opens a dedicated
inspector. Fields whose type resolves uniquely to another indexed project class
are navigable recursively; the inspector provides a return action to the
containing class. External and ambiguous field types remain plain text.
Persistence classes declared in a dependent build module are attached to the
owning service collection through the indexed module-dependency graph. When
dependency metadata is unavailable, a workspace-wide class is used only if it
is the unique candidate for that collection name; ambiguous candidates remain
unassociated rather than being guessed.
The topic detail lists resolved Kafka DTOs once. It lists message types only
when no matching DTO has been resolved, avoiding duplicate published and
consumed type lists when they describe the same contract.
Indexing issues that have a source endpoint expose a VS Code link to the
associated file and line. The HTML export provides dedicated OpenAPI, Kafka,
Mongo, Request/reply, and Build views. OpenAPI and Kafka both support
filtering their complete list (OpenAPI by path or service, DTOs by simple name
or package); Persistence filters by class, package, collection, or service. A
persistent inventory status reports whether unresolved indexing facts exist and
opens their review view.

`--topic-strategy strategy1` adds opt-in convention extraction for selected
`getTopics()` accessors and `envoyerMessageKafka*(kafkaProperties.getTopics().getXxx(), payload)` calls
(including `envoyerMessageKafkaRequest` and `envoyerMessageKafkaReply`),
`${kafka.topics.*.name}` expressions and configured REST client constants. It
also enables the `getXxxServiceUrl()` REST target-name convention.
For a `src/main/resources/openapi/xxx.rest` publication declaration, it
searches the entire indexed repository for same-named `xxx.yaml`,
`xxx.yml`, or `xxx.json` OpenAPI contracts, including contracts in a sibling
shared module without an `openapi-generator` Maven configuration. It also
searches every YAML or JSON document below
`model-xxx/src/main/resources/openapi/`, so that module may contain several
contracts with distinct names. Only a valid OpenAPI document is attached, and
its resulting endpoints remain attributed to the module that owns the `.rest`
declaration.

Independently of Strategy1, each build module inventories every valid YAML or
JSON OpenAPI document under its own `src/main/resources/openapi/` directory;
contract file names do not need to follow an `openapi.*` or `swagger.*`
pattern.
With Strategy1, it may also derive a high-confidence request/reply pair when
both sides follow the `retour_<request-topic>` convention.

## Incrementality and freshness

The index stores SHA-256 values for eligible files. A normal run parses added or
changed files and purges facts for deleted files. A full refresh is forced when
the endpoint extractor signature, analysis configuration signature, selected
topic strategy, Spring configuration file, or Maven/Gradle build descriptor
changes. Spring properties and build descriptors can affect facts attributed to
otherwise unchanged Java source files. Explicit manifests are included even when
otherwise excluded.

The index is `.systemlens/findings.db` for compatibility with prior releases. It is a
local implementation detail, not a contract for direct SQL writes.

## MCP

The MCP server exposes a deliberately small control surface for the
index-then-enrich workflow. It no longer mirrors every read-only CLI command:

| Tool | Purpose |
|---|---|
| `index_repository` | Index or refresh the current repository; preserves graph enrichment facts. |
| `graph_fact_exists` | Check a semantic node/edge fact before proposing it. |
| `add_graph_fact` | Add an AI/user node or edge assertion with confidence and optional relative evidence; rejects semantic duplicates. |
| `import_graph_facts` | Validate and atomically upsert a `systemlens-ai-graph-v1` manifest into one enrichment namespace, optionally removing stale facts for a complete snapshot. |
| `remove_graph_fact` | Remove an assertion previously added through MCP; never removes extracted source facts. |
| `list_graph_facts` | List the persisted enrichment layer. |
| `architecture_graph` | Return the complete generic dependency graph (services, APIs, topics, data schemas and external resources) merged with persisted enrichment facts. |

Only `index_repository` creates or refreshes source-derived facts. Enrichment
facts are stored separately in `graph_facts`, survive reindexing, and are never
treated as source evidence. Nodes require `fact_type=node`, `kind` and `name`;
edges require source/target kinds and names plus `relation`. Evidence paths are
relative to the indexed repository and may not escape it.
`add_graph_fact` remains an additive single-fact API and rejects duplicates.
For iterative analysis, use `import_graph_facts`: it reconciles by the
manifest namespace and stable node/edge id, replacing the complete stored
value for an existing AI fact. A partial manifest never removes facts; a
manifest with `mode=complete` (or an explicit `complete=true`) removes stale
facts only from that namespace. Source-derived facts are stored separately and
are never overwritten. The import is transactional and returns inserted,
updated and removed counts.
For generic middleware, use `kind=data_schema` or `kind=message_channel`, set
`technology` to the concrete implementation, and put provider-specific facts
such as database/schema/table, exchange/queue or partition in `metadata`.

## Boundaries

The inventory is static. Reflection, arbitrary string construction, runtime
routing and undeclared external contracts can remain unresolved. Consumers must
use `topic_dynamic`, confidence and source evidence when interpreting the graph.
