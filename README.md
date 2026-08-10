# ccc-radar (`cccr`)

Semgrep findings index and Java/Spring architecture inventory.

`cccr` indexes a project's Semgrep findings locally in `.cccr/findings.db`,
queries them with precise lexical search.

The product has two complementary uses:

- **Semgrep findings** for agents and developers: `init`, `index`, `findings`,
  `summary`, and their related MCP tools.
- **Java/Spring exploration**: REST/Kafka inventory, an inter-service graph,
  and navigation through microservices, topics, APIs, MongoDB collections, and modules.

## Positioning

`cccr` maintains its local Semgrep index and architecture inventory. The
`cccr index --engine cocoindex` engine is experimental; it adds a local
code-chunk index without changing architecture exploration.

Implementation, storage, and MCP details are in
[`docs/SPEC-TECH.md`](docs/SPEC-TECH.md).

## Documentation

- [`AGENT.md`](AGENT.md) — documentation maintenance guide.
- [`docs/PRD.md`](docs/PRD.md) — product vision, personas, and metrics.
- [`docs/SPEC-FONC.md`](docs/SPEC-FONC.md) — CLI, MCP, and error behavior.
- [`docs/SPEC-TECH.md`](docs/SPEC-TECH.md) — modules, storage, and algorithms.
- [`docs/ADR.md`](docs/ADR.md) — architecture decisions.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — maintainer code map and ownership boundaries.
- [`reports/README.md`](reports/README.md) — index of example reports.
The Claude Code skill (`SKILL.md`) is distributed separately in
[`ccc-radar-skill`](https://github.com/elkouhen/ccc-radar-skill); its behavior
remains documented in
[`docs/SPEC-FONC.md`](docs/SPEC-FONC.md).

## Related Projects

- [`ccc-radar-skill`](https://github.com/elkouhen/ccc-radar-skill) —
  Claude Code skill for an agent.

## Installation

Prerequisites: `uv` and `pipx`.

```bash
uv tool install ccc-radar
pipx install semgrep
env -u SSL_CERT_FILE uvx --from huggingface_hub hf download jinaai/jina-code-embeddings-1.5b --local-dir ~/models/jina-code-embeddings-1.5b
```

The default embedding model is `~/models/jina-code-embeddings-1.5b`. When
downloading with `hf`, removing `SSL_CERT_FILE` avoids TLS errors observed on
some workstations.

## Quick Start

### Semgrep Findings

This workflow requires neither an external code-search tool nor the architecture packs.

```bash
cccr init
cccr index
cccr summary
cccr findings "sql injection" --severity ERROR
```

Without local configuration, `cccr init` enables the registry rulesets
`p/security-audit`, `p/java`, `p/owasp-top-ten`, and `p/secrets`. Indexing is
incremental; its last line summarizes processed files and findings.

### Java/Spring Architecture Exploration

REST/Kafka inventory requires the CCCR `default`, `liveness`, `rest`, `kafka`,
and `kafka-security` packs. Install the skill, then point to its rules:

```bash
npx skills add elkouhen/ccc-radar-skill
export CCCR_RULES_ROOT="/chemin/vers/ccc-radar-skill/skills/cccr/rules"
cccr init
cccr doctor
cccr index
```

`cccr doctor` must confirm the architecture packs before a graph is
interpreted. Semgrep registry rulesets produce findings but do not detect the
APIs and topics required for architecture mapping.

```bash
cccr microservices
cccr microservices show order-service
cccr microservices topics order-service
cccr topics show orders.created
cccr topics consumers orders.created
cccr dtos show OrderCreated
cccr apis consumers "POST /payments"
cccr mongodb services orders
cccr analyze microservices path order-service shipping-service
cccr analyze audit
cccr analyze coverage
cccr analyze request-reply
cccr export request-reply --html request-reply.html
```

Kafka summaries include the statically inferred Java payload types for each
published and consumed topic. Types are extracted from explicit listener
parameters and Kafka client generics; an unknown type is left empty rather
than inferred from a topic name or serializer configuration.

For projects that centralize their Kafka configuration in a `getTopics()`
object, or HTTP client domains in `Rest*Config*` classes,
use the opt-in `strategy1` extractor:

```bash
cccr index --topic-strategy strategy1
```

It normalizes a producer call `getTopics().getAbcDefGhiJkl()` and a
`@KafkaListener` placeholder `${kafka.topics.abc_def_ghi_jkl.name}` to the
physical topic `ABC_DEF_GHI_JKL`; it also creates configured HTTP-client
relations from every uppercase constant containing an underscore in those REST
configuration classes. A Maven OpenAPI generator configured with
`inputSpecRootDirectory` (or `inputSpec`) is resolved through a service
declaration `src/main/resources/openapi/<api>.rest`: the service publishes the
operations of the contract `<api>.yaml` (or equivalent extension) configured
in a `model-*` module, without relying on generated `XxxApi` Java interfaces.
Changing the strategy triggers a full
inventory refresh. `strategy1` is available with the default `manual` engine,
not with the experimental `cocoindex` engine.

Exports keep the runtime graph (microservices, Kafka, MongoDB) separate from
module build dependencies:

```bash
cccr export microservices --html graph.html
cccr export microservices --c4 architecture-likec4
cccr export modules --html modules.html
```

Dans le fichier HTML des microservices, les interrupteurs `HTTP` et `Kafka`
permettent d'afficher indépendamment ces deux types de relations.

`cccr index --manifest kafka-flow-graph-anonymous.json` imports a Kafka JSON
or Markdown manifest when relationships cannot be detected from code. A
`topics trace` flow is a static hypothesis, never a production trace.

## Upgrade

```bash
uv tool upgrade ccc-radar
uv tool upgrade --all
```

If an existing configuration contains `p/spring`, remove that entry from
`.cccr/config.yml`: the ruleset does not exist in the Semgrep registry.

## Development

```bash
uv sync
uv run cccr version
uv run pytest
```

## MCP Server

`cccr` exposes an MCP server over stdin/stdout through `cccr mcp`.

- **Semgrep findings**: `search_findings`, `findings_summary`, `search`, and
  `reindex_findings`.
- **Java/Spring exploration**: `list_endpoints`, `graph`, `dependency_graph`,
  `audit_dependency_graph`, `list_workspace_services`, `list_modules`, and
  `trace_message_flow`.

### Prerequisites

Install `cccr`, initialize the repository, and build its index before starting
an MCP client. The server uses its process working directory as the project
root, so start the client from the indexed repository.

```bash
uv tool install ccc-radar
cd /path/to/project
cccr init
cccr index
```

### Codex

Register the stdio server once in the Codex user configuration:

```bash
codex mcp add cccr -- cccr mcp
codex mcp get cccr
```

Restart Codex, then open the indexed repository. The server can use the
project's `.cccr/config.yml` and `.cccr/findings.db` only when Codex is started
from that repository.

### Claude Code

```json
{"mcpServers": {"cccr": {"command": "cccr", "args": ["mcp"]}}}
```

### Pi

Pi requires an MCP extension. Install the community adapter once, then add a
project-local `.mcp.json` file in the indexed repository:

```bash
pi install npm:pi-mcp-adapter
```

```json
{
  "mcpServers": {
    "cccr": {
      "command": "cccr",
      "args": ["mcp"]
    }
  }
}
```

Start `pi` from that repository and use `/mcp` to inspect the connection. Pi
does not include MCP support by default; `pi-mcp-adapter` provides it.
See the adapter's [configuration guide](https://github.com/nicobailon/pi-mcp-adapter#config)
for Pi-specific options.

To verify a file after a fix (remediation loop, see
[`ccc-radar-skill`](https://github.com/elkouhen/ccc-radar-skill)), also
register the official Semgrep MCP server:

```json
{"mcpServers": {"semgrep": {"command": "uvx", "args": ["semgrep-mcp"]}}}
```

For `.cccr/config.yml` field details, see
[`docs/SPEC-FONC.md`](docs/SPEC-FONC.md).

## License

[Apache License 2.0](LICENSE).
