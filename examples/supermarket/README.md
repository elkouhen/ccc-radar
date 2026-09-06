# Supermarket demo dataset

This synthetic `systemlens-ai-graph-v1` dataset models the large supermarket
platform shown in the documentation site. It contains exactly 50 microservices,
100 Kafka topics, 50 data schemas, and 300 confirmed relationships. The ten
bounded contexts below are also the graph's ten runtime-namespace clusters:

- customer, catalog, inventory, procurement, commerce
- payment, store, delivery, marketing, platform

All evidence is synthetic and relative to the project root; the dataset has no
production identifiers, credentials, or source-code dependency.

## Generate and render

Regenerate the large synthetic manifest, then export it directly:

```bash
uv run python scripts/generate_supermarket_demo.py
uv run systemlens export microservices \
  --graph examples/supermarket/architecture.supermarket.pass-001.json \
  --html examples/supermarket/supermarket.html
```

The documentation site's compact model is not synthetic. It is an exported,
source-evidenced snapshot of the sibling
`systemlens-observability-lab/apps/supermarket-demo` application. Its evidence
paths remain relative to that application's root.

The manifest can alternatively be imported as replaceable facts using the
`supermarket-demo` namespace.
