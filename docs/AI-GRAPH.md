# AI graph manifest (`systemlens-ai-graph-v1`)

An AI may produce an architecture graph when source conventions are too
complex for deterministic extractors. The same manifest can be rendered as a
temporary graph or imported as replaceable enrichment facts; it never replaces
the SQLite source inventory.

## Contract

```json
{
  "format": "systemlens-ai-graph-v1",
  "project": "string",
  "generated_by": {"agent": "string", "model": "string", "source_revision": "string", "pass": "pass-001", "namespace": "ai-architecture"},
  "nodes": [
    {
      "id": "stable-id",
      "kind": "service | external_service | topic | collection | data_schema | message_channel",
      "name": "display name",
      "owner": "service-id or service name, required for collection",
      "evidence": [{"path": "relative/path", "start_line": 1, "end_line": 2, "quote": "optional short quote"}]
    }
  ],
  "edges": [
    {
      "id": "stable-edge-id",
      "source": "node id",
      "target": "node id",
      "kind": "http | event | data | serves | calls | reads | writes | publishes | consumes | provides",
      "status": "confirmed | proposed | ambiguous | unresolved",
      "confidence": "high | medium | low | unknown",
      "channel": "topic, route, bucket, or logical channel",
      "label": "optional display label",
      "message_type": "optional event payload type",
      "reason": "required for ambiguous/unresolved claims",
      "evidence": [{"path": "relative/path", "start_line": 1, "end_line": 2, "quote": "optional short quote"}]
    }
  ],
  "mode": "partial | complete"
}
```

`source` and `target` always reference node IDs. `channel` is required for
`event` relations and should preserve the exact topic or channel expression;
the AI must not invent a concrete value for a dynamic expression. Evidence
paths are relative to the analyzed project root and must not contain secrets or
absolute machine paths.

SystemLens renders `confirmed` and `proposed` relations. It does not render
`ambiguous` or `unresolved` relations as dependencies; it reports them in the
quality panel with their reason. `confidence` describes the AI analysis and
`status` describes whether the claim is safe to draw. A low-confidence claim
may be proposed, but must remain visibly attributable to the AI manifest.

The HTML adapter maps `service` and `external_service` to service nodes,
`event` to a service/topic/service path, and `collection` to the existing
collection visual. Generic `data_schema` and `message_channel` nodes are
available in the generic graph and enrichment layer.

## Invocation

```bash
systemlens export microservices --graph architecture.ai-graph.json --html architecture.html
```

The command is read-only with respect to the project index. Use
`--root-path` only when evidence links should resolve to a local checkout.

## Persistent import

```bash
systemlens import-facts architecture.ai-graph.pass-002.json \
  --namespace ai-architecture
```

The command upserts facts by `(namespace, fact_type, id)`. Re-importing a
revised fact replaces its evidence, status, confidence and metadata. Use
`--complete` only for a full snapshot of that namespace; partial passes leave
unmentioned facts untouched. The equivalent MCP tool is
`import_graph_facts(manifest_path, namespace, complete)`.
