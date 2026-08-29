# AI graph manifest (`systemlens-ai-graph-v1`)

An AI may produce an architecture graph when source conventions are too
complex for deterministic extractors. The manifest is an analysis artifact,
not a replacement for the SQLite source inventory and is never persisted by
SystemLens.

## Contract

```json
{
  "format": "systemlens-ai-graph-v1",
  "project": "string",
  "generated_by": {"agent": "string", "model": "string", "source_revision": "string"},
  "nodes": [
    {
      "id": "stable-id",
      "kind": "service | external_service | topic | collection",
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
      "kind": "http | event | data",
      "status": "confirmed | proposed | ambiguous | unresolved",
      "confidence": "high | medium | low | unknown",
      "channel": "topic, route, bucket, or logical channel",
      "label": "optional display label",
      "message_type": "optional event payload type",
      "reason": "required for ambiguous/unresolved claims",
      "evidence": [{"path": "relative/path", "start_line": 1, "end_line": 2, "quote": "optional short quote"}]
    }
  ]
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

The initial HTML adapter maps `service` and `external_service` to service
nodes, `event` to a service/topic/service path, and `collection` to the
existing collection visual. Generic stores such as S3, PostgreSQL or
OpenSearch should use `collection` only when that visual approximation is
acceptable; a future graph renderer may add first-class store kinds.

## Invocation

```bash
systemlens export microservices --graph architecture.ai-graph.json --html architecture.html
```

The command is read-only with respect to the project index. Use
`--root-path` only when evidence links should resolve to a local checkout.
