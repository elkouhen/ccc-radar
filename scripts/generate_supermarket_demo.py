#!/usr/bin/env python3
"""Generate the deterministic supermarket architecture demonstration.

The resulting AI graph is intentionally synthetic.  Its size makes it useful
for exercising SystemLens HTML rendering without exposing production data.
"""

from __future__ import annotations

import json
from pathlib import Path


DEMO_ROOT = Path(__file__).parents[1] / "examples" / "supermarket"
MANIFEST_PATH = DEMO_ROOT / "architecture.supermarket.pass-001.json"

# Each bounded context is a rendering cluster and contains five services.
DOMAINS = {
    "customer": ["customer-profile", "loyalty-account", "customer-preferences", "customer-segmentation", "customer-support"],
    "catalog": ["product-catalog", "assortment", "pricing", "promotion", "product-media"],
    "inventory": ["stock-ledger", "availability", "replenishment", "warehouse", "shelf-management"],
    "procurement": ["supplier", "purchase-order", "goods-receipt", "supply-contract", "demand-forecast"],
    "commerce": ["shopping-cart", "checkout", "order-management", "basket-pricing", "fulfillment"],
    "payment": ["payment", "invoicing", "refund", "fraud-screening", "fiscal-receipt"],
    "store": ["store-directory", "point-of-sale", "cash-drawer", "shift-management", "workforce"],
    "delivery": ["delivery-routing", "picking", "last-mile", "delivery-slot", "returns"],
    "marketing": ["campaign", "coupon", "personalization", "recommendation", "retail-analytics"],
    "platform": ["identity", "notification", "search", "reporting", "observability"],
}

SCHEMA_SUFFIXES = ("record", "command", "event", "projection", "audit")
LAYERS = ("api", "application", "domain", "infrastructure", "persistence")


def evidence(domain: str, name: str, kind: str) -> list[dict[str, object]]:
    return [{
        "path": f"synthetic/{domain}/{name}/{kind}.yaml",
        "start_line": 1,
        "end_line": 1,
        "quote": f"Synthetic supermarket {kind}: {name}",
    }]


def metadata(domain: str, layer: str) -> dict[str, object]:
    namespace = f"supermarket-{domain}"
    return {"namespace": namespace, "namespaces": [namespace], "layer": layer, "domain": domain}


def generate() -> dict[str, object]:
    nodes: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    services: list[tuple[str, str, str]] = []

    for domain_index, (domain, names) in enumerate(DOMAINS.items()):
        for service_index, name in enumerate(names):
            layer = LAYERS[(domain_index + service_index) % len(LAYERS)]
            services.append((domain, name, layer))
            nodes.append({
                "id": f"service-{name}", "kind": "service", "name": name,
                "status": "confirmed", "confidence": "high",
                "metadata": metadata(domain, layer),
                "evidence": evidence(domain, name, "service"),
            })

    # Every service emits a topic that its successor consumes: 100 Kafka edges.
    # A second business-event topic per service brings the Kafka total to 100 topics
    # and 200 publish/consume relationships.
    for index, (domain, service, layer) in enumerate(services):
        consumer = services[(index + 1) % len(services)]
        for event_index, event in enumerate(("changed", "processed")):
            topic_name = f"supermarket.{domain}.{service}.{event}.v1"
            topic_id = f"topic-{index + 1:02d}-{event_index + 1}"
            nodes.append({
                "id": topic_id, "kind": "message_channel", "name": topic_name,
                "status": "confirmed", "confidence": "high",
                "technology": "Kafka", "metadata": metadata(domain, layer),
                "evidence": evidence(domain, topic_name, "topic"),
            })
            for direction, source, target in (("publishes", service, topic_id), ("consumes", topic_id, consumer[1])):
                edges.append({
                    "id": f"edge-{len(edges) + 1:03d}",
                    "source": f"service-{source}" if direction == "publishes" else source,
                    "target": target if direction == "publishes" else f"service-{target}",
                    "kind": direction, "relation": direction,
                    "status": "confirmed", "confidence": "high", "channel": topic_name,
                    "message_type": f"{service.replace('-', ' ').title().replace(' ', '')}{event.title()}Event",
                    "metadata": metadata(domain, layer),
                    "evidence": evidence(domain, topic_name, direction),
                })

    # Five schemas per domain, each written by its owning service and read by
    # the next service.  This yields 50 schemas and the remaining 100 relations.
    for domain, names in DOMAINS.items():
        for index, (service, suffix) in enumerate(zip(names, SCHEMA_SUFFIXES, strict=True)):
            reader = names[(index + 1) % len(names)]
            schema_name = f"{domain}-{suffix}-schema"
            schema_id = f"schema-{domain}-{suffix}"
            layer = LAYERS[index]
            nodes.append({
                "id": schema_id, "kind": "data_schema", "name": schema_name,
                "status": "confirmed", "confidence": "high",
                "metadata": metadata(domain, layer),
                "evidence": evidence(domain, schema_name, "schema"),
            })
            for relation, source, target in (("writes", service, schema_id), ("reads", reader, schema_id)):
                edges.append({
                    "id": f"edge-{len(edges) + 1:03d}",
                    "source": f"service-{source}", "target": target,
                    "kind": relation, "relation": relation,
                    "status": "confirmed", "confidence": "high",
                    "metadata": metadata(domain, layer),
                    "evidence": evidence(domain, schema_name, relation),
                })

    assert len(services) == 50
    assert sum(node["kind"] == "message_channel" for node in nodes) == 100
    assert sum(node["kind"] == "data_schema" for node in nodes) == 50
    assert len(edges) == 300
    return {
        "format": "systemlens-ai-graph-v1",
        "project": "supermarket-demo",
        "generated_by": {
            "agent": "systemlens-demo-generator", "model": "synthetic-fixture",
            "source_revision": "demo-2026-09-06", "pass": "supermarket-001",
            "namespace": "supermarket-demo",
        },
        "mode": "complete", "nodes": nodes, "edges": edges,
    }


def main() -> None:
    DEMO_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(generate(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
