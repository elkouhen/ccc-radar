import json
import re
from pathlib import Path

from ccc_radar.models import MessageEndpoint, compute_endpoint_id
from ccc_radar.modules import DiscoveredModule
from ccc_radar.render import render_graph_html


def _kafka_endpoint(role: str, message_type: str, path: str) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, "orders.created", path),
        role=role,
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path=path,
        start_line=1,
        end_line=1,
        snippet="",
        message_type=message_type,
    )


def _html_graph_data(document: str) -> dict[str, object]:
    match = re.search(
        r'<script id="graph-data" type="application/json">(.*)</script>', document
    )
    assert match is not None
    return json.loads(match.group(1))


def test_graph_html_recursively_exposes_kafka_produced_and_consumed_dtos(tmp_path: Path) -> None:
    source_root = tmp_path / "orders" / "src" / "main" / "java" / "com" / "example" / "events"
    source_root.mkdir(parents=True)
    (source_root / "OrderCreated.java").write_text(
        """package com.example.events;

import java.util.List;

public record OrderCreated(OrderDetails details, List<LineItem> lines) {}
class OrderDetails { Customer customer; }
record LineItem(String sku, Price price, PaymentStatus status) {}
record Customer(String id, Address address) {}
record Price(String currency) {}
record Address(String city) {}
enum PaymentStatus { AUTHORIZED, DECLINED }
""",
        encoding="utf-8",
    )
    module = DiscoveredModule(
        name="orders",
        path=tmp_path / "orders",
        build_system="maven",
        version=None,
        kind="library",
        starts_application=True,
        configuration_example="",
    )
    document = render_graph_html(
        {
            "producer": [_kafka_endpoint("produce", "com.example.events.OrderCreated", "OrderPublisher.java")],
            "consumer": [_kafka_endpoint("consume", "com.example.events.OrderCreated", "OrderConsumer.java")],
        },
        [],
        build_modules=[module],
    )

    graph_data = _html_graph_data(document)
    kafka_dtos = {dto["name"]: dto for dto in graph_data["kafka_dtos"]}
    definitions = {
        dto["name"]: dto
        for dto in [*graph_data["kafka_dtos"], *graph_data["project_dto_definitions"]]
    }

    assert kafka_dtos["OrderCreated"]["producers"] == ["producer"]
    assert kafka_dtos["OrderCreated"]["consumers"] == ["consumer"]
    assert kafka_dtos["OrderCreated"]["topics"] == ["orders.created"]
    assert definitions["OrderCreated"]["fields"] == [
        {"name": "details", "type": "OrderDetails", "dto_references": ["OrderDetails"]},
        {"name": "lines", "type": "List<LineItem>", "dto_references": ["LineItem"]},
    ]
    assert definitions["OrderDetails"]["fields"] == [
        {"name": "customer", "type": "Customer", "dto_references": ["Customer"]}
    ]
    assert definitions["Customer"]["fields"] == [
        {"name": "id", "type": "String"},
        {"name": "address", "type": "Address", "dto_references": ["Address"]},
    ]
    assert definitions["LineItem"]["fields"] == [
        {"name": "sku", "type": "String"},
        {"name": "price", "type": "Price", "dto_references": ["Price"]},
        {"name": "status", "type": "PaymentStatus", "dto_references": ["PaymentStatus"]},
    ]
    assert definitions["Address"]["fields"] == [{"name": "city", "type": "String"}]
    assert definitions["Price"]["fields"] == [{"name": "currency", "type": "String"}]
    assert definitions["PaymentStatus"]["enum_values"] == ["AUTHORIZED", "DECLINED"]
    assert 'appendDtoInspectorSection("Valeurs enum", dto.enum_values || [])' in document
