import json
import re
from dataclasses import replace
from pathlib import Path

from ccc_radar.models import MessageEndpoint, compute_endpoint_id
from ccc_radar.modules import DiscoveredModule
from ccc_radar.render import render_graph_html


def _kafka_endpoint(
    role: str, message_type: str, path: str, qualified_name: str | None = None
) -> MessageEndpoint:
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
        qualified_name=qualified_name,
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
        {"name": "details", "type": "OrderDetails", "dto_references": ["com.example.events.OrderDetails"]},
        {"name": "lines", "type": "List<LineItem>", "dto_references": ["com.example.events.LineItem"]},
    ]
    assert definitions["OrderDetails"]["fields"] == [
        {"name": "customer", "type": "Customer", "dto_references": ["com.example.events.Customer"]}
    ]
    assert definitions["Customer"]["fields"] == [
        {"name": "id", "type": "String"},
        {"name": "address", "type": "Address", "dto_references": ["com.example.events.Address"]},
    ]
    assert definitions["LineItem"]["fields"] == [
        {"name": "sku", "type": "String"},
        {"name": "price", "type": "Price", "dto_references": ["com.example.events.Price"]},
        {"name": "status", "type": "PaymentStatus", "dto_references": ["com.example.events.PaymentStatus"]},
    ]
    assert definitions["Address"]["fields"] == [{"name": "city", "type": "String"}]
    assert definitions["Price"]["fields"] == [{"name": "currency", "type": "String"}]
    assert definitions["PaymentStatus"]["enum_values"] == ["AUTHORIZED", "DECLINED"]
    assert 'appendDtoInspectorSection("Valeurs enum", dto.enum_values || [])' in document
    assert "Que voulez-vous comprendre ?" in document
    assert "Qui produit ou consomme un topic Kafka ?" in document
    assert 'id="advanced-controls"' in document
    assert 'id="resources-tab"' in document
    assert '>Ajuster</button>' in document
    assert '>Effacer</button>' in document
    assert 'id="dto-reference-filter"' in document
    assert 'id="openapi-reference-filter"' in document
    assert 'id="resources-tab"' in document
    assert 'id="resources-panel"' in document
    assert 'id="show-request-reply"' in document
    assert 'id="show-dependencies"' in document
    assert 'id="graph-legend"' in document
    assert 'graphLegend.hidden = !showingGraph' in document
    assert 'issue.vscode_uri ? "a" : "code"' in document
    assert "max-height: calc(100vh - 32px)" in document
    assert "scroll-padding-bottom: 8px" in document
    assert 'placeholder="orders ou orders -> payments"' in document
    assert "function resolveExactNodeName(name)" in document
    assert "function runExploreSearch()" in document
    assert "Aucun chemin oriente ne passe par les noeuds demandes dans cet ordre." in document
    assert "${nodeKindLabel(node)}${dtoSuffix}" in document
    assert "function appendServiceKafkaActivities" in document


def test_graph_html_distinguishes_dtos_with_the_same_simple_name_by_package(tmp_path: Path) -> None:
    source_root = tmp_path / "service" / "src" / "main" / "java"
    (source_root / "com" / "acme" / "one").mkdir(parents=True)
    (source_root / "com" / "acme" / "two").mkdir(parents=True)
    (source_root / "com" / "acme" / "publishers").mkdir(parents=True)
    (source_root / "com" / "acme" / "one" / "Event.java").write_text(
        "package com.acme.one; public record Event(String orderId) {}",
        encoding="utf-8",
    )
    (source_root / "com" / "acme" / "two" / "Event.java").write_text(
        "package com.acme.two; public record Event(String customerId) {}",
        encoding="utf-8",
    )
    (source_root / "com" / "acme" / "publishers" / "FirstPublisher.java").write_text(
        "package com.acme.publishers; import com.acme.one.Event; class FirstPublisher {}",
        encoding="utf-8",
    )
    (source_root / "com" / "acme" / "publishers" / "SecondPublisher.java").write_text(
        "package com.acme.publishers; import com.acme.two.Event; class SecondPublisher {}",
        encoding="utf-8",
    )
    module = DiscoveredModule(
        name="service",
        path=tmp_path / "service",
        build_system="maven",
        version=None,
        kind="application",
        starts_application=True,
        configuration_example="",
    )
    first = _kafka_endpoint("produce", "Event", "FirstPublisher.java", "com.acme.publishers.FirstPublisher")
    second = _kafka_endpoint("produce", "Event", "SecondPublisher.java", "com.acme.publishers.SecondPublisher")

    graph_data = _html_graph_data(render_graph_html({"one": [first], "two": [second]}, [], build_modules=[module]))
    definitions = {dto["id"]: dto for dto in graph_data["kafka_dtos"]}

    assert set(definitions) == {"com.acme.one.Event", "com.acme.two.Event"}
    assert definitions["com.acme.one.Event"]["fields"] == [{"name": "orderId", "type": "String"}]
    assert definitions["com.acme.two.Event"]["fields"] == [{"name": "customerId", "type": "String"}]
    assert definitions["com.acme.one.Event"]["producers"] == ["one"]
    assert definitions["com.acme.two.Event"]["producers"] == ["two"]


def test_graph_html_links_an_indexing_issue_to_its_source_file() -> None:
    endpoint = replace(_kafka_endpoint("consume", "Event", "Listener.java"), topic_dynamic=True)

    graph_data = _html_graph_data(render_graph_html({"orders": [endpoint]}, []))
    issue = graph_data["indexing_issues"][0]

    assert issue["location"] == "Listener.java:1"
    assert issue["vscode_uri"].startswith("vscode://file/")
