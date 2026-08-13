from dataclasses import replace
from pathlib import Path

from systemlens.architecture import build_catalog, indexing_issues, request_reply_patterns
from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.modules import DiscoveredModule, ModuleDependency, MongoMethod
from systemlens.relations import build_architecture_relations


def _endpoint(
    role: str,
    system: str,
    topic: str,
    *,
    message_type: str | None = None,
    snippet: str = "",
) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, "OrderIntegration.java", 12),
        role=role,
        system=system,
        topic=topic,
        topic_dynamic=False,
        source="code",
        framework="spring",
        path="src/main/java/OrderIntegration.java",
        start_line=12,
        end_line=12,
        snippet=snippet,
        module="orders",
        qualified_name="com.example.OrderIntegration",
        message_type=message_type,
    )


def test_relations_materialize_kafka_http_mongo_and_module_dependency(tmp_path: Path) -> None:
    module = DiscoveredModule(
        name="orders",
        path=tmp_path / "orders",
        build_system="maven",
        version=None,
        kind="library",
        starts_application=True,
        configuration_example="",
        mongo_methods=(MongoMethod("save", "mongoTemplate", "Store.java", 24, "orders", owner_method="saveOrder"),),
    )
    shared = DiscoveredModule(
        name="shared",
        path=tmp_path / "shared",
        build_system="maven",
        version=None,
        kind="library",
        starts_application=False,
        configuration_example="",
    )

    relations = build_architecture_relations(
        [module, shared],
        [
            _endpoint(
                "produce", "kafka", "orders.created", message_type="OrderCreated",
                snippet='@KafkaListener(topics = "${kafka.topics.orders.name}")',
            ),
            _endpoint("call", "rest", "POST /payments"),
        ],
        [ModuleDependency("orders", "shared")],
    )

    facts = {
        (relation.source_kind, relation.source_name, relation.relation, relation.target_kind, relation.target_name)
        for relation in relations
    }
    assert ("microservice", "orders", "publishes", "topic", "orders.created") in facts
    assert ("topic", "orders.created", "publishes_type", "dto", "OrderCreated") in facts
    assert ("class", "com.example.OrderIntegration", "implements", "topic", "orders.created") in facts
    assert ("microservice", "orders", "calls", "api", "POST /payments") in facts
    assert ("microservice", "orders", "writes", "collection", "orders") in facts
    assert ("method", "orders:saveOrder", "writes", "collection", "orders") in facts
    assert ("class", "com.example.OrderIntegration", "uses_configuration", "property", "kafka.topics.orders.name") in facts
    assert ("microservice", "orders", "depends_on", "module", "shared") in facts


def test_strategy1_links_a_reply_topic_to_its_request_topic(tmp_path: Path) -> None:
    relations = build_architecture_relations(
        [],
        [
            _endpoint("produce", "kafka", "orders.request"),
            _endpoint("consume", "kafka", "retour_orders.request"),
        ],
        [],
        kafka_reply_strategy1=True,
    )

    assert any(
        relation.source_name == "orders.request"
        and relation.relation == "request_reply"
        and relation.target_name == "retour_orders.request"
        for relation in relations
    )


def test_relations_materialize_the_resolved_interservice_topology() -> None:
    call = _endpoint("call", "rest", "GET /payments", snippet="http://payments")
    served = replace(
        _endpoint("serve", "rest", "GET /payments"),
        module="payments",
        qualified_name="com.example.PaymentController",
    )

    relations = build_architecture_relations([], [call, served], [])
    catalog = build_catalog([], [call, served], relations)

    assert any(
        relation.source_name == "orders"
        and relation.relation == "calls_service"
        and relation.target_name == "payments"
        for relation in relations
    )
    assert catalog.relations == tuple(relations)


def test_catalog_topology_uses_persisted_relations_not_a_new_endpoint_match() -> None:
    call = _endpoint("call", "rest", "GET /payments", snippet="http://payments")
    served = replace(
        _endpoint("serve", "rest", "GET /payments"),
        module="payments",
        qualified_name="com.example.PaymentController",
    )

    assert build_catalog([], [call, served], []).edges == ()

    relations = build_architecture_relations([], [call, served], [])
    catalog = build_catalog([], [call, served], relations)
    assert [(edge.kind, edge.from_service, edge.to_service) for edge in catalog.edges] == [
        ("rest", "orders", "payments")
    ]


def test_request_reply_view_requires_a_persisted_strategy_relation() -> None:
    endpoints = [
        _endpoint("produce", "kafka", "orders.request"),
        _endpoint("consume", "kafka", "retour_orders.request"),
    ]

    assert request_reply_patterns(build_catalog([], endpoints, []) )["count"] == 0

    relations = build_architecture_relations([], endpoints, [], kafka_reply_strategy1=True)
    assert request_reply_patterns(build_catalog([], endpoints, relations))["count"] == 1


def test_indexing_issues_exposes_source_evidence_for_heuristic_review() -> None:
    dynamic_topic = replace(
        _endpoint("produce", "kafka", "kafkaProperties.getTopics().getOrders()", snippet="send(topic, payload)"),
        topic_dynamic=True,
    )
    unmatched_call = _endpoint("call", "rest", "GET /payments", snippet="client.get()")

    result = indexing_issues(build_catalog([], [dynamic_topic, unmatched_call]))

    assert result["count"] == 3
    assert result["by_code"] == {
        "dynamic_kafka_topic": 1,
        "unknown_kafka_message_type": 1,
        "unmatched_http_call": 1,
    }
    dynamic = next(issue for issue in result["issues"] if issue["code"] == "dynamic_kafka_topic")
    assert dynamic["service"] == "orders"
    assert dynamic["framework"] == "spring"
    assert dynamic["source"] == {
        "path": "src/main/java/OrderIntegration.java",
        "start_line": 12,
        "end_line": 12,
        "snippet": "send(topic, payload)",
    }


def test_indexing_issues_distinguishes_an_ambiguous_explicit_http_target() -> None:
    call = _endpoint("call", "rest", "GET /orders", snippet="http://orders")
    alternate = replace(call, id="alternate", role="serve", module="ORDERS")

    result = indexing_issues(build_catalog([], [call, alternate]))

    assert result["by_code"] == {"ambiguous_http_target": 1}
    assert result["issues"][0]["severity"] == "warning"
