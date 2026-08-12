from pathlib import Path

from codeatlas.graph import (
    build_graph,
    find_outbound_calls_in_consumers,
    graph_edge_rest_resource,
    group_endpoints_by_module,
    paths_match,
    qualified_rest_resource,
)
from dataclasses import replace

from codeatlas.models import MessageEndpoint, compute_endpoint_id
from codeatlas.store import Store


def make_endpoint(
    role: str,
    topic: str,
    path: str,
    start_line: int = 1,
    end_line: int = 1,
    system: str = "rest",
    framework: str | None = None,
    module: str | None = None,
    snippet: str = "",
) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, path, start_line, end_line),
        role=role,
        system=system,
        topic=topic,
        topic_dynamic=False,
        source="code",
        framework=framework,
        path=path,
        start_line=start_line,
        end_line=end_line,
        snippet=snippet,
        module=module,
    )


def _three_service_fixture() -> dict[str, list[MessageEndpoint]]:
    service_a = [
        make_endpoint("serve", "GET /a-status", "a/Controller.java", 10, 10),
        make_endpoint(
            "call", "GET /b-status", "a/Client.java", 5, 5, snippet="http://service-b"
        ),
    ]
    service_b = [
        make_endpoint("serve", "GET /b-status", "b/Controller.java", 10, 10),
        make_endpoint(
            "call", "GET /c-status", "b/Client.java", 5, 5, snippet="http://service-c"
        ),
    ]
    service_c = [
        make_endpoint("serve", "GET /c-status", "c/Controller.java", 10, 10),
        make_endpoint(
            "call", "GET /a-status", "c/Client.java", 5, 5, snippet="http://service-a"
        ),
    ]
    return {"service-a": service_a, "service-b": service_b, "service-c": service_c}


def test_paths_match_literal_call_against_template_serve() -> None:
    assert paths_match("GET /orders/123", "GET /orders/{id}")


def test_qualified_rest_resource_keeps_provider_identity_separate_from_route_matching() -> None:
    assert qualified_rest_resource("domain-annuaire", "GET /annuaire") == (
        "domain-annuaire: GET /annuaire"
    )


def test_paths_match_requires_same_method() -> None:
    assert not paths_match("POST /orders/123", "GET /orders/{id}")


def test_paths_match_rejects_different_segment_count() -> None:
    assert not paths_match("GET /orders/123/status", "GET /orders/{id}")


def test_paths_match_rejects_shorter_literal_call_without_wildcard() -> None:
    assert not paths_match("GET /orders", "GET /orders/{id}")


def test_paths_match_rejects_fully_dynamic_call() -> None:
    assert not paths_match("GET <dynamic>", "GET /orders/{id}")


def test_paths_match_rejects_unrelated_paths() -> None:
    assert not paths_match("GET /payments/{id}", "GET /orders/{id}")


def test_paths_match_ignores_query_string_for_path_comparison() -> None:
    assert paths_match("GET /orders", "GET /orders?consumerId")
    assert not paths_match("GET /orders/{id}", "GET /orders?consumerId")


def test_paths_match_allows_gateway_wildcard_to_match_deeper_serve_routes() -> None:
    assert paths_match("POST /orders/**", "POST /orders/{id}/cancel")
    assert not paths_match("POST /orders/**", "POST /orders")


def test_graph_links_a_gateway_yaml_proxy_once_to_its_load_balanced_service() -> None:
    gateway_call = make_endpoint(
        "call",
        "ANY /**",
        "gateway/application.yml",
        module="api-gateway",
        framework="spring-cloud-gateway",
        snippet="Path=/api/vet/**; uri=lb://vets-service",
    )
    vets_get = make_endpoint(
        "serve", "GET /vets", "vets/VetResource.java", module="vets-service"
    )
    vets_post = make_endpoint(
        "serve", "POST /vets", "vets/VetResource.java", module="vets-service"
    )

    edges = build_graph({"api-gateway": [gateway_call], "vets-service": [vets_get, vets_post]})

    assert len(edges) == 1
    assert edges[0].from_service == "api-gateway"
    assert edges[0].to_service == "vets-service"


def test_build_graph_creates_rest_edges_between_distinct_services_only() -> None:
    edges = build_graph(_three_service_fixture())

    assert len(edges) == 3
    assert {(e.from_service, e.to_service) for e in edges} == {
        ("service-a", "service-b"),
        ("service-b", "service-c"),
        ("service-c", "service-a"),
    }
    assert all(e.kind == "rest" for e in edges)


def test_build_graph_skips_same_service_calls() -> None:
    endpoints_by_service = {
        "service-a": [
            make_endpoint("serve", "GET /a-status", "a/Controller.java", 10, 10),
            make_endpoint("call", "GET /a-status", "a/Client.java", 5, 5),
        ]
    }

    assert build_graph(endpoints_by_service) == []


def test_build_graph_links_compatible_rest_routes_without_a_service_target() -> None:
    edges = build_graph(
        {
            "caller-service": [make_endpoint("call", "GET /health", "Caller.java")],
            "unrelated-service": [make_endpoint("serve", "GET /health", "Health.java")],
        }
    )

    assert len(edges) == 1
    assert (edges[0].from_service, edges[0].to_service) == ("caller-service", "unrelated-service")


def test_build_graph_creates_kafka_edges_on_matching_topic_only() -> None:
    endpoints_by_service = {
        "producer-svc": [
            make_endpoint("produce", "orders.created", "app/producer.py", 1, 1, system="kafka")
        ],
        "consumer-svc": [
            make_endpoint("consume", "orders.created", "app/consumer.py", 1, 1, system="kafka"),
            make_endpoint(
                "consume", "orders.cancelled", "app/other_consumer.py", 1, 1, system="kafka"
            ),
        ],
    }

    edges = build_graph(endpoints_by_service)

    assert len(edges) == 1
    assert edges[0].kind == "kafka"
    assert edges[0].to_endpoint.path == "app/consumer.py"


def test_build_graph_uses_manifest_kafka_endpoints_as_service_authority() -> None:
    manifest_produce = replace(
        make_endpoint("produce", "documented.topic", "topics.md", system="kafka"),
        source="manifest",
    )
    manifest_consume = replace(
        make_endpoint("consume", "documented.topic", "topics.md", 2, 2, system="kafka"),
        source="manifest",
    )
    endpoints_by_service = {
        "producer-svc": [
            make_endpoint("produce", "detected.but.undocumented", "producer/Code.java", system="kafka"),
            manifest_produce,
        ],
        "consumer-svc": [
            make_endpoint("consume", "detected.but.undocumented", "consumer/Code.java", system="kafka"),
            manifest_consume,
        ],
    }

    edges = build_graph(endpoints_by_service)

    assert [(edge.from_endpoint.topic, edge.to_endpoint.topic) for edge in edges] == [
        ("documented.topic", "documented.topic")
    ]


def test_build_graph_deduplicates_duplicate_edges() -> None:
    call = make_endpoint(
        "call", "GET /b-status", "a/Client.java", 5, 5, snippet="http://service-b"
    )
    serve = make_endpoint("serve", "GET /b-status", "b/Controller.java", 10, 10)

    edges = build_graph({"service-a": [call, call], "service-b": [serve, serve]})

    assert len(edges) == 1


def test_build_graph_uses_service_url_getter_hint_only_with_strategy1() -> None:
    call = make_endpoint(
        "call",
        "GET /orders/{orderId}",
        "gateway/Proxy.java",
        module="api-gateway",
        snippet='webClient.get().uri(orderDestinations.getOrderServiceUrl() + "/orders/{orderId}", orderId)',
    )
    order_service = make_endpoint(
        "serve",
        "GET /orders/{orderId}",
        "order/Controller.java",
        module="ftgo-order-service",
    )
    order_history_service = make_endpoint(
        "serve",
        "GET /orders/{orderId}",
        "history/Controller.java",
        module="ftgo-order-history-service",
    )

    endpoints_by_service = {
        "ftgo-api-gateway": [call],
        "ftgo-order-service": [order_service],
        "ftgo-order-history-service": [order_history_service],
    }

    assert {edge.to_service for edge in build_graph(endpoints_by_service)} == {
        "ftgo-order-service", "ftgo-order-history-service"
    }
    strategy1_edges = build_graph(endpoints_by_service, strategy1=True)
    assert len(strategy1_edges) == 1
    assert strategy1_edges[0].to_service == "ftgo-order-service"


def test_build_graph_uses_domain_from_rest_configuration_to_disambiguate_targets() -> None:
    call = replace(
        make_endpoint(
            "call",
            "GET <dynamic>",
            "gateway/DirectoryClient.java",
            snippet="directoryClient.getForObject(\"/directory/{id}\", Object.class)\ncodeatlas-api-domain:domain-annuaire",
        ),
        topic_dynamic=True,
    )
    directory = make_endpoint("serve", "GET /directory/{id}", "directory/Controller.java")
    directory_search = make_endpoint(
        "serve", "GET /directory/search", "directory/Controller.java", 2, 2
    )
    directory_history = make_endpoint(
        "serve", "GET /directory/{id}", "history/Controller.java"
    )

    edges = build_graph(
        {
            "caller-service": [call],
            "domain-annuaire": [directory, directory_search],
            "directory-history-service": [directory_history],
        }
    )

    assert len(edges) == 2
    assert {edge.to_service for edge in edges} == {"domain-annuaire"}
    assert {edge.from_endpoint.topic for edge in edges} == {
        "GET /directory/{id}",
        "GET /directory/search",
    }


def test_build_graph_keeps_configured_client_dependency_without_target_resource() -> None:
    call = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/RestClientConfig.java",
        snippet="codeatlas-api-domain:domain-annuaire",
    )

    edges = build_graph({"caller-service": [call], "domain-annuaire": []})

    assert len(edges) == 1
    assert (edges[0].from_service, edges[0].to_service) == (
        "caller-service",
        "domain-annuaire",
    )
    assert edges[0].to_endpoint is None
    assert graph_edge_rest_resource(edges[0]) == "domain-annuaire: API"
    assert all(not edge.from_endpoint.topic_dynamic for edge in edges)


def test_build_graph_never_links_two_dynamic_kafka_topic_expressions() -> None:
    producer = replace(
        make_endpoint("produce", "<dynamic>", "orders/Publisher.java", system="kafka"),
        topic_dynamic=True,
    )
    consumer = replace(
        make_endpoint("consume", "<dynamic>", "payments/Listener.java", system="kafka"),
        topic_dynamic=True,
    )

    assert build_graph({"orders": [producer], "payments": [consumer]}) == []


def test_find_outbound_calls_in_consumers_flags_call_inside_handler_range() -> None:
    endpoints = [
        make_endpoint("consume", "orders.created", "app/OrderConsumer.java", 15, 25, system="kafka"),
        make_endpoint("call", "POST /payments", "app/OrderConsumer.java", 20, 20),
    ]

    results = find_outbound_calls_in_consumers(endpoints)

    assert len(results) == 1
    assert results[0].call.start_line == 20


def test_find_outbound_calls_in_consumers_ignores_calls_outside_handler_or_other_files() -> None:
    endpoints = [
        make_endpoint("consume", "orders.created", "app/OrderConsumer.java", 15, 25, system="kafka"),
        make_endpoint("call", "POST /payments", "app/OrderConsumer.java", 40, 40),
        make_endpoint("call", "POST /payments", "app/OtherFile.java", 20, 20),
    ]

    assert find_outbound_calls_in_consumers(endpoints) == []


def test_no_graph_table_in_sqlite_schema(tmp_path: Path) -> None:
    with Store(tmp_path) as store:
        tables = {
            row["name"]
            for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    assert not any("graph" in name or "cycle" in name for name in tables)


def test_group_endpoints_by_module_groups_by_maven_module() -> None:
    order_endpoint = make_endpoint(
        "produce", "orders.created", "order-service/Producer.java", module="order-service"
    )
    payment_endpoint = make_endpoint(
        "consume", "orders.created", "payment-service/Consumer.java", module="payment-service"
    )

    grouped = group_endpoints_by_module([order_endpoint, payment_endpoint])

    assert grouped == {
        "order-service": [order_endpoint],
        "payment-service": [payment_endpoint],
    }


def test_group_endpoints_by_module_ignores_endpoints_without_a_module() -> None:
    unattributed = make_endpoint("serve", "GET /health", "app/Health.java", module=None)

    assert group_endpoints_by_module([unattributed]) == {}


def test_build_graph_works_from_module_grouped_endpoints() -> None:
    endpoints = [
        make_endpoint(
            "produce",
            "orders.created",
            "order-service/Producer.java",
            system="kafka",
            module="order-service",
        ),
        make_endpoint(
            "consume",
            "orders.created",
            "payment-service/Consumer.java",
            system="kafka",
            module="payment-service",
        ),
    ]

    grouped = group_endpoints_by_module(endpoints)
    edges = build_graph(grouped)

    assert len(edges) == 1
    assert edges[0].from_service == "order-service"
    assert edges[0].to_service == "payment-service"
    assert edges[0].kind == "kafka"
