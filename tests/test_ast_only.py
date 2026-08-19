from __future__ import annotations

import shutil
import json
from dataclasses import replace
from pathlib import Path

import pytest
from typer.testing import CliRunner

import systemlens.indexer as indexer_module
from systemlens.cli import app
from systemlens.config import Config
from systemlens.flow import group_endpoints_by_module_for_flow, trace_flow
from systemlens.graph import build_graph
from systemlens.indexer import index_repo
from systemlens.architecture_inventory import load_architecture_inventory
from systemlens.mcp_server import reindex_architecture
from systemlens.scanner import (
    infer_framework_endpoints,
    infer_kafka_endpoints,
    infer_kafka_topic_strategy1_endpoints,
)
from systemlens.store import Store


FIXTURES = Path(__file__).parent / "fixtures"
RUNNER = CliRunner()


def test_init_writes_ast_only_configuration(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = RUNNER.invoke(app, ["init"])

    assert result.exit_code == 0
    content = (tmp_path / ".systemlens" / "config.yml").read_text()
    assert "include:" in content
    assert "rules:" not in content
    assert "embedding_model:" not in content


def test_ast_extractors_find_rest_and_kafka_facts() -> None:
    rest = infer_framework_endpoints(FIXTURES / "rest_repo")
    kafka = infer_kafka_endpoints(FIXTURES / "kafka_repo")

    assert any(endpoint.role == "serve" and endpoint.system == "rest" for endpoint in rest)
    assert any(endpoint.role == "call" and endpoint.framework == "feign" for endpoint in rest)
    assert any(endpoint.role == "produce" and endpoint.system == "kafka" for endpoint in kafka)
    assert any(endpoint.role == "consume" and endpoint.message_type for endpoint in kafka)


def test_strategy1_recognizes_envoyer_message_kafka_method_family_as_producers(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "com" / "example" / "Publisher.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        """package com.example;
class Publisher {
  void publish() {
    OrderCreated event = new OrderCreated("42");
    kafkaService.envoyerMessageKafka(kafkaProperties.getTopics().getOrdersCreated(), event);
    RequestCreated request = new RequestCreated("43");
    kafkaService.envoyerMessageKafkaRequest(kafkaProperties.getTopics().getRequestsCreated(), request);
    ReplyCreated reply = new ReplyCreated("44");
    kafkaService.envoyerMessageKafkaReply(kafkaProperties.getTopics().getRepliesCreated(), reply);
  }
}
record OrderCreated(String orderId) {}
record RequestCreated(String requestId) {}
record ReplyCreated(String replyId) {}
""",
        encoding="utf-8",
    )

    endpoints = infer_kafka_topic_strategy1_endpoints(
        tmp_path, ["src/main/java/com/example/Publisher.java"]
    )

    assert sorted(
        (endpoint.role, endpoint.topic, endpoint.message_type, endpoint.framework)
        for endpoint in endpoints
    ) == [
        ("produce", "ORDERS_CREATED", "OrderCreated", "kafka-topic-strategy1"),
        ("produce", "REPLIES_CREATED", "ReplyCreated", "kafka-topic-strategy1"),
        ("produce", "REQUESTS_CREATED", "RequestCreated", "kafka-topic-strategy1"),
    ]


def test_index_is_incremental_without_embeddings(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "endpoint_index_repo", repo)

    with Store(repo) as store:
        first = index_repo(repo, Config(), store)
        second = index_repo(repo, Config(), store)
        endpoints = store.all_endpoints()

    assert first.scanned == 2
    assert first.endpoints_added == 2
    assert len(endpoints) == 2
    assert second.scanned == 0


def test_incremental_property_change_reindexes_dependent_java_endpoints(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "kafka_repo", repo)

    with Store(repo) as store:
        index_repo(repo, Config(), store)
        config = repo / "src" / "main" / "resources" / "application.yml"
        config.write_text(config.read_text().replace("payments.received", "payments.changed"))
        report = index_repo(repo, Config(), store)
        topics = {endpoint.topic for endpoint in store.all_endpoints()}

    assert report.scanned > 1
    assert "payments.changed" in topics
    assert "payments.received" not in topics


def test_incremental_property_deletion_reindexes_dependent_java_endpoints(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "kafka_repo", repo)

    with Store(repo) as store:
        index_repo(repo, Config(), store)
        (repo / "src" / "main" / "resources" / "application.yml").unlink()
        report = index_repo(repo, Config(), store)
        endpoints = store.all_endpoints()

    assert report.scanned > 1
    assert any(
        endpoint.topic == "app.kafka.topics.payments" and endpoint.topic_dynamic
        for endpoint in endpoints
    )


def test_incremental_build_identity_change_reattributes_unchanged_endpoints(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    shutil.copytree(FIXTURES / "kafka_workspace", repo)

    with Store(repo) as store:
        index_repo(repo, Config(), store)
        pom = repo / "order-service" / "pom.xml"
        pom.write_text(pom.read_text().replace("order-service", "orders-renamed", 1))
        report = index_repo(repo, Config(), store)
        endpoint_modules = {endpoint.module for endpoint in store.all_endpoints()}
        relation_modules = {relation.module for relation in store.all_architecture_relations()}

    assert report.scanned > 1
    assert "orders-renamed" in endpoint_modules
    assert "order-service" not in endpoint_modules
    assert "order-service" not in relation_modules


@pytest.mark.parametrize("topic_strategy", ["default", "strategy1"])
def test_mcp_reindex_preserves_the_persisted_topic_strategy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, topic_strategy: str
) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "kafka_repo", repo)
    monkeypatch.chdir(repo)
    assert RUNNER.invoke(app, ["init"]).exit_code == 0

    with Store(repo) as store:
        index_repo(repo, Config(), store, topic_strategy=topic_strategy)

    reindex_architecture()

    with Store(repo, readonly=True) as store:
        assert store.get_meta("topic_strategy") == topic_strategy
    inventory = load_architecture_inventory(repo)
    assert inventory.profile.topic_strategy == topic_strategy


def test_index_persists_kafka_dto_source_definitions(tmp_path: Path) -> None:
    (tmp_path / "pom.xml").write_text(
        "<project><modelVersion>4.0.0</modelVersion><artifactId>orders</artifactId></project>",
        encoding="utf-8",
    )
    source_root = tmp_path / "src" / "main" / "java" / "com" / "example"
    source_root.mkdir(parents=True)
    (source_root / "OrderCreated.java").write_text(
        "package com.example; public record OrderCreated(String id) {}",
        encoding="utf-8",
    )
    (source_root / "Publisher.java").write_text(
        "package com.example; class Publisher { KafkaTemplate<String, OrderCreated> kafkaTemplate; "
        "void publish(OrderCreated event) { kafkaTemplate.send(\"orders.created\", event); } }",
        encoding="utf-8",
    )
    contract = tmp_path / "src" / "main" / "resources" / "openapi.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("openapi: 3.0.0\npaths: {}\n", encoding="utf-8")

    with Store(tmp_path) as store:
        index_repo(tmp_path, Config(), store, full=True)
        definitions = store.all_kafka_dto_definitions()
        contracts = store.all_openapi_contracts()

    assert definitions[0]["name"] == "OrderCreated"
    assert definitions[0]["source"] == "src/main/java/com/example/OrderCreated.java"
    assert definitions[0]["fields"] == [{"type": "String", "name": "id"}]
    assert contracts == [{
        "module": "orders", "path": "src/main/resources/openapi.yaml",
        "spec": {"openapi": "3.0.0", "paths": {}},
    }]


def test_index_stores_nested_module_openapi_contract_once(tmp_path: Path) -> None:
    (tmp_path / "pom.xml").write_text(
        "<project><modelVersion>4.0.0</modelVersion><artifactId>workspace</artifactId>"
        "<packaging>pom</packaging></project>",
        encoding="utf-8",
    )
    module = tmp_path / "orders"
    (module / "pom.xml").parent.mkdir(parents=True)
    (module / "pom.xml").write_text(
        "<project><modelVersion>4.0.0</modelVersion><artifactId>orders</artifactId></project>",
        encoding="utf-8",
    )
    contract = module / "src" / "main" / "resources" / "swagger.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("swagger: '2.0'\npaths: {}\n", encoding="utf-8")

    with Store(tmp_path) as store:
        index_repo(tmp_path, Config(), store, full=True)
        contracts = store.all_openapi_contracts()

    assert contracts == [{
        "module": "orders", "path": "src/main/resources/swagger.yaml",
        "spec": {"swagger": "2.0", "paths": {}},
    }]


def test_index_rollback_keeps_the_previous_complete_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "endpoint_index_repo", repo)

    with Store(repo) as store:
        index_repo(repo, Config(), store)
        before = (
            store.get_file_hashes(),
            store.all_endpoints(),
            store.all_modules(),
            store.all_module_dependencies(),
            store.all_architecture_relations(),
            store.get_meta("endpoint_inventory_signature"),
        )
        source = repo / "app" / "OrderConsumer.java"
        source.write_text(source.read_text() + "\n// changed after the snapshot\n")

        def fail_relation_projection(*_args, **_kwargs):
            raise RuntimeError("forced relation projection failure")

        monkeypatch.setattr(indexer_module, "build_architecture_relations", fail_relation_projection)
        with pytest.raises(RuntimeError, match="forced relation projection failure"):
            index_repo(repo, Config(), store)

        after = (
            store.get_file_hashes(),
            store.all_endpoints(),
            store.all_modules(),
            store.all_module_dependencies(),
            store.all_architecture_relations(),
            store.get_meta("endpoint_inventory_signature"),
        )

    assert after == before


def test_read_only_store_sees_previous_snapshot_until_transaction_commits(tmp_path: Path) -> None:
    with Store(tmp_path) as writer:
        writer.set_meta("snapshot", "before")

    with Store(tmp_path) as writer:
        with writer.transaction():
            writer.set_meta("snapshot", "after")
            with Store(tmp_path, readonly=True) as reader:
                assert reader.get_meta("snapshot") == "before"

    with Store(tmp_path, readonly=True) as reader:
        assert reader.get_meta("snapshot") == "after"


def test_index_persists_a_safe_java_parse_diagnostic(tmp_path: Path) -> None:
    source = tmp_path / "Broken.java"
    source.write_text("class Broken { void run( {")

    with Store(tmp_path) as store:
        index_repo(tmp_path, Config(), store)
        diagnostics = store.all_extraction_diagnostics()

    assert [(item.path, item.extractor, item.category, item.severity) for item in diagnostics] == [
        ("Broken.java", "tree-sitter-java", "parse_failed", "warning")
    ]
    assert "class Broken" not in diagnostics[0].detail


def test_cli_index_does_not_require_an_embedding_model(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "endpoint_index_repo", repo)
    monkeypatch.chdir(repo)
    assert RUNNER.invoke(app, ["init"]).exit_code == 0
    result = RUNNER.invoke(app, ["index"])

    assert result.exit_code == 0
    assert "+integrations=2" in result.output
    assert "systemlens export microservices --html architecture.html" in result.output


def test_cli_indexing_issues_emits_ai_ready_json(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "endpoint_index_repo", repo)
    monkeypatch.chdir(repo)
    assert RUNNER.invoke(app, ["init"]).exit_code == 0
    assert RUNNER.invoke(app, ["index"]).exit_code == 0

    result = RUNNER.invoke(app, ["analyze", "indexing-issues", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["kind"] == "indexing_issues"
    assert isinstance(payload["issues"], list)


def test_infer_framework_endpoints_reads_json_openapi_contract(tmp_path: Path) -> None:
    contract = tmp_path / "src" / "main" / "resources" / "openapi.json"
    contract.parent.mkdir(parents=True)
    contract.write_text(
        json.dumps(
            {
                "openapi": "3.0.0",
                "paths": {
                    "/orders/{id}": {
                        "get": {"summary": "Get an order"},
                        "delete": {"summary": "Remove an order"},
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    endpoints = infer_framework_endpoints(
        tmp_path, [str(contract.relative_to(tmp_path).as_posix())]
    )

    routes = {(endpoint.topic) for endpoint in endpoints}
    assert "GET /orders/{id}" in routes
    assert "DELETE /orders/{id}" in routes
    assert all(endpoint.system == "rest" and endpoint.framework == "openapi" for endpoint in endpoints)


def test_infer_framework_endpoints_attributes_a_shared_module_contract_to_the_implementing_service(
    tmp_path: Path,
) -> None:
    """A `model-*` module commonly hosts the OpenAPI contract configured by a
    sibling implementing service's `openapi-generator-maven-plugin`. Both the
    ``pom.xml`` scan and a direct scan of the physical contract file must
    attribute the resulting endpoints to the implementing service, never to
    the contract's own (often non-runtime, library) enclosing module.
    """
    implementing = tmp_path / "orders-service"
    (implementing / "src" / "main" / "java" / "com" / "acme").mkdir(parents=True)
    (implementing / "src" / "main" / "java" / "com" / "acme" / "OrdersController.java").write_text(
        "package com.acme;\n"
        "import org.springframework.web.bind.annotation.RestController;\n"
        "@RestController\npublic class OrdersController { }\n"
    )
    (implementing / "pom.xml").write_text(
        """<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <artifactId>orders-service</artifactId>
    <version>1.0.0</version>
    <build><plugins><plugin>
        <groupId>org.openapitools</groupId>
        <artifactId>openapi-generator-maven-plugin</artifactId>
        <version>7.0.0</version>
        <executions><execution><goals><goal>generate</goal></goals>
            <configuration>
                <inputSpec>${project.basedir}/../model-common/src/main/resources/orders.yaml</inputSpec>
            </configuration>
        </execution></executions>
    </plugin></plugins></build>
</project>"""
    )
    shared = tmp_path / "model-common" / "src" / "main" / "resources"
    shared.mkdir(parents=True)
    (shared / "orders.yaml").write_text(
        "openapi: 3.0.0\npaths:\n  /orders:\n    get:\n      summary: List orders\n"
    )
    (tmp_path / "model-common" / "pom.xml").write_text(
        '<project xmlns="http://maven.apache.org/POM/4.0.0">'
        "<modelVersion>4.0.0</modelVersion><artifactId>model-common</artifactId>"
        "<version>1.0.0</version></project>"
    )

    endpoints = infer_framework_endpoints(tmp_path)

    contract_endpoints = [endpoint for endpoint in endpoints if endpoint.framework == "openapi"]
    assert contract_endpoints, "the shared contract must be discovered"
    for endpoint in contract_endpoints:
        assert endpoint.module == "orders-service"
        assert endpoint.path == "model-common/src/main/resources/orders.yaml"
    assert {endpoint.topic for endpoint in contract_endpoints} == {"GET /orders"}


def test_strategy1_rest_declaration_finds_same_named_contract_in_another_module(
    tmp_path: Path,
) -> None:
    """Strategy1 must search the indexed repository, not just the publisher's
    module or an openapi-generator-configured ``model-*`` module."""
    publisher = tmp_path / "orders-service"
    declaration = publisher / "src" / "main" / "resources" / "openapi" / "orders.rest"
    declaration.parent.mkdir(parents=True)
    declaration.write_text("published API declaration\n")
    (publisher / "pom.xml").write_text(
        '<project xmlns="http://maven.apache.org/POM/4.0.0">'
        "<modelVersion>4.0.0</modelVersion><artifactId>orders-service</artifactId>"
        "<version>1.0.0</version></project>"
    )
    contract = tmp_path / "shared-contracts" / "api" / "orders.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text(
        "openapi: 3.0.0\npaths:\n  /orders:\n    get:\n      summary: List orders\n"
    )

    endpoints = infer_framework_endpoints(
        tmp_path, configured_api_client_strategy1=True
    )

    contract_endpoints = [endpoint for endpoint in endpoints if endpoint.framework == "openapi"]
    assert [(endpoint.module, endpoint.path, endpoint.topic) for endpoint in contract_endpoints] == [
        ("orders-service", "orders-service/src/main/resources/openapi/orders.rest", "GET /orders")
    ]
    assert "systemlens-openapi-contract:shared-contracts/api/orders.yaml" in contract_endpoints[0].snippet


def test_indexed_kafka_facts_build_a_traceable_service_edge(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURES / "kafka_repo", repo)

    with Store(repo) as store:
        index_repo(repo, Config(), store)
        endpoints = store.all_endpoints()

    by_service = group_endpoints_by_module_for_flow(endpoints)
    topic = next(endpoint.topic for endpoint in endpoints if endpoint.system == "kafka")
    flow = trace_flow(topic, by_service)

    assert flow.resolved_topic == topic
    assert flow.sites


def test_spring_data_rest_exposes_annotated_repository_crud_and_search_resources() -> None:
    endpoints = infer_framework_endpoints(FIXTURES / "spring_data_rest_repo")
    order_topics = {
        endpoint.topic
        for endpoint in endpoints
        if endpoint.framework == "spring-data-rest" and "OrderRepository" in endpoint.qualified_name
    }

    assert {"GET /order", "POST /order", "GET /order/{id}", "PUT /order/{id}",
            "PATCH /order/{id}", "DELETE /order/{id}"} <= order_topics
    assert "GET /order/search/lastUpdate" in order_topics
    assert "GET /order/search/internalOnly" not in order_topics


def test_spring_data_rest_exposes_default_pluralized_path_and_search_resource() -> None:
    endpoints = infer_framework_endpoints(FIXTURES / "spring_data_rest_repo")
    user_topics = {
        endpoint.topic
        for endpoint in endpoints
        if endpoint.framework == "spring-data-rest" and "UserRepository" in endpoint.qualified_name
    }

    assert {"GET /users", "POST /users", "GET /users/{id}", "PUT /users/{id}",
            "PATCH /users/{id}", "DELETE /users/{id}"} <= user_topics
    assert "GET /users/search/findByUsernameCaseInsensitive" in user_topics


def test_rest_graph_uses_ast_endpoint_facts() -> None:
    endpoints = infer_framework_endpoints(FIXTURES / "rest_repo")
    served = next(endpoint for endpoint in endpoints if endpoint.role == "serve")
    called = replace(
        next(endpoint for endpoint in endpoints if endpoint.role == "call"),
        topic=served.topic,
        snippet="http://server",
    )
    graph = build_graph({"server": [served], "caller": [called]})

    assert graph
