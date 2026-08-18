import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from systemlens import cli
from systemlens.cli import app
from systemlens.config import Config
from systemlens.indexer import index_repo
from systemlens.models import MessageEndpoint
from systemlens.modules import (
    DiscoveredModule,
    ModuleDependency,
    _deduplicate_openapi_contract_owners,
    discover_module_dependencies,
    discover_modules,
    module_identity,
)
from systemlens.store import Store
from systemlens.workspace import discover_workspace_services, load_federation

runner = CliRunner()


def test_microservice_html_export_uses_the_explicit_root_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}
    graph_data = SimpleNamespace(
        services_by_name={}, edges=[], collections_by_service={}, modules_by_service={},
        warnings=[], build_modules=[], module_dependencies=[], source_roots=[],
        strategy1=False, diagnostics=[], result={"note": None},
        kafka_dto_definitions=None,
        openapi_contracts=None,
    )
    monkeypatch.setattr(cli, "_load_microservice_graph", lambda *_args, **_kwargs: graph_data)
    monkeypatch.setattr(
        cli, "render_graph_html",
        lambda *args, **_kwargs: captured.update({"args": args}) or "<html></html>",
    )

    cli.export_microservices_cmd(
        workspace=None, html=tmp_path / "architecture.html", c4=None,
        root_path=Path("/exported/repository"), json_output=False,
    )

    assert captured["args"][9] == Path("/exported/repository")


def test_microservice_html_export_defaults_root_path_to_the_current_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}
    graph_data = SimpleNamespace(
        services_by_name={}, edges=[], collections_by_service={}, modules_by_service={},
        warnings=[], build_modules=[], module_dependencies=[], source_roots=[],
        strategy1=False, diagnostics=[], result={"note": None},
        kafka_dto_definitions=None,
        openapi_contracts=None,
    )
    monkeypatch.setattr(cli, "_load_microservice_graph", lambda *_args, **_kwargs: graph_data)
    monkeypatch.setattr(
        cli, "render_graph_html",
        lambda *args, **_kwargs: captured.update({"args": args}) or "<html></html>",
    )

    cli.export_microservices_cmd(
        workspace=None, html=tmp_path / "architecture.html", c4=None,
        root_path=None, json_output=False,
    )

    assert captured["args"][9] == Path.cwd()


def _write_pom(
    path: Path,
    artifact: str,
    version: str | None,
    packaging: str = "jar",
    dependencies: tuple[str, ...] = (),
) -> None:
    version_xml = f"<version>{version}</version>" if version is not None else ""
    dependencies_xml = "".join(
        f"<dependency><groupId>example</groupId><artifactId>{dependency}</artifactId></dependency>"
        for dependency in dependencies
    )
    path.write_text(
        "<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
        "<modelVersion>4.0.0</modelVersion>"
        f"<artifactId>{artifact}</artifactId>{version_xml}"
        f"<packaging>{packaging}</packaging><dependencies>{dependencies_xml}</dependencies></project>"
    )


def test_discover_modules_includes_maven_aggregators_libraries_and_gradle_projects(
    tmp_path: Path,
) -> None:
    _write_pom(tmp_path / "pom.xml", "platform", "1.2.3", packaging="pom")
    library = tmp_path / "shared"
    library.mkdir()
    _write_pom(library / "pom.xml", "shared-kernel", "1.2.3")
    gradle = tmp_path / "adapter"
    gradle.mkdir()
    (gradle / "build.gradle").write_text("archivesBaseName = 'adapter-api'\nversion = '2.0.0'\n")

    modules = discover_modules(tmp_path)

    assert [(module.name, module.build_system, module.version, module.kind) for module in modules] == [
        ("platform", "maven", "1.2.3", "aggregator"),
        ("adapter-api", "gradle", "2.0.0", "library"),
        ("shared-kernel", "maven", "1.2.3", "library"),
    ]


def test_microservices_list_uses_the_architecture_catalog_shape(tmp_path: Path) -> None:
    _write_pom(tmp_path / "pom.xml", "orders", "1.0.0")
    application = tmp_path / "src" / "main" / "java" / "OrdersApplication.java"
    application.parent.mkdir(parents=True)
    application.write_text(
        "class OrdersApplication {\n"
        "  public static void main(String[] args) {\n"
        "    SpringApplication.run(OrdersApplication.class, args);\n"
        "  }\n"
        "}\n"
    )

    result = runner.invoke(app, ["microservices", "--root", str(tmp_path), "--json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == [
        {
            "kind": "microservice",
            "name": "orders",
            "language": "Java",
            "build_tool": "maven",
            "version": "1.0.0",
            "exposes_http_api": False,
            "http_apis_exposed": [],
            "http_apis_consumed": [],
            "kafka_topics_published": [],
            "kafka_topics_consumed": [],
            "kafka_message_types_published": {},
            "kafka_message_types_consumed": {},
            "databases": {"mongodb_collections": []},
            "technologies": ["Java", "Spring Boot"],
            "openapi": False,
            "openapi_files": [],
            "scheduled_tasks": [],
            "scheduled_tasks_detection": "not_available",
                "dependencies": {"outgoing_modules": [], "incoming_modules": []},
                "external_apis": [],
                "kubernetes_workloads": [],
        }
    ]


def test_discover_modules_excludes_maven_and_gradle_modules_in_test_directories(tmp_path: Path) -> None:
    maven_test = tmp_path / "orders-tests"
    gradle_test = tmp_path / "contract-test-kit"
    production = tmp_path / "orders"
    for module in (maven_test, gradle_test, production):
        module.mkdir()
    _write_pom(maven_test / "pom.xml", "orders-api", "1.0.0")
    _write_pom(production / "pom.xml", "orders-api", "1.0.0")
    (gradle_test / "build.gradle").write_text("archivesBaseName = 'contract-api'\n")

    assert [module.name for module in discover_modules(tmp_path)] == [
        "contract-api", "orders-api", "orders-api"
    ]


def test_duplicate_artifact_names_are_persisted_with_distinct_identities(tmp_path: Path) -> None:
    north = tmp_path / "north"
    south = tmp_path / "south"
    north.mkdir()
    south.mkdir()
    _write_pom(north / "pom.xml", "orders", "1.0.0")
    _write_pom(south / "pom.xml", "orders", "1.0.0")

    modules = discover_modules(tmp_path)
    identities = {module_identity(module) for module in modules}
    assert identities == {"orders@north", "orders@south"}

    with Store(tmp_path) as store:
        store.replace_modules(modules)
        stored = store.all_modules()

    assert {module_identity(module) for module in stored} == identities


def test_workspace_federation_namespaces_direct_indexes_with_duplicate_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    for directory in ("north", "south"):
        service = workspace / directory
        source = service / "src" / "main" / "java" / "App.java"
        source.parent.mkdir(parents=True)
        _write_pom(service / "pom.xml", "orders", "1.0.0")
        source.write_text(
            "class App { void send() { kafkaTemplate.send(\"orders.created\", new Object()); } }"
        )
        with Store(service) as store:
            index_repo(service, Config(), store)

    federation = load_federation(discover_workspace_services(workspace))

    assert set(federation.endpoints_by_module) == {"orders@north", "orders@south"}
    assert {
        endpoint.module
        for endpoints in federation.endpoints_by_module.values()
        for endpoint in endpoints
    } == {"orders@north", "orders@south"}


def test_discover_modules_excludes_maven_and_gradle_mock_projects(tmp_path: Path) -> None:
    maven_mock = tmp_path / "orders-mock"
    gradle_mock = tmp_path / "payment-stubs"
    production = tmp_path / "orders"
    for module in (maven_mock, gradle_mock, production):
        module.mkdir()
    _write_pom(maven_mock / "pom.xml", "orders-mock", "1.0.0")
    (gradle_mock / "build.gradle").write_text("archivesBaseName = 'payment-mock'\n")
    _write_pom(production / "pom.xml", "orders-api", "1.0.0")

    assert [module.name for module in discover_modules(tmp_path)] == ["orders-api"]


def test_discover_modules_excludes_maven_and_gradle_archteype_projects(tmp_path: Path) -> None:
    maven_archteype = tmp_path / "maven-template"
    gradle_archteype = tmp_path / "gradle-template"
    production = tmp_path / "orders"
    for module in (maven_archteype, gradle_archteype, production):
        module.mkdir()
    _write_pom(maven_archteype / "pom.xml", "orders-archteype", "1.0.0")
    (gradle_archteype / "build.gradle").write_text("archivesBaseName = 'payment-ARCHTEYPE'\n")
    _write_pom(production / "pom.xml", "orders-api", "1.0.0")

    assert [module.name for module in discover_modules(tmp_path)] == ["orders-api"]


def test_discover_module_dependencies_keeps_only_local_maven_and_gradle_targets(
    tmp_path: Path,
) -> None:
    shared = tmp_path / "shared"
    orders = tmp_path / "orders"
    gradle_core = tmp_path / "gradle-core"
    gradle_app = tmp_path / "gradle-app"
    for module in (shared, orders, gradle_core, gradle_app):
        module.mkdir()
    _write_pom(shared / "pom.xml", "shared-kernel", "1.0.0")
    _write_pom(
        orders / "pom.xml", "orders-api", "1.0.0", dependencies=("shared-kernel", "external-client")
    )
    (gradle_core / "build.gradle").write_text("archivesName = 'gradle-core'\n")
    (gradle_app / "build.gradle").write_text(
        "archivesName = 'gradle-app'\ndependencies { implementation project(':gradle-core') }\n"
    )

    modules = discover_modules(tmp_path)

    assert discover_module_dependencies(tmp_path, modules) == [
        ModuleDependency(source="gradle-app", target="gradle-core"),
        ModuleDependency(source="orders-api", target="shared-kernel"),
    ]


def test_discover_modules_limits_nested_build_discovery_to_five_levels(tmp_path: Path) -> None:
    at_limit = tmp_path / "one" / "two" / "three" / "four" / "five"
    beyond_limit = at_limit / "six"
    at_limit.mkdir(parents=True)
    beyond_limit.mkdir()
    _write_pom(at_limit / "pom.xml", "at-limit", "1.0.0")
    _write_pom(beyond_limit / "pom.xml", "too-deep", "1.0.0")

    modules = discover_modules(tmp_path)

    assert [module.name for module in modules] == ["at-limit"]


def test_discover_modules_enriches_child_module_files_under_an_aggregator(tmp_path: Path) -> None:
    (tmp_path / "pom.xml").write_text(
        "<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
        "<modelVersion>4.0.0</modelVersion>"
        "<artifactId>platform</artifactId><version>1.0.0</version><packaging>pom</packaging>"
        "<modules><module>orders</module></modules>"
        "</project>"
    )
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrdersController.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "1.0.0")
    source.write_text(
        """import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RestController;
@RestController
class OrdersController {
  KafkaTemplate<String, String> kafkaTemplate;
  @KafkaListener(topics = "orders.created")
  void consume(String payload) { kafkaTemplate.send("orders.validated", payload); }
}
"""
    )

    modules = discover_modules(tmp_path)
    child = next(item for item in modules if item.name == "orders-api")

    assert child.rest_controllers == ("OrdersController (src/main/java/OrdersController.java)",)
    assert [(item.role, item.mechanism, item.method, item.topic) for item in child.kafka_methods] == [
        ("receive", "spring-kafka-listener", "consume", "orders.created"),
        ("send", "spring-kafka-template", "consume", "orders.validated"),
    ]


def test_nested_module_owns_a_shared_openapi_contract_not_workspace_root(tmp_path: Path) -> None:
    contract = tmp_path / "swagger.yaml"
    contract.write_text("swagger: '2.0'\npaths: {}\n", encoding="utf-8")
    nested = tmp_path / "orders"
    nested.mkdir()
    root_module = DiscoveredModule(
        name="workspace", path=tmp_path, build_system="maven", version=None,
        kind="aggregator", starts_application=False, configuration_example="",
        openapi_files=("swagger.yaml",),
    )
    nested_module = DiscoveredModule(
        name="orders", path=nested, build_system="maven", version=None,
        kind="library", starts_application=True, configuration_example="",
        openapi_files=("../swagger.yaml",),
    )

    modules = _deduplicate_openapi_contract_owners([root_module, nested_module])

    by_name = {module.name: module for module in modules}
    assert by_name["workspace"].openapi_files == ()
    assert by_name["orders"].openapi_files == ("../swagger.yaml",)


def test_module_start_attribute_is_detected_from_its_java_entrypoint(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrdersApplication.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        """import org.springframework.boot.SpringApplication;
class OrdersApplication {
  public static void main(String[] args) { SpringApplication.run(OrdersApplication.class, args); }
}
"""
    )

    modules = discover_modules(tmp_path)

    assert modules[0].kind == "library"
    assert modules[0].starts_application is True
    assert modules[0].application_entrypoint is not None
    assert modules[0].application_entrypoint.snippet.startswith("SpringApplication.run")


def test_modules_can_disable_all_tree_sitter_usage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrdersApplication.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        """import org.springframework.boot.SpringApplication;
class OrdersApplication {
  public static void main(String[] args) { SpringApplication.run(OrdersApplication.class, args); }
}
"""
    )
    monkeypatch.setattr(
        "systemlens.modules._starts_application",
        lambda *_args, **_kwargs: pytest.fail("Tree-sitter entrypoint detection must stay disabled"),
    )

    modules = discover_modules(tmp_path, use_tree_sitter=False)

    assert modules[0].name == "orders-api"
    assert modules[0].starts_application is False
    assert modules[0].application_entrypoint is None


def test_modules_cli_lists_then_returns_module_detail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "App.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text('@Value("${server.port}") class App {}\n')
    with Store(tmp_path) as store:
        store.replace_modules(discover_modules(tmp_path))
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["modules", "--json"])

    assert result.exit_code == 0
    modules = json.loads(result.output)
    assert modules == [
        {
            "name": "orders-api",
            "path": str(module.resolve()),
            "build_system": "maven",
            "version": "3.1.0",
            "kind": "library",
            "starts_application": False,
            "mongo_collections": [],
            "mongo_method_count": 0,
            "kafka_method_count": 0,
            "blocking_point_count": 0,
            "openapi_files": [],
            "rest_controllers": [],
            "openapi_generated_clients": [],
        }
    ]

    detail = runner.invoke(app, ["modules", "show", "orders-api", "--json"])
    assert detail.exit_code == 0
    assert json.loads(detail.output)["configuration_example"] == "server:\n  port: 0\n"


def test_modules_index_mongo_facts_and_openapi_files_from_java_ast(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrderStore.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        """package com.example.orders;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.MongoRepository;
@Document(collection = \"orders\") class Order { String id; int quantity; }
class OrderStore {
  MongoTemplate mongoTemplate;
  OrderRepository orderRepository;
  void save(Order order) {
    mongoTemplate.save(order);
    mongoTemplate.getCollection(\"audit\");
    mongoTemplate.find(null, Order.class, \"orders_archive\");
    orderRepository.findById(\"id\");
  }
}
interface OrderRepository extends MongoRepository<Order, String> {}
"""
    )
    contract = module / "src" / "main" / "resources" / "openapi.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("openapi: 3.1.0\ninfo: {title: Orders, version: v1}\n")

    with Store(tmp_path) as store:
        store.replace_modules(discover_modules(tmp_path))
        indexed = store.all_modules()[0]

    assert indexed.mongo_collections == ("audit", "orders", "orders_archive")
    assert len(indexed.mongo_persistence_classes) == 2
    persistence_class = next(
        item for item in indexed.mongo_persistence_classes if item.collection == "orders"
    )
    assert (persistence_class.collection, persistence_class.qualified_name, persistence_class.path) == (
        "orders", "com.example.orders.Order", "src/main/java/OrderStore.java"
    )
    assert [(field.type, field.name) for field in persistence_class.fields] == [
        ("String", "id"), ("int", "quantity")
    ]
    assert [
        item.qualified_name
        for item in indexed.mongo_persistence_classes
        if item.collection == "orders_archive"
    ] == ["com.example.orders.Order"]
    assert [(item.operation, item.receiver, item.collection) for item in indexed.mongo_methods] == [
        ("save", "mongoTemplate", None),
        ("getCollection", "mongoTemplate", "audit"),
        ("find", "mongoTemplate", "orders_archive"),
        ("findById", "orderRepository", "orders"),
    ]
    assert {item.owner_method for item in indexed.mongo_methods} == {"save"}
    assert indexed.openapi_files == ("src/main/resources/openapi.yaml",)
    proof = indexed.mongo_methods[0].evidence
    assert proof is not None
    assert proof.start_line == 11
    assert proof.snippet == "mongoTemplate.save(order)"
    assert proof.source_hash.startswith("sha256:")


def test_modules_resolve_injected_repository_collection_and_this_receiver(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrderStore.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        "import org.springframework.data.mongodb.core.mapping.Document;\n"
        "import org.springframework.data.mongodb.repository.MongoRepository;\n"
        "@Document(collection = \"orders\") class Order {}\n"
        "interface OrderRepository extends MongoRepository<Order, String> {}\n"
        "class OrderStore {\n"
        "  private final OrderRepository orderRepository;\n"
        "  OrderStore(OrderRepository orderRepository) { this.orderRepository = orderRepository; }\n"
        "  void load() { this.orderRepository.findById(\"id\"); }\n"
        "}\n"
    )

    methods = discover_modules(tmp_path)[0].mongo_methods

    assert [(item.operation, item.receiver, item.collection, item.line) for item in methods] == [
        ("findById", "orderRepository", "orders", 8),
    ]


def test_modules_infer_mongo_classes_without_document_annotation(tmp_path: Path) -> None:
    module = tmp_path / "customers"
    source = module / "src" / "main" / "java" / "CustomerStore.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "customers-api", "1.0.0")
    source.write_text(
        "package com.example.customers;\n"
        "import org.springframework.data.mongodb.core.MongoTemplate;\n"
        "import org.springframework.data.mongodb.repository.MongoRepository;\n"
        "class Customer { String id; }\n"
        "record CustomerArchive(String id) {}\n"
        "interface CustomerRepository extends MongoRepository<Customer, String> {}\n"
        "class CustomerStore {\n"
        "  MongoTemplate mongoTemplate; CustomerRepository customerRepository;\n"
        "  void load() { customerRepository.findById(\"id\"); }\n"
        "  void archive() { mongoTemplate.find(null, CustomerArchive.class, \"customer_archive\"); }\n"
        "}\n"
    )

    indexed = discover_modules(tmp_path)[0]

    assert indexed.mongo_collections == ("customer", "customer_archive")
    assert [
        (item.collection, item.qualified_name)
        for item in indexed.mongo_persistence_classes
    ] == [
        ("customer", "com.example.customers.Customer"),
        ("customer_archive", "com.example.customers.CustomerArchive"),
    ]
    assert [(item.operation, item.collection) for item in indexed.mongo_methods] == [
        ("findById", "customer"),
        ("find", "customer_archive"),
    ]


def test_modules_keep_valid_document_when_another_declaration_has_parse_errors(
    tmp_path: Path,
) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "Order.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "1.0.0")
    source.write_text(
        "import org.springframework.data.mongodb.core.mapping.Document;\n"
        "@Document(collection = \"orders\") class Order { String id; }\n"
        "class Broken { void incomplete( { }\n"
    )

    indexed = discover_modules(tmp_path)[0]

    assert indexed.mongo_collections == ("orders",)
    assert [
        (item.collection, item.qualified_name)
        for item in indexed.mongo_persistence_classes
    ] == [("orders", "Order")]


def test_modules_index_kafka_send_and_receive_methods_from_java_ast(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrderMessaging.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        """import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
class OrderMessaging {
  KafkaTemplate<String, String> kafkaTemplate;
  KafkaConsumer<String, String> consumer;
  @KafkaListener(topics = "orders.created")
  void consume(String payload) { kafkaTemplate.send("orders.validated", payload); }
  void pollBroker() { consumer.poll(java.time.Duration.ofSeconds(1)); }
}
"""
    )

    module_info = discover_modules(tmp_path)[0]

    assert [(item.role, item.mechanism, item.method, item.topic) for item in module_info.kafka_methods] == [
        ("receive", "spring-kafka-listener", "consume", "orders.created"),
        ("send", "spring-kafka-template", "consume", "orders.validated"),
        ("receive", "kafka-clients-poll", "pollBroker", None),
    ]


def test_modules_unwrap_spring_kafka_topic_expressions_from_java_ast(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrderMessaging.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        """import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.SendTo;
class OrderMessaging {
  KafkaTemplate<String, String> kafkaTemplate;
  @KafkaListener(topics = "${kafka.topic}") @SendTo("#{kafka.topic}")
  void consumePlaceholder(String payload) {}
  @KafkaListener(topics = "#{kafka.topic}")
  void consumeSpel(String payload) {}
  void publish(String payload) { kafkaTemplate.send("#{'${kafka.topic}'}", payload); }
}
"""
    )

    module_info = discover_modules(tmp_path)[0]

    assert [(item.role, item.mechanism, item.method, item.topic) for item in module_info.kafka_methods] == [
        ("receive", "spring-kafka-listener", "consumePlaceholder", "kafka.topic"),
        ("send", "spring-kafka-send-to", "consumePlaceholder", "kafka.topic"),
        ("receive", "spring-kafka-listener", "consumeSpel", "kafka.topic"),
        ("send", "spring-kafka-template", "publish", "kafka.topic"),
    ]


def test_modules_can_disable_tree_sitter_architecture_enrichment(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrderMessaging.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        """import org.springframework.kafka.annotation.KafkaListener;
class OrderMessaging {
  @KafkaListener(topics = "orders.created")
  void consume(String payload) {}
}
"""
    )
    contract = module / "src" / "main" / "resources" / "openapi.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("openapi: 3.1.0\ninfo: {title: Orders, version: v1}\n")

    module_info = discover_modules(tmp_path, enrich_architecture=False)[0]

    assert module_info.name == "orders-api"
    assert module_info.openapi_files == ("src/main/resources/openapi.yaml",)
    assert module_info.mongo_collections == ()
    assert module_info.mongo_methods == ()
    assert module_info.kafka_methods == ()
    assert module_info.blocking_points == ()


def test_modules_index_blocking_points_from_java_ast(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    source = module / "src" / "main" / "java" / "OrderLock.java"
    source.parent.mkdir(parents=True)
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    source.write_text(
        """import org.springframework.data.mongodb.core.MongoTemplate;
class OrderLock {
  MongoTemplate mongoTemplate;
  void pause() throws InterruptedException { Thread.sleep(10); }
  void acquireLock() { mongoTemplate.findAndModify(null, null, Object.class); }
  void guarded() { synchronized (this) { mongoTemplate.findOne(null, Object.class); } }
}
"""
    )

    points = discover_modules(tmp_path)[0].blocking_points

    assert [(point.mechanism, point.method, point.detail) for point in points] == [
        ("thread-sleep", "pause", "Thread.sleep"),
        ("mongo-pessimistic-lock", "acquireLock", "findAndModify"),
        ("jvm-synchronized", "guarded", "synchronized block"),
    ]


def test_modules_cli_requires_an_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["modules", "--json"])

    assert result.exit_code == 2
    assert "Index absent" in result.output


def test_modules_cli_rejects_removed_root_and_properties_options(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["modules", "--root", str(tmp_path)])

    assert result.exit_code == 2
    assert "No such option: --root" in result.output
    properties = runner.invoke(app, ["modules", "--properties"])
    assert properties.exit_code == 2
    assert "No such option: --properties" in properties.output


def test_modules_cli_subcommands_render_endpoints_properties_and_openapi(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    orders = tmp_path / "orders"
    payments = tmp_path / "payments"
    for module, artifact in ((orders, "orders-api"), (payments, "payments-api")):
        (module / "src" / "main" / "resources").mkdir(parents=True)
        _write_pom(module / "pom.xml", artifact, "3.1.0")
    (orders / "src" / "main" / "resources" / "openapi.yml").write_text("openapi: 3.0.0\npaths: {}\n")
    call = MessageEndpoint("call", "call", "rest", "GET /payments", False, "code", "resttemplate", "OrderClient.java", 1, 1, "", "orders-api")
    serve = MessageEndpoint("serve", "serve", "rest", "GET /payments", False, "code", "spring", "PaymentController.java", 1, 1, "", "payments-api")
    with Store(tmp_path) as store:
        store.replace_modules(discover_modules(tmp_path))
        store.replace_endpoints_for_files([call.path, serve.path], [call, serve])
    monkeypatch.chdir(tmp_path)

    endpoints = runner.invoke(app, ["modules", "integrations", "orders-api", "--json"])
    assert endpoints.exit_code == 0
    assert json.loads(endpoints.output)[0]["topic"] == "GET /payments"
    properties = runner.invoke(app, ["modules", "properties", "orders-api", "--json"])
    assert properties.exit_code == 0
    assert properties.output
    openapi = runner.invoke(app, ["modules", "openapi", "orders-api", "--json"])
    assert openapi.exit_code == 0
    assert json.loads(openapi.output)["contracts"][0]["path"] == "src/main/resources/openapi.yml"


def test_modules_openapi_renders_plugin_referenced_contract_for_generated_rest_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = tmp_path / "orders"
    (module / "src" / "main" / "java").mkdir(parents=True)
    (module / "src" / "main" / "openapi").mkdir(parents=True)
    (module / "src" / "main" / "java" / "OrdersApiController.java").write_text(
        "import org.springframework.web.bind.annotation.RestController;\n"
        "@RestController\n"
        "class OrdersApiController implements OrdersApi {}\n"
    )
    (module / "src" / "main" / "openapi" / "published-api.yaml").write_text("openapi: 3.0.0\npaths: {}\n")
    (module / "pom.xml").write_text(
        "<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
        "<modelVersion>4.0.0</modelVersion>"
        "<artifactId>orders-api</artifactId><version>3.1.0</version>"
        "<build><plugins><plugin>"
        "<groupId>org.openapitools</groupId>"
        "<artifactId>openapi-generator-maven-plugin</artifactId>"
        "<executions><execution><configuration>"
        "<inputSpec>${project.basedir}/src/main/openapi/published-api.yaml</inputSpec>"
        "</configuration></execution></executions>"
        "</plugin></plugins></build></project>"
    )
    with Store(tmp_path) as store:
        store.replace_modules(discover_modules(tmp_path))
    monkeypatch.chdir(tmp_path)

    openapi = runner.invoke(app, ["modules", "openapi", "orders-api", "--json"])

    assert openapi.exit_code == 0
    assert json.loads(openapi.output)["contracts"] == [
        {"path": "src/main/openapi/published-api.yaml", "content": "openapi: 3.0.0\npaths: {}\n"}
    ]


def test_modules_graph_reads_indexed_build_dependencies_and_exports_html(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = tmp_path / "shared"
    orders = tmp_path / "orders"
    core = tmp_path / "core"
    shared.mkdir()
    orders.mkdir()
    core.mkdir()
    _write_pom(core / "pom.xml", "core-kernel", "1.0.0")
    _write_pom(shared / "pom.xml", "shared-kernel", "1.0.0", dependencies=("core-kernel",))
    _write_pom(orders / "pom.xml", "orders-api", "1.0.0", dependencies=("shared-kernel",))
    modules = discover_modules(tmp_path)
    serve = MessageEndpoint(
        "serve-orders", "serve", "rest", "GET /orders", False, "code", "spring",
        "OrderController.java", 1, 1, "", "orders-api",
    )
    publish = MessageEndpoint(
        "publish-orders", "produce", "kafka", "orders.created", False, "code", "spring-kafka",
        "OrderPublisher.java", 1, 1, "", "orders-api",
    )
    consume = MessageEndpoint(
        "consume-orders", "consume", "kafka", "payments.completed", False, "code", "spring-kafka",
        "PaymentConsumer.java", 1, 1, "", "orders-api",
    )
    with Store(tmp_path) as store:
        store.replace_modules(modules)
        store.replace_module_dependencies(discover_module_dependencies(tmp_path, modules))
        store.replace_endpoints_for_files(
            [serve.path, publish.path, consume.path], [serve, publish, consume]
        )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["modules", "graph", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "modules": ["core-kernel", "orders-api", "shared-kernel"],
        "dependencies": [
            {"source": "orders-api", "target": "shared-kernel"},
            {"source": "shared-kernel", "target": "core-kernel"},
        ],
    }

    html = tmp_path / "module-dependencies.html"
    html_export = runner.invoke(app, ["export", "modules", "--html", str(html)])
    assert html_export.exit_code == 0
    document = html.read_text(encoding="utf-8")
    assert "new Sigma(network" in document
    assert "G6" not in document
    graph_data = json.loads(
        re.search(r'<script id="module-graph-data" type="application/json">(.*)</script>', document).group(1)
    )
    y_by_module_html = {node["name"]: node["y"] for node in graph_data["nodes"]}
    assert y_by_module_html["orders-api"] > y_by_module_html["shared-kernel"] > y_by_module_html["core-kernel"]
    orders_node = next(node for node in graph_data["nodes"] if node["name"] == "orders-api")
    assert orders_node["httpApisExposed"] == ["GET /orders"]
    assert orders_node["kafkaTopicsPublished"] == ["orders.created"]
    assert orders_node["kafkaTopicsConsumed"] == ["payments.completed"]
    assert 'appendList("APIs exposees", node.httpApisExposed)' in document

def test_modules_are_read_from_the_persisted_index_snapshot(tmp_path: Path) -> None:
    module = tmp_path / "orders"
    module.mkdir()
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")
    with Store(tmp_path) as store:
        store.replace_modules(discover_modules(tmp_path))
    (module / "pom.xml").unlink()

    with Store(tmp_path, readonly=True) as store:
        persisted = store.all_modules()

    assert [(item.name, item.version) for item in persisted] == [("orders-api", "3.1.0")]


def test_index_repo_materializes_modules_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = tmp_path / "orders"
    module.mkdir()
    _write_pom(module / "pom.xml", "orders-api", "3.1.0")

    with Store(tmp_path) as store:
        index_repo(tmp_path, Config(), store)
        persisted = store.all_modules()

    assert [(item.name, item.version) for item in persisted] == [("orders-api", "3.1.0")]


def test_index_repo_materializes_local_module_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared = tmp_path / "shared"
    orders = tmp_path / "orders"
    shared.mkdir()
    orders.mkdir()
    _write_pom(shared / "pom.xml", "shared-kernel", "3.1.0")
    _write_pom(orders / "pom.xml", "orders-api", "3.1.0", dependencies=("shared-kernel",))

    with Store(tmp_path) as store:
        index_repo(tmp_path, Config(), store)
        dependencies = store.all_module_dependencies()

    assert dependencies == [ModuleDependency(source="orders-api", target="shared-kernel")]
