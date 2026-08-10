from pathlib import Path

import pytest

from ccc_radar.config import Config
from ccc_radar.scanner import (
    apply_kafka_topic_strategy1,
    infer_kafka_endpoints,
    infer_kafka_topic_strategy1_endpoints,
    infer_json_kafka_flow_graph_endpoints,
    infer_markdown_topic_manifest_endpoints,
    resolve_spring_property,
)
from ccc_radar.semgrep import run_semgrep_endpoints
from ccc_radar.models import MessageEndpoint, compute_endpoint_id

# Le pack de règles vit dans le repo skill (ccc-radar-skill/skills/cccr/
# rules/kafka/), pas dans ce repo (ADR-24). Cible d'analyse : Java + Spring
# + Maven uniquement (pas de pack Python/JS).
FIXTURES_DIR = Path(__file__).parent / "fixtures"
KAFKA_REPO = FIXTURES_DIR / "kafka_repo"


def make_config(**overrides: object) -> Config:
    defaults: dict = {"rules": ["rules/java.yaml"]}
    defaults.update(overrides)
    return Config(**defaults)


@pytest.mark.integration
def test_kafka_consume_extracts_literal_topic_from_annotation() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/OrderConsumer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    literal = by_line[7]
    assert literal.role == "consume"
    assert literal.system == "kafka"
    assert literal.framework == "spring-kafka"
    assert literal.topic == "orders.created"
    assert literal.topic_dynamic is False
    assert literal.message_type == "String"
    # BACKLOG-13 M1 : kafka_repo n'a pas de pom.xml -> pas de module Maven ;
    # OrderConsumer.java déclare `package com.example.app;`.
    assert literal.module is None
    assert literal.qualified_name == "com.example.app.OrderConsumer"


@pytest.mark.integration
def test_kafka_consume_resolves_spring_property_placeholder() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/OrderConsumer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    resolved = by_line[11]  # topics = "${app.kafka.topics.payments}"
    assert resolved.topic == "payments.received"
    assert resolved.topic_dynamic is False


@pytest.mark.integration
def test_kafka_consume_uses_spring_default_when_property_key_missing() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/OrderConsumer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    # topics = "${app.kafka.topics.unknown:orders.fallback}" : clé absente
    # de application.yml, mais un défaut Spring est fourni.
    defaulted = by_line[15]
    assert defaulted.topic == "orders.fallback"
    assert defaulted.topic_dynamic is False


@pytest.mark.integration
def test_kafka_consume_keeps_placeholder_dynamic_when_unresolved() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/OrderConsumer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    # topics = "${app.kafka.topics.missing}" : ni dans application.yml, ni
    # de défaut Spring — jamais résolu au hasard.
    unresolved = by_line[19]
    assert unresolved.topic == "app.kafka.topics.missing"
    assert unresolved.topic_dynamic is True


@pytest.mark.integration
def test_kafka_produce_template_and_record_extract_topics() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/OrderProducer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}

    literal_send = by_line[15]  # kafkaTemplate.send("orders.created", payload)
    assert literal_send.role == "produce"
    assert literal_send.framework == "spring-kafka"
    assert literal_send.topic == "orders.created"
    assert literal_send.topic_dynamic is False
    assert literal_send.message_type == "String"

    placeholder_send = by_line[19]  # send("${app.kafka.topics.payments}", key, payload)
    assert placeholder_send.topic == "payments.received"
    assert placeholder_send.topic_dynamic is False

    producer_record = by_line[23]  # new ProducerRecord("orders.updated", payload)
    assert producer_record.framework == "kafka-clients"
    assert producer_record.topic == "orders.updated"
    assert producer_record.topic_dynamic is False
    assert producer_record.message_type == "String"

    dynamic_send = by_line[28]  # send(topic, payload) : variable, aucun littéral
    assert dynamic_send.topic == "<dynamic>"
    assert dynamic_send.topic_dynamic is True


@pytest.mark.integration
def test_kafka_produce_single_arg_send_of_producer_record_is_not_double_counted() -> None:
    """kafkaTemplate.send(record) (1 seul argument, un ProducerRecord déjà
    construit) ne doit pas être capté par produce-template — le topic est
    déjà porté par le new ProducerRecord(...) correspondant, capté à part."""
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/OrderProducer.java"]
    )

    assert not any(e.snippet.strip().startswith("kafkaTemplate.send(record)") for e in endpoints)
    # 1 seul produce pour le ProducerRecord de la ligne 22-23 (pas 2)
    record_endpoints = [e for e in endpoints if e.framework == "kafka-clients"]
    assert len(record_endpoints) == 1


@pytest.mark.integration
def test_kafka_listener_resolves_value_annotated_field_variable() -> None:
    """`@KafkaListener(topics = ordersTopic)` où `ordersTopic` n'est pas un
    littéral mais un champ `@Value("${...}")` — résolu contre ce champ,
    puis contre application.yml, sans jamais deviner (BACKLOG-10 K2)."""
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/ValueAnnotatedConsumer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    resolved = by_line[24]  # topics = ordersTopic -> @Value("${app.kafka.topics.orders}")
    assert resolved.topic == "orders.created"
    assert resolved.topic_dynamic is False


@pytest.mark.integration
def test_kafka_send_resolves_value_annotated_field_with_default() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/ValueAnnotatedConsumer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    # send(fallbackTopic, payload) -> @Value("${app.kafka.topics.missing:orders.fallback}")
    defaulted = by_line[29]
    assert defaulted.topic == "orders.fallback"
    assert defaulted.topic_dynamic is False


@pytest.mark.integration
def test_kafka_send_keeps_value_annotated_field_dynamic_when_unresolvable() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/ValueAnnotatedConsumer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    # send(unresolvableTopic, payload) -> @Value("${app.kafka.topics.unresolvable}"),
    # clé absente d'application.yml et pas de défaut : jamais résolu au hasard.
    unresolved = by_line[33]
    assert unresolved.topic == "<dynamic>"
    assert unresolved.topic_dynamic is True


def test_kafka_topic_strategy1_reads_get_topics_accessors_and_listener_keys(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "EventAdapter.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        """import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

class EventAdapter {
  private KafkaTemplate<String, OrderCreated> kafkaTemplate;

  void publish(OrderCreated event) {
    kafkaTemplate.send(properties.getTopics().getAbcDefGhiJkl(), event);
  }

  @KafkaListener(topics = "${kafka.topics.abc_def_ghi_jkl.name}")
  public void consume(OrderCreated event) {}
}
"""
    )

    endpoints = infer_kafka_topic_strategy1_endpoints(tmp_path)

    assert {(endpoint.role, endpoint.topic, endpoint.message_type) for endpoint in endpoints} == {
        ("produce", "ABC_DEF_GHI_JKL", "OrderCreated"),
        ("consume", "ABC_DEF_GHI_JKL", "OrderCreated"),
    }
    assert {endpoint.framework for endpoint in endpoints} == {"kafka-topic-strategy1"}


def test_kafka_topic_strategy1_reads_the_producer_method_parameter_type(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "EventAdapter.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        """class EventAdapter {
  private EventPublisher publisher;

  public void publish(OrderCreated event) {
    publisher.send(properties.getTopics().getOrdersCreated(), event);
  }
}
"""
    )

    endpoints = infer_kafka_topic_strategy1_endpoints(tmp_path)

    assert [(endpoint.topic, endpoint.message_type) for endpoint in endpoints] == [
        ("ORDERS_CREATED", "OrderCreated")
    ]


def test_kafka_topic_strategy1_replaces_standard_extraction_at_covered_sites(tmp_path: Path) -> None:
    standard = MessageEndpoint(
        id=compute_endpoint_id("produce", "<dynamic>", "EventAdapter.java", 7, 7),
        role="produce",
        system="kafka",
        topic="<dynamic>",
        topic_dynamic=True,
        source="code",
        framework="spring-kafka",
        path="EventAdapter.java",
        start_line=7,
        end_line=7,
        snippet="kafkaTemplate.send(properties.getTopics().getOrdersCreated(), event)",
    )
    strategy = MessageEndpoint(
        id=compute_endpoint_id("produce", "ORDERS_CREATED", "EventAdapter.java", 7, 7),
        role="produce",
        system="kafka",
        topic="ORDERS_CREATED",
        topic_dynamic=False,
        source="code",
        framework="kafka-topic-strategy1",
        path="EventAdapter.java",
        start_line=7,
        end_line=7,
        snippet="kafkaTemplate.send(properties.getTopics().getOrdersCreated(), event)",
        message_type="OrderCreated",
    )

    endpoints = apply_kafka_topic_strategy1([standard], [strategy])

    assert endpoints == [strategy]


def test_kafka_dependencies_cover_listener_arrays_partitions_patterns_and_stream_bridge(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "EventAdapter.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        """import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.cloud.stream.function.StreamBridge;

class EventAdapter {
  private StreamBridge streamBridge;

  @KafkaListener(topics = {"orders.created", "orders.cancelled"})
  void consumeMany(String event) {}

  @KafkaListener(topicPartitions = @TopicPartition(topic = "orders.partitioned", partitions = {"0", "1"}))
  void consumePartition(String event) {}

  @KafkaListener(topicPattern = "orders\\..*")
  void consumePattern(String event) {}

  void publish(OrderCreated event) {
    streamBridge.send("orders.enriched", event);
  }
}
"""
    )

    endpoints = infer_kafka_endpoints(tmp_path)
    facts = {(endpoint.role, endpoint.topic, endpoint.topic_dynamic, endpoint.framework) for endpoint in endpoints}

    assert ("consume", "orders.created", False, "spring-kafka") in facts
    assert ("consume", "orders.cancelled", False, "spring-kafka") in facts
    assert ("consume", "orders.partitioned", False, "spring-kafka") in facts
    assert ("consume", "orders\\..*", True, "spring-kafka") in facts
    assert ("produce", "orders.enriched", False, "spring-cloud-stream") in facts


@pytest.mark.integration
def test_kafka_raw_consumer_subscribe_extracts_literal_topic() -> None:
    """API bas niveau (confluent-kafka / kafka-clients, hors Spring) :
    KafkaConsumer.subscribe(Collections.singletonList("...")) (BACKLOG-10 K2)."""
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/RawKafkaConsumer.java"]
    )

    assert len(endpoints) == 1
    endpoint = endpoints[0]
    assert endpoint.role == "consume"
    assert endpoint.system == "kafka"
    assert endpoint.framework == "kafka-clients"
    assert endpoint.topic == "orders.created"
    assert endpoint.topic_dynamic is False


@pytest.mark.integration
def test_kafka_streams_consume_and_produce_extract_topics() -> None:
    """BACKLOG Q25 : StreamsBuilder.stream(...)/KStream.to(...) (Kafka
    Streams), second style d'intégration Kafka distinct de
    @KafkaListener/KafkaTemplate.send — vérifié sur
    sample-spring-kafka-microservices/order-service/OrderApp.java."""
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/KafkaStreamsApp.java"]
    )

    by_line = {endpoint.start_line: endpoint for endpoint in endpoints}
    assert by_line[17].message_type == "Order"
    assert by_line[26].message_type == "Order"

    by_line = {e.start_line: e for e in endpoints}
    assert len(endpoints) == 4
    assert {e.framework for e in endpoints} == {"kafka-streams"}

    payment_orders = by_line[17]  # builder.stream("payment-orders", Consumed.with(...))
    assert payment_orders.role == "consume"
    assert payment_orders.topic == "payment-orders"
    assert payment_orders.topic_dynamic is False

    # stock-orders : forme imbriquée dans .join(builder.stream(...), ...)
    stock_orders = by_line[20]
    assert stock_orders.role == "consume"
    assert stock_orders.topic == "stock-orders"
    assert stock_orders.topic_dynamic is False

    # .peek(...).to("orders") : republication après jointure
    republished = by_line[26]
    assert republished.role == "produce"
    assert republished.topic == "orders"
    assert republished.topic_dynamic is False

    # matérialisation KTable : même topic "orders", consommé cette fois
    materialized = by_line[33]
    assert materialized.role == "consume"
    assert materialized.topic == "orders"
    assert materialized.topic_dynamic is False


@pytest.mark.integration
def test_kafka_message_builder_send_extracts_topics() -> None:
    endpoints = run_semgrep_endpoints(
        KAFKA_REPO, make_config(), files=["app/java/MessageBuilderProducer.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    assert len(endpoints) == 2

    literal_send = by_line[26]
    assert literal_send.role == "produce"
    assert literal_send.framework == "spring-kafka"
    assert literal_send.topic == "orders.confirmed"
    assert literal_send.topic_dynamic is False

    resolved_send = by_line[34]
    assert resolved_send.role == "produce"
    assert resolved_send.framework == "spring-kafka"
    assert resolved_send.topic == "payments.received"
    assert resolved_send.topic_dynamic is False


@pytest.mark.integration
def test_kafka_pack_runs_standalone_without_other_backlog_tasks() -> None:
    endpoints = run_semgrep_endpoints(KAFKA_REPO, make_config())

    # OrderConsumer/OrderProducer : 4 consume + 4 produce ; ValueAnnotatedConsumer :
    # 1 consume + 2 produce ; RawKafkaConsumer : 1 consume (kafka-clients) ;
    # KafkaStreamsApp (Q25) : 3 consume + 1 produce (kafka-streams) ;
    # MessageBuilderProducer : 2 produce (spring-kafka, inférés)
    assert len(endpoints) == 18
    assert {e.role for e in endpoints} == {"consume", "produce"}
    assert {e.system for e in endpoints} == {"kafka"}


def test_markdown_topic_manifest_declares_producers_and_consumers(tmp_path: Path) -> None:
    manifest = tmp_path / "TOPICS.md"
    manifest.write_text(
        "### module-a\n\n"
        "**Producer**\n\n"
        "| Topic | Nom physique |\n"
        "|---|---|\n"
        "| `flux1` | `FLUX1` |\n\n"
        "**Consumer**\n\n"
        "| Topic | Nom physique |\n"
        "|---|---|\n"
        "| `flux2` | `FLUX2` |\n",
        encoding="utf-8",
    )

    endpoints = infer_markdown_topic_manifest_endpoints(tmp_path)

    assert [(e.role, e.topic, e.module) for e in endpoints] == [
        ("produce", "FLUX1", "module-a"),
        ("consume", "FLUX2", "module-a"),
    ]
    assert {e.source for e in endpoints} == {"manifest"}
    assert {e.framework for e in endpoints} == {"markdown-topic-manifest"}
    assert {e.topic_dynamic for e in endpoints} == {False}


def test_json_kafka_flow_graph_manifest_declares_producers_and_consumers(tmp_path: Path) -> None:
    manifest = tmp_path / "kafka-flow-graph-anonymous.json"
    manifest.write_text(
        """{
  "topics": {"topic_a": "TOPIC_A", "topic_b": "TOPIC_B"},
  "categories": {"service-alpha": "acquisition"},
  "producers": {"service-alpha": ["topic_a"]},
  "consumers": {"service-beta": ["topic_a", "topic_b"]}
}
""",
        encoding="utf-8",
    )

    endpoints = infer_json_kafka_flow_graph_endpoints(tmp_path)

    assert {(e.role, e.topic, e.module) for e in endpoints} == {
        ("produce", "TOPIC_A", "service-alpha"),
        ("consume", "TOPIC_A", "service-beta"),
        ("consume", "TOPIC_B", "service-beta"),
    }
    assert {e.source for e in endpoints} == {"manifest"}
    assert {e.framework for e in endpoints} == {"json-kafka-flow-graph"}
    assert {e.path for e in endpoints} == {"kafka-flow-graph-anonymous.json"}
    assert all(endpoint.start_line > 0 for endpoint in endpoints)


def test_json_kafka_flow_graph_manifest_ignores_unrelated_json(tmp_path: Path) -> None:
    (tmp_path / "package.json").write_text('{"name": "sample"}\n', encoding="utf-8")

    assert infer_json_kafka_flow_graph_endpoints(tmp_path) == []


# -- resolve_spring_property (unitaire, sans Semgrep) --


def test_resolve_spring_property_reads_nested_yaml_key(tmp_path: Path) -> None:
    resources = tmp_path / "src" / "main" / "resources"
    resources.mkdir(parents=True)
    (resources / "application.yml").write_text("app:\n  name: orders-service\n")

    assert resolve_spring_property(tmp_path, "app.name") == "orders-service"


def test_resolve_spring_property_reads_dotted_properties_file(tmp_path: Path) -> None:
    resources = tmp_path / "src" / "main" / "resources"
    resources.mkdir(parents=True)
    (resources / "application.properties").write_text("app.name=orders-service\n")

    assert resolve_spring_property(tmp_path, "app.name") == "orders-service"


def test_resolve_spring_property_uses_default_when_key_absent(tmp_path: Path) -> None:
    assert resolve_spring_property(tmp_path, "app.missing:fallback-value") == "fallback-value"


def test_resolve_spring_property_returns_none_without_default_or_file(tmp_path: Path) -> None:
    assert resolve_spring_property(tmp_path, "app.missing") is None


def test_resolve_spring_property_prefers_yml_over_properties_when_both_exist(
    tmp_path: Path,
) -> None:
    resources = tmp_path / "src" / "main" / "resources"
    resources.mkdir(parents=True)
    (resources / "application.yml").write_text("app:\n  name: from-yaml\n")
    (resources / "application.properties").write_text("app.name=from-properties\n")

    assert resolve_spring_property(tmp_path, "app.name") == "from-yaml"


def test_resolve_spring_property_reads_profile_specific_config_when_base_is_absent(
    tmp_path: Path,
) -> None:
    resources = tmp_path / "src" / "main" / "resources"
    resources.mkdir(parents=True)
    (resources / "application-prod.yml").write_text("app:\n  name: from-profile\n")

    assert resolve_spring_property(tmp_path, "app.name") == "from-profile"


def test_resolve_spring_property_prefers_the_service_module_of_the_source_file(
    tmp_path: Path,
) -> None:
    root_resources = tmp_path / "src" / "main" / "resources"
    module_resources = tmp_path / "services" / "orders" / "src" / "main" / "resources"
    java_file = (
        tmp_path
        / "services"
        / "orders"
        / "src"
        / "main"
        / "java"
        / "com"
        / "example"
        / "OrderConsumer.java"
    )
    root_resources.mkdir(parents=True)
    module_resources.mkdir(parents=True)
    java_file.parent.mkdir(parents=True)
    java_file.write_text("class OrderConsumer {}\n")
    (root_resources / "application.yml").write_text("app:\n  name: root-app\n")
    (module_resources / "application.yml").write_text("app:\n  name: orders-service\n")

    assert (
        resolve_spring_property(
            tmp_path,
            "app.name",
            source_path="services/orders/src/main/java/com/example/OrderConsumer.java",
        )
        == "orders-service"
    )


# -- module Maven attribué à chaque endpoint (BACKLOG-13 M1) --


@pytest.mark.integration
def test_endpoints_are_attributed_to_their_own_maven_module_from_a_single_parent_scan() -> None:
    """Indexer le répertoire *parent* directement (un seul scan Semgrep sur
    les deux modules) doit attribuer à chaque endpoint le bon module —
    order-service pour le producteur, payment-service pour le
    consommateur — sans passer par la fédération multi-dépôts (K7/A2)."""
    kafka_workspace = FIXTURES_DIR / "kafka_workspace"
    endpoints = run_semgrep_endpoints(
        kafka_workspace,
        make_config(rules=["order-service/rules/java.yaml"]),
        files=[
            "order-service/app/OrderProducer.java",
            "payment-service/app/OrderConsumer.java",
        ],
    )

    by_path = {e.path: e for e in endpoints}
    assert by_path["order-service/app/OrderProducer.java"].module == "order-service"
    assert by_path["payment-service/app/OrderConsumer.java"].module == "payment-service"
