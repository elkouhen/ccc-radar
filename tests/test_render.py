import json
import re
from dataclasses import replace
from pathlib import Path

from systemlens.models import GraphFact, MessageEndpoint, compute_endpoint_id
from systemlens.graph import GraphEdge
from systemlens.kubernetes import KubernetesWorkload
from systemlens.modules import (
    DiscoveredModule,
    ModuleDependency,
    MongoField,
    MongoPersistenceClass,
    discover_modules,
)
from systemlens.render import _vscode_file_uri, render_graph_html
from systemlens.store import Store


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


def _rest_endpoint(role: str, resource: str, path: str) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, resource, path),
        role=role,
        system="rest",
        topic=resource,
        topic_dynamic=False,
        source="code",
        framework="spring-mvc",
        path=path,
        start_line=1,
        end_line=1,
        snippet="",
    )


def _html_graph_data(document: str) -> dict[str, object]:
    match = re.search(
        r'<script id="graph-data" type="application/json">(.*)</script>', document
    )
    assert match is not None
    return json.loads(match.group(1))


def test_microservice_graph_exposes_software_layers_and_namespaces() -> None:
    module = DiscoveredModule(
        name="domain-orders",
        path=Path("/workspace/domain-orders"),
        build_system="maven",
        version=None,
        kind="library",
        starts_application=False,
        configuration_example="",
        kubernetes_workloads=(KubernetesWorkload(
            kind="Deployment",
            namespace="orders-prod",
            name="domain-orders",
            replicas=2,
            cpu_request_millicores=None,
            memory_request_bytes=None,
            cpu_limit_millicores=None,
            memory_limit_bytes=None,
        ),),
    )
    fact = GraphFact(
        id="fact-1",
        fact_type="node",
        kind="microservice",
        name="domain-orders",
        source_kind=None,
        source_name=None,
        target_kind=None,
        target_name=None,
        relation=None,
        origin="ai",
        confidence="medium",
        namespace="ai-boundaries",
    )

    graph_data = _html_graph_data(render_graph_html(
        {"domain-orders": []}, [], modules_by_service={"domain-orders": module},
        graph_facts=[fact],
        strategy1=True,
    ))
    node = next(item for item in graph_data["nodes"] if item["name"] == "domain-orders")
    assert node["layer"] == "domain"
    assert node["runtime_namespaces"] == ["orders-prod"]
    assert node["fact_namespaces"] == ["ai-boundaries"]
    assert node["project_namespace"] == "workspace"
    assert "domain" in graph_data["software_layers"]
    assert graph_data["runtime_namespaces"] == ["orders-prod"]
    assert graph_data["fact_namespaces"] == ["ai-boundaries"]


def test_graph_html_uses_only_indexed_kafka_dto_facts(tmp_path: Path) -> None:
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
    assert kafka_dtos["OrderCreated"]["producers"] == ["producer"]
    assert kafka_dtos["OrderCreated"]["consumers"] == ["consumer"]
    assert kafka_dtos["OrderCreated"]["topics"] == ["orders.created"]
    assert kafka_dtos["OrderCreated"]["fields"] == []
    assert graph_data["project_dto_definitions"] == []
    assert 'appendDtoInspectorSection("Valeurs enum", dto.enum_values || [])' in document
    assert "Que voulez-vous comprendre ?" in document
    assert "Qui produit ou consomme un topic Kafka ?" in document
    assert 'id="advanced-controls"' in document
    assert 'id="openapi-tab"' in document
    assert 'id="kafka-tab"' in document
    assert 'id="persistence-tab"' in document
    assert '>Mongo</button>' in document
    assert 'id="request-reply-tab"' in document
    assert 'id="build-tab"' in document
    assert '>Ajuster</button>' in document
    assert '>Effacer</button>' in document
    assert 'id="dto-reference-filter"' in document
    assert 'id="openapi-reference-filter"' in document
    assert 'id="openapi-panel"' in document
    assert 'id="kafka-panel"' in document
    assert 'id="persistence-panel"' in document
    assert 'id="request-reply-panel"' in document
    assert 'id="dependencies-panel"' in document
    assert 'id="graph-legend"' in document
    assert 'graphLegend.hidden = !showingGraph' in document
    assert 'issue.vscode_uri ? "a" : "code"' in document
    assert "max-height: calc(100vh - 32px)" in document
    assert "scroll-padding-bottom: 8px" in document
    assert 'placeholder="orders ou orders -> payments"' in document
    assert "function resolveExactNodeName(name, allowedKinds = null)" in document
    assert "function runExploreSearch()" in document
    assert "Aucun itineraire Kafka oriente ne passe par les noeuds demandes dans cet ordre." in document
    assert 'link => link.kind === "kafka"' in document
    assert "${nodeKindLabel(node)}${dtoSuffix}" in document
    assert "function appendServiceKafkaActivities" in document
    assert document.count('createDetailsGroup("Relations")') == 3
    assert 'appendRelationList("APIs consommees"' in document
    assert 'appendServiceKafkaActivities(node, "produce", "Topics publies"' in document
    assert 'appendRelationList("Services utilisant cette collection"' in document
    assert 'appendList("Stockee par", [node.owner], relationsGroup)' not in document
    assert "function rebuildGraph()" in document
    assert "const visibleLinks = graphData.links.filter(link => (" in document
    assert "renderer?.refresh();" in document
    assert 'id="node-suggestions"' in document
    assert 'id="inventory-status"' in document
    assert "const isolatedNodeIds = new Set(" in document
    assert "function layoutIsolatedNodes(nodes, connectedNodes)" in document
    assert "...layoutIsolatedNodes(isolatedNodes, positionedConnectedNodes)" in document
    assert 'id="layout-elk"' in document
    assert 'id="layout-cluster"' in document
    assert 'src="https://cdn.jsdelivr.net/npm/elkjs@0.12.0/lib/elk.bundled.js"' in document
    assert 'src="https://unpkg.com/cytoscape-fcose@2.2.0/cytoscape-fcose.js"' in document
    assert '"elk.algorithm": "layered"' in document
    assert "async function applyElkLayout(libraries)" in document
    assert "function packLayeredClusterGraphPositions()" in document
    assert "layoutLayerGroups" in document
    assert "const overflowing = groups.filter" in document
    assert "group.columns += 1" in document
    assert "outer width is recomputed" in document
    assert "const maxClusterRows = 3" in document
    assert "group.minY = clusterY - group.height - clusterPaddingY" in document
    assert '"external"]' in document
    assert '"orchestration"' in document
    assert 'external: "#64748b"' in document
    assert "layeredClusterView = layout === \"elk\"" in document
    assert "packLayeredClusterGraphPositions();" in document
    assert "const idsByLayer = new Map" in document
    assert "if (!nodePoints.size)" in document
    assert "if (!layerCenters.length) return" in document
    assert "allLayerPoints.length" in document
    assert "if (layeredClusterView)" in document
    assert "const projectedBounds = viewportBoundsFromGraphBounds" in document
    assert "function namespaceForNode(node)" in document
    assert 'link.target === node' in document
    assert "preferredOwnerForNode" in document
    assert "layerRank.get(right.layer)" in document
    assert 'if (data?.kind !== "microservice") return "root"' in document
    assert "? [namespaceForNode(id)]" in document
    assert "const viewportBoundsFromGraphBounds" in document
    assert "const namespaceBounds = new Map()" in document
    assert "const childNamespaceBounds" in document
    assert "parent group is defined by its rendered children" in document
    assert "namespaceGroup.namespace === group.namespace" in document
    assert 'group.namespace === "root" ? "ROOT" : group.namespace' in document
    assert "left: 50%; max-width: calc(100% - 20px)" in document
    assert "left: 50%; max-width: calc(100% - 26px)" in document
    assert 'const libraries = layout === "cluster" ? {} : await layoutLibraries' in document
    assert "async function applyFcoseClusterLayout()" in document
    assert 'const nextLayout = !layeredView' in document
    assert '? "cluster"' in document
    assert "labelGridCellSize: 160" in document
    assert "labelRenderedSizeThreshold: 10" in document
    assert "Connectivité relative :" in document
    assert "const visualNodeKind = node" in document
    assert "Namespace architectural" not in document
    assert "function clusterPathForNode(node)" in document
    assert "function architectureLayerForNode(node)" in document
    assert "Chemin des clusters : ${clusterPath}" in document
    assert "Chemin des clusters : ${clusterPathForNode(id)}" in document
    assert "project_namespace_path" in document
    assert "architecture_namespace_path" in document
    assert 'labelAlignment: "center"' in document
    assert "legend-resource-mark collection" in document
    assert 'class="brand-mark">SL</span>' in document
    assert "grid-template-columns: repeat(4, minmax(0, 1fr))" in document
    assert "--accent: #3156d3" in document
    assert "backdrop-filter: blur(18px)" in document
    assert "@media (max-width: 700px)" in document


def test_graph_html_uses_persisted_kafka_dto_source_definitions(tmp_path: Path) -> None:
    module_root = tmp_path / "orders"
    module = DiscoveredModule(
        name="orders", path=module_root, build_system="maven", version=None,
        kind="application", starts_application=True, configuration_example="",
    )
    endpoint = _kafka_endpoint("produce", "com.example.OrderCreated", "Publisher.java")
    document = render_graph_html(
        {"orders": [endpoint]}, [], modules_by_service={"orders": module},
        build_modules=[module],
        kafka_dto_definitions=[{
            "id": "com.example.OrderCreated", "name": "OrderCreated",
            "qualified_name": "com.example.OrderCreated", "module": "orders",
            "source": "src/main/java/com/example/OrderCreated.java", "root": True,
            "fields": [{"name": "id", "type": "String"}],
            "producers": ["orders"], "consumers": [], "topics": ["orders.created"],
        }],
    )

    dto = _html_graph_data(document)["kafka_dtos"][0]
    assert dto["source"] == "src/main/java/com/example/OrderCreated.java"
    assert dto["fields"] == [{"name": "id", "type": "String"}]
    assert dto["vscode_uri"].endswith("/src/main/java/com/example/OrderCreated.java")


def test_graph_html_uses_persisted_openapi_specs() -> None:
    module = DiscoveredModule(
        name="orders", path=Path("/workspace/orders"), build_system="maven", version=None,
        kind="application", starts_application=True, configuration_example="",
        openapi_files=("src/main/resources/openapi.yaml",),
    )
    graph_data = _html_graph_data(render_graph_html(
        {"orders": []}, [], modules_by_service={"orders": module},
        openapi_contracts=[{
            "module": "orders", "path": "src/main/resources/openapi.yaml",
            "spec": {"openapi": "3.0.0", "paths": {}},
        }],
    ))

    assert graph_data["nodes"][0]["openapi_contracts"][0]["spec"] == {
        "openapi": "3.0.0", "paths": {}
    }

def test_graph_html_deduplicates_repository_and_module_relative_openapi_paths() -> None:
    module = DiscoveredModule(
        name="products", path=Path("/workspace/products"), build_system="maven", version=None,
        kind="application", starts_application=True, configuration_example="",
        openapi_files=("src/main/resources/openapi/products.yaml",),
    )
    endpoint = MessageEndpoint(
        id=compute_endpoint_id("serve", "GET /products", "products/src/main/resources/openapi/products.yaml"),
        role="serve", system="rest", topic="GET /products", topic_dynamic=False,
        source="code", framework="openapi",
        path="products/src/main/resources/openapi/products.yaml", start_line=1, end_line=1,
        snippet="",
    )

    graph_data = _html_graph_data(render_graph_html(
        {"products": [endpoint]}, [], modules_by_service={"products": module},
        openapi_contracts=[{
            "module": "products", "path": "src/main/resources/openapi/products.yaml",
            "spec": {"openapi": "3.0.0", "paths": {"/products": {}}},
        }],
    ))

    contracts = graph_data["nodes"][0]["openapi_contracts"]
    assert [contract["path"] for contract in contracts] == ["src/main/resources/openapi/products.yaml"]
    assert contracts[0]["resources"] == ["GET /products"]
    assert contracts[0]["spec"] == {"openapi": "3.0.0", "paths": {"/products": {}}}


def test_graph_html_lists_a_shared_openapi_file_only_for_its_enclosing_module() -> None:
    workspace = DiscoveredModule(
        name="workspace", path=Path("/workspace"), build_system="maven", version=None,
        kind="aggregator", starts_application=False, configuration_example="",
        openapi_files=("swagger.yaml",),
    )
    orders = DiscoveredModule(
        name="orders", path=Path("/workspace/orders"), build_system="maven", version=None,
        kind="application", starts_application=True, configuration_example="",
        openapi_files=("../swagger.yaml",),
    )

    graph_data = _html_graph_data(render_graph_html(
        {"workspace": [], "orders": []}, [],
        modules_by_service={"workspace": workspace, "orders": orders},
        openapi_contracts=[{
            "module": "workspace", "path": "swagger.yaml",
            "spec": {"swagger": "2.0", "paths": {}},
        }],
    ))

    by_name = {node["name"]: node for node in graph_data["nodes"]}
    assert [contract["path"] for contract in by_name["workspace"]["openapi_contracts"]] == ["swagger.yaml"]
    assert by_name["orders"]["openapi_contracts"] == []


def test_graph_html_normalizes_openapi_evidence_for_a_deeply_nested_module() -> None:
    """A module more than one directory level below the repo root must not
    show its own contract twice under two different path strings.

    ``_module_openapi_contract_path`` used to strip only the module
    directory's last path segment, so a module such as
    ``services/orders-api`` (nested two levels deep) never had its
    repository-relative evidence normalized to the module-relative path
    used by ``module.openapi_files`` -- producing a duplicate entry and a
    missing parsed ``spec``.
    """
    module = DiscoveredModule(
        name="orders-api", path=Path("/workspace/services/orders-api"), build_system="maven",
        version=None, kind="application", starts_application=True, configuration_example="",
        openapi_files=("src/main/resources/openapi.yaml",),
    )
    endpoint = MessageEndpoint(
        id=compute_endpoint_id("serve", "GET /orders", "services/orders-api/src/main/resources/openapi.yaml"),
        role="serve", system="rest", topic="GET /orders", topic_dynamic=False,
        source="code", framework="openapi",
        path="services/orders-api/src/main/resources/openapi.yaml", start_line=1, end_line=1,
        snippet="",
    )

    graph_data = _html_graph_data(render_graph_html(
        {"orders-api": [endpoint]}, [], modules_by_service={"orders-api": module},
        openapi_contracts=[{
            "module": "orders-api", "path": "src/main/resources/openapi.yaml",
            "spec": {"openapi": "3.0.0", "paths": {"/orders": {}}},
        }],
    ))

    node = graph_data["nodes"][0]
    assert node["openapi_files"] == ["src/main/resources/openapi.yaml"]
    contracts = node["openapi_contracts"]
    assert [contract["path"] for contract in contracts] == ["src/main/resources/openapi.yaml"]
    assert contracts[0]["spec"] == {"openapi": "3.0.0", "paths": {"/orders": {}}}


def test_graph_html_resolves_strategy1_shared_contract_spec_and_owner() -> None:
    """A Strategy1 declaration can publish a contract that physically lives
    in a different (shared ``model-*``) module than the publishing service.

    Normalizing that evidence against the *publishing* module's own
    directory name produced a bogus path (the shared module's name nested
    under the publisher), which never matched the ``openapi_contracts``
    entry indexed under the *owning* module -- rendering the contract with
    no parsed spec ("contrat non detecte").
    """
    publisher = DiscoveredModule(
        name="microservice-order", path=Path("/workspace/microservice-order"), build_system="maven",
        version=None, kind="application", starts_application=True, configuration_example="",
    )
    shared_model = DiscoveredModule(
        name="model-order-api", path=Path("/workspace/model-order-api"), build_system="maven",
        version=None, kind="library", starts_application=False, configuration_example="",
        openapi_files=("src/main/resources/order.yaml",),
    )
    declaration_path = "microservice-order/src/main/resources/openapi/order.rest"
    endpoint = MessageEndpoint(
        id=compute_endpoint_id("serve", "GET /orders", declaration_path),
        role="serve", system="rest", topic="GET /orders", topic_dynamic=False,
        source="code", framework="openapi",
        path=declaration_path, start_line=1, end_line=1,
        snippet=(
            "Publication OpenAPI declaree par microservice-order/src/main/resources/openapi/order.rest\n"
            "systemlens-openapi-contract:model-order-api/src/main/resources/order.yaml\n"
        ),
    )

    graph_data = _html_graph_data(render_graph_html(
        {"microservice-order": [endpoint]},
        [],
        modules_by_service={"microservice-order": publisher},
        build_modules=[publisher, shared_model],
        openapi_contracts=[{
            "module": "model-order-api", "path": "src/main/resources/order.yaml",
            "spec": {"openapi": "3.0.0", "paths": {"/orders": {}}},
        }],
    ))

    node = graph_data["nodes"][0]
    assert node["openapi_files"] == ["src/main/resources/order.yaml"]
    contracts = node["openapi_contracts"]
    assert [contract["path"] for contract in contracts] == ["src/main/resources/order.yaml"]
    assert contracts[0]["spec"] == {"openapi": "3.0.0", "paths": {"/orders": {}}}


def test_graph_html_does_not_infer_dto_packages_from_live_sources(tmp_path: Path) -> None:
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

    assert set(definitions) == {"Event"}
    assert definitions["Event"]["fields"] == []
    assert definitions["Event"]["producers"] == ["one", "two"]


def test_graph_html_links_an_indexing_issue_to_its_source_file() -> None:
    endpoint = replace(_kafka_endpoint("consume", "Event", "Listener.java"), topic_dynamic=True)

    graph_data = _html_graph_data(render_graph_html({"orders": [endpoint]}, []))
    issue = graph_data["indexing_issues"][0]

    assert issue["location"] == "Listener.java:1"
    assert issue["vscode_uri"].startswith("vscode://file/")


def test_graph_html_reports_an_ambiguous_explicit_http_target() -> None:
    call = replace(_rest_endpoint("call", "GET /orders", "Client.java"), snippet="http://orders")
    orders = _rest_endpoint("serve", "GET /orders", "OrdersController.java")
    alternate = replace(
        _rest_endpoint("serve", "GET /orders", "AlternateController.java"),
        id="alternate",
    )

    graph_data = _html_graph_data(
        render_graph_html(
            {"caller": [call], "orders": [orders], "ORDERS": [alternate]}, []
        )
    )

    issue = graph_data["indexing_issues"][0]
    assert issue["severity"] == "warning"
    assert issue["category"] == "Cible HTTP ambiguë"
    assert issue["message"] == (
        "caller : la cible explicite 'orders' correspond à plusieurs microservices."
    )
    assert issue["location"] == "Client.java:1"
    assert issue["vscode_uri"].endswith("/Client.java:1")


def test_graph_html_colours_topics_and_mongodb_collections_by_connectivity() -> None:
    producer = _kafka_endpoint("produce", "OrderCreated", "Publisher.java")
    consumer = _kafka_endpoint("consume", "OrderCreated", "Consumer.java")
    orders_module = DiscoveredModule(
        name="orders", path=Path("/workspace/orders"), build_system="maven",
        version=None, kind="application", starts_application=True,
        configuration_example="", mongo_collections=("orders",),
        mongo_persistence_classes=(MongoPersistenceClass(
            collection="orders", name="Order", qualified_name="com.example.Order",
            path="src/main/java/com/example/Order.java", line=7,
            fields=(MongoField("id", "String"),),
        ),),
    )

    graph_document = render_graph_html(
        {"orders": [producer], "payments": [consumer]},
        [GraphEdge("kafka", "orders", "payments", producer, consumer)],
        collections_by_service={"orders": ["orders"]},
        modules_by_service={"orders": orders_module},
    )
    graph_data = _html_graph_data(graph_document)
    nodes = {node["id"]: node for node in graph_data["nodes"]}

    service = nodes["microservice:orders"]
    topic = nodes["kafka_topic:orders.created"]
    collection = nodes["mongodb_collection:orders:orders"]
    assert service["build_system"] == "maven"
    assert service["vscode_uri"] == "vscode://file//workspace/orders"
    assert "Ouvrir le module ${buildSystem} dans VS Code" in graph_document
    assert topic["complexity"] == {
        "score": 2,
        "level": "low",
        "relations": 2,
        "breakdown": {"http": 0, "kafka": 2, "mongodb": 0},
        "rank": 1,
        "population": 1,
        "tier_start": 1,
        "tier_end": 1,
    }
    assert topic["color"] == "#2563eb"
    assert topic["size"] == 50
    assert topic["label"] == "orders.created"
    assert collection["complexity"] == {
        "score": 1,
        "level": "low",
        "relations": 1,
        "breakdown": {"http": 0, "kafka": 0, "mongodb": 1},
        "rank": 1,
        "population": 1,
        "tier_start": 1,
        "tier_end": 1,
    }
    assert collection["color"] == "#2563eb"
    assert collection["label"] == "orders"
    assert collection["persistence_classes"][0]["qualified_name"] == "com.example.Order"
    assert graph_data["mongo_persistence_classes"][0]["fields"] == [
        {"name": "id", "type": "String", "references": []}
    ]
    document = render_graph_html(
        {"orders": [producer], "payments": [consumer]},
        [GraphEdge("kafka", "orders", "payments", producer, consumer)],
    )
    assert "float bar1" in document
    assert "vec2 bounds = vec2(.46, .34);" in document
    assert "gl_FragColor = vec4(v_color.rgb, v_color.a * alpha);" not in document
    assert "kafka_topic: createNodeProgram(KAFKA_TOPIC_FRAGMENT_SHADER)" in document


def test_graph_html_uses_root_path_for_module_directories(tmp_path: Path) -> None:
    module_root = tmp_path / "orders"
    module_root.mkdir()
    source = module_root / "Publisher.java"
    source.write_text("class Publisher {}", encoding="utf-8")
    module = DiscoveredModule(
        name="orders", path=module_root, build_system="gradle", version=None,
        kind="application", starts_application=True, configuration_example="",
    )
    endpoint = _kafka_endpoint("produce", "OrderCreated", "Publisher.java")
    document = render_graph_html(
        {"orders": [endpoint]}, [], modules_by_service={"orders": module},
        build_modules=[module], source_roots=[tmp_path], root_path=Path("/exported/repository"),
    )

    service = next(
        node for node in _html_graph_data(document)["nodes"]
        if node["id"] == "microservice:orders"
    )
    export_root = "vscode://file//exported/repository"
    assert service["vscode_uri"] == f"{export_root}/orders"
    assert service["kafka_endpoints"][0]["vscode_uri"] == (
        f"{export_root}/orders/Publisher.java:1"
    )


def test_vscode_uri_joins_root_path_with_indexed_relative_path(tmp_path: Path) -> None:
    source = tmp_path / "orders" / "src" / "Order.java"
    assert _vscode_file_uri(
        source, Path("/exported/repository"), [tmp_path], 14
    ) == "vscode://file//exported/repository/orders/src/Order.java:14"


def test_graph_html_resolves_mongo_class_from_dependent_persistence_module() -> None:
    application = DiscoveredModule(
        name="orders-app", path=Path("/workspace/orders-app"), build_system="maven",
        version=None, kind="application", starts_application=True,
        configuration_example="", mongo_collections=("orders",),
    )
    model = DiscoveredModule(
        name="orders-model", path=Path("/workspace/orders-model"), build_system="maven",
        version=None, kind="library", starts_application=False,
        configuration_example="", mongo_persistence_classes=(MongoPersistenceClass(
            collection="orders", name="Order", qualified_name="com.example.orders.Order",
            path="src/main/java/com/example/orders/Order.java", line=5,
        ),),
    )
    unrelated = DiscoveredModule(
        name="legacy-model", path=Path("/workspace/legacy-model"), build_system="maven",
        version=None, kind="library", starts_application=False,
        configuration_example="", mongo_persistence_classes=(MongoPersistenceClass(
            collection="orders", name="LegacyOrder", qualified_name="legacy.LegacyOrder",
            path="src/main/java/legacy/LegacyOrder.java", line=3,
        ),),
    )

    graph_data = _html_graph_data(render_graph_html(
        {"orders-app": []}, [],
        collections_by_service={"orders-app": ["orders"]},
        modules_by_service={"orders-app": application},
        build_modules=[application, model, unrelated],
        module_dependencies=[ModuleDependency("orders-app", "orders-model")],
    ))

    persistence_classes = graph_data["mongo_persistence_classes"]
    assert [item["qualified_name"] for item in persistence_classes] == [
        "com.example.orders.Order"
    ]
    assert persistence_classes[0]["module"] == "orders-model"
    collection = next(node for node in graph_data["nodes"] if node["kind"] == "mongodb_collection")
    assert collection["persistence_classes"] == persistence_classes


def test_graph_html_lists_document_class_from_persisted_maven_module(tmp_path: Path) -> None:
    module_root = tmp_path / "orders"
    (module_root / "pom.xml").parent.mkdir(parents=True, exist_ok=True)
    (module_root / "pom.xml").write_text(
        "<project><modelVersion>4.0.0</modelVersion>"
        "<groupId>com.example</groupId><artifactId>orders</artifactId>"
        "<version>1.0.0</version></project>"
    )
    source_root = module_root / "src" / "main" / "java" / "com" / "example"
    source_root.mkdir(parents=True)
    (source_root / "OrdersApplication.java").write_text(
        "package com.example; class OrdersApplication { public static void main(String[] args) { SpringApplication.run(OrdersApplication.class, args); } }"
    )
    (source_root / "Order.java").write_text(
        "package com.example; import org.springframework.data.mongodb.core.mapping.Document; "
        "@Document(collection = \"orders\") class Order { String id; Address address; } "
        "record Address(String city) {}"
    )

    with Store(tmp_path) as store:
        store.replace_modules(discover_modules(tmp_path))
        persisted_module = store.all_modules()[0]

    graph_data = _html_graph_data(render_graph_html(
        {"orders": []}, [],
        collections_by_service={"orders": ["orders"]},
        modules_by_service={"orders": persisted_module},
        build_modules=[persisted_module],
    ))

    definitions = {
        item["qualified_name"]: item
        for item in graph_data["mongo_persistence_classes"]
    }
    order = definitions["com.example.Order"]
    address = definitions["com.example.Address"]
    assert order["root"] is True
    assert address["root"] is False
    assert order["fields"] == [
        {"name": "id", "type": "String", "references": []},
        {
            "name": "address",
            "type": "Address",
            "references": [address["id"]],
        },
    ]
    assert address["fields"] == [
        {"name": "city", "type": "String", "references": []}
    ]
    collection = next(
        node for node in graph_data["nodes"]
        if node["kind"] == "mongodb_collection"
    )
    assert [item["qualified_name"] for item in collection["persistence_classes"]] == [
        "com.example.Order"
    ]
    document = render_graph_html(
        {"orders": []}, [],
        collections_by_service={"orders": ["orders"]},
        modules_by_service={"orders": persisted_module},
        build_modules=[persisted_module],
    )
    assert "function openNestedMongoPersistenceInspector" in document
    assert "returnToContainingMongoClass" in document


def test_graph_html_microservice_complexity_counts_distinct_direct_clients() -> None:
    producer = _kafka_endpoint("produce", "OrderCreated", "Publisher.java")
    consumer = _kafka_endpoint("consume", "OrderCreated", "Consumer.java")
    first_call = _rest_endpoint("call", "GET /payments", "PaymentClient.java")
    first_serve = _rest_endpoint("serve", "GET /payments", "PaymentController.java")
    second_call = _rest_endpoint("call", "POST /payments", "PaymentClient.java")
    second_serve = _rest_endpoint("serve", "POST /payments", "PaymentController.java")

    graph_data = _html_graph_data(render_graph_html(
        {"orders": [producer, first_call, second_call], "payments": [consumer, first_serve, second_serve]},
        [
            GraphEdge("kafka", "orders", "payments", producer, consumer),
            GraphEdge("rest", "orders", "payments", first_call, first_serve),
            GraphEdge("rest", "orders", "payments", second_call, second_serve),
        ],
        collections_by_service={"orders": ["orders"]},
    ))
    nodes = {node["id"]: node for node in graph_data["nodes"]}

    # orders -> payments is one HTTP client relation despite two called routes.
    assert nodes["microservice:orders"]["complexity"]["score"] == 3
    assert nodes["microservice:orders"]["label"] == "orders"
    assert nodes["microservice:orders"]["complexity"]["breakdown"] == {
        "http": 1, "kafka": 1, "mongodb": 1
    }
    assert nodes["microservice:payments"]["complexity"]["score"] == 2
    assert nodes["microservice:payments"]["complexity"]["breakdown"] == {
        "http": 1, "kafka": 1, "mongodb": 0
    }


def test_graph_html_keeps_kafka_topic_in_producer_namespace_cluster() -> None:
    producer = _kafka_endpoint("produce", "OrderCreated", "Publisher.java")
    consumer = _kafka_endpoint("consume", "OrderCreated", "Consumer.java")
    parent = Path("/workspace/PORTAIL")
    producer_module = DiscoveredModule(
        name="orders", path=parent / "orders", build_system="maven", version=None,
        kind="application", starts_application=True, configuration_example="",
    )
    consumer_module = DiscoveredModule(
        name="payments", path=parent / "payments", build_system="maven", version=None,
        kind="application", starts_application=True, configuration_example="",
    )
    graph_data = _html_graph_data(render_graph_html(
        {"orders": [producer], "payments": [consumer]},
        [GraphEdge("kafka", "orders", "payments", producer, consumer)],
        modules_by_service={"orders": producer_module, "payments": consumer_module},
        build_modules=[producer_module, consumer_module],
    ))
    topic = next(node for node in graph_data["nodes"] if node["kind"] == "kafka_topic")
    producer_node = next(node for node in graph_data["nodes"] if node["name"] == "orders")
    assert producer_node["project_namespace"] == "PORTAIL"
    assert topic["architecture_namespace"] == producer_node["project_namespace"]
    assert all(topic["id"] not in group["children"] for group in graph_data["groups"])
