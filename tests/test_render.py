import json
import re
import subprocess
from pathlib import Path

import ccc_radar.render as render_module
from ccc_radar.graph import build_graph
from ccc_radar.models import Finding, MessageEndpoint, compute_endpoint_id
from ccc_radar.modules import DiscoveredModule, ModuleDependency, MongoMethod
from ccc_radar.render import (
    render_graph_d2,
    render_graph_html,
    render_graph_json,
    render_graph_likec4,
    render_graph_text,
    write_graph_d2,
)


def make_endpoint(
    role: str,
    topic: str,
    path: str,
    start_line: int = 1,
    end_line: int = 1,
    system: str = "rest",
    framework: str | None = None,
    snippet: str = "",
    message_type: str | None = None,
    topic_dynamic: bool = False,
) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, path, start_line, end_line),
        role=role,
        system=system,
        topic=topic,
        topic_dynamic=topic_dynamic,
        source="code",
        framework=framework,
        path=path,
        start_line=start_line,
        end_line=end_line,
        snippet=snippet,
        message_type=message_type,
    )


def _fixture() -> dict[str, list[MessageEndpoint]]:
    return {
        "service-a": [
            make_endpoint(
                "call",
                "GET /orders",
                "a/Client.java",
                3,
                3,
                snippet="http://service-b",
            ),
            make_endpoint(
                "produce", "orders.created", "a/Producer.java", 5, 5,
                system="kafka", message_type="OrderCreated",
            ),
        ],
        "service-b": [
            make_endpoint("serve", "GET /orders", "b/Controller.java", 10, 10),
            make_endpoint(
                "consume", "orders.created", "b/Listener.java", 12, 12,
                system="kafka", message_type="OrderCreated",
            ),
        ],
    }


def test_render_graph_json_expands_kafka_edges_via_topic_nodes() -> None:
    endpoints_by_service = _fixture()
    edges = build_graph(endpoints_by_service)

    rendered = render_graph_json(list(endpoints_by_service), edges, [], cross_module_data_available=True)

    assert rendered["nodes"] == [
        {"name": "service-a", "kind": "microservice"},
        {"name": "service-b", "kind": "microservice"},
        {"name": "orders.created", "kind": "kafka_topic"},
    ]
    assert rendered["edges"] == [
        {
            "kind": "rest",
            "from_node": "service-a",
            "from_kind": "microservice",
            "to_node": "service-b",
            "to_kind": "microservice",
            "label": "service-b: GET /orders",
            "from_site": {
                "path": "a/Client.java",
                "start_line": 3,
                "end_line": 3,
                "topic": "GET /orders",
            },
            "to_site": {
                "path": "b/Controller.java",
                "start_line": 10,
                "end_line": 10,
                "topic": "GET /orders",
            },
        },
        {
            "kind": "kafka_produce",
            "from_node": "service-a",
            "from_kind": "microservice",
            "to_node": "orders.created",
            "to_kind": "kafka_topic",
            "label": "orders.created",
            "from_site": {
                "path": "a/Producer.java",
                "start_line": 5,
                "end_line": 5,
                "topic": "orders.created",
            },
            "to_site": None,
        },
        {
            "kind": "kafka_consume",
            "from_node": "orders.created",
            "from_kind": "kafka_topic",
            "to_node": "service-b",
            "to_kind": "microservice",
            "label": "orders.created",
            "from_site": None,
            "to_site": {
                "path": "b/Listener.java",
                "start_line": 12,
                "end_line": 12,
                "topic": "orders.created",
            },
        },
    ]


def test_render_graph_json_returns_note_when_cross_module_data_is_missing() -> None:
    rendered = render_graph_json([], [], [], cross_module_data_available=False)

    assert rendered["services"] == []
    assert rendered["edges"] == []
    assert "topologie inter-services" in rendered["note"]


def test_render_graph_json_includes_strategy1_external_microservice() -> None:
    endpoints_by_service = {
        "caller-service": [
            make_endpoint(
                "call",
                "ANY <dynamic>",
                "caller/RestPartnerConfig.java",
                snippet="cccr-external-microservice:partner-catalog",
                topic_dynamic=True,
            )
        ]
    }

    rendered = render_graph_json(
        list(endpoints_by_service),
        build_graph(endpoints_by_service),
        [],
        cross_module_data_available=True,
    )

    assert rendered["services"] == ["caller-service", "partner-catalog"]
    assert next(node for node in rendered["nodes"] if node["name"] == "partner-catalog") == {
        "name": "partner-catalog",
        "kind": "microservice",
        "external": True,
        "shape": "triangle",
    }


def test_renderers_use_a_triangle_for_external_microservices() -> None:
    endpoints_by_service = {
        "caller-service": [
            make_endpoint(
                "call",
                "ANY <dynamic>",
                "caller/RestPartnerConfig.java",
                snippet="cccr-external-microservice:partner-catalog",
                topic_dynamic=True,
            )
        ]
    }
    edges = build_graph(endpoints_by_service)

    d2 = render_graph_d2(endpoints_by_service, edges)
    assert "shape: triangle" in d2

    html = render_graph_html(endpoints_by_service, edges)
    assert "EXTERNAL_MICROSERVICE_FRAGMENT_SHADER" in html
    assert 'external_microservice: createNodeProgram(EXTERNAL_MICROSERVICE_FRAGMENT_SHADER)' in html


def test_render_graph_html_exposes_all_indexing_issues() -> None:
    endpoints_by_service = {
        "orders": [
            make_endpoint(
                "produce", "${orders.topic}", "orders/Publisher.java", system="kafka", topic_dynamic=True
            ),
            make_endpoint("call", "GET /payments", "orders/PaymentClient.java"),
        ]
    }

    document = render_graph_html(
        endpoints_by_service,
        [],
        indexing_warnings=["orders : inventaire obsolete"],
    )

    assert 'id="issues-tab"' in document
    assert 'id="issues-panel"' in document
    assert 'id="indexing-issues"' in document
    assert "Problemes d'indexation" in document
    assert "Avertissement d'inventaire" in document
    assert "Topic Kafka dynamique" in document
    assert "Type Kafka inconnu" in document
    assert "Appel HTTP non rapproche" in document
    assert "function renderIndexingIssues()" in document


def test_render_graph_text_formats_services_edges_and_outbound_calls() -> None:
    endpoints_by_service = _fixture()
    edges = build_graph(endpoints_by_service)
    result = render_graph_json(list(endpoints_by_service), edges, [], cross_module_data_available=True)

    text = render_graph_text(result)

    assert "Services (2) : service-a, service-b" in text
    assert "Topics Kafka (1) : orders.created" in text
    assert "[rest] service-a (a/Client.java:3) --service-b: GET /orders--> service-b" in text
    assert "[kafka_produce] service-a" in text
    assert "[kafka_consume] orders.created --orders.created--> service-b" in text




def test_graph_renderers_include_mongodb_collections_when_requested() -> None:
    endpoints_by_service = _fixture()
    edges = build_graph(endpoints_by_service)
    collections = {"service-a": ["orders"], "service-b": ["payments"]}

    document = render_graph_html(endpoints_by_service, edges, collections)
    assert '"id": "mongodb_collection:service-a:orders"' in document
    assert '"id": "mongodb_collection:service-b:payments"' in document

    d2 = render_graph_d2(endpoints_by_service, edges, collections)
    assert 'label: "orders"' in d2
    assert 'label: "payments"' in d2
    assert 'svc_0 -> mongo_0: "stocke" {' in d2


def test_render_graph_likec4_preserves_http_kafka_and_mongodb_relations() -> None:
    endpoints_by_service = _fixture()
    endpoints_by_service["service-a"].append(
        make_endpoint("call", "POST /orders", "a/SecondClient.java", 7, 7)
    )
    endpoints_by_service["service-b"].append(
        make_endpoint("serve", "POST /orders", "b/Controller.java", 20, 20)
    )
    endpoints_by_service["service-a"].append(
        make_endpoint("call", "POST /external-orders", "a/ExternalClient.java", 8, 8)
    )
    endpoints_by_service["service-a"].append(
        make_endpoint(
            "call",
            "ANY <dynamic>",
            "a/RestPartnerConfig.java",
            12,
            12,
            snippet="cccr-external-microservice:partner-catalog",
            topic_dynamic=True,
        )
    )
    edges = build_graph(endpoints_by_service)
    findings = [
        Finding("finding-1", "rule-1", "ERROR", "Failure", "a/Client.java", 1, 1, "", None, [], [], "service-a"),
        Finding("finding-2", "rule-2", "ERROR", "Failure", "a/Producer.java", 1, 1, "", None, [], [], "service-a"),
    ]

    document = render_graph_likec4(
        endpoints_by_service,
        edges,
        {"service-a": ["orders"], "service-b": ["payments"]},
        {"service-a": findings},
        {
            "service-a": DiscoveredModule(
                "service-a", Path("service-a"), "maven", None, "library", True, "",
                mongo_methods=(
                    MongoMethod("find", "mongoTemplate", "a/Repository.java", 12, "orders"),
                    MongoMethod("save", "repository", "a/Repository.java", 15, "orders"),
                ),
                openapi_files=("src/main/resources/openapi.yaml",),
            ),
            "service-b": DiscoveredModule(
                "service-b", Path("service-b"), "maven", None, "library", True, "",
                mongo_methods=(MongoMethod("find", "mongoTemplate", "b/Repository.java", 12, "payments"),),
            ),
        },
        ["payment-service: non indexé, ignoré"],
        [
            DiscoveredModule("service-a", Path("service-a"), "maven", None, "library", True, ""),
            DiscoveredModule("shared-kernel", Path("shared-kernel"), "gradle", None, "library", False, ""),
        ],
        [ModuleDependency("service-a", "shared-kernel")],
    )

    assert "specification {" in document
    assert "element microservice" in document
    assert "element external_microservice" in document
    assert "shape triangle" in document
    assert "element kafka_topic" in document
    assert "element mongodb_collection" in document
    assert "element external_api" in document
    assert "relationship http" in document
    assert "relationship publishes" in document
    assert "relationship consumes" in document
    assert "relationship calls_external" in document
    assert "relationship reads_data" in document
    assert "relationship writes_data" in document
    assert "shape component" in document
    assert "shape queue" in document
    assert "shape rectangle" in document
    assert "color outgoing" in document
    assert "color incoming" in document
    assert "style { color complexity_low }" in document
    assert "2 findings (ERROR=2)" in document
    assert "OpenAPI contracts: src/main/resources/openapi.yaml" in document
    assert "service_service-a -[http]-> service_service-b 'HTTP'" in document
    assert document.count("service_service-a -[http]-> service_service-b") == 1
    assert "service_partner-catalog = external_microservice 'partner-catalog'" in document
    assert "External microservice" in document
    assert "service_service-a -[http]-> service_partner-catalog 'HTTP'" in document
    assert "service_service-a -[publishes]-> topic_orders_created 'publishes OrderCreated'" in document
    assert "topic_orders_created -[consumes]-> service_service-b 'consumes OrderCreated'" in document
    assert "service_service-a -[calls_external]-> external_api_POST_external-orders 'POST /external-orders'" in document
    assert "service_service-a -[reads_data]-> collection_service-a_orders 'reads'" in document
    assert "service_service-a -[writes_data]-> collection_service-a_orders 'writes'" in document
    assert "view runtime" in document
    assert "view contracts" in document
    assert "view build" in document
    assert "view quality" in document
    assert "HTTP published: GET /orders" in document
    assert "Published Java types: OrderCreated" in document
    assert "Maven and Gradle module dependencies" in document
    assert "payment-service: non indexé, ignoré" in document


def test_render_graph_html_embeds_sigma_and_safe_graph_data() -> None:
    endpoints_by_service = {
        "service-</script>": [
            make_endpoint("produce", "orders.created", "producer/Producer.java", system="kafka")
        ],
        "service-b": [
            make_endpoint("consume", "orders.created", "consumer/Consumer.java", system="kafka")
        ],
    }

    document = render_graph_html(endpoints_by_service, build_graph(endpoints_by_service))

    assert 'src="https://cdnjs.cloudflare.com/ajax/libs/graphology/0.25.4/graphology.umd.min.js"' in document
    assert 'src="https://cdnjs.cloudflare.com/ajax/libs/sigma.js/2.4.0/sigma.min.js"' in document
    assert "new Sigma(network" in document
    assert "new graphology.MultiDirectedGraph" in document
    assert "for (let iteration = 0; iteration < 720; iteration += 1)" in document
    assert '<script type="module">' in document
    assert 'id="layout-forceatlas2"' in document
    assert 'id="layout-noverlap"' in document
    assert 'id="layout-forceatlas2-noverlap"' in document
    assert "graphology-layout-forceatlas2@0.10.1" in document
    assert "graphology-layout-noverlap@0.4.2" in document
    assert "function applyLayout(layout)" in document
    assert 'applyLayout("forceatlas2-noverlap")' in document
    assert 'id="layout-flow"' not in document
    assert 'id="layout-force"' not in document
    assert 'id="dependencies-tab"' in document
    assert 'id="dependencies-panel"' in document
    assert 'id="dependency-graph"' in document
    assert "function dependencyGraphData()" in document
    assert "function sugiyamaPositions(nodes, links)" not in document
    assert "function buildHierarchyPositions(nodes, links)" in document
    assert "const dependencyPositions = buildHierarchyPositions(dependencyData.nodes, dependencyData.links);" in document
    assert "function ensureDependencyRenderer()" in document
    assert "let dependencyRenderer = null;" in document
    assert "dependencyRenderer = new Sigma(dependencyNetwork" in document
    assert "function applyLayout(layout, persist = true)" not in document
    assert "APIs publiees" in document
    assert "APIs REST consommees" in document
    assert "function contractsForPublishedRestResource(node, resource)" in document
    assert "Ouvrir le contrat OpenAPI ${contract.path}" in document
    assert 'Contrat OpenAPI · ${contract.path}' in document
    assert '${source.name} · Contrat OpenAPI · ${contract.path}' in document
    assert "API de ${nodeDataById.get(link.target).name}" in document
    assert "Topics Kafka" in document
    assert "Contrats de messages" in document
    assert "Consommateurs REST detectes" in document
    assert "function appendActionList(title, entries, container = details)" in document
    assert "function focusPublishedRestResource(id, resource)" in document
    assert "REST · ${resource}" in document
    assert "${direction} · ${topic.name}" in document
    assert "Collections MongoDB utilisees" in document
    assert "function appendRelationList(title, links, currentId, labelForLink, container = details)" in document
    assert "Interactions" in document
    assert "Qualité" in document
    assert document.index(">Interactions</button>") < document.index(">Parcours</button>")
    assert document.index(">Parcours</button>") < document.index(">Contrats &amp; DTO</button>")
    assert document.index(">Contrats &amp; DTO</button>") < document.index(">Dependency Tree</button>")
    assert document.index(">Dependency Tree</button>") < document.index(">Qualité</button>")
    assert 'data-preset="selection"' in document
    assert "function createDetailsGroup(title, open = true)" in document
    assert "function activeRenderer()" in document
    assert "activeRenderer().getCamera().animatedZoom" in document
    assert 'id="fit-view"' in document
    assert 'id="relation-http"' in document
    assert 'id="relation-kafka"' in document
    assert 'id="relation-mongodb"' in document
    assert 'id="node-microservice"' in document
    assert 'id="node-external-microservice"' in document
    assert 'id="node-kafka-topic"' in document
    assert 'id="node-mongodb-collection"' in document
    assert 'class="relation-filters"' in document
    assert 'header.className = "details-header"' in document
    assert 'meta.className = "details-meta"' in document
    assert 'section.className = "details-section"' in document
    assert 'scoreBadge.className = `detail-badge complexity ${complexity.level}`' in document
    assert "function setDetailsEmpty(message)" in document
    assert '<details class="path-controls">' in document
    assert 'id="graph-tab"' in document
    assert 'id="paths-tab"' in document
    assert 'id="analyzed-paths"' in document
    assert "function renderAnalyzedPaths()" in document
    assert "function rememberAnalyzedPath(stops)" in document
    assert "function replayAnalyzedPath(stops)" in document
    assert "function persistAnalyzedPaths()" in document
    assert "localStorage.setItem(pathHistoryStorageKey" in document
    assert "stored.filter(isValidPathStops).forEach" in document
    assert "analyzedPaths.splice(30)" not in document
    assert "Reanalyser ce chemin" in document
    assert "Supprimer ce chemin analyse" in document
    assert "function setPathMicroserviceOrder(path)" in document
    assert "return `${order}. ${node.name}`;" in document
    assert "label: `${order}. ${data.label}`" in document
    assert ".path-history-header" in document
    assert ".path-details-header" in document
    assert ".path-overview-item" in document
    assert ".path-step" not in document
    assert '<details class="legend"' in document
    assert '.toolbar input:not([type="checkbox"])' in document
    assert "Appel HTTP" in document
    assert "Publication Kafka" in document
    assert "Consommation Kafka" in document
    assert "const RELATION_COLORS = Object.freeze" in document
    assert "function relationColor(link)" in document
    assert 'http: "#D55E00"' in document
    assert 'kafkaPublish: "#009E73"' in document
    assert 'kafkaConsume: "#0072B2"' in document
    assert 'mongodb: "#CC79A7"' in document
    assert "function isVisibleRelation(kind)" in document
    assert "function isVisibleNode(node)" in document
    assert 'node.external ? nodeExternalMicroservice.checked : nodeMicroservice.checked' in document
    assert "!isVisibleNodeId(network.source(edge))" in document
    assert 'kind !== "mongodb" || relationMongodb.checked' in document
    assert 'hidden: true' in document
    assert 'data.type === "kafka_topic" && !relationKafka.checked' not in document
    assert 'data.type === "mongodb_collection" && !relationMongodb.checked' not in document
    assert 'item.kind !== "kafka_topic" || relationKafka.checked' not in document
    assert 'item.kind !== "mongodb_collection" || relationMongodb.checked' not in document
    assert document.index("const relationHttp") < document.index("const renderer")
    assert 'renderer.on("clickNode"' in document
    assert "nodeReducer:" in document
    assert "<\\/script>" in document
    assert "service-</script>" not in document


def test_render_graph_html_renders_rest_and_kafka_relations() -> None:
    document = render_graph_html(_fixture(), build_graph(_fixture()))

    graph_data = json.loads(
        re.search(
            r'<script id="graph-data" type="application/json">(.*)</script>', document
        ).group(1)
    )

    assert [link["kind"] for link in graph_data["links"]] == ["rest", "kafka", "kafka"]
    assert [link["direction"] for link in graph_data["links"]] == ["outgoing", "outgoing", "incoming"]
    assert graph_data["links"][1]["published_message_types"] == ["OrderCreated"]
    assert graph_data["links"][2]["consumed_message_types"] == ["OrderCreated"]


def test_render_graph_html_embeds_maven_gradle_dependency_tree() -> None:
    modules = [
        DiscoveredModule("orders-service", Path("orders"), "maven", None, "library", True, ""),
        DiscoveredModule("shared-kernel", Path("shared"), "gradle", None, "library", False, ""),
        DiscoveredModule("standalone-tool", Path("tool"), "maven", None, "library", False, ""),
    ]
    document = render_graph_html(
        {},
        [],
        build_modules=modules,
        module_dependencies=[ModuleDependency("orders-service", "shared-kernel")],
    )
    graph_data = json.loads(
        re.search(r'<script id="graph-data" type="application/json">(.*)</script>', document).group(1)
    )

    assert graph_data["build_dependencies"] == {
        "nodes": [
            {
                "id": "module:orders-service",
                "name": "orders-service",
                "kind": "build_module",
                "build_system": "maven",
                "color": "#2563eb",
                "size": 17,
            },
            {
                "id": "module:shared-kernel",
                "name": "shared-kernel",
                "kind": "build_module",
                "build_system": "gradle",
                "color": "#64748b",
                "size": 14,
            },
            {
                "id": "module:standalone-tool",
                "name": "standalone-tool",
                "kind": "build_module",
                "build_system": "maven",
                "color": "#64748b",
                "size": 14,
            },
        ],
        "links": [
            {
                "source": "module:orders-service",
                "target": "module:shared-kernel",
                "kind": "build",
                "label": "dépend de",
            }
        ],
    }


def test_render_graph_html_embeds_openapi_and_kafka_dto_inspectors(tmp_path: Path) -> None:
    module_path = tmp_path / "orders"
    java_path = module_path / "src/main/java/com/example/OrderCreated.java"
    openapi_path = module_path / "src/main/openapi.yaml"
    java_path.parent.mkdir(parents=True)
    openapi_path.parent.mkdir(parents=True, exist_ok=True)
    java_path.write_text(
        "package com.example;\n"
        "public record OrderCreated(String id, java.math.BigDecimal amount) {}\n",
        encoding="utf-8",
    )
    openapi_path.write_text(
        "openapi: 3.0.3\ninfo: {title: Orders, version: v1}\npaths: {}\n",
        encoding="utf-8",
    )
    module = DiscoveredModule(
        "orders-service", module_path, "maven", None, "library", True, "",
        openapi_files=("src/main/openapi.yaml",),
    )
    endpoints_by_service = {
        "orders-service": [
            make_endpoint("produce", "orders.created", "Publisher.java", system="kafka", message_type="OrderCreated"),
        ]
    }

    document = render_graph_html(
        endpoints_by_service,
        build_graph(endpoints_by_service),
        modules_by_service={"orders-service": module},
        build_modules=[module],
    )
    graph_data = json.loads(
        re.search(r'<script id="graph-data" type="application/json">(.*)</script>', document).group(1)
    )
    service = next(node for node in graph_data["nodes"] if node["id"] == "microservice:orders-service")

    assert service["openapi_contracts"][0]["spec"]["openapi"] == "3.0.3"
    assert graph_data["kafka_dtos"] == [
        {
            "name": "OrderCreated",
            "fields": [
                {"type": "String", "name": "id"},
                {"type": "java.math.BigDecimal", "name": "amount"},
                ],
                "source": "src/main/java/com/example/OrderCreated.java",
                "vscode_uri": f"vscode://file/{java_path}",
                "producers": ["orders-service"],
            "consumers": [],
            "topics": ["orders.created"],
        }
    ]
    assert "swagger-ui-bundle.js" in document
    assert "function openOpenApiContract(contract)" in document
    assert "function openDtoInspector(dtoName)" in document
    assert 'id="references-tab"' in document
    assert 'id="references-panel"' in document
    assert 'id="openapi-references"' in document
    assert 'id="dto-references"' in document
    assert "function renderReferences()" in document


def test_render_graph_html_recursively_links_project_dtos(tmp_path: Path) -> None:
    module_path = tmp_path / "orders"
    source_root = module_path / "src/main/java/com/example"
    source_root.mkdir(parents=True)
    (source_root / "OrderCreated.java").write_text(
        "package com.example; public record OrderCreated(OrderPayload payload) {}\n",
        encoding="utf-8",
    )
    (source_root / "OrderPayload.java").write_text(
        "package com.example; class OrderPayload { java.util.List<LineItem> items; }\n",
        encoding="utf-8",
    )
    (source_root / "LineItem.java").write_text(
        "package com.example; record LineItem(Money amount) {}\n",
        encoding="utf-8",
    )
    (source_root / "Money.java").write_text(
        "package com.example; record Money(String currency) {}\n",
        encoding="utf-8",
    )
    module = DiscoveredModule("orders-service", module_path, "maven", None, "library", True, "")
    endpoints_by_service = {
        "orders-service": [
            make_endpoint("produce", "orders.created", "Publisher.java", system="kafka", message_type="OrderCreated"),
        ]
    }

    document = render_graph_html(endpoints_by_service, build_graph(endpoints_by_service), build_modules=[module])
    graph_data = json.loads(
        re.search(r'<script id="graph-data" type="application/json">(.*)</script>', document).group(1)
    )

    root_dto = graph_data["kafka_dtos"][0]
    assert root_dto["fields"][0]["dto_references"] == ["OrderPayload"]
    definitions = {definition["name"]: definition for definition in graph_data["project_dto_definitions"]}
    assert definitions["OrderPayload"]["fields"][0]["dto_references"] == ["LineItem"]
    assert definitions["LineItem"]["fields"][0]["dto_references"] == ["Money"]
    assert definitions["Money"]["fields"] == [{"type": "String", "name": "currency"}]
    assert "function dtoDefinition(dtoName)" in document
    assert "Ouvrir le type projet ${references[0]}" in document
    assert "function returnToContainingDto()" in document
    assert "← Retour" in document


def test_render_graph_html_keeps_openapi_contract_evidence_navigable() -> None:
    endpoints_by_service = {
        "annuaire": [
            make_endpoint(
                "serve",
                "GET /directory",
                "src/main/resources/openapi/annuaire.rest",
                framework="openapi",
                snippet="cccr-openapi-contract:model-annuaire/src/main/openapi/annuaire.yaml",
            )
        ]
    }
    document = render_graph_html(endpoints_by_service, build_graph(endpoints_by_service))
    graph_data = json.loads(
        re.search(r'<script id="graph-data" type="application/json">(.*)</script>', document).group(1)
    )
    service = next(node for node in graph_data["nodes"] if node["id"] == "microservice:annuaire")

    assert service["openapi_files"] == ["model-annuaire/src/main/openapi/annuaire.yaml"]
    assert service["openapi_contracts"] == [
        {
            "path": "model-annuaire/src/main/openapi/annuaire.yaml",
            "resources": ["GET /directory"],
        }
    ]


def test_render_graph_html_resolves_strategy1_contract_from_workspace_root(tmp_path: Path) -> None:
    contract_path = tmp_path / "model-annuaire/src/main/openapi/annuaire.yaml"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(
        "openapi: 3.0.3\ninfo: {title: Annuaire, version: v1}\npaths: {}\n"
        "x-published-on: 2026-07-19\n",
        encoding="utf-8",
    )
    service_module = DiscoveredModule(
        "domaine-annuaire", tmp_path / "domaine-annuaire", "maven", None, "library", True, ""
    )
    endpoints_by_service = {
        "domaine-annuaire": [
            make_endpoint(
                "serve", "GET /directory", "src/main/resources/openapi/annuaire.rest",
                framework="openapi",
                snippet="cccr-openapi-contract:model-annuaire/src/main/openapi/annuaire.yaml",
            )
        ]
    }

    document = render_graph_html(
        endpoints_by_service,
        build_graph(endpoints_by_service),
        modules_by_service={"domaine-annuaire": service_module},
        source_roots=[tmp_path],
    )
    graph_data = json.loads(
        re.search(r'<script id="graph-data" type="application/json">(.*)</script>', document).group(1)
    )
    service = next(node for node in graph_data["nodes"] if node["id"] == "microservice:domaine-annuaire")

    assert service["openapi_contracts"][0]["spec"]["info"]["title"] == "Annuaire"
    assert service["openapi_contracts"][0]["spec"]["x-published-on"] == "2026-07-19"


def test_render_graph_html_keeps_complexity_architecture_only() -> None:
    endpoints_by_service = _fixture()
    document = render_graph_html(
        endpoints_by_service,
        build_graph(endpoints_by_service),
        modules_by_service={
            "service-a": DiscoveredModule(
                "service-a", Path("service-a"), "maven", None, "library", True, "",
                openapi_files=("src/main/resources/openapi.yaml",),
            )
        },
    )
    graph_data = json.loads(
        re.search(r'<script id="graph-data" type="application/json">(.*)</script>', document).group(1)
    )
    service = next(node for node in graph_data["nodes"] if node["id"] == "microservice:service-a")
    topic = next(node for node in graph_data["nodes"] if node["id"] == "kafka_topic:orders.created")

    assert service["complexity"] == {
        "score": 2,
        "level": "low",
        "relations": 2,
    }
    assert service["openapi_files"] == ["src/main/resources/openapi.yaml"]
    assert service["openapi_contracts"][0]["path"] == "src/main/resources/openapi.yaml"
    assert service["openapi_contracts"][0]["resources"] == []
    assert service["openapi_contracts"][0]["vscode_uri"].endswith(
        "/service-a/src/main/resources/openapi.yaml"
    )
    assert '"findings"' not in document
    assert "severity_counts" not in document
    assert service["color"] == "#2563eb"
    assert "complexity" not in topic
    assert topic["color"] == "#64748b"
    assert "Connectivite : ${complexity.level} (${complexity.score})" in document
    assert 'type: "arrow"' in document
    assert 'type: node.external ? "external_microservice" : node.kind,' in document
    assert "nodeProgramClasses:" in document
    assert "MICROSERVICE_FRAGMENT_SHADER" in document
    assert "KAFKA_TOPIC_FRAGMENT_SHADER" in document
    assert "MONGODB_COLLECTION_FRAGMENT_SHADER" in document


def test_complexity_levels_split_all_nodes_into_balanced_terciles() -> None:
    levels = render_module._complexity_levels(
        {
            "microservice:orders": 8,
            "microservice:payments": 4,
            "kafka_topic:orders.created": 6,
            "kafka_topic:payments.completed": 2,
            "mongodb_collection:orders:orders": 5,
            "mongodb_collection:payments:payments": 1,
            "external_api:billing": 3,
            "external_api:catalog": 7,
        }
    )

    assert {level: list(levels.values()).count(level) for level in ("low", "medium", "high")} == {
        "low": 3,
        "medium": 3,
        "high": 2,
    }
    assert levels["mongodb_collection:payments:payments"] == "low"
    assert levels["microservice:orders"] == "high"




















def test_render_graph_d2_encodes_rest_and_kafka_edges() -> None:
    endpoints_by_service = _fixture()
    edges = build_graph(endpoints_by_service)

    rendered = render_graph_d2(endpoints_by_service, edges)

    assert "direction: down" in rendered
    assert "  **service-a**" in rendered
    assert "  **service-b**" in rendered
    assert "  - `GET /orders`" in rendered
    assert 'label: "orders.created"' in rendered
    assert 'svc_0 -> svc_1: "service-b: GET /orders" {' in rendered
    assert 'svc_0 -> topic_0: "orders.created" {' in rendered
    assert 'topic_0 -> svc_1: "orders.created" {' in rendered
    assert "style.stroke-dash: 3" in rendered




def test_write_graph_d2_writes_raw_source_when_extension_is_d2(tmp_path) -> None:
    out_file = tmp_path / "graph.d2"

    write_graph_d2(out_file, "a -> b\n")

    assert out_file.read_text(encoding="utf-8") == "a -> b\n"


def test_write_graph_d2_renders_via_d2_cli(monkeypatch, tmp_path) -> None:
    out_file = tmp_path / "graph.svg"
    calls = {}

    def fake_run(*args, **kwargs):
        calls["cmd"] = args[0]
        calls["input"] = kwargs["input"]
        out_file.write_text("<svg />", encoding="utf-8")
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(render_module.subprocess, "run", fake_run)

    write_graph_d2(out_file, "a -> b\n", layout="elk")

    assert calls["cmd"] == ["d2", "--layout", "elk", "-", str(out_file)]
    assert calls["input"] == "a -> b\n"
    assert out_file.read_text(encoding="utf-8") == "<svg />"
