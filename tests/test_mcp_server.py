import asyncio
import shutil
from pathlib import Path

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from typer.testing import CliRunner

from ccc_radar.cli import app
from ccc_radar.flow import FlowError
from ccc_radar.mcp_server import (
    audit_dependency_graph,
    dependency_graph,
    findings_summary,
    graph,
    list_endpoints,
    list_workspace_services,
    mcp,
    reindex_findings,
    search_findings,
    trace_message_flow,
)
from ccc_radar.models import Finding, MessageEndpoint, compute_endpoint_id
from ccc_radar.modules import DiscoveredModule, MongoMethod
from ccc_radar.inventory_freshness import current_endpoint_inventory_signature
from ccc_radar.store import Store

FIXTURES_DIR = Path(__file__).parent / "fixtures"
VULN_REPO = FIXTURES_DIR / "vuln_repo"
MAVEN_WORKSPACE = FIXTURES_DIR / "maven_workspace"

runner = CliRunner()


@pytest.fixture
def indexed_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    dest = tmp_path / "vuln_repo"
    shutil.copytree(VULN_REPO, dest)
    monkeypatch.chdir(dest)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    runner.invoke(app, ["index", "--semgrep"])
    return dest


@pytest.mark.integration
def test_search_findings_tool_returns_expected_json(indexed_repo: Path) -> None:
    result = search_findings("injection sql")

    # La recherche est précision-first : tous les termes de la requête doivent
    # être couverts par un finding, comme pour la commande `cccr findings`.
    assert len(result) == 1
    assert result[0]["rule_id"].endswith("custom.sql-fstring")
    assert {"id", "rule_id", "severity", "path", "score"} <= set(result[0].keys())


@pytest.mark.integration
def test_search_findings_tool_hybrid_matches_exact_rule_id(indexed_repo: Path) -> None:
    result = search_findings("custom.subprocess-shell-true")

    assert result[0]["rule_id"].endswith("custom.subprocess-shell-true")


@pytest.mark.integration
def test_search_findings_tool_rejects_invalid_severity(indexed_repo: Path) -> None:
    """BACKLOG-16 P4 : côté MCP aussi, une sévérité invalide doit lever une
    erreur métier propre (`SearchError`), pas un `ValueError` non géré."""
    from ccc_radar.search import SearchError

    with pytest.raises(SearchError, match="HIGH"):
        search_findings("injection sql", severity="HIGH")


@pytest.mark.integration
def test_findings_summary_tool_returns_expected_json(indexed_repo: Path) -> None:
    result = findings_summary()

    assert result["by_severity"] == {"ERROR": 2, "WARNING": 2}


@pytest.mark.integration
def test_reindex_findings_tool_returns_report(indexed_repo: Path) -> None:
    result = reindex_findings()

    assert result.scanned == 0
    assert result.findings_added == 0


@pytest.mark.integration
def test_reindex_findings_tool_refreshes_code_chunks_on_cocoindex_engine(
    indexed_repo: Path,
) -> None:
    """BACKLOG-16 P3 : un repo indexé avec `--engine cocoindex` doit voir
    ses chunks de code rafraîchis par `reindex_findings` (MCP), pas
    seulement ses findings — sinon `search` (MCP) sert un contenu de chunk
    périmé après modification d'un fichier."""
    reindex_result = runner.invoke(app, ["index", "--engine", "cocoindex"])
    assert reindex_result.exit_code == 0

    db_path = indexed_repo / "app" / "db.py"
    db_path.write_text(db_path.read_text() + "\n\ndef new_marker(): pass\n")

    result = reindex_findings()
    assert result.scanned == 1

    with Store(indexed_repo) as store:
        chunk_contents = "\n".join(
            chunk.content for chunk in store.all_code_chunks() if chunk.path == "app/db.py"
        )
    assert "new_marker" in chunk_contents


def test_search_findings_tool_on_unindexed_repo_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(RuntimeError, match="Index absent"):
        search_findings("injection sql")


def test_search_findings_tool_on_unindexed_repo_surfaces_as_mcp_tool_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Going through the actual MCP dispatch (not the bare Python function): a
    failing tool must raise ToolError, which the protocol layer turns into
    `isError: true` — not a `{"error": ...}` payload disguised as success."""
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ToolError, match="Index absent"):
        asyncio.run(mcp.call_tool("search_findings", {"query": "injection sql"}))

    # le serveur reste utilisable ensuite, sans crash
    with pytest.raises(ToolError):
        asyncio.run(mcp.call_tool("findings_summary", {}))


def test_mcp_help_documents_client_registration_block() -> None:
    result = runner.invoke(app, ["mcp", "--help"])

    assert result.exit_code == 0
    assert '{"mcpServers": {"cccr": {"command": "cccr", "args": ["mcp"]}}}' in result.output


@pytest.mark.integration


def test_graph_tool_returns_outbound_calls_in_kafka_consumer_handlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    consumer = MessageEndpoint(
        id=compute_endpoint_id(
            "consume", "orders.created", "app/OrderConsumer.java", 15, 25
        ),
        role="consume",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework=None,
        path="app/OrderConsumer.java",
        start_line=15,
        end_line=25,
        snippet="",
    )
    call = MessageEndpoint(
        id=compute_endpoint_id(
            "call", "POST /payments", "app/OrderConsumer.java", 20, 20
        ),
        role="call",
        system="rest",
        topic="POST /payments",
        topic_dynamic=False,
        source="code",
        framework="resttemplate",
        path="app/OrderConsumer.java",
        start_line=20,
        end_line=20,
        snippet="",
    )
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(["app/OrderConsumer.java"], [consumer, call])

    result = graph()

    assert result["services"] == []
    assert result["edges"] == []
    assert len(result["outbound_calls_in_consumers"]) == 1
    assert result["outbound_calls_in_consumers"][0]["call"]["topic"] == "POST /payments"


def test_dependency_graph_and_audit_tools_report_data_access_and_event_cycles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    order_producer = MessageEndpoint(
        compute_endpoint_id("produce", "orders.created", "orders/Publisher.java", 10, 10),
        "produce", "kafka", "orders.created", False, "code", "spring-kafka",
        "orders/Publisher.java", 10, 10, "", "orders", message_type="OrderCreated",
    )
    order_consumer = MessageEndpoint(
        compute_endpoint_id("consume", "payments.completed", "orders/Consumer.java", 10, 30),
        "consume", "kafka", "payments.completed", False, "code", "spring-kafka",
        "orders/Consumer.java", 10, 30, "", "orders", message_type="PaymentCompleted",
    )
    external_call = MessageEndpoint(
        compute_endpoint_id("call", "GET /ledger", "orders/Consumer.java", 20, 20),
        "call", "rest", "GET /ledger", False, "code", "resttemplate",
        "orders/Consumer.java", 20, 20, "", "orders",
    )
    payment_consumer = MessageEndpoint(
        compute_endpoint_id("consume", "orders.created", "payments/Consumer.java", 5, 15),
        "consume", "kafka", "orders.created", False, "code", "spring-kafka",
        "payments/Consumer.java", 5, 15, "", "payments", message_type="OrderCreated",
    )
    payment_producer = MessageEndpoint(
        compute_endpoint_id("produce", "payments.completed", "payments/Publisher.java", 20, 20),
        "produce", "kafka", "payments.completed", False, "code", "spring-kafka",
        "payments/Publisher.java", 20, 20, "", "payments", message_type="PaymentCompleted",
    )
    modules = [
        DiscoveredModule(
            "orders", tmp_path / "orders", "maven", None, "library", True, "",
            mongo_collections=("orders",),
            mongo_methods=(MongoMethod("save", "repository", "orders/Repository.java", 8, "orders"),),
        ),
        DiscoveredModule(
            "payments", tmp_path / "payments", "maven", None, "library", True, "",
            mongo_collections=("payments",),
            mongo_methods=(MongoMethod("find", "repository", "payments/Repository.java", 8, "payments"),),
        ),
    ]
    with Store(tmp_path) as store:
        store.replace_modules(modules)
        endpoints = [order_producer, order_consumer, external_call, payment_consumer, payment_producer]
        store.replace_endpoints_for_files([endpoint.path for endpoint in endpoints], endpoints)

    topology = dependency_graph()

    assert topology["summary"] == {
        "microservices": 2,
        "topics": 2,
        "mongodb_collections": 2,
        "external_apis": 1,
        "relations": 7,
        "configured_client_relations": 0,
    }
    assert {edge["kind"] for edge in topology["edges"]} == {
        "publishes", "consumes", "calls_external", "writes", "reads",
    }
    assert any(edge["label"] == "publishes OrderCreated" for edge in topology["edges"])
    assert any(node["id"] == "mongodb_collection:orders:orders" for node in topology["nodes"])

    audit = audit_dependency_graph()

    assert audit["cycles"][0]["services"] == ["orders", "payments"]
    assert {node["kind"] for node in audit["cycles"][0]["nodes"]} == {"microservice", "topic"}
    assert {issue["id"] for issue in audit["issues"]} >= {
        "event-dependency-cycle", "synchronous-http-in-kafka-consumer",
    }

    protocol_result = asyncio.run(mcp.call_tool("audit_dependency_graph", {}))
    assert protocol_result[1]["cycles"][0]["services"] == ["orders", "payments"]


def test_graph_tool_on_unindexed_repo_surfaces_as_mcp_tool_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ToolError, match="Index absent"):
        asyncio.run(mcp.call_tool("graph", {}))


def test_list_endpoints_tool_filters_by_role(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    consume = MessageEndpoint(
        id=compute_endpoint_id("consume", "orders.created", "app/Consumer.java", 7, 9),
        role="consume",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path="app/Consumer.java",
        start_line=7,
        end_line=9,
        snippet="",
    )
    call = MessageEndpoint(
        id=compute_endpoint_id("call", "POST /payments", "app/Consumer.java", 20, 20),
        role="call",
        system="rest",
        topic="POST /payments",
        topic_dynamic=False,
        source="code",
        framework="resttemplate",
        path="app/Consumer.java",
        start_line=20,
        end_line=20,
        snippet="",
    )
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(["app/Consumer.java"], [consume, call])

    result = list_endpoints(role="consume")

    assert len(result) == 1
    assert result[0]["topic"] == "orders.created"


def test_list_endpoints_tool_on_unindexed_repo_surfaces_as_mcp_tool_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ToolError, match="Index absent"):
        asyncio.run(mcp.call_tool("list_endpoints", {}))


def test_list_workspace_services_tool_discovers_and_flags_unindexed(tmp_path: Path) -> None:
    dest = tmp_path / "maven_workspace"
    shutil.copytree(MAVEN_WORKSPACE, dest)
    with Store(dest / "service-a"):
        pass

    result = list_workspace_services(str(dest))

    by_name = {s["name"]: s for s in result["services"]}
    assert by_name["order-service"]["indexed"] is True
    assert by_name["common-lib"]["kind"] == "shared-module"
    assert any("payment-service" in w for w in result["warnings"])


def test_list_workspace_services_tool_discovers_gradle_services(tmp_path: Path) -> None:
    project = tmp_path / "billing-service" / "billing-service-main"
    (project / "build.gradle").parent.mkdir(parents=True)
    (project / "build.gradle").write_text("archivesName = 'billing-service'\n")
    service = project / "src" / "main" / "java"
    service.mkdir(parents=True)
    (service / "BillingServiceMain.java").write_text(
        """
import org.springframework.boot.SpringApplication;

public class BillingServiceMain {
    public static void main(String[] args) {
        SpringApplication.run(BillingServiceMain.class, args);
    }
}
""".strip()
    )
    with Store(tmp_path):
        pass

    result = list_workspace_services(str(tmp_path))

    assert result["services"] == [
        {
            "name": "billing-service",
            "kind": "microservice",
            "starts_application": True,
            "indexed": True,
            "integration_count": 0,
            "finding_count": 0,
            "exposes_http_api": False,
            "http_apis_exposed": [],
            "http_apis_consumed": [],
            "kafka_topics_published": [],
            "kafka_topics_consumed": [],
            "kafka_message_types_published": {},
            "kafka_message_types_consumed": {},
            "mongo_collections": [],
            "openapi_files": [],
        }
    ]
    assert result["warnings"] == []


@pytest.mark.integration
def test_graph_tool_with_workspace_root_reports_a_real_cross_service_topology(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Le graphe MCP fédéré remonte bien la topologie inter-services."""
    from ccc_radar.cli import app as cli_app

    rest_cycle_workspace = FIXTURES_DIR / "rest_cycle_workspace"
    dest = tmp_path / "rest_cycle_workspace"
    shutil.copytree(rest_cycle_workspace, dest)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    for service in ("service-x", "service-y", "service-z"):
        monkeypatch.chdir(dest / service)
        runner.invoke(cli_app, ["init", "--rules", "rules/java.yaml"])
        index_result = runner.invoke(cli_app, ["index", "--semgrep"])
        assert index_result.exit_code == 0

    monkeypatch.chdir(dest / "service-x")
    result = graph(workspace_root=str(dest))

    assert set(result["services"]) == {"service-x", "service-y", "service-z"}
    assert len(result["edges"]) == 3
    assert {edge["label"] for edge in result["edges"]} == {
        "service-x: GET /x-status",
        "service-y: GET /y-status",
        "service-z: GET /z-status",
    }
    assert result["note"] == ""


@pytest.mark.integration
def test_graph_tool_keeps_available_rest_dependencies_when_workspace_is_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Les relations indexées restent visibles avec un avertissement explicite."""
    from ccc_radar.cli import app as cli_app

    rest_cycle_workspace = FIXTURES_DIR / "rest_cycle_workspace"
    dest = tmp_path / "rest_cycle_workspace"
    shutil.copytree(rest_cycle_workspace, dest)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    for service in ("service-x", "service-y"):
        monkeypatch.chdir(dest / service)
        assert runner.invoke(cli_app, ["init", "--rules", "rules/java.yaml"]).exit_code == 0
        assert runner.invoke(cli_app, ["index", "--semgrep"]).exit_code == 0

    monkeypatch.chdir(dest / "service-x")

    result = graph(workspace_root=str(dest))

    assert [(edge["from_node"], edge["to_node"], edge["label"]) for edge in result["edges"]] == [
        ("service-x", "service-y", "service-y: GET /y-status")
    ]
    assert "Dépendances inter-microservices partielles" in result["note"]
    assert "service-z" in result["note"]


@pytest.mark.integration
def test_dependency_graph_keeps_available_kafka_facts_when_workspace_is_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Un producteur indexé reste visible, même si le consommateur manque."""
    from ccc_radar.cli import app as cli_app

    kafka_workspace = FIXTURES_DIR / "kafka_workspace"
    dest = tmp_path / "kafka_workspace"
    shutil.copytree(kafka_workspace, dest)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    monkeypatch.chdir(dest / "order-service")
    assert runner.invoke(cli_app, ["init", "--rules", "rules/java.yaml"]).exit_code == 0
    assert runner.invoke(cli_app, ["index", "--semgrep"]).exit_code == 0

    result = dependency_graph(workspace_root=str(dest))

    assert [(edge["kind"], edge["label"]) for edge in result["edges"]] == [
        ("publishes", "publishes String")
    ]
    assert {node["id"] for node in result["nodes"]} == {
        "microservice:order-service",
        "topic:orders.created",
    }
    assert any("payment-service" in warning for warning in result["warnings"])
    assert any("Dépendances inter-microservices partielles" in warning for warning in result["warnings"])


def test_trace_message_flow_tool_lists_sites_with_overlapping_finding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    produce = MessageEndpoint(
        id=compute_endpoint_id("produce", "orders.created", "app/Producer.java", 10, 10),
        role="produce",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path="app/Producer.java",
        start_line=10,
        end_line=10,
        snippet="",
    )
    consume = MessageEndpoint(
        id=compute_endpoint_id("consume", "orders.created", "app/Consumer.java", 5, 7),
        role="consume",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path="app/Consumer.java",
        start_line=5,
        end_line=7,
        snippet="",
    )
    finding = Finding(
        id="finding-1",
        rule_id="cccr.demo.fire-and-forget",
        severity="WARNING",
        message="message",
        path="app/Producer.java",
        start_line=10,
        end_line=10,
        snippet="",
        fix=None,
        cwe=[],
        owasp=[],
    )
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(["app/Producer.java", "app/Consumer.java"], [produce, consume])
        store.replace_findings_for_files(["app/Producer.java"], [finding])
        store.set_meta("endpoint_inventory_signature", current_endpoint_inventory_signature())

    result = trace_message_flow("orders.created")

    assert result["resolved_topic"] == "orders.created"
    by_path = {site["path"]: site for site in result["sites"]}
    assert by_path["app/Producer.java"]["finding_rule_ids"] == ["cccr.demo.fire-and-forget"]
    assert by_path["app/Consumer.java"]["finding_rule_ids"] == []
    assert result["warnings"] == []


def test_trace_message_flow_tool_reports_stale_endpoint_inventory_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    produce = MessageEndpoint(
        id=compute_endpoint_id("produce", "orders.created", "app/Producer.java", 10, 10),
        role="produce",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path="app/Producer.java",
        start_line=10,
        end_line=10,
        snippet="",
    )
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(["app/Producer.java"], [produce])
        store.set_meta("endpoint_inventory_signature", "endpoint-inventory-v0")

    result = trace_message_flow("orders.created")

    assert any("inventaire des intégrations potentiellement obsolète" in w for w in result["warnings"])


def test_trace_message_flow_tool_falls_back_to_similarity_when_textual_resolution_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BACKLOG-10 K3 : substitue `resolve_topic_by_similarity` (déjà testée
    isolément avec des vecteurs contrôlés dans tests/test_flow.py) pour
    vérifier le câblage sans dépendre d'un embedder réel/faux sur du texte
    arbitraire."""
    import ccc_radar.mcp_server as mcp_server_module

    monkeypatch.chdir(tmp_path)
    produce = MessageEndpoint(
        id=compute_endpoint_id("produce", "orders.created", "app/Producer.java", 10, 10),
        role="produce",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path="app/Producer.java",
        start_line=10,
        end_line=10,
        snippet="",
    )
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(["app/Producer.java"], [produce])

    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    monkeypatch.setattr(
        mcp_server_module, "resolve_topic_by_similarity", lambda *a, **kw: "orders.created"
    )

    result = trace_message_flow("who creates an order")

    assert result["resolved_topic"] == "orders.created"


def test_trace_message_flow_tool_unknown_query_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with Store(tmp_path):
        pass

    with pytest.raises(FlowError):
        trace_message_flow("does-not-exist")


def test_trace_message_flow_tool_on_unindexed_repo_surfaces_as_mcp_tool_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ToolError, match="Index absent"):
        asyncio.run(mcp.call_tool("trace_message_flow", {"query": "orders.created"}))


def test_trace_message_flow_tool_unknown_query_surfaces_as_mcp_tool_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with Store(tmp_path):
        pass

    with pytest.raises(ToolError):
        asyncio.run(mcp.call_tool("trace_message_flow", {"query": "does-not-exist"}))


@pytest.mark.integration
def test_trace_message_flow_tool_with_workspace_root_traces_across_services(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BACKLOG-10 K6 : trace_message_flow(workspace_root=...) relie un
    producteur et un consommateur fédérés depuis deux services indexés
    séparément (même fixture que tests/test_k5_flow_e2e.py, côté CLI)."""
    kafka_workspace = FIXTURES_DIR / "kafka_workspace"
    dest = tmp_path / "kafka_workspace"
    shutil.copytree(kafka_workspace, dest)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    for service in ("order-service", "payment-service"):
        monkeypatch.chdir(dest / service)
        runner.invoke(app, ["init", "--rules", "rules/java.yaml"])
        index_result = runner.invoke(app, ["index", "--semgrep"])
        assert index_result.exit_code == 0

    monkeypatch.chdir(dest / "order-service")
    result = trace_message_flow("orders.created", workspace_root=str(dest))

    services = {site["service"] for site in result["sites"]}
    assert services == {"order-service", "payment-service"}
