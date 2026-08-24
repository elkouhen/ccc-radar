from http import HTTPStatus
from pathlib import Path

from systemlens.architecture_inventory import AnalysisProfile, ArchitectureInventory
from systemlens.models import ArchitectureRelation, MessageEndpoint
from systemlens.web import SystemLensWebApplication


def _application() -> SystemLensWebApplication:
    return SystemLensWebApplication(
        Path.cwd(),
        since="1h",
        environment=None,
        endpoint=None,
        api_key=None,
        insecure_tls=False,
        max_services=30,
        max_transactions=50,
        max_dependencies=80,
        max_buckets=1_000,
        max_timeline_events=500,
    )


def test_web_home_links_the_two_existing_views() -> None:
    status, document = _application().document("/")

    assert status is HTTPStatus.OK
    assert 'href="/architecture"' in document
    assert 'href="/runtime"' in document


def test_web_returns_a_safe_not_found_document() -> None:
    status, document = _application().document("/not-a-systemlens-route")

    assert status is HTTPStatus.NOT_FOUND
    assert "Page not found" in document
    assert "/not-a-systemlens-route" not in document


def test_web_offers_to_create_a_missing_architecture_index(tmp_path: Path) -> None:
    application = SystemLensWebApplication(
        tmp_path,
        since="1h",
        environment=None,
        endpoint=None,
        api_key=None,
        insecure_tls=False,
        max_services=30,
        max_transactions=50,
        max_dependencies=80,
        max_buckets=1_000,
        max_timeline_events=500,
    )

    status, document = application.document("/architecture")

    assert status is HTTPStatus.OK
    assert 'method="post" action="/architecture/index"' in document
    assert "Create architecture index" in document


def test_web_creates_a_missing_architecture_index(tmp_path: Path) -> None:
    application = SystemLensWebApplication(
        tmp_path,
        since="1h",
        environment=None,
        endpoint=None,
        api_key=None,
        insecure_tls=False,
        max_services=30,
        max_transactions=50,
        max_dependencies=80,
        max_buckets=1_000,
        max_timeline_events=500,
    )

    status, document = application.post("/architecture/index")

    assert status is HTTPStatus.OK
    assert (tmp_path / ".systemlens" / "findings.db").is_file()
    assert "Architecture index not found" not in document


def test_web_architecture_excludes_edges_to_test_microservices(
    tmp_path: Path, monkeypatch
) -> None:
    endpoint = MessageEndpoint(
        id="caller", role="call", system="rest", topic="GET /health",
        topic_dynamic=False, source="code", framework="feign",
        path="src/main/java/Client.java", start_line=10, end_line=10,
        snippet="", module="caller-service",
    )
    relation = ArchitectureRelation(
        id="caller-to-test", source_kind="microservice", source_name="caller-service",
        relation="calls_service", target_kind="microservice",
        target_name="test-integration-microservice", origin="code", confidence="high",
        module="caller-service", path=endpoint.path, start_line=endpoint.start_line,
    )
    inventory = ArchitectureInventory(
        endpoints_by_service={
            "caller-service": [endpoint],
            "test-integration-microservice": [],
        },
        endpoints_by_module={}, findings_by_service={}, endpoints=[endpoint], findings=[],
        modules=[], modules_by_service={}, module_dependencies=[], relations=[relation],
        diagnostics=[], warnings=[], source_roots=[], profile=AnalysisProfile(),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr("systemlens.web.db_path", lambda _root: tmp_path / "findings.db")
    (tmp_path / "findings.db").touch()
    monkeypatch.setattr("systemlens.web.load_architecture_inventory", lambda *_args, **_kwargs: inventory)
    monkeypatch.setattr(
        "systemlens.web.render_graph_html",
        lambda services, edges, *_args, **_kwargs: captured.update(services=services, edges=edges) or "<html>",
    )

    status, _document = _application()._architecture_document()

    assert status is HTTPStatus.OK
    assert captured["services"] == {"caller-service": [endpoint]}
    assert captured["edges"] == []
