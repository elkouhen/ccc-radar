from pathlib import Path

from systemlens.dependency_analysis import build_dependency_graph
from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.modules import DiscoveredModule
from systemlens.scanner import infer_framework_endpoints


def make_endpoint(
    role: str,
    topic: str,
    path: str,
    start_line: int = 1,
    end_line: int = 1,
    *,
    module: str | None = None,
    snippet: str = "",
    topic_dynamic: bool = False,
    system: str = "rest",
) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, path, start_line, end_line),
        role=role,
        system=system,
        topic=topic,
        topic_dynamic=topic_dynamic,
        source="code",
        framework=None,
        path=path,
        start_line=start_line,
        end_line=end_line,
        snippet=snippet,
        module=module,
    )


def _module(name: str) -> DiscoveredModule:
    return DiscoveredModule(
        name=name,
        path=Path(f"/repo/{name}"),
        build_system="maven",
        version=None,
        kind="microservice",
        starts_application=True,
        configuration_example="",
    )


def _client_calls(edges):
    return [e for e in edges if e["kind"] == "http" and e["label"].endswith(": API")]


def test_configured_client_relation_emitted_when_host_in_endpoints() -> None:
    caller = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/Client.java",
        module="caller-service",
        snippet="annuaireApi.getDirectory()\nsystemlens-api-domain:annuaire",
        topic_dynamic=True,
    )
    host = make_endpoint("serve", "GET /annuaire", "annuaire/Controller.java", module="annuaire")

    result = build_dependency_graph(
        {"caller-service": [caller], "annuaire": [host]},
        {},
    )

    client_edges = _client_calls(result["edges"])
    assert len(client_edges) == 1
    assert client_edges[0]["source"] == "microservice:caller-service"
    assert client_edges[0]["target"] == "microservice:annuaire"
    assert result["summary"]["configured_client_relations"] == 1
    # Aucune arête par route fabriquée ni de pollution calls_external.
    assert not [e for e in result["edges"] if e["kind"] == "http" and e["label"] != "annuaire: API"]
    assert not [e for e in result["edges"] if e["kind"] == "calls_external"]


def test_configured_client_dependency_from_bean_declaration_without_resolved_type() -> None:
    """La déclaration du bean (RestConfiguration) suffit à établir A→B, même si
    aucun site d'appel ne résout le type de l'API consommée.

    Un seul endpoint « bean » porteur du domaine, aucun appel résolu : on veut
    néanmoins la dépendance service A consomme API service B.
    """
    bean_declaration = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/RestConfiguration.java",
        module="caller-service",
        snippet=(
            "return webClientHelper.createInternalClientApi("
            "ApiDomains.DOMAIN_ANNUAIRE, AnnuaireApi.class);"
            "\nsystemlens-api-domain:domain-annuaire"
        ),
        topic_dynamic=True,
    )

    result = build_dependency_graph(
        {"caller-service": [bean_declaration]},
        {"domain-annuaire": _module("domain-annuaire")},
    )

    client_edges = _client_calls(result["edges"])
    assert len(client_edges) == 1
    assert client_edges[0]["source"] == "microservice:caller-service"
    assert client_edges[0]["target"] == "microservice:domain-annuaire"
    assert result["warnings"] == []


def test_rest_configuration_bean_links_to_the_normalized_host_microservice(tmp_path: Path) -> None:
    """Le domaine `DOMAIN_ANNUAIRE` désigne bien `domain-annuaire`.

    Ce test couvre la chaîne réelle scanner → fédération → dépendance, plutôt
    qu'un marqueur `systemlens-api-domain` construit à la main.
    """
    (tmp_path / "pom.xml").write_text(
        "<project><artifactId>caller-service</artifactId><version>1</version></project>"
    )
    config = tmp_path / "src" / "main" / "java" / "RestConfiguration.java"
    config.parent.mkdir(parents=True)
    config.write_text(
        "import org.springframework.context.annotation.Bean;\n"
        "class RestConfiguration {\n"
        "  WebClientHelper webClientHelper;\n"
        "  @Bean\n"
        "  AnnuaireApi annuaireApi() {\n"
        "    return webClientHelper.createInternalClientApi(ApiDomains.DOMAIN_ANNUAIRE, AnnuaireApi.class);\n"
        "  }\n"
        "}\n"
    )
    rel_path = config.relative_to(tmp_path).as_posix()

    endpoints = infer_framework_endpoints(
        tmp_path, files=[rel_path], configured_api_client_strategy1=True
    )
    result = build_dependency_graph(
        {"caller-service": endpoints},
        {"domain-annuaire": _module("domain-annuaire")},
    )

    assert [(edge["source"], edge["target"], edge["label"]) for edge in _client_calls(result["edges"])] == [
        ("microservice:caller-service", "microservice:domain-annuaire", "domain-annuaire: API")
    ]


def test_rest_configuration_bean_resolves_domain_key_in_uri_path(tmp_path: Path) -> None:
    """La variante `getUriPath(..., DOMAIN_*.getKey())` déclare A→B."""
    (tmp_path / "pom.xml").write_text(
        "<project><artifactId>caller-service</artifactId><version>1</version></project>"
    )
    config = tmp_path / "src" / "main" / "java" / "RestClientConfig.java"
    config.parent.mkdir(parents=True)
    config.write_text(
        "import org.springframework.context.annotation.Bean;\n"
        "class RestClientConfig {\n"
        "  WebClientHelper webClientHelper;\n"
        "  @Bean\n"
        "  AnnuaireApi annuaireApi() {\n"
        "    return webClientHelper.createClientApi(\n"
        "      getUriPath(\"/internal\", ApiDomains.DOMAIN_ANNUAIRE.getKey()), AnnuaireApi.class);\n"
        "  }\n"
        "}\n"
    )
    rel_path = config.relative_to(tmp_path).as_posix()

    endpoints = infer_framework_endpoints(
        tmp_path, files=[rel_path], configured_api_client_strategy1=True
    )
    result = build_dependency_graph(
        {"caller-service": endpoints},
        {"domain-annuaire": _module("domain-annuaire")},
    )

    assert len(endpoints) == 1
    assert "systemlens-api-domain:domain-annuaire" in endpoints[0].snippet
    assert [(edge["source"], edge["target"], edge["label"]) for edge in _client_calls(result["edges"])] == [
        ("microservice:caller-service", "microservice:domain-annuaire", "domain-annuaire: API")
    ]


def test_configured_client_relation_when_host_known_via_modules_only() -> None:
    caller = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/Client.java",
        module="caller-service",
        snippet="annuaireApi.get()\nsystemlens-api-domain:annuaire",
        topic_dynamic=True,
    )

    result = build_dependency_graph(
        {"caller-service": [caller]},
        {"annuaire": _module("annuaire")},
    )

    client_edges = _client_calls(result["edges"])
    assert len(client_edges) == 1
    assert client_edges[0]["target"] == "microservice:annuaire"
    # Le nœud hôte existe même sans aucun endpoint détecté.
    assert any(n["id"] == "microservice:annuaire" for n in result["nodes"])
    assert result["warnings"] == []


def test_configured_client_relation_does_not_require_a_detected_published_resource() -> None:
    """Le service cible peut être indexé sans endpoint REST `serve` détecté."""
    caller = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/Client.java",
        module="caller-service",
        snippet="annuaireApi.get()\nsystemlens-api-domain:domain-annuaire",
        topic_dynamic=True,
    )

    result = build_dependency_graph(
        {"caller-service": [caller], "domain-annuaire": []},
        {},
    )

    assert [(edge["source"], edge["target"], edge["label"]) for edge in _client_calls(result["edges"])] == [
        ("microservice:caller-service", "microservice:domain-annuaire", "domain-annuaire: API")
    ]
    assert result["warnings"] == []


def test_unresolved_configured_client_domain_emits_warning() -> None:
    caller = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/Client.java",
        module="caller-service",
        snippet="ghostApi.get()\nsystemlens-api-domain:ghost",
        topic_dynamic=True,
    )

    result = build_dependency_graph({"caller-service": [caller]}, {})

    assert _client_calls(result["edges"]) == []
    assert not [e for e in result["edges"] if e["kind"] == "calls_external"]
    assert any("ghost" in warning for warning in result["warnings"])
    assert result["summary"]["configured_client_relations"] == 0


def test_targetless_rest_call_is_reported_as_external_not_an_internal_relation() -> None:
    caller = make_endpoint("call", "GET /health", "caller/Client.java", module="caller")
    unrelated = make_endpoint(
        "serve", "GET /health", "unrelated/HealthController.java", module="unrelated"
    )

    result = build_dependency_graph(
        {"caller": [caller], "unrelated": [unrelated]}, {}
    )

    assert not [edge for edge in result["edges"] if edge["kind"] == "http"]
    assert [(edge["source"], edge["target"], edge["kind"]) for edge in result["edges"] if edge["kind"] == "calls_external"] == [
        ("microservice:caller", "external_api:GET /health", "calls_external")
    ]


def test_dynamic_kafka_topics_remain_service_scoped_unresolved_evidence() -> None:
    producer = make_endpoint(
        "produce", "<dynamic>", "orders/Publisher.java", module="orders",
        system="kafka", topic_dynamic=True,
    )
    consumer = make_endpoint(
        "consume", "<dynamic>", "payments/Listener.java", module="payments",
        system="kafka", topic_dynamic=True,
    )

    result = build_dependency_graph({"orders": [producer], "payments": [consumer]}, {})

    dynamic_topics = [node for node in result["nodes"] if node["kind"] == "topic"]
    assert {node["id"] for node in dynamic_topics} == {
        "topic:orders:<dynamic>", "topic:payments:<dynamic>",
    }
    assert not any(edge["source"] == "topic:orders:<dynamic>" for edge in result["edges"])


def test_external_rest_api_properties_client_creates_an_annotated_microservice() -> None:
    caller = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/RestPartnerConfig.java",
        module="caller-service",
        snippet="systemlens-external-microservice:partner-catalog",
        topic_dynamic=True,
    )

    result = build_dependency_graph({"caller-service": [caller]}, {})

    assert (
        "microservice:caller-service",
        "microservice:partner-catalog",
        "partner-catalog: API",
    ) in {(edge["source"], edge["target"], edge["label"]) for edge in result["edges"]}
    assert next(node for node in result["nodes"] if node["id"] == "microservice:partner-catalog")["external"] is True


def test_multiple_configured_call_sites_collapse_to_single_relation() -> None:
    call_a = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/A.java",
        5,
        5,
        module="caller-service",
        snippet="annuaireApi.a()\nsystemlens-api-domain:annuaire",
        topic_dynamic=True,
    )
    call_b = make_endpoint(
        "call",
        "ANY <dynamic>",
        "caller/B.java",
        9,
        9,
        module="caller-service",
        snippet="annuaireApi.b()\nsystemlens-api-domain:annuaire",
        topic_dynamic=True,
    )
    host = make_endpoint("serve", "GET /annuaire", "annuaire/Controller.java", module="annuaire")

    result = build_dependency_graph(
        {"caller-service": [call_a, call_b], "annuaire": [host]},
        {},
    )

    assert len(_client_calls(result["edges"])) == 1
