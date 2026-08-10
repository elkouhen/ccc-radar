from pathlib import Path

import pytest

from ccc_radar.config import Config
from ccc_radar.graph import build_graph
from ccc_radar.scanner import (
    infer_framework_endpoints,
)
from ccc_radar.semgrep import SemgrepError, parse_semgrep_endpoints, run_semgrep_endpoints

# Le pack de règles vit dans le repo skill (ccc-radar-skill/skills/cccr/
# rules/rest/), pas dans ce repo (ADR-24). Les fixtures ci-dessous sont une
# copie de test tenue à jour manuellement avec cette source.
#
# Cible d'analyse : Java + Spring + Maven uniquement (pas de pack Python).
FIXTURES_DIR = Path(__file__).parent / "fixtures"
REST_REPO = FIXTURES_DIR / "rest_repo"


def make_config(**overrides: object) -> Config:
    defaults: dict = {"rules": ["rules/java.yaml"]}
    defaults.update(overrides)
    return Config(**defaults)


@pytest.mark.integration
def test_java_server_routes_extract_role_method_and_literal_path() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/OrderController.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    get_route = by_line[8]
    assert get_route.role == "serve"
    assert get_route.system == "rest"
    assert get_route.framework == "spring"
    assert get_route.topic == "GET /orders/{id}"
    assert get_route.topic_dynamic is False
    assert get_route.source == "code"

    assert by_line[13].topic == "POST /orders"
    assert by_line[18].topic == "PUT /orders/{id}"
    assert by_line[23].topic == "DELETE /orders/{id}"
    assert by_line[27].topic == "PATCH /orders/{id}/status"
    # @RequestMapping(value = "...", method = RequestMethod.GET) reconnu
    # comme GET au même titre que @GetMapping
    assert by_line[32].topic == "GET /orders/{id}/summary"
    # @RequestMapping(..., method = RequestMethod.{PUT,DELETE,PATCH}) : mêmes
    # verbes non-GET reconnus au même titre que les annotations dédiées
    assert by_line[37].topic == "PUT /orders/{id}/cancel"
    assert by_line[42].topic == "DELETE /orders/{id}/archive"
    assert by_line[46].topic == "PATCH /orders/{id}/pause"


@pytest.mark.integration
def test_java_client_calls_flag_concatenated_url_as_dynamic() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/OrderClient.java"]
    )

    by_line = {e.start_line: e for e in endpoints}

    concatenated = by_line[14]  # getForObject("http://.../orders/" + id, ...)
    assert concatenated.role == "call"
    assert concatenated.framework == "resttemplate"
    assert concatenated.topic_dynamic is True
    assert concatenated.topic == "GET /orders/"

    literal = by_line[18]  # postForObject("http://.../orders", order, ...)
    assert literal.topic == "POST /orders"
    assert literal.topic_dynamic is False

    # put/delete concatènent aussi l'id : dynamiques, comme getForObject
    assert by_line[22].topic_dynamic is True
    assert by_line[26].topic_dynamic is True


@pytest.mark.integration
def test_java_class_level_request_mapping_prefix_is_merged_into_method_path() -> None:
    # BACKLOG Q24 : @RequestMapping("/owners") sur la classe, fusionné avec
    # le chemin (ou l'absence de chemin) de chaque méthode annotée.
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/OwnerController.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    assert len(endpoints) == 4

    # @PostMapping seul (pas de valeur explicite) : hérite entièrement du
    # préfixe de classe, ce n'est plus "<dynamic>".
    create = by_line[9]
    assert create.topic == "POST /owners"
    assert create.topic_dynamic is False

    # @GetMapping("/{ownerId}") : préfixe de classe + chemin méthode fusionnés.
    find_one = by_line[14]
    assert find_one.topic == "GET /owners/{ownerId}"
    assert find_one.topic_dynamic is False

    # @GetMapping seul : même cas que create, sur GET.
    find_all = by_line[19]
    assert find_all.topic == "GET /owners"
    assert find_all.topic_dynamic is False

    # @RequestMapping(method = ..., value = "/{ownerId}") : la forme
    # générique avec value= explicite fusionne aussi correctement.
    update = by_line[24]
    assert update.topic == "PUT /owners/{ownerId}"
    assert update.topic_dynamic is False


@pytest.mark.integration
def test_java_generic_request_mapping_without_http_method_is_inferred_as_any() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/RootController.java"]
    )

    assert [endpoint.topic for endpoint in endpoints] == ["ANY /"]
    assert endpoints[0].framework == "spring"
    assert endpoints[0].role == "serve"


@pytest.mark.integration
def test_framework_generated_endpoints_are_inferred() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO,
        make_config(),
        files=[
            "app/java/OrderRepository.java",
            "app/java/SwaggerConfig.java",
            "app/resources/application.properties",
        ],
    )

    topics = {endpoint.topic: endpoint.framework for endpoint in endpoints}
    assert topics["GET /order"] == "spring-data-rest"
    assert topics["POST /order"] == "spring-data-rest"
    assert topics["GET /order/{id}"] == "spring-data-rest"
    assert topics["PUT /order/{id}"] == "spring-data-rest"
    assert topics["PATCH /order/{id}"] == "spring-data-rest"
    assert topics["DELETE /order/{id}"] == "spring-data-rest"
    assert topics["GET /swagger-ui.html"] == "swagger-ui"
    assert topics["GET /actuator/**"] == "spring-actuator"


def test_spring_data_rest_default_path_when_data_rest_present(tmp_path: Path) -> None:
    """Un repository Spring Data sans `path` ni `exported=false` est auto-exposé
    par Spring Data REST sur `/<entité-pluriel>`, mais seulement si le module
    déclare `spring-boot-starter-data-rest`. Régression pour `microservices-kafka-mq`
    où `UserRepository extends JpaRepository<User, ...>` (sans annotation) expose `/users`."""
    module = tmp_path / "microservice-order"
    java_dir = module / "src" / "main" / "java" / "de" / "f"
    java_dir.mkdir(parents=True)
    (module / "pom.xml").write_text(
        "<project><artifactId>microservice-order</artifactId>"
        "<dependencies>"
        "<dependency><groupId>org.springframework.boot</groupId>"
        "<artifactId>spring-boot-starter-data-rest</artifactId></dependency>"
        "</dependencies></project>"
    )
    repo = java_dir / "UserRepository.java"
    repo.write_text(
        "package de.f;\n"
        "import org.springframework.data.jpa.repository.JpaRepository;\n"
        "import org.springframework.stereotype.Repository;\n"
        "@Repository\n"
        "public interface UserRepository extends JpaRepository<User, Integer> {}\n"
    )
    rel = repo.relative_to(tmp_path).as_posix()

    endpoints = infer_framework_endpoints(tmp_path, files=[rel])

    assert {endpoint.topic for endpoint in endpoints} == {
        "GET /users", "POST /users", "GET /users/{id}",
        "PUT /users/{id}", "PATCH /users/{id}", "DELETE /users/{id}",
    }
    assert all(endpoint.framework == "spring-data-rest" for endpoint in endpoints)


def test_spring_data_rest_default_path_suppressed_without_data_rest(tmp_path: Path) -> None:
    """Sans `spring-boot-starter-data-rest`, un repository JPA sans annotation
    n'est PAS exposé (garde-fou faux positif, ex. `InvoiceRepository` côté invoicing)."""
    module = tmp_path / "microservice-invoicing"
    java_dir = module / "src" / "main" / "java" / "de" / "f"
    java_dir.mkdir(parents=True)
    (module / "pom.xml").write_text(
        "<project><artifactId>microservice-invoicing</artifactId>"
        "<dependencies>"
        "<dependency><groupId>org.springframework.boot</groupId>"
        "<artifactId>spring-boot-starter-data-jpa</artifactId></dependency>"
        "</dependencies></project>"
    )
    repo = java_dir / "InvoiceRepository.java"
    repo.write_text(
        "package de.f;\n"
        "import org.springframework.data.repository.PagingAndSortingRepository;\n"
        "public interface InvoiceRepository extends PagingAndSortingRepository<Invoice, Long> {}\n"
    )
    rel = repo.relative_to(tmp_path).as_posix()

    assert infer_framework_endpoints(tmp_path, files=[rel]) == []


def test_spring_data_rest_exported_false_suppresses_default_path(tmp_path: Path) -> None:
    """`@RepositoryRestResource(exported = false)` supprime l'exposition même
    avec data-rest présent."""
    module = tmp_path / "m"
    java_dir = module / "src" / "main" / "java"
    java_dir.mkdir(parents=True)
    (module / "pom.xml").write_text(
        "<project><dependencies>"
        "<dependency><groupId>org.springframework.boot</groupId>"
        "<artifactId>spring-boot-starter-data-rest</artifactId></dependency>"
        "</dependencies></project>"
    )
    repo = java_dir / "CustomerRepository.java"
    repo.write_text(
        "package m;\n"
        "import org.springframework.data.repository.PagingAndSortingRepository;\n"
        "import org.springframework.data.rest.core.annotation.RepositoryRestResource;\n"
        "@RepositoryRestResource(exported = false)\n"
        "public interface CustomerRepository extends PagingAndSortingRepository<Customer, Long> {}\n"
    )
    rel = repo.relative_to(tmp_path).as_posix()

    assert infer_framework_endpoints(tmp_path, files=[rel]) == []


def test_framework_inferences_use_java_ast_for_multiline_annotations(tmp_path: Path) -> None:
    """Comments and line breaks must not be interpreted as Java declarations."""
    source = tmp_path / "src" / "main" / "java" / "InventoryController.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        """
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

// @RequestMapping("/comment-only")
@RequestMapping(
    path = "/inventory"
)
class InventoryController {
  @RequestMapping(
      produces = "application/json"
  )
  Object list(@RequestParam(
      name = "page"
  ) int page) { return null; }
}
""".strip()
    )

    endpoints = infer_framework_endpoints(tmp_path, files=[source.relative_to(tmp_path).as_posix()])

    assert [(endpoint.topic, endpoint.framework) for endpoint in endpoints] == [
        ("ANY /inventory?page", "spring")
    ]


def test_openapi_contract_operations_are_inferred_with_contract_evidence(tmp_path: Path) -> None:
    contract = tmp_path / "src" / "main" / "resources" / "openapi.yml"
    contract.parent.mkdir(parents=True)
    contract.write_text(
        "openapi: 3.0.0\npaths:\n  /orders:\n    get:\n      responses: {}\n"
        "    post:\n      responses: {}\n  /orders/{id}:\n    delete:\n      responses: {}\n"
    )

    endpoints = infer_framework_endpoints(tmp_path, files=["src/main/resources/openapi.yml"])

    assert {(endpoint.topic, endpoint.framework, endpoint.path, endpoint.start_line) for endpoint in endpoints} == {
        ("GET /orders", "openapi", "src/main/resources/openapi.yml", 4),
        ("POST /orders", "openapi", "src/main/resources/openapi.yml", 6),
        ("DELETE /orders/{id}", "openapi", "src/main/resources/openapi.yml", 9),
    }


def test_strategy1_openapi_declaration_publishes_model_contract_from_single_input_spec(
    tmp_path: Path,
) -> None:
    service = tmp_path / "domain-annuaire"
    declaration = service / "src" / "main" / "resources" / "openapi" / "annuaire.rest"
    declaration.parent.mkdir(parents=True)
    declaration.write_text("publication annuaire\n")
    (service / "pom.xml").write_text("<project><artifactId>domain-annuaire</artifactId></project>")
    model = tmp_path / "model-shared-contracts"
    contract = model / "src" / "main" / "openapi" / "annuaire.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text(
        "openapi: 3.0.0\npaths:\n  /directory:\n    get:\n      responses: {}\n"
        "    post:\n      responses: {}\n"
    )
    (model / "pom.xml").write_text(
        "<project><artifactId>model-shared-contracts</artifactId><build><plugins><plugin>"
        "<artifactId>openapi-generator-maven-plugin</artifactId><configuration>"
        "<inputSpec>${project.basedir}/src/main/openapi/annuaire.yaml</inputSpec>"
        "</configuration></plugin></plugins></build></project>"
    )
    rel_path = declaration.relative_to(tmp_path).as_posix()

    assert infer_framework_endpoints(tmp_path, files=[rel_path]) == []

    endpoints = infer_framework_endpoints(
        tmp_path, files=[rel_path], configured_api_client_strategy1=True
    )

    assert {(endpoint.role, endpoint.topic, endpoint.framework, endpoint.path, endpoint.module) for endpoint in endpoints} == {
        ("serve", "GET /directory", "openapi", rel_path, "domain-annuaire"),
        ("serve", "POST /directory", "openapi", rel_path, "domain-annuaire"),
    }
    assert all("cccr-openapi-contract:model-shared-contracts/src/main/openapi/annuaire.yaml" in endpoint.snippet for endpoint in endpoints)


def test_strategy1_configured_client_consumes_api_published_through_model_contract(
    tmp_path: Path,
) -> None:
    publisher = tmp_path / "domain-annuaire"
    declaration = publisher / "src" / "main" / "resources" / "openapi" / "annuaire.rest"
    declaration.parent.mkdir(parents=True)
    declaration.write_text("publication annuaire\n")
    (publisher / "pom.xml").write_text("<project><artifactId>domain-annuaire</artifactId></project>")
    model = tmp_path / "model-annuaire"
    contract = model / "src" / "main" / "openapi" / "annuaire.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("openapi: 3.0.0\npaths:\n  /directory:\n    get:\n      responses: {}\n")
    (model / "pom.xml").write_text(
        "<project><artifactId>model-annuaire</artifactId><build><plugins><plugin>"
        "<artifactId>openapi-generator-maven-plugin</artifactId><configuration>"
        "<inputSpec>${project.basedir}/src/main/openapi/annuaire.yaml</inputSpec>"
        "</configuration></plugin></plugins></build></project>"
    )
    caller = tmp_path / "caller-service"
    config = caller / "src" / "main" / "java" / "RestAnnuaireConfig.java"
    config.parent.mkdir(parents=True)
    config.write_text("class RestAnnuaireConfig { Object api = DOMAIN_ANNUAIRE; }\n")
    (caller / "pom.xml").write_text("<project><artifactId>caller-service</artifactId></project>")

    endpoints = infer_framework_endpoints(tmp_path, configured_api_client_strategy1=True)
    endpoints_by_service = {
        service: [endpoint for endpoint in endpoints if endpoint.module == service]
        for service in ("caller-service", "domain-annuaire")
    }
    edges = build_graph(endpoints_by_service)

    assert [(edge.from_service, edge.to_service, edge.from_endpoint.topic, edge.to_endpoint.topic) for edge in edges] == [
        ("caller-service", "domain-annuaire", "GET /directory", "GET /directory")
    ]


def test_openapi_generator_pom_points_to_authoritative_contract_for_rest_controller(tmp_path: Path) -> None:
    controller = tmp_path / "src" / "main" / "java" / "OrdersApiController.java"
    controller.parent.mkdir(parents=True)
    controller.write_text(
        "import org.springframework.web.bind.annotation.RestController;\n"
        "@RestController\n"
        "class OrdersApiController implements OrdersApi {}\n"
    )
    contract = tmp_path / "src" / "main" / "openapi" / "published-api.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text(
        "openapi: 3.0.0\npaths:\n  /orders:\n    get:\n      responses: {}\n"
        "  /orders/{id}:\n    patch:\n      responses: {}\n"
    )
    (tmp_path / "pom.xml").write_text(
        "<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
        "<modelVersion>4.0.0</modelVersion>"
        "<artifactId>orders-api</artifactId>"
        "<build><plugins><plugin>"
        "<groupId>org.openapitools</groupId>"
        "<artifactId>openapi-generator-maven-plugin</artifactId>"
        "<executions><execution><goals><goal>generate</goal></goals>"
        "<configuration><inputSpec>${project.basedir}/src/main/openapi/published-api.yaml</inputSpec></configuration>"
        "</execution></executions>"
        "</plugin></plugins></build></project>"
    )

    endpoints = infer_framework_endpoints(tmp_path, files=["pom.xml"])

    assert {(endpoint.topic, endpoint.framework, endpoint.path) for endpoint in endpoints} == {
        ("GET /orders", "openapi", "src/main/openapi/published-api.yaml"),
        ("PATCH /orders/{id}", "openapi", "src/main/openapi/published-api.yaml"),
    }


def test_strategy1_openapi_generator_selects_named_contract_in_input_spec_root_directory(
    tmp_path: Path,
) -> None:
    """A declaration selects its named contract from a ``model-*`` directory."""
    service = tmp_path / "domain-annuaire"
    declaration = service / "src" / "main" / "resources" / "openapi" / "annuaire.rest"
    declaration.parent.mkdir(parents=True)
    declaration.write_text("publication annuaire\n")
    (service / "pom.xml").write_text("<project><artifactId>domain-annuaire</artifactId></project>")
    model = tmp_path / "model-annuaire"
    specs = model / "src" / "main" / "resources" / "openapi"
    specs.mkdir(parents=True)
    (specs / "annuaire.yaml").write_text(
        "openapi: 3.0.0\npaths:\n  /annuaire:\n    get:\n      responses: {}\n"
    )
    (specs / "billing.yaml").write_text(
        "openapi: 3.0.0\npaths:\n  /billing:\n    post:\n      responses: {}\n"
    )
    (model / "pom.xml").write_text(
        "<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
        "<modelVersion>4.0.0</modelVersion><artifactId>model-annuaire</artifactId>"
        "<build><plugins><plugin><groupId>org.openapitools</groupId>"
        "<artifactId>openapi-generator-maven-plugin</artifactId>"
        "<configuration><inputSpecRootDirectory>"
        "${project.basedir}/src/main/resources/openapi"
        "</inputSpecRootDirectory></configuration>"
        "</plugin></plugins></build></project>"
    )

    declaration_path = declaration.relative_to(tmp_path).as_posix()
    assert infer_framework_endpoints(tmp_path, files=[declaration_path]) == []

    endpoints = infer_framework_endpoints(
        tmp_path, files=[declaration_path], configured_api_client_strategy1=True
    )

    assert {(endpoint.topic, endpoint.path, endpoint.framework) for endpoint in endpoints} == {
        ("GET /annuaire", declaration_path, "openapi"),
    }


def test_java_client_call_with_variable_base_extracts_literal_suffix_as_dynamic() -> None:
    # getForObject(base + "/orders/" + id, ...) : premier littéral trouvé
    # au milieu de l'expression, toujours marqué dynamique (concaténation).
    raw = """
    {"results": [{
        "check_id": "rules.cccr.rest.java.call-get",
        "path": "app/java/OrderClient.java",
        "start": {"line": 31}, "end": {"line": 31},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "role": "call", "http_method": "GET",
                                "framework": "resttemplate"}}
    }]}
    """
    endpoints = parse_semgrep_endpoints(raw, REST_REPO)

    assert endpoints[0].topic == "GET /orders/"
    assert endpoints[0].topic_dynamic is True


def test_restclient_concatenation_preserves_path_variable_and_framework(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "CustomerClient.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        "import org.springframework.web.client.RestClient;\n"
        "class CustomerClient {\n"
        "  RestClient client;\n"
        "  void add(int ownerId) {\n"
        "    client.post()\n"
        "      .uri(getCustomerServiceUri() + \"/owners/\" + ownerId + \"/pets\")\n"
        "      .retrieve();\n"
        "  }\n"
        "}\n"
    )
    raw = """
    {"results": [{
        "check_id": "rules.cccr.rest.java.webclient-post",
        "path": "src/main/java/CustomerClient.java",
        "start": {"line": 5}, "end": {"line": 7},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "role": "call", "http_method": "POST",
                                "framework": "webclient"}}
    }]}
    """

    endpoints = parse_semgrep_endpoints(raw, tmp_path)

    assert endpoints[0].framework == "restclient"
    assert endpoints[0].topic == "POST /owners/{ownerId}/pets"
    assert endpoints[0].topic_dynamic is True


def test_regular_semgrep_call_does_not_infer_a_configured_client_domain(tmp_path: Path) -> None:
    """La stratégie simplifiée ne relie plus les sites d'appel aux beans REST."""
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
    client = tmp_path / "src" / "main" / "java" / "OrderClient.java"
    client.write_text(
        "interface AnnuaireApi { Object getDirectory(); }\n"
        "class OrderClient {\n"
        "  private final AnnuaireApi annuaireApi;\n"
        "  OrderClient(AnnuaireApi annuaireApi) { this.annuaireApi = annuaireApi; }\n"
        "  Object get() { return annuaireApi.getDirectory(); }\n"
        "}\n"
    )
    raw = """
    {"results": [{
        "check_id": "rules.cccr.rest.java.call-get",
        "path": "src/main/java/OrderClient.java",
        "start": {"line": 5}, "end": {"line": 5},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "role": "call", "http_method": "GET",
                                "framework": "generated-api"}}
    }]}
    """

    endpoint = parse_semgrep_endpoints(raw, tmp_path)[0]

    assert endpoint.topic == "GET <dynamic>"
    assert "cccr-api-domain:" not in endpoint.snippet


def test_rest_configuration_domains_are_inferred_only_in_strategy1(tmp_path: Path) -> None:
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
    client = tmp_path / "src" / "main" / "java" / "OrderClient.java"
    client.write_text(
        "interface AnnuaireApi { Object getDirectory(); }\n"
        "class OrderClient {\n"
        "  private final AnnuaireApi annuaireApi;\n"
        "  OrderClient(AnnuaireApi annuaireApi) { this.annuaireApi = annuaireApi; }\n"
        "  Object get() { return annuaireApi.getDirectory(); }\n"
        "}\n"
    )
    rel = config.relative_to(tmp_path).as_posix()

    assert infer_framework_endpoints(tmp_path, files=[rel]) == []
    endpoints = infer_framework_endpoints(
        tmp_path, files=[rel], configured_api_client_strategy1=True
    )

    assert len(endpoints) == 1
    assert endpoints[0].framework == "configured-api-client-configuration"
    assert endpoints[0].topic == "ANY <dynamic>"
    assert endpoints[0].topic_dynamic is True
    assert "cccr-api-domain:domain-annuaire" in endpoints[0].snippet


def test_rest_configuration_ignores_constant_shaped_text_outside_java_identifiers(tmp_path: Path) -> None:
    config = tmp_path / "src" / "main" / "java" / "RestConfiguration.java"
    config.parent.mkdir(parents=True)
    config.write_text(
        "class RestConfiguration {\n"
        "  // DOMAIN_COMMENT_ONLY\n"
        "  String documentation = \"DOMAIN_STRING_ONLY\";\n"
        "  Object client() { return ApiDomains.DOMAIN_ANNUAIRE; }\n"
        "}\n"
    )

    endpoints = infer_framework_endpoints(
        tmp_path,
        files=[config.relative_to(tmp_path).as_posix()],
        configured_api_client_strategy1=True,
    )

    assert [endpoint.snippet.rsplit("\n", 1)[-1] for endpoint in endpoints] == [
        "cccr-api-domain:domain-annuaire"
    ]


def test_rest_configuration_domain_declares_dependency_without_a_bean(
    tmp_path: Path,
) -> None:
    """Toute constante DOMAIN_* d'une configuration REST émet la dépendance."""
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
    rel = config.relative_to(tmp_path).as_posix()

    endpoints = infer_framework_endpoints(
        tmp_path, files=[rel], configured_api_client_strategy1=True
    )

    assert len(endpoints) == 1
    configuration = endpoints[0]
    assert configuration.framework == "configured-api-client-configuration"
    assert configuration.topic == "ANY <dynamic>"
    assert configuration.module == "caller-service"
    assert "cccr-api-domain:domain-annuaire" in configuration.snippet


def test_strategy1_get_rest_declares_an_external_microservice(tmp_path: Path) -> None:
    (tmp_path / "pom.xml").write_text(
        "<project><artifactId>caller-service</artifactId><version>1</version></project>"
    )
    config = tmp_path / "src" / "main" / "java" / "RestPartnerConfig.java"
    config.parent.mkdir(parents=True)
    config.write_text(
        "class RestPartnerConfig {\n"
        "  Object client() { return getRest().get(\"partner-catalog\"); }\n"
        "}\n"
        "class OtherConfig {\n"
        "  Object ignored() { return getRest().get(\"ignored\"); }\n"
        "}\n"
    )

    endpoints = infer_framework_endpoints(
        tmp_path,
        files=[config.relative_to(tmp_path).as_posix()],
        configured_api_client_strategy1=True,
    )

    assert len(endpoints) == 1
    endpoint = endpoints[0]
    assert endpoint.framework == "configured-external-rest-api-properties"
    assert endpoint.module == "caller-service"
    assert "cccr-external-microservice:partner-catalog" in endpoint.snippet


def test_rest_configuration_collects_each_domain_constant(
    tmp_path: Path,
) -> None:
    """La factory est ignorée : seules la classe REST et ses DOMAIN_* comptent."""
    (tmp_path / "pom.xml").write_text(
        "<project><artifactId>caller-service</artifactId><version>1</version></project>"
    )
    config = tmp_path / "src" / "main" / "java" / "RestPartnerConfigProperties.java"
    config.parent.mkdir(parents=True)
    config.write_text(
        "import org.springframework.context.annotation.Bean;\n"
        "class RestPartnerConfigProperties {\n"
        "  WebClientHelper webClientHelper;\n"
        "  @Bean\n"
        "  AnnuaireApi annuaireApi() {\n"
        "    return webClientHelper.createExternalClientApi(ApiDomains.ANNUAIRE_PARTNER, AnnuaireApi.class);\n"
        "  }\n"
        "  @Bean\n"
        "  AnnuaireApi ignoredFactory() {\n"
        "    return webClientHelper.createExternalApi(ApiDomains.IGNORED_PARTNER, AnnuaireApi.class);\n"
        "  }\n"
        "}\n"
        "class RestUtility {\n"
        "  @Bean\n"
        "  AnnuaireApi ignored() {\n"
        "    return webClientHelper.createInternalClientApi(ApiDomains.IGNORED_PARTNER, AnnuaireApi.class);\n"
        "  }\n"
        "}\n"
    )
    rel = config.relative_to(tmp_path).as_posix()

    endpoints = infer_framework_endpoints(
        tmp_path, files=[rel], configured_api_client_strategy1=True
    )

    assert len(endpoints) == 2
    assert {endpoint.framework for endpoint in endpoints} == {"configured-api-client-configuration"}
    assert {"cccr-api-domain:annuaire-partner", "cccr-api-domain:ignored-partner"} == {
        endpoint.snippet.rsplit("\n", 1)[-1] for endpoint in endpoints
    }

    client = tmp_path / "src" / "main" / "java" / "Caller.java"
    client.write_text(
        "class Caller {\n"
        "  private final AnnuaireApi annuaireApi;\n"
        "  Caller(AnnuaireApi annuaireApi) { this.annuaireApi = annuaireApi; }\n"
        "  Object call() { return annuaireApi.directory(); }\n"
        "}\n"
    )

    calls = infer_framework_endpoints(
        tmp_path,
        files=[client.relative_to(tmp_path).as_posix()],
        configured_api_client_strategy1=True,
    )

    assert calls == []


def test_rest_configuration_emits_one_fact_independent_of_client_invocations(tmp_path: Path) -> None:
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
    client = tmp_path / "src" / "main" / "java" / "OrderClient.java"
    client.write_text(
        "interface AnnuaireApi { Object getDirectory(); }\n"
        "class OrderClient {\n"
        "  private final AnnuaireApi annuaireApi;\n"
        "  OrderClient(AnnuaireApi annuaireApi) { this.annuaireApi = annuaireApi; }\n"
        "  Object get() { return annuaireApi.getDirectory(); }\n"
        "}\n"
    )

    endpoints = infer_framework_endpoints(tmp_path, configured_api_client_strategy1=True)

    assert len(endpoints) == 1
    assert endpoints[0].framework == "configured-api-client-configuration"


def test_parse_semgrep_kafka_endpoint_does_not_depend_on_restclient_state(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "OrderListener.java"
    source.parent.mkdir(parents=True)
    source.write_text('@KafkaListener(topics = "orders.created")\nvoid consume() {}\n')
    raw = """
    {"results": [{
        "check_id": "rules.cccr.kafka.java.consume-listener",
        "path": "src/main/java/OrderListener.java",
        "start": {"line": 1}, "end": {"line": 1},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "system": "kafka", "role": "consume",
                                "framework": "spring-kafka"}}
    }]}
    """

    endpoints = parse_semgrep_endpoints(raw, tmp_path)

    assert endpoints == []


def test_parse_semgrep_kafka_endpoint_unwraps_spring_topic_expressions(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "OrderListener.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        '@KafkaListener(topics = "${kafka.topic}")\n'
        '@KafkaListener(topics = "#{kafka.topic}")\n'
        '@KafkaListener(topics = "#{\'${kafka.topic}\'}")\n'
    )
    raw = """
    {"results": [
      {
        "check_id": "rules.cccr.kafka.java.consume-listener",
        "path": "src/main/java/OrderListener.java",
        "start": {"line": 1}, "end": {"line": 1},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "system": "kafka", "role": "consume",
                                "framework": "spring-kafka"}}
      },
      {
        "check_id": "rules.cccr.kafka.java.consume-listener",
        "path": "src/main/java/OrderListener.java",
        "start": {"line": 2}, "end": {"line": 2},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "system": "kafka", "role": "consume",
                                "framework": "spring-kafka"}}
      },
      {
        "check_id": "rules.cccr.kafka.java.consume-listener",
        "path": "src/main/java/OrderListener.java",
        "start": {"line": 3}, "end": {"line": 3},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "system": "kafka", "role": "consume",
                                "framework": "spring-kafka"}}
      }
    ]}
    """

    endpoints = parse_semgrep_endpoints(raw, tmp_path)

    assert endpoints == []


def test_spring_cloud_gateway_yaml_routes_are_inferred(tmp_path: Path) -> None:
    config = tmp_path / "src" / "main" / "resources" / "application.yml"
    config.parent.mkdir(parents=True)
    config.write_text(
        """
spring:
  cloud:
    gateway:
      server:
        webflux:
          routes:
            - id: vets
              uri: lb://vets-service
              predicates:
                - Path=/api/vet/**
              filters:
                - StripPrefix=2
""".strip()
    )

    endpoints = infer_framework_endpoints(tmp_path, files=["src/main/resources/application.yml"])

    assert {(endpoint.role, endpoint.topic, endpoint.framework) for endpoint in endpoints} == {
        ("serve", "ANY /api/vet/**", "spring-cloud-gateway"),
        ("call", "ANY /**", "spring-cloud-gateway"),
    }
    assert all("lb://vets-service" in endpoint.snippet for endpoint in endpoints)


def test_parse_semgrep_endpoints_renders_request_param_as_query_string(tmp_path: Path) -> None:
    source = tmp_path / "src" / "main" / "java" / "OrderHistoryController.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        """
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/orders")
public class OrderHistoryController {
  @RequestMapping(method = RequestMethod.GET)
  public void getOrders(@RequestParam(name = "consumerId") String consumerId) {}
}
""".strip()
    )
    raw = """
    {"results": [{
        "check_id": "rules.cccr.rest.java.serve-get",
        "path": "src/main/java/OrderHistoryController.java",
        "start": {"line": 9}, "end": {"line": 10},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "role": "serve", "http_method": "GET",
                                "framework": "spring"}}
    }]}
    """

    endpoints = parse_semgrep_endpoints(raw, tmp_path)

    assert len(endpoints) == 1
    assert endpoints[0].topic == "GET /orders?consumerId"
    assert endpoints[0].topic_dynamic is False


@pytest.mark.integration
def test_java_feign_client_methods_are_call_sites_not_server_routes() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/PaymentClient.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    assert len(endpoints) == 3
    for endpoint in endpoints:
        assert endpoint.role == "call"
        assert endpoint.framework == "feign"

    assert by_line[9].topic == "GET /payments/{id}"
    assert by_line[12].topic == "POST /payments"
    # @RequestMapping(..., method = RequestMethod.PUT) sur une interface Feign
    assert by_line[15].topic == "PUT /payments/{id}/cancel"


@pytest.mark.integration
def test_java_feign_client_url_property_is_merged_into_method_routes() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/ResolvedFeignClient.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    assert len(endpoints) == 2
    assert by_line[14].topic == "GET /api/v1/customers/{customer-id}"
    assert by_line[17].topic == "POST /api/v1/customers"
    for endpoint in endpoints:
        assert endpoint.role == "call"
        assert endpoint.framework == "feign"
        assert endpoint.topic_dynamic is False


@pytest.mark.integration
def test_resttemplate_exchange_resolves_value_annotated_base_urls() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/ExchangeClient.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    assert len(endpoints) == 2

    purchase = by_line[26]
    assert purchase.role == "call"
    assert purchase.framework == "resttemplate"
    assert purchase.topic == "POST /api/v1/products/purchase"
    assert purchase.topic_dynamic is False

    fetch_one = by_line[36]
    assert fetch_one.topic == "GET /api/v1/products/"
    assert fetch_one.topic_dynamic is True


@pytest.mark.integration
def test_spring_cloud_config_server_properties_resolve_for_feign_and_exchange() -> None:
    feign_endpoints = run_semgrep_endpoints(
        REST_REPO,
        make_config(),
        files=["services/order/src/main/java/com/example/CloudFeignClient.java"],
    )
    feign_by_line = {e.start_line: e for e in feign_endpoints}
    assert feign_by_line[14].topic == "GET /api/v1/customers/{customer-id}"
    assert feign_by_line[17].topic == "POST /api/v1/customers"
    assert all(not endpoint.topic_dynamic for endpoint in feign_endpoints)

    exchange_endpoints = run_semgrep_endpoints(
        REST_REPO,
        make_config(),
        files=["services/order/src/main/java/com/example/CloudExchangeClient.java"],
    )
    assert len(exchange_endpoints) == 1
    assert exchange_endpoints[0].topic == "POST /api/v1/products/purchase"
    assert exchange_endpoints[0].topic_dynamic is False


@pytest.mark.integration
def test_non_resttemplate_put_calls_are_filtered_out() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/HashMapWriter.java"]
    )

    assert endpoints == []


@pytest.mark.integration
def test_java_webclient_fluent_calls_are_call_sites() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO, make_config(), files=["app/java/WebClientCaller.java"]
    )

    by_line = {e.start_line: e for e in endpoints}
    assert len(endpoints) == 3
    for endpoint in endpoints:
        assert endpoint.role == "call"
        assert endpoint.framework == "webclient"

    assert by_line[14].topic == "GET /orders/{id}"
    assert by_line[18].topic == "POST /orders"
    # BACKLOG-10 K13 : `.uri(...)` sur une ligne distincte de `.patch()`.
    assert by_line[22].topic == "PATCH /orders/{id}/cancel"
    assert not by_line[22].topic_dynamic


@pytest.mark.integration
def test_spring_cloud_gateway_and_webflux_routes_are_inferred() -> None:
    endpoints = run_semgrep_endpoints(
        REST_REPO,
        make_config(),
        files=[
            "app/java/GatewayConsumerConfiguration.java",
            "app/java/GatewayOrderConfiguration.java",
        ],
    )

    serves = {(endpoint.topic, endpoint.framework) for endpoint in endpoints if endpoint.role == "serve"}
    calls = {(endpoint.topic, endpoint.framework) for endpoint in endpoints if endpoint.role == "call"}

    assert serves == {
        ("POST /consumers", "spring-cloud-gateway"),
        ("PUT /consumers", "spring-cloud-gateway"),
        ("POST /orders", "spring-cloud-gateway"),
        ("PUT /orders", "spring-cloud-gateway"),
        ("POST /orders/**", "spring-cloud-gateway"),
        ("PUT /orders/**", "spring-cloud-gateway"),
        ("GET /orders", "spring-cloud-gateway"),
        ("GET /orders/{orderId}", "spring-webflux"),
    }
    assert calls == {
        ("POST /consumers", "spring-cloud-gateway"),
        ("PUT /consumers", "spring-cloud-gateway"),
        ("POST /orders", "spring-cloud-gateway"),
        ("PUT /orders", "spring-cloud-gateway"),
        ("POST /orders/**", "spring-cloud-gateway"),
        ("PUT /orders/**", "spring-cloud-gateway"),
        ("GET /orders", "spring-cloud-gateway"),
    }


@pytest.mark.integration
def test_rest_endpoint_pack_runs_standalone_without_other_backlog_tasks() -> None:
    endpoints = run_semgrep_endpoints(REST_REPO, make_config())

    # java : 9 serve (OrderController) + 4 serve (OwnerController, Q24)
    # + 1 serve générique @RequestMapping (RootController)
    # + 6 serve Spring Data REST + 1 Swagger UI + 1 Actuator
    # + 5 call resttemplate (OrderClient) + 2 call exchange (ExchangeClient)
    # + 1 call exchange via config-server (CloudExchangeClient)
    # + 3 call feign (PaymentClient) + 2 call feign résolus (ResolvedFeignClient)
    # + 2 call feign via config-server (CloudFeignClient)
    # + 3 call webclient (WebClientCaller)
    # + 8 serve gateway/webflux + 7 call gateway proxy routes
    assert len(endpoints) == 55
    assert {e.role for e in endpoints} == {"serve", "call"}
    assert {e.system for e in endpoints} == {"rest"}
    assert {e.source for e in endpoints} == {"code"}
    assert {e.framework for e in endpoints} == {
        "spring",
        "spring-data-rest",
        "spring-actuator",
        "swagger-ui",
        "resttemplate",
        "feign",
        "webclient",
        "spring-cloud-gateway",
        "spring-webflux",
    }


def test_parse_semgrep_endpoints_missing_role_raises_semgrep_error() -> None:
    raw = """
    {"results": [{
        "check_id": "rules.cccr.rest.java.call-get",
        "path": "app/java/OrderClient.java",
        "start": {"line": 1}, "end": {"line": 1},
        "extra": {"metadata": {"category": "endpoint-inventory",
                                "http_method": "GET"}}
    }]}
    """
    with pytest.raises(SemgrepError):
        parse_semgrep_endpoints(raw, REST_REPO)


def test_parse_semgrep_endpoints_ignores_non_inventory_results() -> None:
    raw = """
    {"results": [{
        "check_id": "rules.custom.sql-fstring",
        "path": "app/java/OrderClient.java",
        "start": {"line": 1}, "end": {"line": 1},
        "extra": {"metadata": {}}
    }]}
    """
    assert parse_semgrep_endpoints(raw, REST_REPO) == []
