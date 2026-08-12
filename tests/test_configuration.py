from pathlib import Path

from codeatlas.configuration import service_configuration_example
from codeatlas.render import render_workspace_json
from codeatlas.workspace import DiscoveredService, FederationResult


def test_service_configuration_example_builds_structure_from_production_code(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src" / "main" / "java" / "App.java"
    source.parent.mkdir(parents=True)
    source.write_text(
        "@Value(\"${server.port}\") int port;\n"
        "@Value(\"${app.kafka.topic}\") String topic;\n"
        "environment.getProperty(\"client.payment.url\");\n"
        "@ConditionalOnProperty(name = \"security.enabled\")\n"
        "class App {}\n"
    )
    test_source = tmp_path / "src" / "test" / "java" / "TestApp.java"
    test_source.parent.mkdir(parents=True)
    test_source.write_text('@Value("${test.only.value}") class TestApp {}\n')

    example = service_configuration_example(tmp_path)

    assert "port: 0" in example
    assert "url: <string>" in example
    assert "topic: <string>" in example
    assert "enabled: false" in example
    assert "test:" not in example


def test_service_configuration_example_reports_when_no_config_exists(tmp_path: Path) -> None:
    assert service_configuration_example(tmp_path) == (
        "# Aucune propriété Spring détectée dans le code de production.\n"
    )


def test_workspace_result_does_not_include_configuration_content_by_default(tmp_path: Path) -> None:
    source = tmp_path / "orders" / "src" / "main" / "java" / "App.java"
    source.parent.mkdir(parents=True)
    source.write_text('@Value("${spring.application.name}") class App {}\n')
    service = DiscoveredService(
        name="orders",
        path=tmp_path / "orders",
        kind="microservice",
        indexed=False,
        index_root=tmp_path / "orders",
    )

    result = render_workspace_json(
        [service], FederationResult(endpoints_by_service={}, findings_by_service={}, warnings=[])
    )

    assert "configuration_examples" not in result
