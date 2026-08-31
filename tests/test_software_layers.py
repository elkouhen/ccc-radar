from pathlib import Path

from systemlens.models import MessageEndpoint
from systemlens.modules import DiscoveredModule, ModuleDependency
from systemlens.render import render_software_layers_html, software_layer


def _module(name: str, *, starts_application: bool = False) -> DiscoveredModule:
    return DiscoveredModule(
        name=name,
        path=Path("/workspace") / name,
        build_system="maven",
        version="1.0",
        kind="library",
        starts_application=starts_application,
        configuration_example="",
    )


def test_domain_prefix_has_priority_over_application_classification() -> None:
    assert software_layer(_module("domain-orders", starts_application=True)) == "domain"
    assert software_layer(_module("orders-service", starts_application=True)) == "application"
    assert software_layer(_module("orders-api")) == "api"
    assert software_layer(_module("orders-repository")) == "persistence"


def test_software_layers_render_contains_layer_metadata_and_dependencies() -> None:
    modules = [_module("orders-service", starts_application=True), _module("domain-orders"), _module("orders-repository")]
    dependencies = [ModuleDependency(source="orders-service", target="domain-orders")]
    html = render_software_layers_html(modules, dependencies, [MessageEndpoint(
        id="endpoint-1",
        role="serve",
        system="rest",
        topic="GET /orders",
        topic_dynamic=False,
        source="controller",
        framework="spring",
        message_type=None,
        path="OrdersController.java",
        start_line=10,
        end_line=12,
        snippet="@GetMapping(\"/orders\")",
        module="orders-service",
        qualified_name="OrdersController",
    )])
    assert '"layer": "domain"' in html
    assert '"name": "domain-orders"' in html
    assert '"source": "orders-service"' in html
    assert '"namespace_groups"' in html
    assert '"y": -5' in html
    assert "Software layers" in html
