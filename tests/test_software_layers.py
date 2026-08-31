from dataclasses import replace
from pathlib import Path

from systemlens.models import MessageEndpoint
from systemlens.modules import DiscoveredModule, ModuleDependency
from systemlens.render import (
    project_namespace,
    project_namespace_path,
    render_namespaces_html,
    render_software_layers_html,
    software_layer,
)


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


def test_strategy1_layer_conventions_are_opt_in() -> None:
    domain = _module("domain-orders", starts_application=True)
    portail = replace(
        _module("portal-service", starts_application=True),
        path=Path("/workspace/PORTAIL/portal-service"),
    )
    cycle = replace(
        _module("lifecycle-service", starts_application=True),
        path=Path("/workspace/CYCLE-DE-VIE/lifecycle-service"),
    )
    assert software_layer(domain) == "application"
    assert software_layer(domain, strategy1=True) == "domain"
    assert software_layer(portail) == "application"
    assert software_layer(portail, strategy1=True) == "api"
    assert software_layer(cycle, strategy1=True) == "orchestration"
    assert software_layer(_module("orders-service", starts_application=True)) == "application"
    assert software_layer(_module("orders-api")) == "module"
    assert software_layer(_module("orders-api"), strategy1=True) == "api"
    assert software_layer(_module("orders-repository")) == "module"
    assert software_layer(_module("orders-repository"), strategy1=True) == "persistence"


def test_software_layers_render_contains_layer_metadata_and_dependencies() -> None:
    modules = [
        _module("orders-service", starts_application=True),
        _module("domain-orders"),
        _module("orders-repository"),
        _module("shared-kernel"),
    ]
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
    )], strategy1=True)
    assert '"layer": "domain"' in html
    assert '"layer": "orchestration"' not in html
    assert '"name": "domain-orders"' in html
    assert '"source": "orders-service"' in html
    assert '"namespace_groups"' in html
    assert '"y": -4' in html
    assert '"name": "orders-repository"' in html
    assert '"name": "shared-kernel"' not in html
    assert "Software layers" in html


def test_namespace_export_groups_modules_by_resolved_namespace() -> None:
    module = _module("orders-service", starts_application=True)
    html = render_namespaces_html([module])

    assert "Namespace hierarchy" in html
    assert '"name": "workspace"' in html
    assert "orders-service" in html


def test_root_project_is_assigned_to_root_namespace() -> None:
    module = _module("orders-service", starts_application=True)
    assert project_namespace(module, Path("/workspace")) == "root"


def test_project_namespace_path_keeps_nested_cluster_directories() -> None:
    module = replace(
        _module("orders-service"),
        path=Path("/workspace/cluster1/cluster2/orders-service"),
    )
    assert project_namespace_path(module, Path("/workspace")) == "cluster1/cluster2"


def test_namespace_export_uses_full_cluster_path() -> None:
    module = replace(
        _module("orders-service", starts_application=True),
        path=Path("/workspace/cluster1/cluster2/orders-service"),
    )
    html = render_namespaces_html([module], Path("/workspace"))
    assert '"name": "cluster1/cluster2"' in html
    assert '"parent": "cluster1"' in html
