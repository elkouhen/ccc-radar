"""Rendering of the software-layer view for indexed build modules."""

import json
from pathlib import Path

from systemlens.models import MessageEndpoint
from systemlens.modules import DiscoveredModule, ModuleDependency, module_identity
from systemlens.render.namespaces import project_namespace

_SOFTWARE_LAYERS_HTML_TEMPLATE = (
    Path(__file__).parent / "assets" / "software_layers.html"
).read_text(encoding="utf-8")

# Render order is top-to-bottom. Persistence is deliberately the lowest layer.
_LAYER_ORDER = ("external", "api", "orchestration", "application", "infrastructure", "domain", "persistence")
_LAYER_LABELS = {
    "application": "Application",
    "external": "External services",
    "domain": "Domain",
    "api": "API / contracts",
    "orchestration": "Orchestration",
    "infrastructure": "Infrastructure",
    "persistence": "Persistence",
}
_LAYER_COLORS = {
    "application": "#2563eb",
    "external": "#64748b",
    "domain": "#7c3aed",
    "api": "#0891b2",
    "orchestration": "#9333ea",
    "infrastructure": "#d97706",
    "persistence": "#0f766e",
}


def software_layer(
    module: DiscoveredModule,
    *,
    strategy1: bool = False,
    root_path: Path | None = None,
) -> str:
    """Classify a module conservatively for the dedicated layer view.

    Strategy1 adds repository-specific conventions: project namespaces
    ``PORTAIL`` and ``CYCLE-DE-VIE`` identify API and Orchestration modules,
    ``DOMAIN-*`` identifies Domain modules, and layer-name prefixes/suffixes
    identify their matching layers. Without Strategy1, these names remain
    ordinary module names and do not alter the portable default classification.
    """
    name = module.name.casefold()
    if strategy1 and project_namespace(module, root_path).casefold() == "portail":
        return "api"
    if strategy1 and project_namespace(module, root_path).casefold() == "cycle-de-vie":
        return "orchestration"
    if strategy1 and name.startswith(("persistence-", "repository-", "storage-", "data-")) or (
        strategy1 and name.endswith(("-persistence", "-repository", "-storage", "-data"))
    ):
        return "persistence"
    if strategy1 and name.startswith("domain-"):
        return "domain"
    if strategy1 and (
        name.startswith(("api-", "contract-", "contracts-", "infra-", "infrastructure-", "shared-", "common-", "lib-", "library-"))
        or name.endswith(("-api", "-contract", "-contracts", "-infra", "-infrastructure"))
    ):
        if name.startswith(("api-", "contract-", "contracts-")) or name.endswith(("-api", "-contract", "-contracts")):
            return "api"
        if name.startswith(("infra-", "infrastructure-")) or name.endswith(("-infra", "-infrastructure")):
            return "infrastructure"
        return "shared"
    if module.starts_application:
        return "application"
    return "module"


def render_software_layers_html(
    modules: list[DiscoveredModule],
    dependencies: list[ModuleDependency],
    endpoints: list[MessageEndpoint],
    *,
    strategy1: bool = False,
    root_path: Path | None = None,
) -> str:
    """Render modules in explicit software-layer columns."""
    classified_modules = [
        (module, software_layer(module, strategy1=strategy1, root_path=root_path))
        for module in modules
    ]
    ordered_modules = sorted(
        [module for module, layer in classified_modules if layer in _LAYER_ORDER],
        key=lambda item: (
            software_layer(item, strategy1=strategy1, root_path=root_path),
            module_identity(item),
        ),
    )
    layer_positions = {layer: index for index, layer in enumerate(_LAYER_ORDER)}
    by_layer: dict[str, list[DiscoveredModule]] = {layer: [] for layer in _LAYER_ORDER}
    for module in ordered_modules:
        by_layer[software_layer(module, strategy1=strategy1, root_path=root_path)].append(module)

    positions: dict[str, tuple[float, float]] = {}
    namespace_groups: list[dict[str, object]] = []
    for layer, items in by_layer.items():
        by_namespace: dict[str, list[DiscoveredModule]] = {}
        for module in items:
            by_namespace.setdefault(project_namespace(module, root_path), []).append(module)
        namespace_cursor = 0
        for namespace, namespace_modules in sorted(by_namespace.items()):
            start = namespace_cursor
            for module in namespace_modules:
                identity = module_identity(module)
                positions[identity] = (namespace_cursor, -layer_positions[layer])
                namespace_cursor += 1
            namespace_groups.append({
                "id": f"{layer}:{namespace}",
                "layer": layer,
                "namespace": namespace,
                "node_ids": [module_identity(module) for module in namespace_modules],
                "start": start,
                "end": namespace_cursor - 1,
            })

    endpoints_by_module: dict[str, list[MessageEndpoint]] = {}
    for endpoint in endpoints:
        if endpoint.module:
            endpoints_by_module.setdefault(endpoint.module, []).append(endpoint)

    nodes = []
    for module in ordered_modules:
        identity = module_identity(module)
        layer = software_layer(module, strategy1=strategy1, root_path=root_path)
        module_endpoints = endpoints_by_module.get(identity, [])
        nodes.append({
            "id": identity,
            "name": module.name,
            "layer": layer,
            "layerLabel": _LAYER_LABELS[layer],
            "color": _LAYER_COLORS[layer],
            "x": positions[identity][0],
            "y": positions[identity][1],
            "kind": "application" if module.starts_application else module.kind,
            "path": str(module.path),
            "httpApisExposed": sorted({
                endpoint.topic for endpoint in module_endpoints
                if endpoint.system == "rest" and endpoint.role == "serve"
            }),
            "kafkaTopicsPublished": sorted({
                endpoint.topic for endpoint in module_endpoints
                if endpoint.system == "kafka" and endpoint.role == "produce"
            }),
            "kafkaTopicsConsumed": sorted({
                endpoint.topic for endpoint in module_endpoints
                if endpoint.system == "kafka" and endpoint.role == "consume"
            }),
        })
    links = [
        {"source": dependency.source, "target": dependency.target}
        for dependency in dependencies
        if dependency.source in positions and dependency.target in positions
    ]
    graph_data = json.dumps(
        {
            "nodes": nodes,
            "links": links,
            "layers": [
                {"id": layer, "label": _LAYER_LABELS[layer], "color": _LAYER_COLORS[layer], "y": -layer_positions[layer]}
                for layer in _LAYER_ORDER
                if by_layer[layer]
            ],
            "namespace_groups": namespace_groups,
        },
        ensure_ascii=False,
    ).replace("</", "<\\/")
    return _SOFTWARE_LAYERS_HTML_TEMPLATE.replace("__SOFTWARE_LAYERS_DATA__", graph_data)
