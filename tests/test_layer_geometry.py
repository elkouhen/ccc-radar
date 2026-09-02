from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


_MODULE = Path(__file__).parents[1] / "src/systemlens/render/assets/layer_geometry.js"


def _run_node(script: str) -> dict[str, object]:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the renderer geometry unit tests")
    result = subprocess.run([node, "-e", script], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def _geometry() -> dict[str, object]:
    script = f"""
const geometry = require({json.dumps(str(_MODULE))});
const bounds = geometry.computeLayerBands([
  {{contentTop: 100, contentBottom: 240}},
  {{contentTop: 320, contentBottom: 470}},
  {{contentTop: 550, contentBottom: 680}}
], {{contentMinX: 300, contentMaxX: 900, viewportWidth: 1200}});
console.log(JSON.stringify({{bounds, overlap: geometry.rectanglesOverlap(bounds[0], bounds[1])}}));
"""
    return _run_node(script)


def test_layer_bands_preserve_order_and_do_not_overlap() -> None:
    result = _geometry()
    bands = result["bounds"]
    assert result["overlap"] is False
    assert bands[0]["top"] < bands[1]["top"] < bands[2]["top"]
    assert bands[0]["top"] + bands[0]["height"] <= bands[1]["top"]
    assert bands[1]["top"] + bands[1]["height"] <= bands[2]["top"]


def test_layer_bands_share_bounds_and_reserve_title_gutter() -> None:
    bands = _geometry()["bounds"]
    assert {band["left"] for band in bands} == {118}
    assert {band["width"] for band in bands} == {782}
    # Content begins at 300px; the 182px title gutter leaves the title area
    # before the first cluster envelope.
    assert all(band["left"] + 182 <= 300 for band in bands)


def test_cluster_sub_layers_place_services_above_resources() -> None:
    script = f"""
const geometry = require({json.dumps(str(_MODULE))});
console.log(JSON.stringify(geometry.computeClusterSubLayers(
  ["service-a", "service-b"], ["topic-a", "collection-a"],
  {{nodeGapX: 240, nodeGapY: 160, subLayerGapY: 120, maxColumns: 5}}
)));
"""
    layout = _run_node(script)
    assert isinstance(layout, dict)
    assert all(layout["positions"][service]["group"] == 0 for service in ["service-a", "service-b"])
    assert all(layout["positions"][resource]["group"] == 1 for resource in ["topic-a", "collection-a"])
    assert min(layout["positions"][service]["y"] for service in ["service-a", "service-b"]) > max(
        layout["positions"][resource]["y"] for resource in ["topic-a", "collection-a"]
    )


def test_cluster_sub_layers_scale_to_thousands_of_cards() -> None:
    """Exercise the large-model path without an O(n²) collision assertion."""
    script = f"""
const geometry = require({json.dumps(str(_MODULE))});
const services = Array.from({{length: 1000}}, (_, index) => `service-${{index}}`);
const resources = Array.from({{length: 2000}}, (_, index) => `resource-${{index}}`);
console.log(JSON.stringify(geometry.computeClusterSubLayers(
  services, resources,
  {{nodeGapX: 240, nodeGapY: 160, subLayerGapY: 120, maxColumns: 5}}
)));
"""
    layout = _run_node(script)
    assert isinstance(layout, dict)
    positions = layout["positions"]
    assert len(positions) == 3000
    assert layout["width"] == 960

    service_positions = [positions[f"service-{index}"] for index in range(1000)]
    resource_positions = [positions[f"resource-{index}"] for index in range(2000)]
    assert all(position["group"] == 0 for position in service_positions)
    assert all(position["group"] == 1 for position in resource_positions)

    # Five columns are used consistently, so cards in each sub-layer occupy
    # distinct grid cells and have the configured positive gaps.
    assert {position["x"] for position in service_positions} == {0, 240, 480, 720, 960}
    assert {position["x"] for position in resource_positions} == {0, 240, 480, 720, 960}
    assert len({(position["x"], position["y"]) for position in service_positions}) == 1000
    assert len({(position["x"], position["y"]) for position in resource_positions}) == 2000
    assert max(position["y"] for position in resource_positions) < min(
        position["y"] for position in service_positions
    )
    assert layout["height"] == -95800


def test_cluster_sub_layers_handle_empty_groups_and_single_column_layout() -> None:
    script = f"""
const geometry = require({json.dumps(str(_MODULE))});
console.log(JSON.stringify({{
  empty: geometry.computeClusterSubLayers([], [], {{}}),
  resourcesOnly: geometry.computeClusterSubLayers(
    [], ["resource-a", "resource-b", "resource-c"],
    {{nodeGapX: 50, nodeGapY: 20, subLayerGapY: 10, maxColumns: 1}}
  )
}}));
"""
    result = _run_node(script)
    assert result["empty"] == {"positions": {}, "width": 0, "height": 0}
    assert result["resourcesOnly"]["width"] == 0
    assert result["resourcesOnly"]["height"] == -40
    assert [result["resourcesOnly"]["positions"][resource]["y"] for resource in [
        "resource-a", "resource-b", "resource-c"
    ]] == [0, -20, -40]


def test_rectangles_touching_at_edge_do_not_overlap() -> None:
    script = f"""
const geometry = require({json.dumps(str(_MODULE))});
const rectangle = {{left: 10, top: 20, width: 30, height: 40}};
console.log(JSON.stringify({{
  touchingX: geometry.rectanglesOverlap(rectangle, {{left: 40, top: 20, width: 5, height: 40}}),
  touchingY: geometry.rectanglesOverlap(rectangle, {{left: 10, top: 60, width: 30, height: 5}}),
  contained: geometry.rectanglesOverlap(rectangle, {{left: 15, top: 25, width: 5, height: 5}})
}}));
"""
    assert _run_node(script) == {"touchingX": False, "touchingY": False, "contained": True}
