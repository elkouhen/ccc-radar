from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


_MODULE = Path(__file__).parents[1] / "src/systemlens/render/assets/layer_geometry.js"


def _geometry() -> dict[str, object]:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the renderer geometry unit tests")
    script = f"""
const geometry = require({json.dumps(str(_MODULE))});
const bounds = geometry.computeLayerBands([
  {{contentTop: 100, contentBottom: 240}},
  {{contentTop: 320, contentBottom: 470}},
  {{contentTop: 550, contentBottom: 680}}
], {{contentMinX: 300, contentMaxX: 900, viewportWidth: 1200}});
console.log(JSON.stringify({{bounds, overlap: geometry.rectanglesOverlap(bounds[0], bounds[1])}}));
"""
    result = subprocess.run([node, "-e", script], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


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
