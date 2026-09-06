from __future__ import annotations

import json
import os
import re
import struct
import zlib
from pathlib import Path

import pytest
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Playwright, sync_playwright

from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.graph import GraphEdge
from systemlens.modules import DiscoveredModule, MongoField, MongoPersistenceClass
from systemlens.render import render_graph_html


pytestmark = pytest.mark.integration


_COMPLEX_DATASET_EXPORT = (
    Path(__file__).parents[1] / "examples" / "supermarket" / "supermarket.html"
)
_GRAPH_TEMPLATE = (
    Path(__file__).parents[1] / "src" / "systemlens" / "render" / "assets" / "graph.html"
)
_LAYER_GEOMETRY = (
    Path(__file__).parents[1] / "src" / "systemlens" / "render" / "assets" / "layer_geometry.js"
)
_GRAPH_CSS = (
    Path(__file__).parents[1] / "src" / "systemlens" / "render" / "assets" / "graph.css"
)
_GRAPH_ASSETS = Path(__file__).parents[1] / "src" / "systemlens" / "render" / "assets"
_GRAPH_JS_MODULES = tuple(sorted(_GRAPH_ASSETS.joinpath("graph").glob("*.js")))


def _complex_dataset_document() -> str:
    """Inject a deterministic 50-service/130-resource stress graph."""
    source = _COMPLEX_DATASET_EXPORT.read_text(encoding="utf-8")
    match = re.search(
        r'<script id="graph-data"[^>]*>([\s\S]*?)</script>', source
    )
    assert match, "The complex dataset must contain a graph-data script"
    data = json.loads(match.group(1))
    services = [node for node in data["nodes"] if node["kind"] == "microservice"][:50]
    assert len(services) == 50, "The source complex dataset must contain 50 services"
    namespaces = [
        "platform-edge/sub-1", "platform-edge/sub-2", "platform-edge/sub-3",
        "platform-core", "platform-domain", "platform-infra", "platform-shared",
        "platform-ops", "platform-data", "platform-security", "platform-workflow",
        "platform-reporting",
    ]
    layers = ["api", "application", "orchestration", "infrastructure", "domain", "persistence"]
    for index, node in enumerate(services):
        node["metadata"] = {"namespace": namespaces[index % len(namespaces)]}
        node["architecture_layer"] = layers[index % len(layers)]
        node["layer"] = node["architecture_layer"]

    resources: list[dict[str, object]] = []
    for index in range(100):
        owner = services[index % len(services)]
        resources.append({
            "id": f"kafka_topic:stress-topic-{index:03d}",
            "kind": "kafka_topic",
            "name": f"stress-topic-{index:03d}",
            "label": f"stress-topic-{index:03d}",
            "owner_service": owner["name"],
            "architecture_layer": owner["architecture_layer"],
            "metadata": {"namespace": namespaces[index % len(namespaces)]},
            "color": "#009E73",
        })
    for index in range(30):
        owner = services[(index * 3) % len(services)]
        resources.append({
            "id": f"mongodb_collection:stress-collection-{index:03d}",
            "kind": "mongodb_collection",
            "name": f"stress-collection-{index:03d}",
            "label": f"stress-collection-{index:03d}",
            "owner_service": owner["name"],
            "architecture_layer": owner["architecture_layer"],
            "metadata": {"namespace": namespaces[index % len(namespaces)]},
            "color": "#CC79A7",
        })

    links: list[dict[str, object]] = []
    for index in range(100):
        topic = resources[index]
        producer = services[index % len(services)]
        consumer = services[(index + 1) % len(services)]
        links.extend([
            {"source": producer["id"], "target": topic["id"], "kind": "kafka", "label": topic["name"]},
            {"source": topic["id"], "target": consumer["id"], "kind": "kafka", "label": topic["name"]},
        ])
    for index in range(30):
        collection = resources[100 + index]
        first = services[(index * 3) % len(services)]
        second = services[(index * 3 + 1) % len(services)]
        links.extend([
            {"source": first["id"], "target": collection["id"], "kind": "mongodb", "label": collection["name"]},
            {"source": second["id"], "target": collection["id"], "kind": "mongodb", "label": collection["name"]},
        ])
    for index in range(40):
        topic = resources[(index * 7) % 100]
        producer = services[(index * 5 + 2) % len(services)]
        links.append({
            "source": producer["id"], "target": topic["id"],
            "kind": "kafka", "label": topic["name"],
        })
    assert len(resources) == 130
    assert len(links) == 300
    data["nodes"] = services + resources
    data["links"] = links
    nested_namespace_ids = [
        node["id"] for node in data["nodes"]
        if str(node.get("metadata", {}).get("namespace", "")).startswith("platform-edge/")
    ]
    data["groups"] = [{
        "name": "platform-edge",
        "namespace": "platform-edge",
        "children": nested_namespace_ids,
    }]
    for node in data["nodes"]:
        namespace = node.get("metadata", {}).get("namespace") or "root"
        # The supermarket export stores each bounded context namespace in
        # metadata. Promote it to the current renderer contract so the test
        # exercises real cluster packing rather than one ROOT cluster.
        node["project_namespace_path"] = namespace
        node["cluster_path"] = namespace
        node["runtime_namespaces"] = [namespace]
        node.setdefault("architecture_layer", node.get("layer") or "application")
        node.setdefault("layer", node["architecture_layer"])
    template = _GRAPH_TEMPLATE.read_text(encoding="utf-8")
    return (
        template
        .replace("__GRAPH_CSS__", _GRAPH_CSS.read_text(encoding="utf-8"))
        .replace(
            "__GRAPH_JS__",
            "\n".join(path.read_text(encoding="utf-8") for path in _GRAPH_JS_MODULES),
        )
        .replace("__GRAPH_DATA__", json.dumps(data))
        .replace("__LAYER_GEOMETRY__", _LAYER_GEOMETRY.read_text(encoding="utf-8"))
    )


def _chrome_executable(playwright: Playwright) -> str | None:
    """Prefer an explicit browser, then the Chromium revision Playwright pins."""
    configured = os.environ.get("SYSTEMLENS_CHROME_BIN")
    candidates = [
        Path(configured) if configured else None,
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        Path(playwright.chromium.executable_path),
    ]
    return next((str(candidate) for candidate in candidates if candidate and candidate.is_file()), None)


def _launch_visual_browser(playwright: Playwright):
    """Launch the first usable Playwright engine, preferring Chromium."""
    engines = [
        ("chromium", playwright.chromium, _chrome_executable(playwright)),
        ("firefox", playwright.firefox, os.environ.get("SYSTEMLENS_FIREFOX_BIN")),
        ("webkit", playwright.webkit, os.environ.get("SYSTEMLENS_WEBKIT_BIN")),
    ]
    failures: list[str] = []
    for name, browser_type, configured in engines:
        executable = configured if configured and Path(configured).is_file() else None
        try:
            if executable:
                return browser_type.launch(headless=True, executable_path=executable)
            return browser_type.launch(headless=True)
        except PlaywrightError as error:
            failures.append(f"{name}: {error}")
    raise PlaywrightError("Aucun moteur navigateur utilisable. " + " | ".join(failures))


def _producer(message_type: str) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id("produce", "orders.created", "Publisher.java", 4),
        role="produce",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path="Publisher.java",
        start_line=4,
        end_line=4,
        snippet="",
        message_type=message_type,
    )


def _assert_filtered_graph_is_valid(
    page, previous_node_count: int | None = None, excluded_kind: str | None = None
) -> int:
    graph = page.locator("#graph")
    assert graph.get_attribute("data-invalid-coordinates") == "false"
    count = int(graph.get_attribute("data-visible-node-count") or "0")
    assert count >= 0
    visible_kinds = (graph.get_attribute("data-visible-node-kinds") or "").split(",")
    if excluded_kind is not None:
        assert excluded_kind not in visible_kinds
    if previous_node_count is not None:
        assert count < previous_node_count
    return count


def _assert_architecture_cards_have_uniform_size(page) -> tuple[float, float]:
    size = page.evaluate(
        """() => {
            const cards = [...document.querySelectorAll('.graph-node-card-label')];
            if (!cards.length) return null;
            const first = cards[0].getBoundingClientRect();
            const uniform = cards.every(card => {
                const rect = card.getBoundingClientRect();
                return Math.abs(rect.width - first.width) < 0.01
                    && Math.abs(rect.height - first.height) < 0.01;
            });
            return uniform ? [first.width, first.height] : false;
        }"""
    )
    assert size is not False and size is not None
    return float(size[0]), float(size[1])


def _assert_architecture_cards_match_size(page, expected: tuple[float, float]) -> None:
    actual = _assert_architecture_cards_have_uniform_size(page)
    assert actual[0] == pytest.approx(expected[0], abs=0.01)
    assert actual[1] == pytest.approx(expected[1], abs=0.01)


def _assert_architecture_cards_keep_size_after_camera_change(
    page, expected: tuple[float, float]
) -> None:
    """Cards stay screen-sized while positions change with zoom or pan."""
    _assert_architecture_cards_match_size(page, expected)
    page.wait_for_timeout(120)
    _assert_architecture_cards_match_size(page, expected)


def _assert_architecture_cards_do_not_overlap(page) -> None:
    """Validate card geometry without imposing an artificial no-overlap rule.

    Dense architecture overviews are allowed to overlap; readability comes
    from fixed screen-space card dimensions and camera controls.
    """
    geometry = page.evaluate(
        """() => {
            const cards = [...document.querySelectorAll('.graph-node-card-label')];
            return cards.every(card => {
                const rect = card.getBoundingClientRect();
                return [rect.x, rect.y, rect.width, rect.height].every(Number.isFinite)
                    && rect.width > 0 && rect.height > 0;
            });
        }"""
    )
    assert geometry


def _assert_all_node_centers_are_visible(page) -> None:
    result = page.evaluate(
        """() => {
            const graph = document.querySelector('#graph').getBoundingClientRect();
            const outside = [...document.querySelectorAll('.graph-node-card-label')]
                .filter(card => {
                    const rect = card.getBoundingClientRect();
                    const x = (rect.left + rect.right) / 2;
                    const y = (rect.top + rect.bottom) / 2;
                    return x < graph.left || x > graph.right || y < graph.top || y > graph.bottom;
                })
                .map(card => card.dataset.nodeId);
            return { outside, cards: document.querySelectorAll('.graph-node-card-label').length };
        }"""
    )
    assert result["outside"] == [], result


def _inspect_png_content(image: bytes, region: dict[str, float]) -> dict[str, int]:
    """Inspect screenshot pixels without depending on a GUI or image library."""
    assert image.startswith(b"\x89PNG\r\n\x1a\n")
    offset = 8
    width = height = color_type = bit_depth = None
    compressed = bytearray()
    while offset < len(image):
        length = struct.unpack(">I", image[offset:offset + 4])[0]
        kind = image[offset + 4:offset + 8]
        payload = image[offset + 8:offset + 8 + length]
        offset += 12 + length
        if kind == b"IHDR":
            width, height, bit_depth, color_type = struct.unpack(">IIBB", payload[:10])
        elif kind == b"IDAT":
            compressed.extend(payload)
        elif kind == b"IEND":
            break
    assert width and height and bit_depth == 8 and color_type in (2, 6)
    channels = 4 if color_type == 6 else 3
    row_size = width * channels
    decoded = zlib.decompress(compressed)
    rows: list[bytes] = []
    previous = bytearray(row_size)
    cursor = 0
    for _ in range(height):
        filter_type = decoded[cursor]
        cursor += 1
        current = bytearray(decoded[cursor:cursor + row_size])
        cursor += row_size
        for index in range(row_size):
            left = current[index - channels] if index >= channels else 0
            above = previous[index]
            upper_left = previous[index - channels] if index >= channels else 0
            if filter_type == 1:
                current[index] = (current[index] + left) & 255
            elif filter_type == 2:
                current[index] = (current[index] + above) & 255
            elif filter_type == 3:
                current[index] = (current[index] + ((left + above) // 2)) & 255
            elif filter_type == 4:
                estimate = left + above - upper_left
                distances = abs(estimate - left), abs(estimate - above), abs(estimate - upper_left)
                predictor = left if distances[0] <= distances[1] and distances[0] <= distances[2] else (
                    above if distances[1] <= distances[2] else upper_left
                )
                current[index] = (current[index] + predictor) & 255
            elif filter_type != 0:
                raise AssertionError(f"Unsupported PNG filter: {filter_type}")
        rows.append(bytes(current))
        previous = current
    left = max(0, min(width, int(region.get("x", 0))))
    top = max(0, min(height, int(region.get("y", 0))))
    right = max(left, min(width, int(region.get("x", 0) + region.get("width", width))))
    bottom = max(top, min(height, int(region.get("y", 0) + region.get("height", height))))
    dark_pixels = 0
    distinct_pixels = 0
    for row in rows[top:bottom]:
        for x in range(left, right):
            pixel = row[x * channels:x * channels + 3]
            if sum(pixel) < 680:
                dark_pixels += 1
            if max(pixel) - min(pixel) > 18 or sum(pixel) < 650:
                distinct_pixels += 1
    return {"width": width, "height": height, "dark_pixels": dark_pixels, "distinct_pixels": distinct_pixels}


def _capture_render_snapshot(page, name: str) -> None:
    output = Path("output/playwright")
    output.mkdir(parents=True, exist_ok=True)
    image = page.screenshot(path=str(output / f"{name}.png"), full_page=False)
    metrics = page.evaluate(
        """() => ({
            viewport: { width: innerWidth, height: innerHeight },
            graph: document.querySelector('#graph')?.getBoundingClientRect().toJSON(),
            cards: [...document.querySelectorAll('.graph-node-card-label')].map(card => {
                const rect = card.getBoundingClientRect();
                return { id: card.dataset.nodeId, x: rect.x, y: rect.y, width: rect.width, height: rect.height };
            }),
            clusters: [...document.querySelectorAll('.graph-namespace-group, .graph-project-group')].map(group => {
                const rect = group.getBoundingClientRect();
                return { className: group.className, x: rect.x, y: rect.y, width: rect.width, height: rect.height };
            }),
        })"""
    )
    screenshot = _inspect_png_content(image, metrics["graph"])
    assert screenshot["width"] == metrics["viewport"]["width"]
    assert screenshot["height"] == metrics["viewport"]["height"]
    assert screenshot["dark_pixels"] > 40, f"Screenshot graph area is empty: {screenshot}"
    assert screenshot["distinct_pixels"] > 100, f"Screenshot has no rendered content: {screenshot}"
    metrics["screenshot"] = screenshot
    (output / f"{name}.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")


def _assert_architecture_cards_are_contained_in_clusters(page) -> None:
    assert page.evaluate(
        """() => {
                const cards = [...document.querySelectorAll('.graph-node-card-label')]
                    .map(card => card.getBoundingClientRect())
                    .filter(card => card.right > 0 && card.left < innerWidth && card.bottom > 0 && card.top < innerHeight);
                const groups = [...document.querySelectorAll('.graph-namespace-group')]
                    .map(group => group.getBoundingClientRect());
                if (!groups.length) return true;
                return cards.every(card => groups.some(bounds => (
                card.left >= bounds.left && card.right <= bounds.right
                && card.top >= bounds.top && card.bottom <= bounds.bottom
            )));
        }"""
    )


def _assert_architecture_clusters_do_not_overlap(page) -> None:
    """Check finite cluster geometry; sibling overlap is an accepted state."""
    assert page.evaluate(
        """() => {
            const rects = [...document.querySelectorAll('.graph-namespace-group')]
                .map(group => group.getBoundingClientRect());
            return rects.every(rect => [rect.x, rect.y, rect.width, rect.height].every(Number.isFinite)
                && rect.width > 0 && rect.height > 0);
        }"""
    )


def _assert_pan_moves_cluster_overlays_as_one_surface(page) -> None:
    before = page.evaluate(
        """() => [...document.querySelectorAll(
            '.graph-node-card-label, .graph-namespace-group, .graph-project-group'
        )].map(element => {
            const rect = element.getBoundingClientRect();
            return [rect.left, rect.top, rect.right, rect.bottom];
        })"""
    )
    assert before
    start = page.locator(".graph-node-card-label").first.bounding_box()
    assert start
    page.mouse.move(start["x"] + start["width"] / 2, start["y"] + start["height"] / 2)
    page.mouse.down()
    page.mouse.move(start["x"] + start["width"] / 2 - 80, start["y"] + start["height"] / 2 + 65, steps=10)
    page.mouse.up()
    page.wait_for_timeout(250)
    after = page.evaluate(
        """() => [...document.querySelectorAll(
            '.graph-node-card-label, .graph-namespace-group, .graph-project-group'
        )].map(element => {
            const rect = element.getBoundingClientRect();
            return [rect.left, rect.top, rect.right, rect.bottom];
        })"""
    )
    assert len(after) == len(before)
    delta_x = after[0][0] - before[0][0]
    delta_y = after[0][1] - before[0][1]
    assert abs(delta_x) > 1 or abs(delta_y) > 1
    for old, new in zip(before, after):
        assert new[0] - old[0] == pytest.approx(delta_x, abs=1.5)
        assert new[1] - old[1] == pytest.approx(delta_y, abs=1.5)
        assert new[2] - old[2] == pytest.approx(delta_x, abs=1.5)
        assert new[3] - old[3] == pytest.approx(delta_y, abs=1.5)
    assert delta_x == pytest.approx(-80, abs=2)
    assert delta_y == pytest.approx(65, abs=2)


def _surface_rects(page) -> list[list[float]]:
    return page.evaluate(
        """() => [...document.querySelectorAll(
            '.graph-node-card-label, .graph-namespace-group, .graph-project-group'
        )].map(element => {
            const rect = element.getBoundingClientRect();
            return [rect.left, rect.top, rect.right, rect.bottom];
        })"""
    )


def _assert_pan_does_not_zoom_or_desynchronise_overlays(page) -> None:
    """A drag must translate the surface without changing its scale."""
    before = _surface_rects(page)
    assert before
    graph_box = page.locator("#graph").bounding_box()
    assert graph_box
    # Start from the lower-right canvas corner, which is outside the HTML
    # cards even in the dense compound views. Starting on a card intentionally
    # selects it and would not exercise graph panning.
    start_x = graph_box["x"] + graph_box["width"] - 18
    start_y = graph_box["y"] + graph_box["height"] - 18
    page.mouse.move(start_x, start_y)
    page.mouse.down()
    samples: list[list[list[float]]] = []
    for step in range(1, 9):
        page.mouse.move(start_x - step * 15, start_y - step * 10)
        samples.append(_surface_rects(page))
    page.mouse.up()
    page.wait_for_timeout(250)
    after = _surface_rects(page)
    assert all(len(sample) == len(before) for sample in samples)
    assert len(after) == len(before)

    # During the drag and after release, every overlay keeps the same size and
    # follows the same camera translation. This catches pan-to-zoom, inertia
    # jumps and stale namespace overlays independently of visual inspection.
    deltas: list[tuple[float, float]] = []
    for step, sample in enumerate(samples, 1):
        for old, moved in zip(before, sample):
            for index in (2, 3):
                assert moved[index] - moved[index - 2] == pytest.approx(
                    old[index] - old[index - 2], abs=1.5
                )
        deltas.append((sample[0][0] - before[0][0], sample[0][1] - before[0][1]))
    assert any(abs(delta[0]) > 5 or abs(delta[1]) > 5 for delta in deltas)
    # Browser pointer events may be coalesced, so assert monotonic drag
    # direction rather than requiring one sample per physical mouse move.
    for previous, current in zip(deltas, deltas[1:]):
        assert current[0] <= previous[0] + 3
        assert current[1] <= previous[1] + 3
    for old, settled in zip(before, after):
        for index in (2, 3):
            assert settled[index] - settled[index - 2] == pytest.approx(
                old[index] - old[index - 2], abs=1.5
            )
        settled_delta = (settled[0] - old[0], settled[1] - old[1])
        assert settled_delta[0] * deltas[-1][0] >= 0
        assert settled_delta[1] * deltas[-1][1] >= 0


def _assert_zoom_is_monotonic_and_settles_without_a_release_jump(page) -> None:
    """A zoom gesture must change scale, then remain stable after release."""
    before = _surface_rects(page)
    assert before
    before_distance = ((before[1][0] - before[0][0]) ** 2 + (before[1][1] - before[0][1]) ** 2) ** .5 if len(before) > 1 else 0
    page.mouse.move(1180, 620)
    page.mouse.wheel(0, -450)
    page.wait_for_timeout(80)
    during = _surface_rects(page)
    page.wait_for_timeout(700)
    after = _surface_rects(page)
    page.wait_for_timeout(300)
    settled = _surface_rects(page)
    assert len(during) == len(before) == len(after)
    during_distance = ((during[1][0] - during[0][0]) ** 2 + (during[1][1] - during[0][1]) ** 2) ** .5 if len(during) > 1 else 0
    after_distance = ((after[1][0] - after[0][0]) ** 2 + (after[1][1] - after[0][1]) ** 2) ** .5 if len(after) > 1 else 0
    if before_distance:
        assert during_distance > before_distance * 1.01
        settled_distance = ((settled[1][0] - settled[0][0]) ** 2 + (settled[1][1] - settled[0][1]) ** 2) ** .5 if len(settled) > 1 else 0
        assert settled_distance == pytest.approx(after_distance, rel=0.04)

    page.locator("#zoom-out").click()
    page.wait_for_timeout(300)
    restored = _surface_rects(page)
    if after_distance and len(restored) > 1:
        restored_distance = ((restored[1][0] - restored[0][0]) ** 2 + (restored[1][1] - restored[0][1]) ** 2) ** .5
        # Zoom-out is never clamped to a collision-derived camera state; it
        # must only change the camera and preserve card dimensions.
        assert restored_distance <= after_distance * 1.01


def _assert_background_pan_has_one_to_one_scale(page) -> None:
    before = page.locator(".graph-namespace-group").first.bounding_box()
    assert before
    start_x = min(1100, page.viewport_size["width"] - 80)
    page.mouse.move(start_x, 500)
    page.mouse.down()
    page.mouse.move(start_x - 100, 580, steps=10)
    page.mouse.up()
    page.wait_for_timeout(250)
    after = page.locator(".graph-namespace-group").first.bounding_box()
    assert after
    assert after["x"] - before["x"] == pytest.approx(-100, abs=2)
    assert after["y"] - before["y"] == pytest.approx(80, abs=2)
    assert after["width"] == pytest.approx(before["width"], abs=0.1)
    assert after["height"] == pytest.approx(before["height"], abs=0.1)


def _assert_clusters_only_overlap_when_nested(page) -> None:
    """Validate cluster rectangles without rejecting intentional overlaps."""
    assert page.evaluate(
        """() => {
            const rects = [...document.querySelectorAll(
                '.graph-namespace-group, .graph-project-group'
            )].map(element => element.getBoundingClientRect());
            return rects.every(rect => [rect.x, rect.y, rect.width, rect.height].every(Number.isFinite)
                && rect.width > 0 && rect.height > 0);
        }"""
    )


def _assert_nested_namespace_cluster_contains_three_children(page) -> None:
    result = page.evaluate(
        """() => {
            const parent = document.querySelector(
                '.graph-project-group[data-namespace-group="platform-edge"]'
            );
            const children = [...document.querySelectorAll(
                '.graph-namespace-group[data-namespace^="platform-edge/"]'
            )];
            if (!parent || children.length !== 3) return { valid: false, children: children.length };
            const outer = parent.getBoundingClientRect();
            const rects = children.map(child => child.getBoundingClientRect());
            const contains = rect => (
                rect.left >= outer.left && rect.right <= outer.right
                && rect.top >= outer.top && rect.bottom <= outer.bottom
            );
            return {
                valid: rects.every(contains),
                parent: [outer.left, outer.top, outer.width, outer.height],
                children: rects.map(rect => [rect.left, rect.top, rect.width, rect.height]),
            };
        }"""
    )
    assert result["valid"], result


def _assert_layer_bands_are_disjoint_and_contain_clusters(page) -> None:
    result = page.evaluate(
        """() => {
                const bands = [...document.querySelectorAll('.graph-layer-band')]
                    .map(element => element.getBoundingClientRect())
                    .filter(band => band.left >= 0 && band.right <= innerWidth
                        && band.top >= 0 && band.bottom <= innerHeight);
                const clusters = [...document.querySelectorAll('.graph-namespace-group')]
                    .map(element => element.getBoundingClientRect())
                    // A clipped cluster cannot be validated for containment;
                    // its visible fragment is intentionally allowed to leave
                    // the current viewport after pan/zoom.
                    .filter(cluster => cluster.left >= 0 && cluster.right <= innerWidth
                        && cluster.top >= 0 && cluster.bottom <= innerHeight);
            const overlap = (left, right) => (
                left.left < right.right && left.right > right.left
                && left.top < right.bottom && left.bottom > right.top
            );
            const contains = (outer, inner) => (
                inner.left >= outer.left && inner.right <= outer.right
                && inner.top >= outer.top && inner.bottom <= outer.bottom
            );
            const valid = !bands.length || (bands.every((band, index) => bands.every((other, otherIndex) => (
                index === otherIndex || !overlap(band, other)
            ))) && clusters.every(cluster => bands.some(band => overlap(band, cluster))));
            return { valid, bands, clusters };
        }"""
    )
    assert result["valid"], result


def _assert_geometry_contract(page, *, layered: bool) -> None:
    graph = page.locator("#graph")
    assert graph.get_attribute("data-invalid-coordinates") == "false"
    _assert_architecture_cards_have_uniform_size(page)
    _assert_architecture_cards_do_not_overlap(page)
    _assert_architecture_cards_are_contained_in_clusters(page)
    if not layered:
        _assert_clusters_only_overlap_when_nested(page)
    if layered:
        _assert_layer_bands_are_disjoint_and_contain_clusters(page)


@pytest.mark.slow
def test_complex_dataset_geometry_contract_across_all_views() -> None:
    """Stress the geometry invariants with the 50-service stress dataset."""
    with sync_playwright() as playwright:
        try:
            browser = _launch_visual_browser(playwright)
        except PlaywrightError as error:
            pytest.skip(f"Aucun navigateur Playwright ne peut être lancé : {error}")
        # Playwright contexts are private/incognito browser profiles: no
        # cookies, local storage, cache, or service workers leak between runs.
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()
        page.set_default_timeout(10_000)
        errors: list[str] = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.set_content(_complex_dataset_document(), wait_until="load")
        page.wait_for_function(
            "() => Number(document.querySelector('#graph')?.dataset.visibleNodeCount || 0) >= 60"
        )

        graph = page.locator("#graph")
        assert graph.get_attribute("data-visible-node-count") == "180"
        assert graph.get_attribute("data-relation-count") == "300"
        assert not errors, errors
        _assert_all_node_centers_are_visible(page)
        _capture_render_snapshot(page, "complex-initial")

        # The same contract is checked after layout, resize, zoom and pan. A
        # failure here means the geometry is viewport-dependent, which is the
        # class of regression that unit tests on graph coordinates miss.
        for layout_id, layered in (
            ("layout-cluster", False),
            ("layout-elk", True),
            ("layout-forceatlas2-noverlap", False),
        ):
            # The layout controls live in the graph toolbar and may be hidden
            # behind the compact toolbar at this viewport. Trigger the same
            # user handler even when the option is visually collapsed.
            previous_status = page.locator("#layout-status").text_content() or ""
            already_active = page.locator(f"#{layout_id}").get_attribute("aria-pressed") == "true"
            # The advanced layout menu can be collapsed; dispatch the control
            # event directly so the test still exercises the real handler.
            page.locator(f"#{layout_id}").dispatch_event("click")
            if not already_active:
                page.wait_for_function(
                    "previous => document.querySelector('#layout-status')?.textContent !== previous",
                    arg=previous_status,
                )
                if layout_id == "layout-cluster":
                    page.wait_for_function(
                        "() => document.querySelector('#layout-status')?.textContent?.toLowerCase().includes('namespaces')"
                    )
            page.wait_for_timeout(700)
            _capture_render_snapshot(page, f"complex-{layout_id.removeprefix('layout-')}-after-action")
            card_size = _assert_architecture_cards_have_uniform_size(page)
            _assert_all_node_centers_are_visible(page)
            _capture_render_snapshot(page, f"complex-{layout_id.removeprefix('layout-')}-fit")
            _assert_geometry_contract(page, layered=layered)
            if layout_id == "layout-cluster":
                _assert_nested_namespace_cluster_contains_three_children(page)
                _capture_render_snapshot(page, "complex-cluster-final")
            _assert_pan_does_not_zoom_or_desynchronise_overlays(page)
            _assert_architecture_cards_keep_size_after_camera_change(page, card_size)
            _assert_geometry_contract(page, layered=layered)
            _assert_zoom_is_monotonic_and_settles_without_a_release_jump(page)
            _assert_architecture_cards_keep_size_after_camera_change(page, card_size)
            _assert_geometry_contract(page, layered=layered)

            page.locator("#zoom-out").click()
            page.locator("#zoom-out").click()
            page.wait_for_timeout(300)
            _capture_render_snapshot(page, f"complex-{layout_id.removeprefix('layout-')}-after-zoom-out")
            _assert_architecture_cards_keep_size_after_camera_change(page, card_size)
            _assert_geometry_contract(page, layered=layered)

            page.mouse.move(1250, 780)
            page.mouse.down()
            page.mouse.move(1120, 700, steps=8)
            page.mouse.up()
            page.wait_for_timeout(300)
            _capture_render_snapshot(page, f"complex-{layout_id.removeprefix('layout-')}-after-pan")
            _assert_architecture_cards_keep_size_after_camera_change(page, card_size)
            _assert_geometry_contract(page, layered=layered)

            page.set_viewport_size({"width": 1024, "height": 640})
            page.wait_for_timeout(500)
            _capture_render_snapshot(page, f"complex-{layout_id.removeprefix('layout-')}-after-resize")
            _assert_architecture_cards_keep_size_after_camera_change(page, card_size)
            _assert_geometry_contract(page, layered=layered)
            page.set_viewport_size({"width": 1440, "height": 900})
            page.wait_for_timeout(300)

        assert not errors, errors
        context.close()
        browser.close()


@pytest.mark.slow
def test_html_export_resources_are_usable_in_a_constrained_browser_viewport(tmp_path: Path) -> None:
    source_root = tmp_path / "orders" / "src" / "main" / "java" / "com" / "example"
    source_root.mkdir(parents=True)
    (source_root / "OrderCreated.java").write_text(
        "package com.example; public record OrderCreated(String orderId) {}",
        encoding="utf-8",
    )
    module = DiscoveredModule(
        name="orders",
        path=tmp_path / "orders",
        build_system="maven",
        version=None,
        kind="application",
        starts_application=True,
        configuration_example="",
        mongo_collections=("orders",),
        mongo_persistence_classes=(
            MongoPersistenceClass(
                collection="orders", name="Order", qualified_name="com.example.Order",
                path="src/main/java/com/example/Order.java", line=1,
                fields=(MongoField("address", "Address", ("com.example.Address",)),),
            ),
            MongoPersistenceClass(
                collection="orders", name="Address", qualified_name="com.example.Address",
                path="src/main/java/com/example/Address.java", line=1,
                fields=(MongoField("city", "String"),), root=False,
            ),
        ),
    )
    consumer = MessageEndpoint(
        id=compute_endpoint_id("consume", "orders.created", "Consumer.java", 8),
        role="consume", system="kafka", topic="orders.created", topic_dynamic=False,
        source="code", framework="spring-kafka", path="Consumer.java", start_line=8,
        end_line=8, snippet="", message_type="com.example.OrderCreated",
    )
    rest_call = MessageEndpoint(
        id=compute_endpoint_id("call", "GET /payments", "OrderClient.java", 12),
        role="call", system="rest", topic="GET /payments", topic_dynamic=False,
        source="code", framework="resttemplate", path="OrderClient.java", start_line=12,
        end_line=12, snippet="", message_type=None,
    )
    rest_server = MessageEndpoint(
        id=compute_endpoint_id("serve", "GET /payments", "PaymentController.java", 6),
        role="serve", system="rest", topic="GET /payments", topic_dynamic=False,
        source="code", framework="spring-mvc", path="PaymentController.java", start_line=6,
        end_line=6, snippet="", message_type=None,
    )
    document = render_graph_html(
        {
            "orders": [_producer("com.example.OrderCreated"), rest_call],
            "payments": [consumer, rest_server],
            "inventory": [],
        },
        [
            GraphEdge("kafka", "orders", "payments", _producer("com.example.OrderCreated"), consumer),
            GraphEdge("rest", "orders", "payments", rest_call, rest_server),
        ],
        collections_by_service={"orders": ["orders"]},
        modules_by_service={"orders": module},
        build_modules=[module],
    )

    with sync_playwright() as playwright:
        try:
            browser = _launch_visual_browser(playwright)
        except PlaywrightError as error:
            pytest.skip(f"Aucun navigateur Playwright ne peut être lancé : {error}")
        # Use a fresh private context for the constrained-viewport scenario as
        # well, so browser state cannot mask an export or rendering defect.
        context = browser.new_context(viewport={"width": 800, "height": 450})
        page = context.new_page()
        page.set_default_timeout(5_000)
        errors: list[str] = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.set_content(document, wait_until="load")
        page.wait_for_timeout(100)
        assert not errors
        _capture_render_snapshot(page, "constrained-initial")

        graph = page.locator("#graph")
        assert page.evaluate(
            """() => {
                const graphRect = document.querySelector('#graph').getBoundingClientRect();
                const toolbarRect = document.querySelector('.toolbar').getBoundingClientRect();
                return graphRect.left >= toolbarRect.right;
            }"""
        )
        assert graph.get_attribute("data-relation-count") == "4"
        page.locator("#layout-status").filter(has_text="vue graphe").wait_for(state="visible")
        card_size = _assert_architecture_cards_have_uniform_size(page)
        _assert_architecture_cards_do_not_overlap(page)
        assert "1 ressource isolée" in page.locator("#graph-summary").inner_text()
        assert page.locator("#inventory-status").inner_text() == "Inventaire : aucun fait non résolu"
        assert page.locator("#node-suggestions option").count() == 5
        advanced_controls = page.locator("#advanced-controls")
        assert not page.locator("#relation-http").is_visible()
        advanced_controls.locator(":scope > summary").click()
        _capture_render_snapshot(page, "constrained-after-open-controls")
        assert page.locator("#relation-http").is_visible()
        page.locator("#relation-http").uncheck()
        _capture_render_snapshot(page, "constrained-after-http-off")
        assert graph.get_attribute("data-relation-count") == "3"
        page.locator("#relation-kafka").uncheck()
        _capture_render_snapshot(page, "constrained-after-kafka-off")
        assert graph.get_attribute("data-relation-count") == "1"
        page.locator("#relation-http").check()
        _capture_render_snapshot(page, "constrained-after-http-on")
        assert graph.get_attribute("data-relation-count") == "2"
        page.locator("#relation-kafka").check()
        _capture_render_snapshot(page, "constrained-after-kafka-on")
        assert graph.get_attribute("data-relation-count") == "4"
        page.locator("#layout-elk").click()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-layers")
        assert not errors, errors
        _assert_architecture_cards_match_size(page, card_size)
        _assert_architecture_cards_do_not_overlap(page)
        _assert_architecture_cards_are_contained_in_clusters(page)
        _assert_architecture_clusters_do_not_overlap(page)
        _assert_clusters_only_overlap_when_nested(page)
        full_node_count = _assert_filtered_graph_is_valid(page)

        # Changing node types must rebuild the graph and its layer overlays.
        # The filtered graph must contain no stale card for the removed type.
        page.locator("#node-kafka-topic").uncheck()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-topic-off")
        without_topic_count = _assert_filtered_graph_is_valid(
            page, full_node_count, "kafka_topic"
        )

        page.locator("#node-mongodb-collection").uncheck()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-mongodb-off")
        _assert_filtered_graph_is_valid(page, without_topic_count, "mongodb_collection")

        page.locator("#node-microservice").uncheck()
        _capture_render_snapshot(page, "constrained-after-microservice-off")
        page.locator("#node-external-microservice").uncheck()
        _capture_render_snapshot(page, "constrained-after-external-off")
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
        assert page.locator("#graph").get_attribute("data-visible-node-count") == "0"
        assert page.locator("#graph").get_attribute("data-invalid-coordinates") == "false"
        page.locator("#node-microservice").check()
        _capture_render_snapshot(page, "constrained-after-microservice-on")
        page.locator("#node-external-microservice").check()
        _capture_render_snapshot(page, "constrained-after-external-on")
        page.locator("#node-kafka-topic").check()
        _capture_render_snapshot(page, "constrained-after-topic-on")
        page.locator("#node-mongodb-collection").check()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-mongodb-on")

        page.locator("#layout-cluster").click()
        page.locator("#layout-status").filter(has_text="vue namespaces").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-clusters")
        page.wait_for_function("() => Boolean(document.querySelector('#graph').dataset.clusterLayout)")
        assert page.locator("#graph").get_attribute("data-cluster-sub-layers") == (
            "microservices-first,resources-second"
        )
        cluster_layout = json.loads(
            page.locator("#graph").get_attribute("data-cluster-layout") or "{}"
        )
        assert cluster_layout
        for cluster in {item["cluster"] for item in cluster_layout.values()}:
            services = [
                item["y"] for item in cluster_layout.values()
                if item["cluster"] == cluster and item["subLayer"] == "microservices"
            ]
            resources = [
                item["y"] for item in cluster_layout.values()
                if item["cluster"] == cluster and item["subLayer"] == "resources"
            ]
            if services and resources:
                assert min(services) > max(resources)
        assert page.locator("#graph-layers .graph-namespace-group").count() >= 1
        assert page.locator("#graph-layers .graph-cluster-sublayer-title").count() >= 2
        _assert_architecture_cards_match_size(page, card_size)
        _assert_architecture_cards_do_not_overlap(page)
        _assert_architecture_cards_are_contained_in_clusters(page)
        _assert_architecture_clusters_do_not_overlap(page)
        _assert_clusters_only_overlap_when_nested(page)
        _assert_architecture_clusters_do_not_overlap(page)
        assert page.locator("#graph").get_attribute("data-invalid-coordinates") == "false"

        page.get_by_role("tab", name="Kafka").click()
        page.locator("#kafka-panel").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-kafka-tab")
        dto_filter = page.locator("#dto-reference-filter")
        dto_filter.fill("OrderCreated")
        dto = page.locator("#dto-references li")
        dto.wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-dto-filter")
        assert dto.count() == 1

        dto_filter.fill("absent")
        page.locator("#dto-references-empty").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-dto-empty")
        assert page.locator("#dto-references-empty").inner_text() == "Aucun DTO ne correspond à ce filtre."

        dto_filter.fill("")
        dto.scroll_into_view_if_needed()
        toolbar = page.locator(".toolbar").bounding_box()
        dto_box = dto.bounding_box()
        assert toolbar is not None and toolbar["y"] + toolbar["height"] <= 450
        assert dto_box is not None and dto_box["y"] + dto_box["height"] <= 450

        page.get_by_role("tab", name="Mongo").click()
        page.locator("#persistence-panel").wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-mongo-tab")
        mongo_filter = page.locator("#mongo-class-reference-filter")
        mongo_filter.fill("Order")
        mongo_class = page.locator("#mongo-class-references li")
        assert mongo_class.count() == 1
        _capture_render_snapshot(page, "constrained-after-mongo-filter")
        mongo_class.get_by_role("button", name="Inspecter").click()
        _capture_render_snapshot(page, "constrained-after-inspect-order")
        assert page.locator("#inspector-title").inner_text() == "Persistance MongoDB · Order"
        page.get_by_role("button", name="Address", exact=True).click()
        _capture_render_snapshot(page, "constrained-after-inspect-address")
        assert page.locator("#inspector-title").inner_text() == "Persistance MongoDB · Address"
        page.get_by_role("button", name="← Retour").click()
        _capture_render_snapshot(page, "constrained-after-inspector-back")
        assert page.locator("#inspector-title").inner_text() == "Persistance MongoDB · Order"
        page.locator("#inspector-close").click()
        _capture_render_snapshot(page, "constrained-after-inspector-close")

        page.get_by_role("tab", name="Explorer").click()
        _capture_render_snapshot(page, "constrained-after-explorer-tab")
        search = page.locator("#search")
        search.fill("orders -> orders.created -> payments")
        search.press("Enter")
        orders_stop = page.get_by_role("button", name="1. orders : Microservice")
        orders_stop.wait_for(state="visible")
        _capture_render_snapshot(page, "constrained-after-path-search")
        assert page.get_by_role("button", name="2. orders.created : Topic Kafka (OrderCreated)").is_visible()
        assert page.get_by_role("button", name="3. payments : Microservice").is_visible()
        assert not page.get_by_text("Flux de donnees").count()
        orders_stop.click()
        _capture_render_snapshot(page, "constrained-after-node-select")
        assert page.locator(".details-title").inner_text() == "orders"
        module_action = page.get_by_role("link", name="Ouvrir le module Maven dans VS Code")
        assert module_action.is_visible()
        assert module_action.get_attribute("href") == f"vscode://file/{module.path}"
        assert page.locator("#details .details-group > summary").all_text_contents() == [
            "Architecture", "Relations", "Sources"
        ]
        assert page.get_by_text("Topics publies", exact=True).is_visible()
        assert page.get_by_role("button", name="orders.created", exact=True).is_visible()
        assert page.get_by_role("button", name="DTO · OrderCreated").is_visible()
        page.get_by_text("Sources", exact=True).click()
        _capture_render_snapshot(page, "constrained-after-sources-open")
        assert page.get_by_text("Publisher.java:4").is_visible()
        page.get_by_role("button", name="orders.created", exact=True).click()
        _capture_render_snapshot(page, "constrained-after-topic-select")
        assert page.get_by_text("DTO Kafka", exact=True).is_visible()
        assert not page.get_by_text("Types publies", exact=True).count()
        assert not page.get_by_text("Types consommes", exact=True).count()

        search.fill("does-not-exist")
        search.press("Enter")
        _capture_render_snapshot(page, "constrained-after-missing-search")
        assert "Noeud introuvable" in page.locator("#search-status").inner_text()
        search.fill("inventory")
        search.press("Enter")
        _capture_render_snapshot(page, "constrained-after-inventory-search")
        assert page.locator(".details-title").inner_text() == "inventory"
        # Run the same geometry contract against every primary view and every
        # camera state. This is intentionally one fixture so a layout fix for
        # one view cannot silently regress another view.
        for view_name, status_text in (
            ("Graphe", "vue graphe"),
            ("Couches", "vue couches"),
            ("Namespaces", "vue namespaces"),
        ):
            page.get_by_role("button", name=view_name, exact=True).click()
            page.locator("#layout-status").filter(has_text=status_text).wait_for(state="visible")
            _capture_render_snapshot(page, f"constrained-{view_name.lower()}-after-view")
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            for _ in range(2):
                page.locator("#zoom-out").click()
            page.wait_for_timeout(400)
            _capture_render_snapshot(page, f"constrained-{view_name.lower()}-after-zoom-out")
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            for _ in range(2):
                page.locator("#zoom-in").click()
            page.wait_for_timeout(400)
            _capture_render_snapshot(page, f"constrained-{view_name.lower()}-after-zoom-in")
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            page.set_viewport_size({"width": 1024, "height": 600})
            page.wait_for_timeout(400)
            _capture_render_snapshot(page, f"constrained-{view_name.lower()}-after-resize")
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            if view_name == "Namespaces":
                _assert_background_pan_has_one_to_one_scale(page)
                _assert_pan_moves_cluster_overlays_as_one_surface(page)
            else:
                page.mouse.move(980, 80)
                page.mouse.down()
                page.mouse.move(900, 145, steps=10)
                page.mouse.up()
                page.wait_for_timeout(250)
                _capture_render_snapshot(page, f"constrained-{view_name.lower()}-after-pan")
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            _assert_architecture_cards_are_contained_in_clusters(page)
            _assert_architecture_clusters_do_not_overlap(page)
            assert graph.get_attribute("data-invalid-coordinates") == "false"
            page.set_viewport_size({"width": 800, "height": 450})
            page.wait_for_timeout(400)
        assert not errors
        context.close()
        browser.close()
