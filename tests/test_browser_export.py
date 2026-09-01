from __future__ import annotations

import json
import os
import re
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
    Path(__file__).parents[1] / "examples" / "plateforme-agree" / "plateforme-agreee.html"
)
_GRAPH_TEMPLATE = (
    Path(__file__).parents[1] / "src" / "systemlens" / "render" / "assets" / "graph.html"
)
_LAYER_GEOMETRY = (
    Path(__file__).parents[1] / "src" / "systemlens" / "render" / "assets" / "layer_geometry.js"
)


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
        "platform-edge", "platform-core", "platform-domain", "platform-infra",
        "platform-shared", "platform-ops", "platform-data", "platform-security",
        "platform-workflow", "platform-reporting",
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
    data["groups"] = []
    for node in data["nodes"]:
        namespace = node.get("metadata", {}).get("namespace") or "root"
        # The historical export stores the complex fixture's namespace in
        # metadata. Promote it to the current renderer contract so the test
        # exercises real cluster packing rather than one ROOT cluster.
        node["project_namespace_path"] = namespace
        node["cluster_path"] = namespace
        node["runtime_namespaces"] = [namespace]
        node.setdefault("architecture_layer", node.get("layer") or "application")
        node.setdefault("layer", node["architecture_layer"])
    template = _GRAPH_TEMPLATE.read_text(encoding="utf-8")
    return template.replace("__GRAPH_DATA__", json.dumps(data)).replace(
        "__LAYER_GEOMETRY__", _LAYER_GEOMETRY.read_text(encoding="utf-8")
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


def _assert_architecture_cards_do_not_overlap(page) -> None:
    overlap = page.evaluate(
        """() => {
            const cards = [...document.querySelectorAll('.graph-node-card-label')];
            const rects = cards.map(card => card.getBoundingClientRect());
            for (let leftIndex = 0; leftIndex < rects.length; leftIndex += 1) {
              for (let rightIndex = leftIndex + 1; rightIndex < rects.length; rightIndex += 1) {
                const left = rects[leftIndex];
                const right = rects[rightIndex];
                if (left.right <= 0 || left.left >= innerWidth || left.bottom <= 0 || left.top >= innerHeight
                    || right.right <= 0 || right.left >= innerWidth || right.bottom <= 0 || right.top >= innerHeight) continue;
                if (
                    left.right > right.left && right.right > left.left
                    && left.bottom > right.top && right.bottom > left.top
                ) return [
                    cards[leftIndex].dataset.nodeId, cards[rightIndex].dataset.nodeId,
                    [left.x, left.y, left.width, left.height],
                    [right.x, right.y, right.width, right.height],
                ];
              }
            }
            return null;
            /* return rects.every((left, leftIndex) => rects.every((right, rightIndex) => (
                leftIndex === rightIndex
                || left.right <= right.left
                || right.right <= left.left
                || left.bottom <= right.top
                || right.bottom <= left.top
            ))); */
        }"""
    )
    assert overlap is None, overlap


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
    assert page.evaluate(
        """() => {
            const rects = [...document.querySelectorAll('.graph-namespace-group')]
                .map(group => group.getBoundingClientRect());
            return rects.every((left, leftIndex) => rects.every((right, rightIndex) => (
                leftIndex === rightIndex
                || left.right <= right.left
                || right.right <= left.left
                || left.bottom <= right.top
                || right.bottom <= left.top
            )));
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
    page.mouse.move(1250, 780)
    page.mouse.down()
    samples: list[list[list[float]]] = []
    for step in range(1, 9):
        page.mouse.move(1250 - step * 15, 780 - step * 10)
        samples.append(_surface_rects(page))
    page.mouse.up()
    page.wait_for_timeout(250)
    after = _surface_rects(page)
    assert all(len(sample) == len(before) for sample in samples)
    assert len(after) == len(before)

    # During the drag and after release, every overlay keeps the same size and
    # follows the same camera translation. This catches pan-to-zoom, inertia
    # jumps and stale namespace overlays independently of visual inspection.
    first_delta = None
    for step, sample in enumerate(samples, 1):
        for old, moved in zip(before, sample):
            for index in (2, 3):
                assert moved[index] - moved[index - 2] == pytest.approx(
                    old[index] - old[index - 2], abs=1.5
                )
            delta = (moved[0] - old[0], moved[1] - old[1])
            if first_delta is None:
                first_delta = delta
            else:
                assert delta[0] == pytest.approx(first_delta[0] * step, abs=3)
                assert delta[1] == pytest.approx(first_delta[1] * step, abs=3)
    for old, settled in zip(before, after):
        for index in (2, 3):
            assert settled[index] - settled[index - 2] == pytest.approx(
                old[index] - old[index - 2], abs=1.5
            )
            assert settled[0] - old[0] == pytest.approx(first_delta[0] * 8, abs=3)
            assert settled[1] - old[1] == pytest.approx(first_delta[1] * 8, abs=3)


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
        assert restored_distance < after_distance * 0.99


def _assert_background_pan_has_one_to_one_scale(page) -> None:
    before = page.locator(".graph-namespace-group").first.bounding_box()
    assert before
    page.mouse.move(1100, 500)
    page.mouse.down()
    page.mouse.move(1000, 580, steps=10)
    page.mouse.up()
    page.wait_for_timeout(250)
    after = page.locator(".graph-namespace-group").first.bounding_box()
    assert after
    assert after["x"] - before["x"] == pytest.approx(-100, abs=2)
    assert after["y"] - before["y"] == pytest.approx(80, abs=2)
    assert after["width"] == pytest.approx(before["width"], abs=0.1)
    assert after["height"] == pytest.approx(before["height"], abs=0.1)


def _assert_clusters_only_overlap_when_nested(page) -> None:
    assert page.evaluate(
        """() => {
            const rects = [...document.querySelectorAll(
                '.graph-namespace-group, .graph-project-group'
            )].map(element => element.getBoundingClientRect());
            const contains = (outer, inner) => (
                inner.left >= outer.left && inner.right <= outer.right
                && inner.top >= outer.top && inner.bottom <= outer.bottom
            );
            return rects.every((left, leftIndex) => rects.every((right, rightIndex) => {
                if (leftIndex === rightIndex
                    || left.right <= right.left
                    || right.right <= left.left
                    || left.bottom <= right.top
                    || right.bottom <= left.top) return true;
                return contains(left, right) || contains(right, left);
            }));
        }"""
    )


def _assert_layer_bands_are_disjoint_and_contain_clusters(page) -> None:
    result = page.evaluate(
        """() => {
                const bands = [...document.querySelectorAll('.graph-layer-band')]
                    .map(element => element.getBoundingClientRect())
                    .filter(band => band.right > 0 && band.left < innerWidth && band.bottom > 0 && band.top < innerHeight);
                const clusters = [...document.querySelectorAll('.graph-namespace-group')]
                    .map(element => element.getBoundingClientRect())
                    .filter(cluster => cluster.right > 0 && cluster.left < innerWidth && cluster.bottom > 0 && cluster.top < innerHeight);
            const overlap = (left, right) => (
                left.left < right.right && left.right > right.left
                && left.top < right.bottom && left.bottom > right.top
            );
            const contains = (outer, inner) => (
                inner.left >= outer.left && inner.right <= outer.right
                && inner.top >= outer.top && inner.bottom <= outer.bottom
            );
            const valid = bands.every((band, index) => bands.every((other, otherIndex) => (
                index === otherIndex || !overlap(band, other)
            ))) && clusters.every(cluster => bands.some(band => contains(band, cluster)));
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
        chrome = _chrome_executable(playwright)
        if chrome is None:
            pytest.skip("Chromium is unavailable; run `uv run playwright install chromium`.")
        try:
            browser = playwright.chromium.launch(headless=True, executable_path=chrome)
        except PlaywrightError as error:
            pytest.skip(f"Chromium cannot be launched in this environment: {error}")
        page = browser.new_page(viewport={"width": 1440, "height": 900})
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
            if layout_id == "layout-cluster":
                screenshot_path = Path("output/playwright/complex-cluster-fit-validation.png")
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(screenshot_path), full_page=False)
            _assert_geometry_contract(page, layered=layered)
            if layout_id == "layout-cluster":
                screenshot_path = Path("output/playwright/complex-cluster-final-validation.png")
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(screenshot_path), full_page=False)
            _assert_pan_does_not_zoom_or_desynchronise_overlays(page)
            _assert_geometry_contract(page, layered=layered)
            _assert_zoom_is_monotonic_and_settles_without_a_release_jump(page)
            _assert_geometry_contract(page, layered=layered)

            page.locator("#zoom-out").click()
            page.locator("#zoom-out").click()
            page.wait_for_timeout(300)
            _assert_geometry_contract(page, layered=layered)

            page.mouse.move(1250, 780)
            page.mouse.down()
            page.mouse.move(1120, 700, steps=8)
            page.mouse.up()
            page.wait_for_timeout(300)
            _assert_geometry_contract(page, layered=layered)

            page.set_viewport_size({"width": 1024, "height": 640})
            page.wait_for_timeout(500)
            _assert_geometry_contract(page, layered=layered)
            page.set_viewport_size({"width": 1440, "height": 900})
            page.wait_for_timeout(300)

        assert not errors, errors
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
        chrome = _chrome_executable(playwright)
        if chrome is None:
            pytest.skip("Chromium is unavailable; run `uv run playwright install chromium`.")
        browser = playwright.chromium.launch(headless=True, executable_path=chrome)
        page = browser.new_page(viewport={"width": 800, "height": 450})
        page.set_default_timeout(5_000)
        errors: list[str] = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.set_content(document, wait_until="load")
        page.wait_for_timeout(100)
        assert not errors

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
        assert page.locator("#relation-http").is_visible()
        page.locator("#relation-http").uncheck()
        assert graph.get_attribute("data-relation-count") == "3"
        page.locator("#relation-kafka").uncheck()
        assert graph.get_attribute("data-relation-count") == "1"
        page.locator("#relation-http").check()
        assert graph.get_attribute("data-relation-count") == "2"
        page.locator("#relation-kafka").check()
        assert graph.get_attribute("data-relation-count") == "4"
        page.locator("#layout-elk").click()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
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
        without_topic_count = _assert_filtered_graph_is_valid(
            page, full_node_count, "kafka_topic"
        )

        page.locator("#node-mongodb-collection").uncheck()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
        _assert_filtered_graph_is_valid(page, without_topic_count, "mongodb_collection")

        page.locator("#node-microservice").uncheck()
        page.locator("#node-external-microservice").uncheck()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")
        assert page.locator("#graph").get_attribute("data-visible-node-count") == "0"
        assert page.locator("#graph").get_attribute("data-invalid-coordinates") == "false"
        page.locator("#node-microservice").check()
        page.locator("#node-external-microservice").check()
        page.locator("#node-kafka-topic").check()
        page.locator("#node-mongodb-collection").check()
        page.locator("#layout-status").filter(has_text="vue couches").wait_for(state="visible")

        page.locator("#layout-cluster").click()
        page.locator("#layout-status").filter(has_text="vue namespaces").wait_for(state="visible")
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
        assert page.evaluate(
            """() => {
                const boxes = [...document.querySelectorAll('#graph-layers .graph-namespace-group')]
                    .map(element => element.getBoundingClientRect());
                return boxes.every((left, leftIndex) => boxes.every((right, rightIndex) => (
                    leftIndex === rightIndex
                    || left.right <= right.left
                    || right.right <= left.left
                    || left.bottom <= right.top
                    || right.bottom <= left.top
                )));
            }"""
        )
        assert page.locator("#graph").get_attribute("data-invalid-coordinates") == "false"

        page.get_by_role("tab", name="Kafka").click()
        page.locator("#kafka-panel").wait_for(state="visible")
        dto_filter = page.locator("#dto-reference-filter")
        dto_filter.fill("OrderCreated")
        dto = page.locator("#dto-references li")
        dto.wait_for(state="visible")
        assert dto.count() == 1

        dto_filter.fill("absent")
        page.locator("#dto-references-empty").wait_for(state="visible")
        assert page.locator("#dto-references-empty").inner_text() == "Aucun DTO ne correspond à ce filtre."

        dto_filter.fill("")
        dto.scroll_into_view_if_needed()
        toolbar = page.locator(".toolbar").bounding_box()
        dto_box = dto.bounding_box()
        assert toolbar is not None and toolbar["y"] + toolbar["height"] <= 450
        assert dto_box is not None and dto_box["y"] + dto_box["height"] <= 450

        page.get_by_role("tab", name="Mongo").click()
        page.locator("#persistence-panel").wait_for(state="visible")
        mongo_filter = page.locator("#mongo-class-reference-filter")
        mongo_filter.fill("Order")
        mongo_class = page.locator("#mongo-class-references li")
        assert mongo_class.count() == 1
        mongo_class.get_by_role("button", name="Inspecter").click()
        assert page.locator("#inspector-title").inner_text() == "Persistance MongoDB · Order"
        page.get_by_role("button", name="Address", exact=True).click()
        assert page.locator("#inspector-title").inner_text() == "Persistance MongoDB · Address"
        page.get_by_role("button", name="← Retour").click()
        assert page.locator("#inspector-title").inner_text() == "Persistance MongoDB · Order"
        page.locator("#inspector-close").click()

        page.get_by_role("tab", name="Explorer").click()
        search = page.locator("#search")
        search.fill("orders -> orders.created -> payments")
        search.press("Enter")
        orders_stop = page.get_by_role("button", name="1. orders : Microservice")
        orders_stop.wait_for(state="visible")
        assert page.get_by_role("button", name="2. orders.created : Topic Kafka (OrderCreated)").is_visible()
        assert page.get_by_role("button", name="3. payments : Microservice").is_visible()
        assert not page.get_by_text("Flux de donnees").count()
        orders_stop.click()
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
        assert page.get_by_text("Publisher.java:4").is_visible()
        page.get_by_role("button", name="orders.created", exact=True).click()
        assert page.get_by_text("DTO Kafka", exact=True).is_visible()
        assert not page.get_by_text("Types publies", exact=True).count()
        assert not page.get_by_text("Types consommes", exact=True).count()

        search.fill("does-not-exist")
        search.press("Enter")
        assert "Noeud introuvable" in page.locator("#search-status").inner_text()
        search.fill("inventory")
        search.press("Enter")
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
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            for _ in range(2):
                page.locator("#zoom-out").click()
            page.wait_for_timeout(400)
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            for _ in range(2):
                page.locator("#zoom-in").click()
            page.wait_for_timeout(400)
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            page.set_viewport_size({"width": 1024, "height": 600})
            page.wait_for_timeout(400)
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
            _assert_architecture_cards_have_uniform_size(page)
            _assert_architecture_cards_do_not_overlap(page)
            _assert_architecture_cards_are_contained_in_clusters(page)
            _assert_architecture_clusters_do_not_overlap(page)
            assert graph.get_attribute("data-invalid-coordinates") == "false"
            page.set_viewport_size({"width": 800, "height": 450})
            page.wait_for_timeout(400)
        assert not errors
        browser.close()
