from __future__ import annotations

import os
from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright

from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.graph import GraphEdge
from systemlens.modules import DiscoveredModule
from systemlens.render import render_graph_html


pytestmark = pytest.mark.integration


def _chrome_executable() -> str | None:
    configured = os.environ.get("SYSTEMLENS_CHROME_BIN")
    candidates = [
        Path(configured) if configured else None,
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
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


@pytest.mark.slow
def test_html_export_resources_are_usable_in_a_constrained_browser_viewport(tmp_path: Path) -> None:
    chrome = _chrome_executable()
    if chrome is None:
        pytest.skip("Chromium is unavailable; install it with `playwright install chromium`.")

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
        {"orders": [_producer("com.example.OrderCreated"), rest_call], "payments": [consumer, rest_server]},
        [
            GraphEdge("kafka", "orders", "payments", _producer("com.example.OrderCreated"), consumer),
            GraphEdge("rest", "orders", "payments", rest_call, rest_server),
        ],
        build_modules=[module],
    )

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, executable_path=chrome)
        page = browser.new_page(viewport={"width": 800, "height": 450})
        errors: list[str] = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.set_content(document, wait_until="load")

        graph = page.locator("#graph")
        assert graph.get_attribute("data-relation-count") == "3"
        page.locator("#relation-http").uncheck()
        assert graph.get_attribute("data-relation-count") == "2"
        page.locator("#relation-kafka").uncheck()
        assert graph.get_attribute("data-relation-count") == "0"
        page.locator("#relation-http").check()
        assert graph.get_attribute("data-relation-count") == "1"

        page.get_by_role("tab", name="Ressources").click()
        page.locator("#resources-panel").wait_for(state="visible")
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

        page.get_by_role("tab", name="Explorer").click()
        search = page.locator("#search")
        search.fill("orders -> orders.created -> payments")
        search.press("Enter")
        orders_stop = page.get_by_role("button", name="1. orders : Microservice")
        assert orders_stop.is_visible()
        assert page.get_by_role("button", name="2. orders.created : Topic Kafka (OrderCreated)").is_visible()
        assert page.get_by_role("button", name="3. payments : Microservice").is_visible()
        assert not page.get_by_text("Flux de donnees").count()
        orders_stop.click()
        assert page.locator(".details-title").inner_text() == "orders"
        assert page.locator("#details .details-group > summary").all_text_contents() == ["Relations", "Sources"]
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
        assert not errors
        browser.close()
