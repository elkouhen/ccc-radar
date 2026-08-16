from __future__ import annotations

import os
from pathlib import Path

import pytest
from playwright.sync_api import Playwright, sync_playwright

from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.graph import GraphEdge
from systemlens.modules import DiscoveredModule, MongoField, MongoPersistenceClass
from systemlens.apm_report import render_runtime_report_html
from systemlens.render import render_graph_html


pytestmark = pytest.mark.integration


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


@pytest.mark.slow
def test_apm_runtime_report_filters_observed_dependency_flows() -> None:
    document = render_runtime_report_html(
        {
            "schema_version": "apm-runtime-report-v1",
            "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
            "environment": "production",
            "services": [
                {"service": "orders", "calls": 10, "failure_calls": 1, "error_rate": 0.1, "average_ms": 80.0, "p95_ms": 240.0}
            ],
            "transactions": [
                {"service": "orders", "transaction": "POST /checkout", "calls": 6, "failure_calls": 1, "error_rate": 0.166667, "average_ms": 90.0, "p95_ms": 250.0},
                {"service": "catalog", "transaction": "GET /products", "calls": 4, "failure_calls": 0, "error_rate": 0.0, "average_ms": 20.0, "p95_ms": 40.0},
            ],
            "dependencies": [
                {"source": "orders", "target": "payments", "target_type": "http", "calls": 6, "failure_calls": 1, "error_rate": 0.166667, "average_ms": 70.0},
                {"source": "catalog", "target": "mongo", "target_type": "db", "calls": 3, "failure_calls": 0, "error_rate": 0.0, "average_ms": 10.0},
            ],
            "coverage": {
                "services": {"items_seen": 1, "items_exported": 1, "truncated": False, "truncation_reasons": []},
                "transactions": {"items_seen": 1, "items_exported": 1, "truncated": False, "truncation_reasons": []},
                "dependencies": {"items_seen": 2, "items_exported": 2, "truncated": False, "truncation_reasons": []},
            },
        }
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

        assert "orders" in page.locator("#services").inner_text()
        assert "POST /checkout" in page.locator("#transactions").inner_text()
        assert "POST /checkout" in page.locator("#transaction-graph").inner_text()
        page.locator("#transaction-service-filter").select_option("orders")
        assert "POST /checkout" in page.locator("#transaction-graph").inner_text()
        assert "GET /products" not in page.locator("#transaction-graph").inner_text()
        assert "payments" in page.locator("#flows").inner_text()
        page.locator("#service-filter").select_option("orders")
        assert "payments" in page.locator("#flows").inner_text()
        assert "mongo" not in page.locator("#flows").inner_text()
        assert "P95 is an approximate percentile" in page.content()
        assert not errors
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
        assert graph.get_attribute("data-relation-count") == "4"
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

        page.get_by_role("tab", name="Persistance").click()
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
        search.fill("inventory")
        search.press("Enter")
        assert page.locator(".details-title").inner_text() == "inventory"
        assert not errors
        browser.close()
