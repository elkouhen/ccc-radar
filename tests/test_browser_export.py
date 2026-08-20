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
                {"service": "orders", "calls": 10, "failure_calls": 1, "outcome_calls": 8, "error_rate": 0.125, "average_ms": 80.0, "p95_ms": 240.0}
            ],
            "transactions": [
                {"service": "orders", "transaction": "POST /checkout", "transaction_type": "request", "calls": 6, "failure_calls": 1, "error_rate": 0.166667, "average_ms": 90.0, "p95_ms": 250.0},
                {"service": "catalog", "transaction": "GET /products", "transaction_type": "messaging", "calls": 4, "failure_calls": 0, "error_rate": 0.0, "average_ms": 20.0, "p95_ms": 40.0},
            ],
            "dependencies": [
                {"source": "orders", "target": "payments", "target_type": "http", "calls": 6, "failure_calls": 1, "error_rate": 0.166667, "average_ms": 70.0},
                {"source": "catalog", "target": "mongo", "target_type": "db", "calls": 3, "failure_calls": 0, "error_rate": 0.0, "average_ms": 10.0},
            ],
            "timeline_spans": [
                {"timestamp": "2026-08-15T09:01:00Z", "service": "orders", "span": "Span", "span_type": "request", "duration_ms": 90.0, "outcome": "success", "waterfall_refs": ["waterfall-1"]},
                {"timestamp": "2026-08-15T09:02:00Z", "service": "catalog", "span": "Span", "span_type": "messaging", "duration_ms": 20.0, "outcome": "failure"},
            ],
            "distributed_traces": [{
                "source_kind": "http_service",
                "source": "orders",
                "timestamp": "2026-08-15T09:01:00Z",
                "service": "orders",
                "name": "HTTP transaction",
                "duration_ms": 90.0,
                "transaction_type": "request",
                "route": ["orders", "payments"],
                "distributed_operations": ["orders · HTTP transaction"],
                "spans": [],
                "truncated": False,
                "waterfall_ref": "waterfall-1",
            }, {
                "source_kind": "http_service",
                "source": "catalog",
                "timestamp": "2026-08-15T09:02:00Z",
                "service": "catalog",
                "name": "HTTP transaction",
                "duration_ms": 20.0,
                "transaction_type": "messaging",
                "route": ["catalog", "orders"],
                "distributed_operations": ["Distributed transaction"],
                "spans": [],
                "truncated": True,
                "waterfall_ref": "waterfall-2",
            }],
            "coverage": {
                "services": {"items_seen": 1, "items_exported": 1, "truncated": False, "truncation_reasons": []},
                "transactions": {"items_seen": 1, "items_exported": 1, "truncated": False, "truncation_reasons": []},
                "dependencies": {"items_seen": 2, "items_exported": 2, "truncated": False, "truncation_reasons": []},
                "timeline": {"items_exported": 2, "truncated": False, "truncation_reasons": [], "available": True},
                "distributed_traces": {"items_exported": 1, "max_traces": 20, "truncated": True, "truncation_reasons": ["max_spans_per_trace"], "available": True},
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
        assert "Outcome coverage" in page.locator("#cards").inner_text()
        assert "Transaction" in page.locator("#transactions").inner_text()
        assert page.locator("#details-slow-transactions").is_visible()
        assert "orders" in page.locator("#transaction-graph").inner_text()
        assert page.locator("#global-service-filter").is_visible()
        page.get_by_role("button", name="Dependencies").click()
        page.locator("#map-panel").wait_for(state="visible")
        dependencies_filter = page.locator("#dependencies-observed-service-filter")
        assert dependencies_filter.is_visible()
        dependencies_filter.select_option("orders")
        assert page.locator("#map-service-filter").is_visible()
        assert page.locator("#map-service-filter").input_value() == "orders"
        page.get_by_role("button", name="Transaction workloads", exact=True).click()
        page.locator("#details-transactions").wait_for(state="visible")
        assert page.locator("#transactions-observed-service-filter").is_visible()
        assert page.locator("#transactions-observed-service-filter").input_value() == "orders"
        assert not page.locator("#details-slow-transactions").is_visible()
        assert not page.locator("#details-distributed-traces").is_visible()
        assert "Partial waterfall" in page.locator("#distributed-traces").inner_text()
        page.locator("#transaction-service-filter").select_option("orders")
        assert "orders" in page.locator("#transaction-graph").inner_text()
        assert "catalog" not in page.locator("#transaction-graph").inner_text()
        page.get_by_role("button", name="Distributed traces").click()
        page.locator("#details-distributed-traces").wait_for(state="visible")
        assert page.locator("#distributed-root-type-filter").is_visible()
        page.locator("#distributed-root-type-filter").select_option("request")
        assert "orders" in page.locator("#distributed-traces").inner_text()
        page.get_by_role("button", name="Dependencies").click()
        page.locator("#details-flows").wait_for(state="visible")
        assert "payments" in page.locator("#flows").inner_text()
        page.locator("#service-filter").select_option("orders")
        assert "payments" in page.locator("#flows").inner_text()
        assert "mongo" not in page.locator("#flows").inner_text()
        page.get_by_role("button", name="Span execution log").click()
        page.locator("#timeline-panel").wait_for(state="visible")
        assert page.locator("#timeline-observed-service-filter").is_visible()
        assert page.locator("#timeline-observed-service-filter").input_value() == "orders"
        assert page.locator("#timeline-span-type-filter").is_visible()
        page.locator("#timeline-service-filter").select_option("orders")
        assert "orders" in page.locator("#timeline-events").inner_text()
        assert "catalog" not in page.locator("#timeline-events").inner_text()
        page.locator("#timeline-span-type-filter").select_option("messaging")
        assert "No recorded span matches this selection." in page.locator("#timeline-events").inner_text()
        page.locator("#timeline-span-type-filter").select_option("request")
        page.locator("#timeline-service-filter").select_option("")
        page.locator("#timeline-span-type-filter").select_option("")
        assert "catalog" in page.locator("#timeline-events").inner_text()
        page.locator("#timeline-service-filter").select_option("orders")
        page.locator("#timeline-events .timeline-item").click()
        page.locator("#details-distributed-traces").wait_for(state="visible")
        assert page.get_by_role("button", name="Distributed traces").get_attribute("class") == "runtime-tab is-active"
        assert page.locator("#distributed-observed-service-filter").is_visible()
        assert page.locator("#clear-timeline-waterfall-focus").is_visible()
        assert "exact" in page.locator("#clear-timeline-waterfall-focus").inner_text()
        assert "Partial waterfall" not in page.locator("#distributed-traces").inner_text()
        assert "catalog" not in page.locator("#distributed-traces").inner_text()
        page.get_by_role("button", name="Transaction workloads", exact=True).click()
        assert not page.locator("#details-distributed-traces").is_visible()
        assert "P95 is an approximate percentile" in page.content()
        assert not errors
        browser.close()


@pytest.mark.slow
def test_apm_runtime_report_limits_spans_and_transactions_to_ten_longest() -> None:
    traces = [{
        "source_kind": "http_service",
        "source": "orders",
        "timestamp": f"2026-08-15T09:{index:02}:00Z",
        "service": "orders",
        "name": "HTTP transaction",
        "transaction_type": "request",
        "duration_ms": float(index),
        "route": ["orders", "payments"],
        "distributed_operations": ["Distributed transaction"],
        "spans": [],
        "truncated": False,
        "waterfall_ref": f"waterfall-{index}",
    } for index in range(12)]
    document = render_runtime_report_html({
        "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
        "services": [{"service": "orders", "calls": 12, "outcome_calls": 12, "failure_calls": 0, "average_ms": 1.0, "p95_ms": 12.0, "error_rate": 0.0}],
        "transactions": [{
            "service": f"service-{index}", "transaction": "HTTP transaction", "transaction_type": "request",
            "calls": 1, "average_ms": float(index), "p95_ms": float(index), "error_rate": 0.0,
        } for index in range(12)],
        "dependencies": [],
        "timeline_spans": [{
            "timestamp": f"2026-08-15T09:{index:02}:00Z",
            "service": f"service-{index}", "span": "Span", "span_type": "request",
            "duration_ms": float(index), "outcome": "success",
            "waterfall_refs": [f"waterfall-{index}"],
        } for index in range(12)],
        "distributed_traces": traces,
        "coverage": {
            "services": {"items_seen": 1, "items_exported": 1, "truncated": False, "truncation_reasons": []},
            "transactions": {"items_seen": 12, "items_exported": 12, "truncated": False, "truncation_reasons": []},
            "dependencies": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "timeline": {"items_exported": 12, "truncated": False, "truncation_reasons": [], "available": True},
            "distributed_traces": {"items_exported": 12, "max_traces": 20, "truncated": False, "truncation_reasons": [], "available": True},
        },
    })

    with sync_playwright() as playwright:
        chrome = _chrome_executable(playwright)
        if chrome is None:
            pytest.skip("Chromium is unavailable; run `uv run playwright install chromium`.")
        browser = playwright.chromium.launch(headless=True, executable_path=chrome)
        page = browser.new_page()
        page.set_content(document, wait_until="load")

        page.get_by_role("button", name="Span execution log").click()
        page.locator("#timeline-top-ten-longest-filter").check()
        assert page.locator("#timeline-events .timeline-item").count() == 10
        assert page.locator("#timeline-events .timeline-item strong").all_text_contents() == [
            f"{index} ms" for index in range(2, 12)
        ]

        page.get_by_role("button", name="Distributed traces").click()
        page.locator("#distributed-top-ten-impact-filter").check()
        assert page.locator("#distributed-traces .distributed-trace").count() == 10
        assert "cumulative duration" in page.locator("#distributed-traces").inner_text()
        browser.close()


@pytest.mark.slow
def test_apm_runtime_report_ranks_spans_and_traces_by_error_count() -> None:
    failures = [
        ("payments", "Database span", "db", 3),
        ("inventory", "Messaging span", "messaging", 2),
        ("orders", "HTTP span", "request", 1),
    ]
    timeline_spans = [
        {
            "timestamp": f"2026-08-15T09:0{index}:00Z", "service": service,
            "span": span, "span_type": span_type, "duration_ms": float(index + 1),
            "outcome": "failure",
        }
        for service, span, span_type, count in failures
        for index in range(count)
    ]
    traces = [
        {
            "source_kind": "http_service", "source": service,
            "timestamp": f"2026-08-15T09:0{index}:00Z", "service": service,
            "name": "HTTP transaction", "transaction_type": "request",
            "duration_ms": float(index + 1), "outcome": "failure",
            "route": [service, "payments"], "distributed_operations": [],
            "spans": [], "truncated": False,
            "waterfall_ref": f"waterfall-{service}-{index}",
        }
        for service, _, _, count in failures
        for index in range(count)
    ]
    document = render_runtime_report_html({
        "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
        "services": [], "transactions": [], "dependencies": [],
        "timeline_spans": timeline_spans, "distributed_traces": traces,
        "coverage": {
            "services": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "transactions": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "dependencies": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "timeline": {"items_exported": 6, "truncated": False, "truncation_reasons": [], "available": True},
            "distributed_traces": {"items_exported": 6, "max_traces": 20, "truncated": False, "truncation_reasons": [], "available": True},
        },
    })

    with sync_playwright() as playwright:
        chrome = _chrome_executable(playwright)
        if chrome is None:
            pytest.skip("Chromium is unavailable; run `uv run playwright install chromium`.")
        browser = playwright.chromium.launch(headless=True, executable_path=chrome)
        page = browser.new_page()
        page.set_content(document, wait_until="load")

        page.get_by_role("button", name="Span execution log").click()
        page.locator("#timeline-top-ten-error-impact-filter").check()
        assert page.locator("#timeline-events .timeline-item").count() == 3
        assert "3 observed failed executions" in page.locator("#timeline-events").inner_text()

        page.get_by_role("button", name="Distributed traces").click()
        page.locator("#distributed-top-ten-error-impact-filter").check()
        assert page.locator("#distributed-traces .distributed-trace").count() == 3
        assert "3 observed failed executions" in page.locator("#distributed-traces").inner_text()
        browser.close()


@pytest.mark.slow
def test_apm_runtime_report_expands_dense_waterfall_subspans() -> None:
    spans = [{
        "service": "orders", "name": "HTTP transaction", "kind": "transaction",
        "transaction_type": "request", "outcome": "success", "depth": 0,
        "offset_ms": 0.0, "duration_ms": 100.0,
    }] + [{
        "service": "orders", "name": "Span", "kind": "span",
        "transaction_type": None, "outcome": "failure" if index == 0 else "success", "depth": 1,
        "error": {"category": "timeout"} if index == 0 else None,
        "offset_ms": float(index + 1), "duration_ms": 1.0,
    } for index in range(25)]
    document = render_runtime_report_html({
        "window": {"from": "2026-08-15T09:00:00Z", "to": "2026-08-15T10:00:00Z"},
        "services": [], "transactions": [], "dependencies": [], "timeline_spans": [],
        "distributed_traces": [{
            "source_kind": "http_service", "source": "orders",
            "timestamp": "2026-08-15T09:00:00Z", "service": "orders",
            "name": "HTTP transaction", "transaction_type": "request",
            "duration_ms": 100.0, "route": ["orders"], "distributed_operations": [],
            "spans": spans, "truncated": False, "waterfall_ref": "waterfall-1",
        }],
        "coverage": {
            "services": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "transactions": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "dependencies": {"items_seen": 0, "items_exported": 0, "truncated": False, "truncation_reasons": []},
            "timeline": {"items_exported": 0, "truncated": False, "truncation_reasons": [], "available": True},
            "distributed_traces": {"items_exported": 1, "max_traces": 20, "truncated": False, "truncation_reasons": [], "available": True},
        },
    })

    with sync_playwright() as playwright:
        chrome = _chrome_executable(playwright)
        if chrome is None:
            pytest.skip("Chromium is unavailable; run `uv run playwright install chromium`.")
        browser = playwright.chromium.launch(headless=True, executable_path=chrome)
        page = browser.new_page()
        page.set_content(document, wait_until="load")
        page.get_by_role("button", name="Distributed traces").click()
        rows = page.locator(".distributed-trace .trace-span")
        assert rows.count() == 26
        assert page.locator(".distributed-trace .trace-span:visible").count() == 1
        page.locator(".trace-span-toggle").click()
        assert page.locator(".distributed-trace .trace-span:visible").count() == 26
        failed_span = page.locator(".trace-span.is-failure")
        assert failed_span.count() == 1
        assert failed_span.locator(".trace-error-badge").inner_text() == "Error"
        assert failed_span.locator(".trace-bar.is-failure").count() == 1
        page.locator(".trace-span-details").nth(1).click()
        detail = page.locator(".trace-span-detail")
        assert detail.is_visible()
        assert "Outcome" in detail.inner_text()
        assert "failure" in detail.inner_text()
        assert "Dependency timed out" in detail.inner_text()
        assert "Error messages are sanitized categories" in detail.inner_text()
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
