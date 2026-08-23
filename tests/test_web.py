from http import HTTPStatus
from pathlib import Path

from systemlens.web import SystemLensWebApplication


def _application() -> SystemLensWebApplication:
    return SystemLensWebApplication(
        Path.cwd(),
        since="1h",
        environment=None,
        endpoint=None,
        api_key=None,
        insecure_tls=False,
        max_services=30,
        max_transactions=50,
        max_dependencies=80,
        max_buckets=1_000,
        max_timeline_events=500,
    )


def test_web_home_links_the_two_existing_views() -> None:
    status, document = _application().document("/")

    assert status is HTTPStatus.OK
    assert 'href="/architecture"' in document
    assert 'href="/runtime"' in document


def test_web_returns_a_safe_not_found_document() -> None:
    status, document = _application().document("/not-a-systemlens-route")

    assert status is HTTPStatus.NOT_FOUND
    assert "Page not found" in document
    assert "/not-a-systemlens-route" not in document
