"""Local web application for the static architecture view.

The application is intentionally built on :mod:`http.server`: SystemLens keeps
its normal installation dependency-free and the view already owns its HTML
and client-side interactions. It normally reads the architecture snapshot, but
can explicitly create a missing local index from the Architecture page.
"""

from __future__ import annotations

from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TypeAlias
from urllib.parse import urlsplit

from systemlens.architecture_inventory import (
    ArchitectureInventoryError,
    is_deployable_service,
    load_architecture_inventory,
)
from systemlens.config import ConfigError, init_config, load_config
from systemlens.graph import graph_edges_from_relations
from systemlens.indexer import index_repo
from systemlens.paths import db_path
from systemlens.render import render_graph_html
from systemlens.store import Store, StoreError

Document: TypeAlias = tuple[HTTPStatus, str]


class SystemLensWebApplication:
    """Serve the static architecture HTML projection from its authoritative input."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def document(self, path: str) -> Document:
        """Return one complete HTML document for a request path."""
        if path in {"/", "/index.html"}:
            return HTTPStatus.OK, _HOME_HTML
        if path == "/architecture":
            return self._architecture_document()
        return HTTPStatus.NOT_FOUND, _error_document("Page not found", "Choose Architecture.")

    def post(self, path: str) -> Document:
        """Handle the small set of state-changing local web actions."""
        if path == "/architecture/index":
            return self._create_architecture_index()
        return HTTPStatus.NOT_FOUND, _error_document("Page not found", "Choose Architecture.")

    def _architecture_document(self) -> Document:
        if not db_path(self.root).is_file():
            return HTTPStatus.OK, _missing_index_document()
        try:
            inventory = load_architecture_inventory(
                self.root, include_runtime_services_without_endpoints=True
            )
        except ArchitectureInventoryError as exc:
            return HTTPStatus.SERVICE_UNAVAILABLE, _error_document("Architecture unavailable", str(exc))
        services = {
            name: endpoints
            for name, endpoints in inventory.endpoints_by_service.items()
            if _is_exportable_microservice(name)
            and is_deployable_service(name, inventory.modules_by_service)
        }
        edges = [
            edge
            for edge in graph_edges_from_relations(inventory.relations, services)
            if _is_exportable_microservice(edge.from_service)
            and _is_exportable_microservice(edge.to_service)
        ]
        collections = {
            service: list(module.mongo_collections)
            for service, module in inventory.modules_by_service.items()
            if module.mongo_collections
        }
        document = render_graph_html(
            services,
            edges,
            collections,
            inventory.modules_by_service,
            inventory.warnings,
            inventory.modules,
            inventory.module_dependencies,
            inventory.source_roots,
            None,
            self.root,
            request_reply_strategy1=inventory.strategy1,
            diagnostics=inventory.diagnostics,
            kafka_dto_definitions=inventory.kafka_dto_definitions,
            openapi_contracts=inventory.openapi_contracts,
        )
        return HTTPStatus.OK, document

    def _create_architecture_index(self) -> Document:
        """Create the default local configuration and its first index on demand."""
        if db_path(self.root).is_file():
            return self._architecture_document()
        try:
            try:
                config = load_config(self.root)
            except ConfigError:
                init_config(self.root)
                config = load_config(self.root)
            with Store(self.root) as store:
                index_repo(self.root, config, store)
                store.set_meta("index_engine", "web")
        except (ConfigError, OSError, RuntimeError, StoreError):
            return (
                HTTPStatus.SERVICE_UNAVAILABLE,
                _error_document(
                    "Architecture index unavailable",
                    "SystemLens could not create the local architecture index. "
                    "Check the repository configuration and try again.",
                ),
            )
        return self._architecture_document()


def create_web_server(
    application: SystemLensWebApplication, host: str, port: int
) -> ThreadingHTTPServer:
    """Create a local HTTP server without exposing filesystem paths as routes."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - required stdlib handler name
            parsed = urlsplit(self.path)
            status, document = application.document(parsed.path)
            payload = document.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802 - required stdlib handler name
            status, document = application.post(self.path.split("?", 1)[0])
            payload = document.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            """Keep normal browser requests out of the terminal output."""

    return ThreadingHTTPServer((host, port), Handler)


def _error_document(title: str, detail: str) -> str:
    return f"""<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>{escape(title)}</title>
<style>body{{font-family:system-ui,sans-serif;max-width:44rem;margin:4rem auto;padding:0 1rem;color:#172033}}a{{color:#2563eb}}</style></head>
<body><h1>{escape(title)}</h1><p>{escape(detail)}</p><p><a href=\"/\">Back to SystemLens</a></p></body></html>"""


_HOME_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8"><title>SystemLens</title>
<style>body{font-family:system-ui,sans-serif;max-width:56rem;margin:4rem auto;padding:0 1rem;color:#172033}.cards{display:flex;gap:1rem;flex-wrap:wrap}.card{border:1px solid #dbe3f0;border-radius:.75rem;padding:1.25rem;flex:1 1 20rem}a{color:#2563eb;font-weight:650}</style></head>
<body><h1>SystemLens</h1><p>Local architecture investigation workspace.</p><div class="cards"><section class="card"><h2>Architecture</h2><p>Indexed microservices, APIs, Kafka, MongoDB, build, and quality views.</p><a href="/architecture">Open architecture</a></section></div></body></html>"""


def _missing_index_document() -> str:
    """Render the explicit local action needed before the static view exists."""
    return """<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Create architecture index</title>
<style>body{font-family:system-ui,sans-serif;max-width:44rem;margin:4rem auto;padding:0 1rem;color:#172033}button{background:#2563eb;border:0;border-radius:.5rem;color:white;cursor:pointer;font:inherit;font-weight:650;padding:.7rem 1rem}button:disabled{cursor:wait;opacity:.7}a{color:#2563eb}</style></head>
<body><h1>Architecture index not found</h1><p>Create a local index of this repository to explore its static architecture.</p><form method="post" action="/architecture/index" onsubmit="this.querySelector('button').disabled=true;this.querySelector('button').textContent='Creating index…'"><button type="submit">Create architecture index</button></form><p><a href="/">Back to SystemLens</a></p></body></html>"""


def _is_exportable_microservice(name: str) -> bool:
    """Keep the web architecture view aligned with the HTML export scope."""
    normalized = name.casefold()
    return "test" not in normalized and not (
        name.startswith("${") and name.endswith("}")
    )
