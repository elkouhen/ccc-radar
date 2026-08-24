"""Local web application for the static architecture and runtime APM views.

The application is intentionally built on :mod:`http.server`: SystemLens keeps
its normal installation dependency-free and the views already own their HTML
and client-side interactions. It normally reads the architecture snapshot, but
can explicitly create a missing local index from the Architecture page. It
never persists APM observations.
"""

from __future__ import annotations

from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TypeAlias
from urllib.parse import parse_qs, urlsplit

from systemlens.apm import ApmError, ElasticApmClient, load_settings
from systemlens.apm_report import build_runtime_report, render_runtime_report_html
from systemlens.architecture_inventory import ArchitectureInventoryError, load_architecture_inventory
from systemlens.config import ConfigError, init_config, load_config
from systemlens.graph import graph_edges_from_relations
from systemlens.indexer import index_repo
from systemlens.paths import db_path
from systemlens.render import render_graph_html
from systemlens.store import Store, StoreError

Document: TypeAlias = tuple[HTTPStatus, str]
_RUNTIME_WINDOWS = frozenset({"15m", "1h", "6h", "24h", "7d"})


class SystemLensWebApplication:
    """Serve the two existing HTML projections from their authoritative inputs."""

    def __init__(
        self,
        root: Path,
        *,
        since: str,
        environment: str | None,
        endpoint: str | None,
        api_key: str | None,
        insecure_tls: bool,
        max_services: int,
        max_transactions: int,
        max_dependencies: int,
        max_buckets: int,
        max_timeline_events: int,
    ) -> None:
        self.root = root.resolve()
        self.since = since
        self.environment = environment
        self.endpoint = endpoint
        self.api_key = api_key
        self.insecure_tls = insecure_tls
        self.max_services = max_services
        self.max_transactions = max_transactions
        self.max_dependencies = max_dependencies
        self.max_buckets = max_buckets
        self.max_timeline_events = max_timeline_events

    def document(self, path: str, *, since: str | None = None) -> Document:
        """Return one complete HTML document for a request path."""
        if path in {"/", "/index.html"}:
            return HTTPStatus.OK, _HOME_HTML
        if path == "/architecture":
            return self._architecture_document()
        if path == "/runtime":
            return self._runtime_document(since if since in _RUNTIME_WINDOWS else self.since)
        return HTTPStatus.NOT_FOUND, _error_document("Page not found", "Choose Architecture or APM runtime.")

    def post(self, path: str) -> Document:
        """Handle the small set of state-changing local web actions."""
        if path == "/architecture/index":
            return self._create_architecture_index()
        return HTTPStatus.NOT_FOUND, _error_document("Page not found", "Choose Architecture or APM runtime.")

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

    def _runtime_document(self, since: str) -> Document:
        try:
            settings = load_settings(
                self.endpoint, self.api_key, insecure_tls=self.insecure_tls
            )
            report = build_runtime_report(
                ElasticApmClient(settings),
                since=since,
                environment=self.environment,
                max_services=self.max_services,
                max_transactions=self.max_transactions,
                max_dependencies=self.max_dependencies,
                max_buckets=self.max_buckets,
                max_timeline_events=self.max_timeline_events,
            )
        except ApmError as exc:
            return HTTPStatus.SERVICE_UNAVAILABLE, _error_document("APM runtime unavailable", str(exc))
        report["selected_window"] = since
        return HTTPStatus.OK, render_runtime_report_html(report)


def create_web_server(
    application: SystemLensWebApplication, host: str, port: int
) -> ThreadingHTTPServer:
    """Create a local HTTP server without exposing filesystem paths as routes."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - required stdlib handler name
            parsed = urlsplit(self.path)
            since = parse_qs(parsed.query).get("since", [None])[0]
            status, document = application.document(parsed.path, since=since)
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
<body><h1>SystemLens</h1><p>Local architecture and runtime investigation workspace.</p><div class="cards"><section class="card"><h2>Architecture</h2><p>Indexed microservices, APIs, Kafka, MongoDB, build, and quality views.</p><a href="/architecture">Open architecture</a></section><section class="card"><h2>APM runtime</h2><p>Bounded read-only Elastic APM aggregate report for the configured window.</p><a href="/runtime">Open APM runtime</a></section></div></body></html>"""


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
