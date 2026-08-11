import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, cast

import click
import typer

from ccc_radar import __version__
from ccc_radar.architecture import (
    analyze as analyze_architecture,
    build_catalog,
    endpoint_implementation,
    find_microservice_paths,
    inventory_coverage,
    list_objects as list_architecture_objects,
    neighbors as architecture_neighbors,
    render_text as render_architecture_text,
    request_reply_patterns,
    show_object as show_architecture_object,
    trace_topic_flows,
)
from ccc_radar.architecture_inventory import load_architecture_inventory
from ccc_radar.audit import assess_architecture, render_audit_json, render_audit_text
from ccc_radar.config import ConfigError, init_config, load_config
from ccc_radar.embedder import EmbeddingError, make_embedder, resolve_embedding_model
from ccc_radar.flow import (
    resolve_topic,
    resolve_topic_by_similarity,
)
from ccc_radar.graph import (
    GraphEdge,
    build_graph,
    find_outbound_calls_in_consumers,
)
from ccc_radar.indexer import index_repo
from ccc_radar.inventory_freshness import endpoint_inventory_warning
from ccc_radar.models import Finding, MessageEndpoint
from ccc_radar.modules import DiscoveredModule, ModuleDependency, discover_modules
from ccc_radar.render import (
    GraphResult,
    render_endpoints_json,
    render_endpoints_text,
    render_graph_html,
    render_graph_likec4,
    render_request_reply_html,
    render_graph_json,
    render_module_detail_json,
    render_module_detail_text,
    render_module_graph_html,
    render_module_graph_json,
    render_module_graph_text,
    render_modules_list_json,
    render_modules_list_text,
    render_search_json,
    render_search_text,
    render_summary_json,
    render_summary_text,
    render_workspace_json,
    render_workspace_text,
)
from ccc_radar.search import SearchError, search_findings
from ccc_radar.search import summary as compute_summary
from ccc_radar.paths import config_path, db_path
from ccc_radar.store import Store, StoreError
from ccc_radar.workspace import (
    discover_maven_services,
    load_federation,
)
from ccc_radar.doctor import has_errors, run_doctor

app = typer.Typer(
    help=(
        "Explorer l'architecture et les constats d'un projet indexé.\n\n"
        "Exemples : `cccr microservices`, `cccr analyze audit`, "
        "`cccr export microservices --html graph.html`."
    )
)
export_app = typer.Typer(
    help=(
        "Exporter les graphes de dépendances d'architecture.\n\n"
        "Exemples : `cccr export microservices --html graph.html`, "
        "`cccr export modules --html modules.html`."
    )
)
topics_app = typer.Typer(
    help="Explorer les topics Kafka indexés.\n\nExemples : `cccr topics`, `cccr topics consumers orders.created`."
)
dtos_app = typer.Typer(
    help="Explorer les DTOs Java échangés via Kafka.\n\nExemples : `cccr dtos`, `cccr dtos consumers OrderCreated`."
)
apis_app = typer.Typer(
    help="Explorer les APIs HTTP indexées.\n\nExemples : `cccr apis`, `cccr apis consumers 'POST /payments'`."
)
mongodb_app = typer.Typer(
    help="Explorer les collections MongoDB indexées.\n\nExemples : `cccr mongodb`, `cccr mongodb services orders`."
)
microservices_app = typer.Typer(
    help="Explorer les microservices indexés.\n\nExemples : `cccr microservices`, `cccr microservices show orders`."
)
modules_app = typer.Typer(
    help="Explorer les modules Maven ou Gradle indexés.\n\nExemples : `cccr modules`, `cccr modules show orders-api`."
)
analyze_app = typer.Typer(
    help="Analyser les impacts et les chemins d'architecture.\n\nExemples : `cccr analyze audit`, `cccr analyze microservices impact orders`."
)
analyze_microservices_app = typer.Typer(
    help="Analyser les relations entre microservices.\n\nExemples : `cccr analyze microservices impact orders`, `cccr analyze microservices path orders payments`."
)
app.add_typer(export_app, name="export")
app.add_typer(topics_app, name="topics")
app.add_typer(dtos_app, name="dtos")
app.add_typer(apis_app, name="apis")
app.add_typer(mongodb_app, name="mongodb")
app.add_typer(microservices_app, name="microservices")
app.add_typer(modules_app, name="modules")
app.add_typer(analyze_app, name="analyze")
analyze_app.add_typer(analyze_microservices_app, name="microservices")



def _current_repo_endpoint_warning(store: Store) -> str | None:
    return endpoint_inventory_warning(
        store.get_meta("endpoint_inventory_signature"),
        scope="ce projet",
        inventory_indexed=store.get_meta("endpoint_inventory_indexed") == "1",
    )


def _echo_index_progress(message: str) -> None:
    typer.echo(message)


def _trace_index(stage: str, **fields: object) -> None:
    if os.environ.get("CCCR_TRACE") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(f"CCCR_TRACE ts={time.monotonic():.6f} stage={stage} {details}".rstrip(), file=sys.stderr, flush=True)


def _manifest_rel_paths(repo_root: Path, paths: list[Path]) -> list[str]:
    manifests: list[str] = []
    seen: set[str] = set()
    for raw_path in paths:
        path = raw_path.expanduser()
        if not path.is_absolute():
            path = repo_root / path
        try:
            rel_path = path.resolve().relative_to(repo_root.resolve()).as_posix()
        except ValueError as exc:
            raise typer.BadParameter(
                f"Le manifeste doit être dans le dépôt indexé : {raw_path}"
            ) from exc
        if not path.is_file():
            raise typer.BadParameter(f"Manifeste introuvable : {raw_path}")
        if path.suffix.lower() not in {".md", ".json"}:
            raise typer.BadParameter(
                f"Le manifeste doit être un fichier Markdown (.md) ou un flux Kafka JSON (.json) : {raw_path}"
            )
        if rel_path not in seen:
            seen.add(rel_path)
            manifests.append(rel_path)
    return manifests


@app.callback()
def main() -> None:
    """ccc-radar: indexe les signaux d'architecture extraits par AST."""


def _emit_architecture(result: object, json_output: bool) -> None:
    typer.echo(json.dumps(result) if json_output else render_architecture_text(result))


def topics_cmd(
    arguments: list[str] = typer.Argument(
        None, help="Commande : list, show, neighbors ou search."
    ),
    root: Optional[Path] = typer.Option(  # noqa: UP007
        None, "--root", help="Répertoire parent indexé. Défaut : répertoire courant."
    ),
    json_output: bool = typer.Option(False, "--json"),
    max_depth: int = typer.Option(
        6, "--max-depth", min=1, max=12, help="Nombre maximal de services suivis par trace.", hidden=True
    ),
    limit: int = typer.Option(
        50, "--limit", min=1, max=200, help="Nombre maximal de chemins retournés par trace.", hidden=True
    ),
) -> None:
    """Parcourir les topics Kafka et les services qui les publient ou consomment.

    Exemples : `cccr topics`, `cccr topics show orders.created`,
    `cccr topics neighbors orders.created`.
    """
    arguments = arguments or []
    json_output = _option_json(json_output)
    workspace_root = _option_root(root)
    catalog = _microservice_catalog(workspace_root)
    if not arguments or arguments[0] == "list":
        if len(arguments) > 1:
            typer.echo("Usage : `cccr topics [list] --root <workspace>`.", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(list_architecture_objects(catalog, "topic"), json_output)
        return
    command = arguments[0]
    if command in {"show", "neighbors", "consumers", "producers", "search", "trace"}:
        if len(arguments) != 2:
            typer.echo(f"`cccr topics {command}` requiert un topic.", err=True)
            raise typer.Exit(code=2)
        topic = arguments[1]
        result: object
        if command == "show":
            result = show_architecture_object(catalog, "topic", topic)
        elif command == "neighbors":
            result = architecture_neighbors(catalog, "topic", topic)
        elif command == "search":
            result = _search_architecture_object(workspace_root, catalog, "topic", "kafka", topic)
        elif command == "trace":
            result = trace_topic_flows(catalog, topic, max_depth=max_depth, limit=limit)
        else:
            result = analyze_architecture(catalog, command, topic)
        if result is None:
            typer.echo(f"Topic introuvable : {topic}", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(result, json_output)
        return
    typer.echo("Usage : `cccr topics [list|show|neighbors|search] [topic]`.", err=True)
    raise typer.Exit(code=2)


def dtos_cmd(
    arguments: list[str] = typer.Argument(
        None, help="Commande : list, show, neighbors, producers, consumers ou search."
    ),
    root: Optional[Path] = typer.Option(  # noqa: UP007
        None, "--root", help="Répertoire parent indexé. Défaut : répertoire courant."
    ),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Parcourir les DTOs Java utilisés par les producers et consumers Kafka.

    Exemples : `cccr dtos`, `cccr dtos show OrderCreated`,
    `cccr dtos consumers OrderCreated`.
    """
    arguments = arguments or []
    json_output = _option_json(json_output)
    workspace_root = _option_root(root)
    catalog = _microservice_catalog(workspace_root)
    if not arguments or arguments[0] == "list":
        if len(arguments) > 1:
            typer.echo("Usage : `cccr dtos [list] --root <workspace>`.", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(list_architecture_objects(catalog, "dto"), json_output)
        return
    command = arguments[0]
    if command in {"show", "neighbors", "consumers", "producers", "search"}:
        if len(arguments) != 2:
            typer.echo(f"`cccr dtos {command}` requiert un DTO.", err=True)
            raise typer.Exit(code=2)
        dto = arguments[1]
        result: object
        if command == "show":
            result = show_architecture_object(catalog, "dto", dto)
        elif command == "neighbors":
            result = architecture_neighbors(catalog, "dto", dto)
        elif command == "search":
            result = _search_dto(catalog, dto)
        else:
            summary = show_architecture_object(catalog, "dto", dto)
            key = "producer_microservices" if command == "producers" else "consumer_microservices"
            result = {"query": command, "dto": dto, "microservices": summary[key]} if summary else None
        if result is None:
            typer.echo(f"DTO introuvable : {dto}", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(result, json_output)
        return
    typer.echo("Usage : `cccr dtos [list|show|neighbors|producers|consumers|search] [dto]`.", err=True)
    raise typer.Exit(code=2)


def apis_cmd(
    arguments: list[str] = typer.Argument(
        None, help="Commande : list, show, neighbors ou search."
    ),
    root: Optional[Path] = typer.Option(  # noqa: UP007
        None, "--root", help="Répertoire parent indexé. Défaut : répertoire courant."
    ),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Parcourir les APIs HTTP et les services qui les exposent ou appellent.

    Exemples : `cccr apis`, `cccr apis show "POST /payments"`,
    `cccr apis search payments`.
    """
    arguments = arguments or []
    json_output = _option_json(json_output)
    workspace_root = _option_root(root)
    catalog = _microservice_catalog(workspace_root)
    if not arguments or arguments[0] == "list":
        if len(arguments) > 1:
            typer.echo("Usage : `cccr apis [list] --root <workspace>`.", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(list_architecture_objects(catalog, "api"), json_output)
        return
    command = arguments[0]
    if command in {"show", "neighbors", "providers", "consumers", "search"}:
        if len(arguments) != 2:
            typer.echo(f"`cccr apis {command}` requiert une API HTTP.", err=True)
            raise typer.Exit(code=2)
        api = arguments[1]
        result: object
        if command == "show":
            result = show_architecture_object(catalog, "api", api)
        elif command == "neighbors":
            result = architecture_neighbors(catalog, "api", api)
        elif command == "search":
            result = _search_architecture_object(workspace_root, catalog, "api", "rest", api)
        else:
            summary = show_architecture_object(catalog, "api", api)
            result = (
                {"query": command, "api": api, "microservices": summary[command]}
                if summary is not None
                else None
            )
        if result is None:
            typer.echo(f"API HTTP introuvable : {api}", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(result, json_output)
        return
    typer.echo("Usage : `cccr apis [list|show|neighbors|search] [api]`.", err=True)
    raise typer.Exit(code=2)


def mongodb_cmd(
    arguments: list[str] = typer.Argument(
        None, help="Commande : list, show, neighbors ou search."
    ),
    root: Optional[Path] = typer.Option(  # noqa: UP007
        None, "--root", help="Répertoire parent indexé. Défaut : répertoire courant."
    ),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Parcourir les collections MongoDB et les microservices qui les utilisent.

    Exemples : `cccr mongodb`, `cccr mongodb show orders`,
    `cccr mongodb neighbors orders`.
    """
    arguments = arguments or []
    json_output = _option_json(json_output)
    catalog = _microservice_catalog(_option_root(root))
    if not arguments or arguments[0] == "list":
        if len(arguments) > 1:
            typer.echo("Usage : `cccr mongodb [list] --root <workspace>`.", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(list_architecture_objects(catalog, "collection"), json_output)
        return
    command = arguments[0]
    if command not in {"show", "neighbors", "services", "search"} or len(arguments) != 2:
        typer.echo("Usage : `cccr mongodb [list|show|neighbors|search] [collection]`.", err=True)
        raise typer.Exit(code=2)
    collection = arguments[1]
    result: object
    if command == "show":
        result = show_architecture_object(catalog, "collection", collection)
    elif command == "neighbors":
        result = architecture_neighbors(catalog, "collection", collection)
    elif command == "services":
        result = _mongodb_services(catalog, collection)
    else:
        result = _search_mongodb_collection(catalog, collection)
    if result is None:
        typer.echo(f"Collection MongoDB introuvable : {collection}", err=True)
        raise typer.Exit(code=2)
    _emit_architecture(result, json_output)


def analyze_cmd(
    arguments: list[str] = typer.Argument(
        None, help="Cible et requête : microservices, topics, apis, mongodb, request-reply, audit ou coverage."
    ),
    root: Optional[Path] = typer.Option(  # noqa: UP007
        None, "--root", help="Répertoire parent indexé. Défaut : répertoire courant."
    ),
    workspace: Optional[Path] = typer.Option(
        None, "--workspace", help="Workspace de services indexés séparément, pour `audit`."
    ),
    json_output: bool = typer.Option(False, "--json"),
    max_depth: int = typer.Option(
        12, "--max-depth", min=1, max=32, help="Nombre maximal de relations ou étapes suivies."
    ),
    limit: int = typer.Option(
        20, "--limit", min=1, max=100, help="Nombre maximal de chemins ou flux retournés."
    ),
) -> None:
    """Répondre aux questions d'architecture à partir du graphe indexé.

    Exemples :
    `cccr analyze microservices path order-service shipping-service`
    `cccr analyze microservices impact order-service`
    `cccr analyze topics consumers orders.created`
    `cccr analyze topics trace orders.created`
    `cccr analyze apis providers "POST /payments"`
    `cccr analyze mongodb services orders`
    `cccr analyze request-reply`
    `cccr analyze audit`
    `cccr analyze coverage`
    """
    arguments = arguments or []
    if not arguments:
        typer.echo(
            "Usage : `cccr analyze <microservices|topics|apis|mongodb|request-reply|audit|coverage> ...`.", err=True
        )
        raise typer.Exit(code=2)
    subject = arguments[0]
    workspace_root = (root or Path.cwd()).resolve()
    if subject == "microservices":
        if len(arguments) < 2:
            typer.echo("Usage : `cccr analyze microservices <calls|external-apis|orphan-integrations|impact|path> ...`.", err=True)
            raise typer.Exit(code=2)
        query = arguments[1]
        if query == "path":
            if len(arguments) != 4:
                typer.echo("`cccr analyze microservices path` requiert une source et une cible.", err=True)
                raise typer.Exit(code=2)
            _render_microservice_path(
                arguments[2], arguments[3], workspace_root, json_output, max_depth=max_depth, limit=limit
            )
            return
        if query in {"calls", "dependencies", "impact"} and len(arguments) != 3:
            typer.echo(f"`cccr analyze microservices {query}` requiert une cible.", err=True)
            raise typer.Exit(code=2)
        if len(arguments) not in {2, 3}:
            typer.echo(f"`cccr analyze microservices {query}` accepte une cible optionnelle.", err=True)
            raise typer.Exit(code=2)
        _render_microservice_analysis(
            query, arguments[2] if len(arguments) == 3 else None, workspace_root, json_output
        )
        return
    if subject == "topics":
        if len(arguments) != 3 or arguments[1] not in {"consumers", "producers", "trace"}:
            typer.echo("Usage : `cccr analyze topics <consumers|producers|trace> <topic>`.", err=True)
            raise typer.Exit(code=2)
        catalog = _microservice_catalog(workspace_root)
        query, topic = arguments[1], arguments[2]
        result = (
            trace_topic_flows(catalog, topic, max_depth=max_depth, limit=limit)
            if query == "trace"
            else analyze_architecture(catalog, query, topic)
        )
        if result is None:
            typer.echo(f"Topic introuvable : {topic}", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(result, json_output)
        return
    if subject == "apis":
        if len(arguments) != 3 or arguments[1] not in {"providers", "consumers"}:
            typer.echo("Usage : `cccr analyze apis <providers|consumers> <api>`.", err=True)
            raise typer.Exit(code=2)
        query, api = arguments[1], arguments[2]
        summary = show_architecture_object(_microservice_catalog(workspace_root), "api", api)
        if summary is None:
            typer.echo(f"API HTTP introuvable : {api}", err=True)
            raise typer.Exit(code=2)
        _emit_architecture({"query": query, "api": api, "microservices": summary[query]}, json_output)
        return
    if subject == "mongodb":
        if len(arguments) != 3 or arguments[1] != "services":
            typer.echo("Usage : `cccr analyze mongodb services <collection>`.", err=True)
            raise typer.Exit(code=2)
        collection = arguments[2]
        result = _mongodb_services(_microservice_catalog(workspace_root), collection)
        if result is None:
            typer.echo(f"Collection MongoDB introuvable : {collection}", err=True)
            raise typer.Exit(code=2)
        _emit_architecture(result, json_output)
        return
    if subject in {"request-reply", "request_reply"} and len(arguments) == 1:
        _render_request_reply_patterns(workspace_root, json_output)
        return
    if subject == "audit" and len(arguments) == 1:
        _render_audit(workspace_root, workspace, json_output)
        return
    if subject == "coverage" and len(arguments) == 1:
        _render_inventory_coverage(workspace_root, json_output)
        return
    typer.echo(
        "Usage : `cccr analyze <microservices|topics|apis|mongodb|request-reply|audit|coverage> ...`.", err=True
    )
    raise typer.Exit(code=2)


def _catalog_root(root: Path | None) -> Path:
    return _option_root(root)


def _option_root(root: Path | None) -> Path:
    """Resolve --root from a command or its parent Typer group."""
    if root is not None:
        return root.resolve()
    context = click.get_current_context(silent=True)
    parent_root = context.parent.params.get("root") if context and context.parent else None
    return (parent_root or Path.cwd()).resolve()


def _option_json(json_output: bool) -> bool:
    """Resolve --json from a command or its parent Typer group."""
    if json_output:
        return True
    context = click.get_current_context(silent=True)
    return bool(context and context.parent and context.parent.params.get("json_output"))


@topics_app.callback(invoke_without_command=True)
def topics_root(
    ctx: typer.Context,
    root: Path | None = typer.Option(None, "--root", help="Répertoire parent à explorer."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Lister les topics sans sous-commande."""
    if ctx.invoked_subcommand is None:
        topics_cmd([], root, json_output, 6, 50)


@topics_app.command("list")
def topics_list(root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les topics Kafka."""
    topics_cmd([], root, json_output, 6, 50)


@topics_app.command("show")
def topics_show(topic: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Résumer un topic Kafka."""
    topics_cmd(["show", topic], root, json_output, 6, 50)


@topics_app.command("neighbors")
def topics_neighbors(topic: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher les producteurs et consommateurs directement liés."""
    topics_cmd(["neighbors", topic], root, json_output, 6, 50)


@topics_app.command("consumers")
def topics_consumers(topic: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices consommateurs d'un topic."""
    topics_cmd(["consumers", topic], root, json_output, 6, 50)


@topics_app.command("producers")
def topics_producers(topic: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices producteurs d'un topic."""
    topics_cmd(["producers", topic], root, json_output, 6, 50)


@topics_app.command("search")
def topics_search(query: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Retrouver un topic par nom ou similarité."""
    topics_cmd(["search", query], root, json_output, 6, 50)


@topics_app.command("trace")
def topics_trace(
    topic: str,
    root: Path | None = typer.Option(None, "--root"),
    json_output: bool = typer.Option(False, "--json"),
    max_depth: int = typer.Option(6, "--max-depth", min=1, max=12),
    limit: int = typer.Option(50, "--limit", min=1, max=200),
) -> None:
    """Afficher les flux Kafka potentiels issus d'un topic."""
    topics_cmd(["trace", topic], root, json_output, max_depth, limit)


@dtos_app.callback(invoke_without_command=True)
def dtos_root(
    ctx: typer.Context,
    root: Path | None = typer.Option(None, "--root", help="Répertoire parent à explorer."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Lister les DTOs Kafka sans sous-commande."""
    if ctx.invoked_subcommand is None:
        dtos_cmd([], root, json_output)


@dtos_app.command("list")
def dtos_list(root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les DTOs Java connus dans les échanges Kafka."""
    dtos_cmd([], root, json_output)


@dtos_app.command("show")
def dtos_show(dto: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Résumer les microservices producteurs et consommateurs d'un DTO."""
    dtos_cmd(["show", dto], root, json_output)


@dtos_app.command("neighbors")
def dtos_neighbors(dto: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher les topics et microservices directement liés à un DTO."""
    dtos_cmd(["neighbors", dto], root, json_output)


@dtos_app.command("producers")
def dtos_producers(dto: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices producteurs d'un DTO."""
    dtos_cmd(["producers", dto], root, json_output)


@dtos_app.command("consumers")
def dtos_consumers(dto: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices consommateurs d'un DTO."""
    dtos_cmd(["consumers", dto], root, json_output)


@dtos_app.command("search")
def dtos_search(query: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Retrouver un DTO par son nom Java."""
    dtos_cmd(["search", query], root, json_output)


@apis_app.callback(invoke_without_command=True)
def apis_root(
    ctx: typer.Context,
    root: Path | None = typer.Option(None, "--root", help="Répertoire parent à explorer."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Lister les APIs sans sous-commande."""
    if ctx.invoked_subcommand is None:
        apis_cmd([], root, json_output)


@apis_app.command("list")
def apis_list(root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les APIs HTTP."""
    apis_cmd([], root, json_output)


@apis_app.command("show")
def apis_show(api: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Résumer une API HTTP."""
    apis_cmd(["show", api], root, json_output)


@apis_app.command("neighbors")
def apis_neighbors(api: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher les microservices liés à une API."""
    apis_cmd(["neighbors", api], root, json_output)


@apis_app.command("providers")
def apis_providers(api: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices qui exposent une API."""
    apis_cmd(["providers", api], root, json_output)


@apis_app.command("consumers")
def apis_consumers(api: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices qui appellent une API."""
    apis_cmd(["consumers", api], root, json_output)


@apis_app.command("search")
def apis_search(query: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Retrouver une API par méthode ou chemin."""
    apis_cmd(["search", query], root, json_output)


@mongodb_app.callback(invoke_without_command=True)
def mongodb_root(
    ctx: typer.Context,
    root: Path | None = typer.Option(None, "--root", help="Répertoire parent à explorer."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Lister les collections sans sous-commande."""
    if ctx.invoked_subcommand is None:
        mongodb_cmd([], root, json_output)


@mongodb_app.command("list")
def mongodb_list(root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les collections MongoDB."""
    mongodb_cmd([], root, json_output)


@mongodb_app.command("show")
def mongodb_show(collection: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Résumer une collection MongoDB."""
    mongodb_cmd(["show", collection], root, json_output)


@mongodb_app.command("neighbors")
def mongodb_neighbors(collection: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher les microservices liés à une collection."""
    mongodb_cmd(["neighbors", collection], root, json_output)


@mongodb_app.command("services")
def mongodb_services(collection: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices utilisant une collection."""
    mongodb_cmd(["services", collection], root, json_output)


@mongodb_app.command("search")
def mongodb_search(query: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Retrouver une collection par son nom."""
    mongodb_cmd(["search", query], root, json_output)


@analyze_app.command("audit")
def analyze_audit(
    workspace: Path | None = typer.Option(None, "--workspace", help="Workspace de services indexés séparément."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Identifier les risques d'architecture."""
    _render_audit(Path.cwd(), workspace, json_output)


@analyze_app.command("coverage")
def analyze_coverage(
    root: Path | None = typer.Option(None, "--root", help="Répertoire indexé à analyser."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Mesurer les relations et intégrations non résolues de l'index."""
    _render_inventory_coverage(_option_root(root), _option_json(json_output))


@analyze_app.command("request-reply")
def analyze_request_reply(
    root: Path | None = typer.Option(None, "--root", help="Répertoire indexé à analyser."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Lister les patterns Kafka request/reply détectés par Strategy1."""
    _render_request_reply_patterns(_option_root(root), _option_json(json_output))


@analyze_microservices_app.command("calls")
def analyze_microservices_calls(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les appels sortants d'un microservice."""
    _render_microservice_analysis("calls", service, _catalog_root(root), json_output)


@analyze_microservices_app.command("dependencies")
def analyze_microservices_dependencies(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les dépendances d'un microservice."""
    _render_microservice_analysis("dependencies", service, _catalog_root(root), json_output)


@analyze_microservices_app.command("impact")
def analyze_microservices_impact(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Identifier les composants impactés par un microservice."""
    _render_microservice_analysis("impact", service, _catalog_root(root), json_output)


@analyze_microservices_app.command("external-apis")
def analyze_microservices_external_apis(service: str | None = typer.Argument(None), root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les APIs externes utilisées, éventuellement par microservice."""
    _render_microservice_analysis("external-apis", service, _catalog_root(root), json_output)


@analyze_microservices_app.command("orphan-integrations")
def analyze_microservices_orphan_integrations(service: str | None = typer.Argument(None), root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Identifier les intégrations sans relation résolue."""
    _render_microservice_analysis("orphan-integrations", service, _catalog_root(root), json_output)


@analyze_microservices_app.command("path")
def analyze_microservices_path(
    source: str,
    target: str,
    root: Path | None = typer.Option(None, "--root"),
    json_output: bool = typer.Option(False, "--json"),
    max_depth: int = typer.Option(12, "--max-depth", min=1, max=32),
    limit: int = typer.Option(20, "--limit", min=1, max=100),
) -> None:
    """Trouver des chemins entre deux microservices."""
    _render_microservice_path(source, target, _catalog_root(root), json_output, max_depth=max_depth, limit=limit)


@app.command()
def version() -> None:
    """Affiche la version du package.

    Exemple : `cccr version`.
    """
    typer.echo(__version__)


@app.command(name="doctor")
def doctor_cmd(json_output: bool = typer.Option(False, "--json")) -> None:
    """Vérifie les prérequis d'un audit d'architecture, sans modifier le projet.

    Exemples : `cccr doctor`, `cccr doctor --json`.
    """
    checks = run_doctor(Path.cwd())
    result = [
        {"name": check.name, "status": check.status, "detail": check.detail}
        for check in checks
    ]
    if json_output:
        typer.echo(json.dumps(result))
    else:
        for check in checks:
            marker = {"ok": "✓", "warning": "⚠", "error": "✗"}[check.status]
            typer.echo(f"{marker} {check.name}: {check.detail}")
    if has_errors(checks):
        raise typer.Exit(code=2)


@app.command()
def init() -> None:
    """Initialise la configuration .cccr/config.yml du projet.

    L'analyse des sources Java/Spring est entièrement locale et fondée sur AST.
    """
    repo_root = Path.cwd()
    if config_path(repo_root).exists():
        typer.echo(f"Une configuration existe déjà : {config_path(repo_root)}.", err=True)
        raise typer.Exit(code=1)

    try:
        path = init_config(repo_root)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Configuration créée : {path}")


@app.command(name="index")
def index_cmd(
    manifest_args: Optional[list[Path]] = typer.Argument(  # noqa: UP007
        None, help="Manifeste(s) Kafka Markdown ou JSON à indexer explicitement."
    ),
    full: bool = typer.Option(False, "--full", help="Force un scan complet."),
    manifests: Optional[list[Path]] = typer.Option(  # noqa: UP007
        None, "--manifest", help="Manifeste Kafka Markdown ou JSON (répétable)."
    ),
    topic_strategy: Literal["default", "strategy1"] = typer.Option(
        "default",
        "--topic-strategy",
        help="Stratégie de conventions : default ou strategy1 (Kafka getTopics/KafkaListener et constantes REST en majuscules).",
    ),
    disable: list[str] = typer.Option(
        None,
        "--disable",
        help=(
            "Type à désactiver : properties, module-architecture "
            "ou module-tree-sitter. Répétable."
        ),
    ),
) -> None:
    """Indexe le code avec les extracteurs AST (incrémental par défaut).

    Exemples : `cccr index`, `cccr index --full`,
    `cccr index --topic-strategy strategy1`,
    `cccr index --manifest TOPICS.md`,
    `cccr index --manifest kafka-flow-graph-anonymous.json`.
    """
    repo_root = Path.cwd()
    _trace_index(
        "cli.index.begin", root=repo_root, full=full, topic_strategy=topic_strategy
    )
    explicit_manifests = _manifest_rel_paths(repo_root, list(manifest_args or []) + list(manifests or []))
    disabled = frozenset(disable or [])
    known_disabled = {"properties", "module-architecture", "module-tree-sitter"}
    unknown = disabled - known_disabled
    if unknown:
        typer.echo(
            "Type d'indexation inconnu : "
            f"{', '.join(sorted(unknown))}. Valeurs : {', '.join(sorted(known_disabled))}.",
            err=True,
        )
        raise typer.Exit(code=2)
    try:
        config = load_config(repo_root)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    resolved_model, model_warning = resolve_embedding_model(config.embedding_model)
    if model_warning is not None:
        typer.echo(f"⚠ {model_warning}")
    _trace_index("embedder.begin", model=resolved_model)
    embedder = make_embedder(resolved_model)
    _trace_index("embedder.end")

    try:
        _trace_index("store.open.begin")
        with Store(repo_root) as store:
            _trace_index("store.open.end")
            report = index_repo(
                repo_root, config, store, embedder, full=full, progress=_echo_index_progress,
                disabled=disabled, extra_files=explicit_manifests,
                topic_strategy=topic_strategy,
            )
            store.set_meta("index_engine", "manual")
            _trace_index("store.close.begin")
    except EmbeddingError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc

    typer.echo(
        f"scanned={report.scanned} skipped={report.skipped} "
        f"+findings={report.findings_added} -findings={report.findings_removed} "
        f"+integrations={report.endpoints_added} -integrations={report.endpoints_removed}"
    )
    _trace_index("cli.index.end")


def _require_index(repo_root: Path) -> None:
    index_path = db_path(repo_root)
    if not index_path.is_file():
        typer.echo("Index absent. Lancez d'abord: cccr index", err=True)
        raise typer.Exit(code=2)


@app.command()


@app.command(name="findings")
def findings_cmd(
    query: Optional[str] = typer.Argument(  # noqa: UP007 (Typer nécessite Optional)
        None, help="Requête précision-first. Omettre pour lister les findings."
    ),
    severity: Optional[str] = typer.Option(None, "--severity"),  # noqa: UP007
    rule: Optional[str] = typer.Option(None, "--rule"),  # noqa: UP007
    path: Optional[str] = typer.Option(None, "--path"),  # noqa: UP007
    limit: int = typer.Option(5, "--limit"),
    offset: int = typer.Option(0, "--offset"),
    context: bool = typer.Option(False, "--context"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Liste les findings indexés ou les filtre par requête précision-first,
    sans recherche de code — pour la recherche code + findings, voir `search`.

    Exemples : `cccr findings`, `cccr findings "sql injection" --json`.
    """
    repo_root = Path.cwd()
    _require_index(repo_root)

    try:
        with Store(repo_root) as store:
            hits = search_findings(
                store,
                object(),  # recherche findings lexicale ; aucun modèle requis
                query,
                severity=severity,
                rule=rule,
                path_glob=path,
                limit=limit,
                offset=offset,
            )
    except SearchError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc

    if json_output:
        typer.echo(json.dumps(render_search_json(hits, repo_root, context)))
    else:
        typer.echo(render_search_text(hits, repo_root, context))


@app.command(name="summary")
def summary_cmd(json_output: bool = typer.Option(False, "--json")) -> None:
    """Vue agrégée des findings (sévérités, top règles, top répertoires).

    Exemples : `cccr summary`, `cccr summary --json`.
    """
    repo_root = Path.cwd()
    _require_index(repo_root)

    with Store(repo_root) as store:
        result = compute_summary(store)

    if json_output:
        typer.echo(json.dumps(render_summary_json(result)))
    else:
        typer.echo(render_summary_text(result))


@dataclass(frozen=True)
class _MicroserviceGraphData:
    services_by_name: dict[str, list[MessageEndpoint]]
    edges: list[GraphEdge]
    collections_by_service: dict[str, list[str]]
    modules_by_service: dict[str, DiscoveredModule]
    findings_by_service: dict[str, list[Finding]]
    build_modules: list[DiscoveredModule]
    module_dependencies: list[ModuleDependency]
    source_roots: list[Path]
    warnings: list[str]
    result: GraphResult


def _is_exportable_microservice(name: str) -> bool:
    """Exclude test fixtures and unresolved Maven placeholder service names."""
    normalized = name.casefold()
    return "test" not in normalized and not (
        name.startswith("${") and name.endswith("}")
    )


def _load_microservice_graph(
    repo_root: Path, workspace: Path | None, include_mongodb: bool
) -> _MicroserviceGraphData:
    inventory = load_architecture_inventory(
        repo_root,
        workspace,
        include_runtime_services_without_endpoints=True,
    )
    services_by_name = {
        name: endpoints
        for name, endpoints in inventory.endpoints_by_service.items()
        if _is_exportable_microservice(name)
    }
    edges = [
        edge
        for edge in build_graph(services_by_name)
        if _is_exportable_microservice(edge.from_service)
        and _is_exportable_microservice(edge.to_service)
    ]
    modules_by_service = (
        {
            name: module
            for name, module in inventory.modules_by_service.items()
            if name in services_by_name
        }
        if include_mongodb
        else {}
    )
    collections_by_service = {
        service: list(module.mongo_collections)
        for service, module in modules_by_service.items()
        if module.mongo_collections
    }

    result = render_graph_json(
        list(services_by_name),
        edges,
        find_outbound_calls_in_consumers(inventory.endpoints),
        warnings=inventory.warnings,
        cross_module_data_available=bool(services_by_name),
    )
    return _MicroserviceGraphData(
        services_by_name,
        edges,
        collections_by_service,
        modules_by_service,
        {
            name: findings
            for name, findings in inventory.findings_by_service.items()
            if name in services_by_name
        },
        inventory.modules,
        inventory.module_dependencies,
        inventory.source_roots,
        inventory.warnings,
        result,
    )


def _write_likec4_project(destination: Path, model: str) -> None:
    """Write a self-contained LikeC4 project that can be started with npm."""
    if destination.exists() and not destination.is_dir():
        typer.echo(f"Le répertoire LikeC4 existe déjà comme fichier : {destination}", err=True)
        raise typer.Exit(code=2)

    destination.mkdir(parents=True, exist_ok=True)
    config = {
        "$schema": "https://likec4.dev/schemas/config.json",
        "name": "cccr-architecture",
        "title": "CCC Radar architecture",
        "implicitViews": True,
    }
    package = {
        "name": "cccr-likec4-architecture",
        "private": True,
        "version": "0.0.0",
        "scripts": {
            "dev": "likec4 start",
            "build": "likec4 build --output dist --base ./",
            "preview": "likec4 preview --output dist",
            "validate": "likec4 validate",
            "format": "likec4 format",
        },
        "devDependencies": {"likec4": "latest"},
    }
    readme = """# LikeC4 Architecture

Generated by `cccr export microservices --c4`.

## Start the site

```bash
npm install
npm run dev
```

The site is then available at `http://localhost:5173`.

## Build the static site

```bash
npm run build
npm run preview
```

The generated site is written to `dist/`.

## Read the graph

- Views: `runtime` maps HTTP, Kafka and MongoDB interactions; `contracts` exposes REST/OpenAPI and Kafka payload information; `build` maps Maven/Gradle dependencies; `quality` reports connectivity complexity, findings and indexing warnings.
- Shapes: component for a microservice and build module, queue for a Kafka topic, rectangle for a MongoDB collection, and browser for an external HTTP API.
- Microservice colors split them into three equally sized complexity groups: blue for the lowest third, amber for the middle third and red for the highest third.
- A microservice score is its number of direct HTTP, Kafka and MongoDB relations. Findings remain visible in details but do not affect the color.
- Outbound calls and publications are green; Kafka consumptions are orange; MongoDB reads and writes are blue and teal.
- Kafka relation labels include statically inferred Java payload types. Microservice descriptions list detected OpenAPI contracts.
"""
    (destination / "architecture.c4").write_text(model, encoding="utf-8")
    (destination / "likec4.config.json").write_text(
        json.dumps(config, indent=2) + "\n", encoding="utf-8"
    )
    (destination / "package.json").write_text(
        json.dumps(package, indent=2) + "\n", encoding="utf-8"
    )
    (destination / ".gitignore").write_text("node_modules/\ndist/\n", encoding="utf-8")
    (destination / "README.md").write_text(readme, encoding="utf-8")


@export_app.command(name="microservices")
def export_microservices_cmd(
    workspace: Optional[Path] = typer.Option(
        None, "--workspace", help="Répertoire contenant plusieurs services indexés séparément."
    ),
    html: Optional[Path] = typer.Option(None, "--html", help="Fichier HTML Sigma.js à produire."),
    c4: Optional[Path] = typer.Option(
        None, "--c4", help="Répertoire du projet LikeC4 à produire."
    ),
    vscode_wsl_distro: Optional[str] = typer.Option(
        None, "--vscode-wsl-distro", help="Distribution WSL des liens VS Code."
    ),
    json_output: bool = typer.Option(False, "--json", help="Écrire le graphe structuré sur la sortie standard."),
) -> None:
    """Exporter les dépendances microservices, topics Kafka et collections MongoDB.

    Exemples : `cccr export microservices --html graph.html`,
    `cccr export microservices --c4 architecture-likec4`,
    `cccr export microservices --json`.
    """
    outputs = [output for output in (html, c4) if output is not None]
    if len(outputs) + int(json_output) != 1:
        typer.echo("Choisissez un seul format parmi --html, --c4 ou --json.", err=True)
        raise typer.Exit(code=2)
    if c4 is not None and c4.suffix:
        typer.echo("`--c4` attend un répertoire de projet, pas un fichier `.c4`.", err=True)
        raise typer.Exit(code=2)
    graph_data = _load_microservice_graph(Path.cwd(), workspace, include_mongodb=True)
    if json_output:
        typer.echo(json.dumps(graph_data.result))
        return
    if html is not None:
        html.write_text(
            render_graph_html(
                graph_data.services_by_name,
                graph_data.edges,
                graph_data.collections_by_service,
                graph_data.modules_by_service,
                graph_data.warnings,
                graph_data.build_modules,
                graph_data.module_dependencies,
                graph_data.source_roots,
                graph_data.findings_by_service,
                vscode_wsl_distro,
                request_reply_strategy1=True,
            ),
            encoding="utf-8",
        )
    else:
        assert c4 is not None
        _write_likec4_project(
            c4,
            render_graph_likec4(
                graph_data.services_by_name,
                graph_data.edges,
                graph_data.collections_by_service,
                graph_data.findings_by_service,
                graph_data.modules_by_service,
                graph_data.warnings,
                graph_data.build_modules,
                graph_data.module_dependencies,
            ),
        )
    output = outputs[0]
    if c4 is not None:
        typer.echo(
            f"Projet LikeC4 écrit dans {output} "
            f"({len(graph_data.services_by_name)} services, {len(graph_data.edges)} arêtes)."
        )
        typer.echo(f"Démarrer le site : `cd {output} && npm install && npm run dev`.")
    else:
        typer.echo(
            f"Export microservices écrit dans {output} "
            f"({len(graph_data.services_by_name)} services, {len(graph_data.edges)} arêtes)."
        )
    if graph_data.result["note"]:
        typer.echo(str(graph_data.result["note"]))


@export_app.command(name="modules")
def export_modules_cmd(
    html: Optional[Path] = typer.Option(None, "--html", help="Fichier HTML Sigma.js à produire."),
) -> None:
    """Exporter les dépendances de build entre modules indexés.

    Exemple : `cccr export modules --html modules.html`.
    """
    if html is None:
        typer.echo("`cccr export modules` requiert --html FILE.", err=True)
        raise typer.Exit(code=2)
    repo_root = Path.cwd()
    if not db_path(repo_root).is_file():
        typer.echo("Index absent : lancez d'abord `cccr index` dans ce répertoire.", err=True)
        raise typer.Exit(code=2)
    with Store(repo_root, readonly=True) as store:
        modules = store.all_modules()
        dependencies = store.all_module_dependencies()
        endpoints = store.all_endpoints()
    html.write_text(render_module_graph_html(modules, dependencies, endpoints), encoding="utf-8")
    typer.echo(
        f"Export modules écrit dans {html} "
        f"({len(modules)} modules, {len(dependencies)} dépendances)."
    )


@export_app.command(name="request-reply")
def export_request_reply_cmd(
    html: Path | None = typer.Option(None, "--html", help="Fichier HTML à produire."),
) -> None:
    """Exporter une vue dédiée des patterns Kafka request/reply Strategy1.

    Exemple : `cccr export request-reply --html request-reply.html`.
    """
    if html is None:
        typer.echo("`cccr export request-reply` requiert --html FILE.", err=True)
        raise typer.Exit(code=2)
    repo_root = Path.cwd()
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        result = request_reply_patterns(build_catalog(store.all_modules(), store.all_endpoints()))
    html.write_text(render_request_reply_html(result), encoding="utf-8")
    typer.echo(f"Vue request/reply écrite dans {html} ({result['count']} pattern(s)).")


def _render_audit(repo_root: Path, workspace: Path | None, json_output: bool) -> None:
    inventory = load_architecture_inventory(repo_root, workspace)
    risks = assess_architecture(
        inventory.endpoints_by_service,
        build_graph(inventory.endpoints_by_service),
        modules=inventory.modules,
        endpoints_by_module=inventory.endpoints_by_module,
    )
    typer.echo(json.dumps(render_audit_json(risks)) if json_output else render_audit_text(risks))


def _render_inventory_coverage(repo_root: Path, json_output: bool) -> None:
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        catalog = build_catalog(store.all_modules(), store.all_endpoints())
        result = inventory_coverage(catalog, store.all_architecture_relations())
    _emit_architecture(result, json_output)


def _render_request_reply_patterns(repo_root: Path, json_output: bool) -> None:
    _require_index(repo_root)
    with Store(repo_root, readonly=True) as store:
        catalog = build_catalog(store.all_modules(), store.all_endpoints())
        result = request_reply_patterns(catalog)
    _emit_architecture(result, json_output)


def microservices_cmd(
    arguments: list[str] = typer.Argument(
        None,
        help="Nom d'un service, ou commande : show, topics, apis, mongodb ou neighbors.",
    ),
    root: Optional[Path] = typer.Option(  # noqa: UP007
        None,
        "--root", help="Répertoire parent à explorer. Défaut : répertoire courant.",
    ),
    json_output: bool = typer.Option(False, "--json"),
    max_depth: int = typer.Option(
        12, "--max-depth", min=1, max=32, help="Nombre maximal de relations pour `path`.", hidden=True
    ),
    limit: int = typer.Option(
        20, "--limit", min=1, max=100, help="Nombre maximal de chemins retournés par `path`.", hidden=True
    ),
) -> None:
    """Lister les microservices ou résumer un microservice.

    Exemples : `cccr microservices`, `cccr microservices orders`,
    `cccr microservices topics orders`, `cccr microservices apis orders`,
    `cccr microservices mongodb orders`, `cccr microservices neighbors orders`.
    """
    arguments = arguments or []
    json_output = _option_json(json_output)
    root = _option_root(root)
    commands = {
        "topics", "apis", "mongodb", "properties", "openapi", "show", "neighbors", "path", "analyze", "implementation"
    }
    if arguments and arguments[0] in commands:
        workspace_root = root
        command = arguments[0]
        if command in {"topics", "apis", "mongodb", "properties", "openapi", "show"}:
            if len(arguments) != 2:
                typer.echo(f"`microservices {command}` requiert un nom de microservice.", err=True)
                raise typer.Exit(code=2)
            service = arguments[1]
        elif command == "neighbors":
            if len(arguments) != 2:
                typer.echo("`microservices neighbors` requiert un nom de microservice.", err=True)
                raise typer.Exit(code=2)
        elif command == "path":
            if len(arguments) != 3:
                typer.echo("`microservices path` requiert un microservice source et un microservice cible.", err=True)
                raise typer.Exit(code=2)
        elif command == "implementation":
            if len(arguments) != 3:
                typer.echo(f"`microservices {command}` requiert un type et un nom.", err=True)
                raise typer.Exit(code=2)
        elif len(arguments) not in {2, 3}:
            typer.echo("`microservices analyze` requiert une question et accepte une cible optionnelle.", err=True)
            raise typer.Exit(code=2)
        if command == "topics":
            _render_microservice_topics(service, workspace_root, json_output)
        elif command == "apis":
            _render_microservice_apis(service, workspace_root, json_output)
        elif command == "mongodb":
            _render_microservice_mongodb(service, workspace_root, json_output)
        elif command == "properties":
            _render_microservice_properties(service, workspace_root, json_output)
        elif command == "openapi":
            _render_microservice_openapi(service, workspace_root, json_output)
        elif command == "show":
            _render_microservice_summary(service, workspace_root, json_output)
        elif command == "neighbors":
            _render_microservice_neighbors(arguments[1], workspace_root, json_output)
        elif command == "path":
            _render_microservice_path(
                arguments[1], arguments[2], workspace_root, json_output, max_depth=max_depth, limit=limit
            )
        elif command == "analyze":
            _render_microservice_analysis(
                arguments[1], arguments[2] if len(arguments) == 3 else None, workspace_root, json_output
            )
        else:
            _render_microservice_implementation(arguments[1], arguments[2], workspace_root, json_output)
        return
    if len(arguments) == 1:
        argument = arguments[0]
        explicit_workspace = (
            Path(argument).is_absolute() or argument in {".", ".."} or argument.startswith(f".{os.sep}")
        )
        if not explicit_workspace:
            _render_microservice_summary(argument, root, json_output)
            return
    if len(arguments) > 1:
        typer.echo("Usage : `cccr microservices [--root <root>]` ou `cccr microservices <service> --root <root>`.", err=True)
        raise typer.Exit(code=2)
    workspace_root = Path(arguments[0]) if arguments else root
    services = [
        service
        for service in discover_maven_services(workspace_root)
        if service.kind == "microservice"
    ]
    federation = load_federation(services)
    result = render_workspace_json(services, federation)

    if json_output:
        typer.echo(json.dumps(result))
    else:
        typer.echo(render_workspace_text(result))


def _selected_microservice(name: str, root: Path):
    services = discover_maven_services(root)
    matches = [service for service in services if service.name == name and service.kind == "microservice"]
    if not matches:
        typer.echo(f"Microservice introuvable : {name}", err=True)
        raise typer.Exit(code=2)
    if len(matches) > 1:
        paths = ", ".join(str(service.path) for service in matches)
        typer.echo(f"Microservice ambigu : {name} ({paths})", err=True)
        raise typer.Exit(code=2)
    return matches[0], load_federation(services)


def _microservice_catalog(root: Path):
    if db_path(root).is_file():
        inventory = load_architecture_inventory(root)
        if inventory.modules:
            return build_catalog(inventory.modules, inventory.endpoints)
    services = discover_maven_services(root)
    federation = load_federation(services)
    modules = [module for module in discover_modules(root) if module.starts_application]
    endpoints = [
        endpoint
        for service_endpoints in federation.endpoints_by_service.values()
        for endpoint in service_endpoints
    ]
    return build_catalog(modules, endpoints)


def _search_architecture_object(
    root: Path, catalog, kind: str, system: str, query: str
) -> dict[str, object] | None:
    endpoints = [endpoint for endpoint in catalog.endpoints if endpoint.system == system]
    resolved = resolve_topic(query, {endpoint.topic for endpoint in endpoints})
    if resolved is None and db_path(root).is_file():
        try:
            with Store(root, readonly=True) as store:
                config = load_config(root)
                resolved = resolve_topic_by_similarity(
                    store, make_embedder(config.embedding_model), query, endpoints
                )
        except (ConfigError, EmbeddingError, StoreError):
            resolved = None
    if resolved is None:
        return None
    summary = show_architecture_object(catalog, kind, resolved)
    return {"query": query, "resolved": resolved, "object": summary} if summary else None


def _search_dto(catalog, query: str) -> dict[str, object] | None:
    dto_names = {
        endpoint.message_type
        for endpoint in catalog.endpoints
        if endpoint.system == "kafka" and endpoint.message_type
    }
    resolved = resolve_topic(query, dto_names)
    if resolved is None:
        return None
    summary = show_architecture_object(catalog, "dto", resolved)
    return {"query": query, "resolved": resolved, "object": summary} if summary else None


def _search_mongodb_collection(catalog, query: str) -> dict[str, object] | None:
    collections = {
        collection
        for module in catalog.modules
        for collection in module.mongo_collections
    }
    resolved = resolve_topic(query, collections)
    if resolved is None:
        return None
    summary = show_architecture_object(catalog, "collection", resolved)
    return {"query": query, "resolved": resolved, "object": summary} if summary else None


def _mongodb_services(catalog, collection: str) -> dict[str, object] | None:
    summary = show_architecture_object(catalog, "collection", collection)
    if summary is None:
        return None
    microservices = [
        module.name
        for module in catalog.modules
        if module.starts_application and collection in module.mongo_collections
    ]
    return {"query": "services", "collection": collection, "microservices": microservices}


def _render_microservice_summary(service: str, root: Path, json_output: bool) -> None:
    result = show_architecture_object(_microservice_catalog(root), "microservice", service)
    if result is None:
        typer.echo(f"Microservice introuvable : {service}", err=True)
        raise typer.Exit(code=2)
    _emit_architecture(result, json_output)


def _render_microservice_neighbors(name: str, root: Path, json_output: bool) -> None:
    result = architecture_neighbors(_microservice_catalog(root), "microservice", name)
    if result is None:
        typer.echo(f"Microservice introuvable : {name}", err=True)
        raise typer.Exit(code=2)
    _emit_architecture(result, json_output)


def _render_microservice_path(
    source: str,
    target: str,
    root: Path,
    json_output: bool,
    *,
    max_depth: int,
    limit: int,
) -> None:
    if source == target:
        typer.echo("La source et la cible doivent être deux microservices distincts.", err=True)
        raise typer.Exit(code=2)
    result = find_microservice_paths(
        _microservice_catalog(root), source, target, max_depth=max_depth, limit=limit
    )
    if result is None:
        typer.echo(f"Microservice source ou cible introuvable : {source} -> {target}", err=True)
        raise typer.Exit(code=2)
    _emit_architecture(result, json_output)


def _render_microservice_analysis(
    query: str, target: str | None, root: Path, json_output: bool
) -> None:
    if query.casefold() in {"consumers", "consumer", "producers", "producer"}:
        typer.echo(
            "Utilisez `cccr analyze topics consumers <topic>` ou "
            "`cccr analyze topics producers <topic>`.",
            err=True,
        )
        raise typer.Exit(code=2)
    result = analyze_architecture(_microservice_catalog(root), query, target)
    if result is None:
        typer.echo(
            "Analyse impossible : vérifiez la question et sa cible (calls/"
            "external-apis/orphan-integrations/impact), ou utilisez `cccr analyze`.",
            err=True,
        )
        raise typer.Exit(code=2)
    _emit_architecture(result, json_output)


def _render_microservice_implementation(
    kind: str, identifier: str, root: Path, json_output: bool
) -> None:
    if kind.casefold() not in {"integration", "endpoint"}:
        typer.echo("Seule l'implémentation d'une intégration est disponible.", err=True)
        raise typer.Exit(code=2)
    result = endpoint_implementation(_microservice_catalog(root), identifier)
    if result is None:
        typer.echo(f"Intégration introuvable : {identifier}", err=True)
        raise typer.Exit(code=2)
    _emit_architecture(result, json_output)


def _render_microservice_topics(service: str, root: Path, json_output: bool) -> None:
    """Liste les topics Kafka publiés et consommés par un microservice."""
    summary = show_architecture_object(_microservice_catalog(root), "microservice", service)
    if summary is None:
        typer.echo(f"Microservice introuvable : {service}", err=True)
        raise typer.Exit(code=2)
    _emit_architecture(
        {
            "microservice": service,
            "published": summary["kafka_topics_published"],
            "consumed": summary["kafka_topics_consumed"],
            "published_message_types": summary["kafka_message_types_published"],
            "consumed_message_types": summary["kafka_message_types_consumed"],
        },
        json_output,
    )


def _render_microservice_apis(service: str, root: Path, json_output: bool) -> None:
    """Liste les APIs HTTP exposées et appelées par un microservice."""
    summary = show_architecture_object(_microservice_catalog(root), "microservice", service)
    if summary is None:
        typer.echo(f"Microservice introuvable : {service}", err=True)
        raise typer.Exit(code=2)
    _emit_architecture(
        {
            "microservice": service,
            "exposed": summary["http_apis_exposed"],
            "consumed": summary["http_apis_consumed"],
        },
        json_output,
    )


def _render_microservice_mongodb(service: str, root: Path, json_output: bool) -> None:
    """Liste les collections MongoDB utilisées par un microservice."""
    summary = show_architecture_object(_microservice_catalog(root), "microservice", service)
    if summary is None:
        typer.echo(f"Microservice introuvable : {service}", err=True)
        raise typer.Exit(code=2)
    databases = cast("dict[str, object]", summary["databases"])
    _emit_architecture(
        {
            "microservice": service,
            "collections": databases["mongodb_collections"],
        },
        json_output,
    )


def _render_microservice_properties(service: str, root: Path, json_output: bool) -> None:
    """Affiche l'exemple YAML de propriétés Spring d'un microservice."""
    selected, _ = _selected_microservice(service, root)
    from ccc_radar.configuration import service_configuration_example

    properties = service_configuration_example(selected.path)
    result = {"name": selected.name, "properties_example": properties}
    typer.echo(json.dumps(result) if json_output else properties.rstrip())


def _render_microservice_openapi(service: str, root: Path, json_output: bool) -> None:
    """Affiche les contrats OpenAPI/Swagger locaux d'un microservice."""
    selected, _ = _selected_microservice(service, root)
    _render_openapi_contracts(selected.name, selected.path, json_output)


def _render_openapi_contracts(name: str, root: Path, json_output: bool) -> None:
    """Rend les contrats OpenAPI/Swagger d'un module ou microservice."""
    contracts = []
    module = next((item for item in discover_modules(root) if item.path == root), None)
    contract_paths = module.openapi_files if module is not None else ()
    for contract_path in contract_paths:
        path = root / contract_path
        if not path.is_file():
            continue
        contracts.append({"path": contract_path, "content": path.read_text(encoding="utf-8", errors="replace")})
    result = {"name": name, "contracts": contracts}
    if json_output:
        typer.echo(json.dumps(result))
    elif not contracts:
        typer.echo("Aucun contrat OpenAPI/Swagger local détecté.")
    else:
        typer.echo("\n\n".join(f"# {contract['path']}\n{contract['content'].rstrip()}" for contract in contracts))


def modules_cmd(
    arguments: list[str] = typer.Argument(
        None, help="Sous-commande et module, ou nom de module à détailler."
    ),
    json_output: bool = typer.Option(False, "--json"),
    html: Optional[Path] = typer.Option(
        None, "--html", help="Exporte le graphe de dépendances de modules en HTML Sigma.js.", hidden=True
    ),
) -> None:
    """Liste les modules indexés ou détaille l'un d'eux.

    `cccr modules` liste. `cccr modules <module>` détaille. Les sous-commandes
    `integrations`, `properties` et `openapi` prennent un module dans le
    répertoire courant déjà indexé. `graph` affiche les dépendances de build
    entre modules. Utilisez `cccr export modules` pour générer le rendu HTML.

    Exemples : `cccr modules`, `cccr modules order-service`,
    `cccr modules integrations order-service`, `cccr modules graph`.
    """
    arguments = arguments or []
    commands = {"integrations", "properties", "openapi", "graph"}
    if arguments and arguments[0] in commands:
        if arguments[0] == "graph":
            if len(arguments) != 1:
                typer.echo("`modules graph` ne prend pas de nom de module.", err=True)
                raise typer.Exit(code=2)
            _render_module_graph(Path.cwd().resolve(), json_output, html)
            return
        if len(arguments) != 2:
            typer.echo(f"`modules {arguments[0]}` requiert un nom de module.", err=True)
            raise typer.Exit(code=2)
        repo_root = Path.cwd().resolve()
        selected = _selected_indexed_module(arguments[1], repo_root)
        with Store(repo_root, readonly=True) as store:
            if arguments[0] == "integrations":
                endpoints = [endpoint for endpoint in store.all_endpoints() if endpoint.module == selected.name]
                typer.echo(json.dumps(render_endpoints_json(endpoints)) if json_output else render_endpoints_text(endpoints))
            elif arguments[0] == "properties":
                result = {"name": selected.name, "properties_example": selected.configuration_example}
                typer.echo(json.dumps(result) if json_output else selected.configuration_example.rstrip())
            else:
                _render_openapi_contracts(selected.name, selected.path, json_output)
        return
    if len(arguments) > 1:
        typer.echo("Usage : `cccr modules [module]` ou `cccr modules <integrations|properties|openapi> <module>` ou `cccr modules graph`.", err=True)
        raise typer.Exit(code=2)
    module = arguments[0] if arguments else None
    repo_root = Path.cwd().resolve()
    if not db_path(repo_root).is_file():
        typer.echo("Index absent : lancez d'abord `cccr index` dans ce répertoire.", err=True)
        raise typer.Exit(code=2)
    try:
        with Store(repo_root, readonly=True) as store:
            modules = store.all_modules()
    except StoreError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    if module is None:
        modules_result = render_modules_list_json(modules)
        typer.echo(json.dumps(modules_result) if json_output else render_modules_list_text(modules_result))
        return
    matches = [item for item in modules if item.name == module]
    if not matches:
        typer.echo(f"Module introuvable : {module}", err=True)
        raise typer.Exit(code=2)
    if len(matches) > 1:
        paths = ", ".join(str(item.path) for item in matches)
        typer.echo(f"Module ambigu : {module} ({paths})", err=True)
        raise typer.Exit(code=2)
    selected = matches[0]
    detail_result = render_module_detail_json(selected)
    typer.echo(json.dumps(detail_result) if json_output else render_module_detail_text(detail_result))


def _render_module_graph(
    repo_root: Path, json_output: bool, html: Path | None
) -> None:
    if not db_path(repo_root).is_file():
        typer.echo("Index absent : lancez d'abord `cccr index` dans ce répertoire.", err=True)
        raise typer.Exit(code=2)
    try:
        with Store(repo_root, readonly=True) as store:
            modules = store.all_modules()
            dependencies = store.all_module_dependencies()
            endpoints = store.all_endpoints()
    except StoreError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    if html is not None:
        html.write_text(render_module_graph_html(modules, dependencies, endpoints), encoding="utf-8")
        typer.echo(f"Graphe écrit dans {html} ({len(modules)} modules, {len(dependencies)} dépendances).")
        return
    result = render_module_graph_json(modules, dependencies)
    typer.echo(json.dumps(result) if json_output else render_module_graph_text(result))


@microservices_app.callback(invoke_without_command=True)
def microservices_root(
    ctx: typer.Context,
    root: Path | None = typer.Option(None, "--root", help="Répertoire parent à explorer."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Lister les microservices sans sous-commande."""
    if ctx.invoked_subcommand is None:
        microservices_cmd([], root, json_output, 12, 20)


@microservices_app.command("list")
def microservices_list(root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les microservices."""
    microservices_cmd([], root, json_output, 12, 20)


@microservices_app.command("show")
def microservices_show(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Résumer un microservice."""
    microservices_cmd(["show", service], root, json_output, 12, 20)


@microservices_app.command("topics")
def microservices_topics(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les topics publiés et consommés par un microservice."""
    microservices_cmd(["topics", service], root, json_output, 12, 20)


@microservices_app.command("apis")
def microservices_apis(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les APIs exposées et appelées par un microservice."""
    microservices_cmd(["apis", service], root, json_output, 12, 20)


@microservices_app.command("mongodb")
def microservices_mongodb(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les collections MongoDB utilisées par un microservice."""
    microservices_cmd(["mongodb", service], root, json_output, 12, 20)


@microservices_app.command("neighbors")
def microservices_neighbors(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher les relations directes d'un microservice."""
    microservices_cmd(["neighbors", service], root, json_output, 12, 20)


@microservices_app.command("implementation")
def microservices_implementation(
    kind: str,
    identifier: str,
    root: Path | None = typer.Option(None, "--root"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Accéder à l'implémentation d'une intégration identifiée."""
    microservices_cmd(["implementation", kind, identifier], root, json_output, 12, 20)


@microservices_app.command("properties")
def microservices_properties(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher explicitement l'exemple de configuration Spring."""
    microservices_cmd(["properties", service], root, json_output, 12, 20)


@microservices_app.command("openapi")
def microservices_openapi(service: str, root: Path | None = typer.Option(None, "--root"), json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher explicitement les contrats OpenAPI locaux."""
    microservices_cmd(["openapi", service], root, json_output, 12, 20)


@modules_app.callback(invoke_without_command=True)
def modules_root(ctx: typer.Context, json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les modules sans sous-commande."""
    if ctx.invoked_subcommand is None:
        modules_cmd([], json_output, None)


@modules_app.command("list")
def modules_list(json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les modules."""
    modules_cmd([], json_output, None)


@modules_app.command("show")
def modules_show(module: str, json_output: bool = typer.Option(False, "--json")) -> None:
    """Résumer un module."""
    modules_cmd([module], json_output, None)


@modules_app.command("integrations")
def modules_integrations(module: str, json_output: bool = typer.Option(False, "--json")) -> None:
    """Lister les intégrations d'un module."""
    modules_cmd(["integrations", module], json_output, None)


@modules_app.command("properties")
def modules_properties(module: str, json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher explicitement la configuration indexée d'un module."""
    modules_cmd(["properties", module], json_output, None)


@modules_app.command("openapi")
def modules_openapi(module: str, json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher explicitement les contrats OpenAPI locaux."""
    modules_cmd(["openapi", module], json_output, None)


@modules_app.command("graph")
def modules_graph(json_output: bool = typer.Option(False, "--json")) -> None:
    """Afficher les dépendances de build entre modules."""
    modules_cmd(["graph"], json_output, None)


def _selected_indexed_module(name: str, repo_root: Path):
    if not db_path(repo_root).is_file():
        typer.echo("Index absent : lancez d'abord `cccr index` dans ce répertoire.", err=True)
        raise typer.Exit(code=2)
    try:
        with Store(repo_root, readonly=True) as store:
            matches = [item for item in store.all_modules() if item.name == name]
    except StoreError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    if not matches:
        typer.echo(f"Module introuvable : {name}", err=True)
        raise typer.Exit(code=2)
    if len(matches) > 1:
        paths = ", ".join(str(item.path) for item in matches)
        typer.echo(f"Module ambigu : {name} ({paths})", err=True)
        raise typer.Exit(code=2)
    return matches[0]
@app.command(name="mcp")
def mcp_cmd() -> None:
    """Lance le serveur MCP (stdio) exposant les findings du repo courant.

    Enregistrement client (ex. Claude Code), à ajouter à la config MCP :

    {"mcpServers": {"cccr": {"command": "cccr", "args": ["mcp"]}}}

    Exemple : `cccr mcp`.
    """
    from ccc_radar.mcp_server import mcp as fastmcp_app

    fastmcp_app.run()


if __name__ == "__main__":
    app()
