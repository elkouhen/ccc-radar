"""Small local static-file server for SystemLens HTML and JSON exports."""

from __future__ import annotations

from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import typer


app = typer.Typer(add_completion=False, invoke_without_command=True)


def create_simpleweb_server(directory: Path, host: str, port: int) -> ThreadingHTTPServer:
    """Serve *directory* through HTTP without changing its contents."""

    root = directory.resolve()

    class Handler(SimpleHTTPRequestHandler):
        def translate_path(self, path: str) -> str:
            """Do not let a symlink in the report directory escape its root."""
            candidate = Path(super().translate_path(path)).resolve()
            try:
                candidate.relative_to(root)
            except ValueError:
                return str(root / "__simpleweb_forbidden_path__")
            return str(candidate)

        def log_message(self, _format: str, *_args: object) -> None:
            """Keep normal browser requests out of the terminal output."""

    return ThreadingHTTPServer(
        (host, port), partial(Handler, directory=str(root))
    )


@app.callback()
def serve(
    directory: Path = typer.Argument(
        Path.cwd(), help="Répertoire contenant les fichiers HTML et JSON à servir."
    ),
    host: str = typer.Option("127.0.0.1", "--host", help="Adresse d'écoute locale."),
    port: int = typer.Option(8000, "--port", min=1, max=65535, help="Port HTTP."),
) -> None:
    """Sert localement des exports HTML et leurs données JSON adjacentes."""
    root = directory.resolve()
    if not root.is_dir():
        typer.echo(f"Répertoire introuvable : {directory}", err=True)
        raise typer.Exit(code=2)

    try:
        server = create_simpleweb_server(root, host, port)
    except OSError as exc:
        typer.echo(f"Impossible de démarrer le serveur web : {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Simpleweb : http://{host}:{port}/ (Ctrl-C pour arrêter)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        typer.echo("\nServeur web arrêté.")
    finally:
        server.server_close()
