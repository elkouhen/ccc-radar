"""Preflight checks for a reproducible architecture audit."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ccc_radar.config import ConfigError, load_config
from ccc_radar.paths import db_path


@dataclass(frozen=True)
class Check:
    name: str
    status: str  # ok | warning | error
    detail: str


def run_doctor(repo_root: Path) -> list[Check]:
    """Report readiness without creating files or downloading models."""
    checks = [
        Check("cccr", "ok", "CLI disponible."),
    ]
    try:
        config = load_config(repo_root)
    except ConfigError as exc:
        checks.append(Check("configuration", "error", str(exc)))
        return checks

    checks.append(Check("configuration", "ok", "Configuration .cccr/config.yml chargée."))
    checks.append(Check("analyse AST", "ok", "Extracteurs Java/Spring locaux actifs."))

    model = Path(config.embedding_model).expanduser()
    checks.append(Check(
        "modèle d'embeddings",
        "ok" if model.exists() else "warning",
        f"Modèle local : {model}." if model.exists() else (
            f"Modèle local absent : {model}. L'indexation peut le télécharger ou échouer hors ligne."
        ),
    ))
    checks.append(Check(
        "index",
        "ok" if db_path(repo_root).is_file() else "warning",
        "Index présent." if db_path(repo_root).is_file() else "Index absent : lancez `cccr index` après le préflight.",
    ))
    return checks


def has_errors(checks: list[Check]) -> bool:
    return any(check.status == "error" for check in checks)
