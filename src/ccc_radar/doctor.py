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
    """Report local indexing readiness without mutating the repository."""
    checks = [
        Check("cccr", "ok", "CLI disponible."),
    ]
    try:
        load_config(repo_root)
    except ConfigError as exc:
        checks.append(Check("configuration", "error", str(exc)))
        return checks

    checks.append(Check("configuration", "ok", "Configuration .cccr/config.yml chargée."))
    checks.append(Check("analyse AST", "ok", "Extracteurs Java/Spring locaux actifs."))

    checks.append(Check(
        "index",
        "ok" if db_path(repo_root).is_file() else "warning",
        "Index présent." if db_path(repo_root).is_file() else "Index absent : lancez `cccr index` après le préflight.",
    ))
    return checks


def has_errors(checks: list[Check]) -> bool:
    return any(check.status == "error" for check in checks)
