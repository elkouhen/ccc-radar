"""Preflight checks for a reproducible architecture audit."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from systemlens.config import ConfigError, load_config
from systemlens import java_parser
from systemlens.inventory_freshness import endpoint_inventory_warning
from systemlens.paths import db_path
from systemlens.store import Store, StoreError


@dataclass(frozen=True)
class Check:
    name: str
    status: str  # ok | warning | error
    detail: str


def run_doctor(repo_root: Path) -> list[Check]:
    """Report local indexing readiness without mutating the repository."""
    checks = [
        Check("systemlens", "ok", "CLI disponible."),
    ]
    try:
        load_config(repo_root)
    except ConfigError as exc:
        checks.append(Check("configuration", "error", str(exc)))
        return checks

    checks.append(Check("configuration", "ok", "Configuration .systemlens/config.yml chargée."))
    try:
        java_parser.java_parser("doctor")
    except Exception as exc:
        checks.append(Check("analyse AST", "error", f"Tree-sitter Java indisponible : {exc}"))
    else:
        checks.append(Check("analyse AST", "ok", "Extracteurs Tree-sitter Java disponibles."))

    if not db_path(repo_root).is_file():
        checks.append(Check("index", "warning", "Index absent : lancez `systemlens index` après le préflight."))
        return checks
    try:
        with Store(repo_root, readonly=True) as store:
            warning = endpoint_inventory_warning(
                store.get_meta("endpoint_inventory_signature"),
                scope="ce projet",
                inventory_indexed=store.get_meta("endpoint_inventory_indexed") == "1",
            )
            diagnostics = store.all_extraction_diagnostics()
    except StoreError as exc:
        checks.append(Check("index", "error", str(exc)))
        return checks
    checks.append(Check("index", "warning" if warning else "ok", warning or "Index et schéma compatibles."))
    checks.append(Check(
        "diagnostics d'extraction",
        "warning" if diagnostics else "ok",
        f"{len(diagnostics)} diagnostic(s) d'extraction indexé(s)." if diagnostics else "Aucun diagnostic d'extraction.",
    ))
    return checks


def has_errors(checks: list[Check]) -> bool:
    return any(check.status == "error" for check in checks)
