from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ccc_radar.paths import config_path, state_dir

DEFAULT_INCLUDE = ["**/*"]
DEFAULT_EXCLUDE = [".git/**", ".venv/**", "node_modules/**", ".cccr/**"]
DEFAULT_MIN_SEVERITY = "INFO"
VALID_SEVERITIES = ("INFO", "WARNING", "ERROR")


class ConfigError(Exception):
    pass


@dataclass
class Config:
    include: list[str] = field(default_factory=lambda: list(DEFAULT_INCLUDE))
    exclude: list[str] = field(default_factory=lambda: list(DEFAULT_EXCLUDE))
    min_severity: str = DEFAULT_MIN_SEVERITY


def load_config(repo_root: Path) -> Config:
    path = config_path(repo_root)
    if not path.is_file():
        raise ConfigError(
            f"Fichier de configuration introuvable : {path}. "
            "Lancez d'abord: cccr init"
        )

    raw = yaml.safe_load(path.read_text()) or {}

    min_severity = raw.get("min_severity", DEFAULT_MIN_SEVERITY)
    if min_severity not in VALID_SEVERITIES:
        raise ConfigError(
            f"min_severity invalide : {min_severity!r}. "
            f"Valeurs autorisées : {VALID_SEVERITIES}."
        )

    return Config(
        include=list(raw.get("include", DEFAULT_INCLUDE)),
        exclude=list(raw.get("exclude", DEFAULT_EXCLUDE)),
        min_severity=min_severity,
    )


def init_config(repo_root: Path) -> Path:
    path = config_path(repo_root)
    if path.exists():
        raise ConfigError(f"Une configuration existe déjà : {path}.")

    state_dir(repo_root).mkdir(parents=True, exist_ok=True)
    content = {
        "include": DEFAULT_INCLUDE,
        "exclude": DEFAULT_EXCLUDE,
        "min_severity": DEFAULT_MIN_SEVERITY,
    }
    path.write_text(yaml.dump(content, sort_keys=False))
    return path
