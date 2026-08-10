from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ccc_radar.paths import config_path, state_dir

DEFAULT_INCLUDE = ["**/*"]
DEFAULT_EXCLUDE = [".git/**", ".venv/**", "node_modules/**", ".cccr/**"]
DEFAULT_MIN_SEVERITY = "INFO"
DEFAULT_EMBEDDING_MODEL = "~/models/jina-code-embeddings-1.5b"
DEFAULT_SEMGREP_TIMEOUT_S = 120

VALID_SEVERITIES = ("INFO", "WARNING", "ERROR")


class ConfigError(Exception):
    pass


@dataclass
class Config:
    rules: list[str]
    include: list[str] = field(default_factory=lambda: list(DEFAULT_INCLUDE))
    exclude: list[str] = field(default_factory=lambda: list(DEFAULT_EXCLUDE))
    min_severity: str = DEFAULT_MIN_SEVERITY
    embedding_model: str = DEFAULT_EMBEDDING_MODEL
    semgrep_timeout_s: int = DEFAULT_SEMGREP_TIMEOUT_S
    semgrep_enabled: bool = True


def load_config(repo_root: Path) -> Config:
    path = config_path(repo_root)
    if not path.is_file():
        raise ConfigError(
            f"Fichier de configuration introuvable : {path}. "
            "Lancez d'abord: cccr init"
        )

    raw = yaml.safe_load(path.read_text()) or {}

    semgrep_enabled = bool(raw.get("semgrep_enabled", True))
    rules = raw.get("rules", [])
    if semgrep_enabled and not rules:
        raise ConfigError(
            f"Le champ 'rules' est requis et doit être non vide dans {path}."
        )

    min_severity = raw.get("min_severity", DEFAULT_MIN_SEVERITY)
    if min_severity not in VALID_SEVERITIES:
        raise ConfigError(
            f"min_severity invalide : {min_severity!r}. "
            f"Valeurs autorisées : {VALID_SEVERITIES}."
        )

    return Config(
        rules=list(rules),
        include=list(raw.get("include", DEFAULT_INCLUDE)),
        exclude=list(raw.get("exclude", DEFAULT_EXCLUDE)),
        min_severity=min_severity,
        embedding_model=raw.get("embedding_model", DEFAULT_EMBEDDING_MODEL),
        semgrep_timeout_s=int(raw.get("semgrep_timeout_s", DEFAULT_SEMGREP_TIMEOUT_S)),
        semgrep_enabled=semgrep_enabled,
    )


def init_config(repo_root: Path, rules_path: list[str]) -> Path:
    path = config_path(repo_root)
    if path.exists():
        raise ConfigError(f"Une configuration existe déjà : {path}.")

    state_dir(repo_root).mkdir(parents=True, exist_ok=True)
    content = {
        "rules": rules_path,
        "include": DEFAULT_INCLUDE,
        "exclude": DEFAULT_EXCLUDE,
        "min_severity": DEFAULT_MIN_SEVERITY,
        "embedding_model": DEFAULT_EMBEDDING_MODEL,
        "semgrep_timeout_s": DEFAULT_SEMGREP_TIMEOUT_S,
        "semgrep_enabled": True,
    }
    path.write_text(yaml.dump(content, sort_keys=False))
    return path
