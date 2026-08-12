from pathlib import Path

PRIMARY_STATE_DIRNAME = ".codeatlas"


def state_dir(repo_root: Path) -> Path:
    return Path(repo_root) / PRIMARY_STATE_DIRNAME


def config_path(repo_root: Path) -> Path:
    return state_dir(repo_root) / "config.yml"


def db_path(repo_root: Path) -> Path:
    return state_dir(repo_root) / "findings.db"
