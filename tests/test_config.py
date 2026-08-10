from pathlib import Path

import pytest

from ccc_radar.config import (
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_EXCLUDE,
    DEFAULT_INCLUDE,
    DEFAULT_MIN_SEVERITY,
    DEFAULT_SEMGREP_TIMEOUT_S,
    Config,
    ConfigError,
    init_config,
    load_config,
)


def write_config(repo_root: Path, content: str) -> None:
    cccr_dir = repo_root / ".cccr"
    cccr_dir.mkdir(parents=True, exist_ok=True)
    (cccr_dir / "config.yml").write_text(content)


def test_load_valid_config_applies_defaults(tmp_path: Path) -> None:
    write_config(tmp_path, "rules:\n  - rules/rules.yml\n")

    config = load_config(tmp_path)

    assert config == Config(
        rules=["rules/rules.yml"],
        include=DEFAULT_INCLUDE,
        exclude=DEFAULT_EXCLUDE,
        min_severity=DEFAULT_MIN_SEVERITY,
        embedding_model=DEFAULT_EMBEDDING_MODEL,
        semgrep_timeout_s=DEFAULT_SEMGREP_TIMEOUT_S,
    )


def test_load_config_overrides_defaults(tmp_path: Path) -> None:
    write_config(
        tmp_path,
        """
        rules:
          - p/security-audit
        include:
          - "src/**"
        exclude:
          - "build/**"
        min_severity: ERROR
        embedding_model: some/model
        semgrep_timeout_s: 30
        """,
    )

    config = load_config(tmp_path)

    assert config.rules == ["p/security-audit"]
    assert config.include == ["src/**"]
    assert config.exclude == ["build/**"]
    assert config.min_severity == "ERROR"
    assert config.embedding_model == "some/model"
    assert config.semgrep_timeout_s == 30


def test_load_config_missing_rules_field_raises(tmp_path: Path) -> None:
    write_config(tmp_path, "min_severity: ERROR\n")

    with pytest.raises(ConfigError, match="rules"):
        load_config(tmp_path)


def test_load_config_allows_no_rules_when_semgrep_is_disabled(tmp_path: Path) -> None:
    write_config(tmp_path, "semgrep_enabled: false\n")

    config = load_config(tmp_path)

    assert config.rules == []
    assert config.semgrep_enabled is False


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cccr init"):
        load_config(tmp_path)


def test_init_config_writes_default_file(tmp_path: Path) -> None:
    path = init_config(tmp_path, ["rules/rules.yml"])

    assert path == tmp_path / ".cccr" / "config.yml"
    config = load_config(tmp_path)
    assert config.rules == ["rules/rules.yml"]
    assert config.min_severity == DEFAULT_MIN_SEVERITY


def test_init_config_refuses_to_overwrite(tmp_path: Path) -> None:
    init_config(tmp_path, ["rules/rules.yml"])

    with pytest.raises(ConfigError, match="existe déjà"):
        init_config(tmp_path, ["other.yml"])
