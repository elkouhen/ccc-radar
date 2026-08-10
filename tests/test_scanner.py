import shutil
import subprocess
from pathlib import Path

import pytest

from ccc_radar.config import Config
from ccc_radar.semgrep import SemgrepError, invoke_semgrep_raw, parse_semgrep_json, run_semgrep

FIXTURES_DIR = Path(__file__).parent / "fixtures"
VULN_REPO = FIXTURES_DIR / "vuln_repo"
SEMGREP_OUTPUT = (FIXTURES_DIR / "semgrep_output.json").read_text()


def make_config(**overrides: object) -> Config:
    defaults: dict = {"rules": ["rules/rules.yml"]}
    defaults.update(overrides)
    return Config(**defaults)


def test_parse_semgrep_json_fixture_returns_expected_findings() -> None:
    findings = parse_semgrep_json(SEMGREP_OUTPUT, VULN_REPO)

    assert len(findings) == 2

    by_path = {f.path: f for f in findings}
    sql_finding = by_path["app/db.py"]
    assert sql_finding.rule_id == "rules.custom.sql-fstring"
    assert sql_finding.severity == "ERROR"
    assert sql_finding.start_line == 6
    assert sql_finding.end_line == 6
    assert sql_finding.cwe == ["CWE-89"]
    assert "cursor.execute" in sql_finding.snippet

    shell_finding = by_path["app/shell.py"]
    assert shell_finding.rule_id == "rules.custom.subprocess-shell-true"
    assert shell_finding.severity == "WARNING"
    assert shell_finding.cwe == ["CWE-78"]

    # ids stables : reparser la même sortie donne les mêmes ids
    findings_again = parse_semgrep_json(SEMGREP_OUTPUT, VULN_REPO)
    assert [f.id for f in findings] == [f.id for f in findings_again]


def test_parse_semgrep_json_malformed_raises_semgrep_error() -> None:
    with pytest.raises(SemgrepError):
        parse_semgrep_json("not json", VULN_REPO)


def test_parse_semgrep_json_missing_results_field_raises_semgrep_error() -> None:
    with pytest.raises(SemgrepError):
        parse_semgrep_json("{}", VULN_REPO)


def test_invoke_semgrep_uses_private_writable_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_SEMGREP_LOG_FILE", str(tmp_path / "semgrep.log"))
    captured: dict[str, object] = {}

    def fake_run(*_args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured.update(kwargs)
        return subprocess.CompletedProcess([], 0, stdout='{"results": []}', stderr="")

    monkeypatch.setattr("ccc_radar.semgrep.subprocess.run", fake_run)

    assert invoke_semgrep_raw(VULN_REPO, make_config()) == '{"results": []}'
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["SEMGREP_LOG_FILE"] == str(tmp_path / "semgrep.log")


@pytest.mark.integration
def test_run_semgrep_full_scan_returns_all_findings() -> None:
    config = make_config()

    findings = run_semgrep(VULN_REPO, config)

    assert len(findings) == 4
    assert {f.path for f in findings} == {
        "app/db.py",
        "app/shell.py",
        "app/yaml_loader.py",
        "app/weak_random.py",
    }


@pytest.mark.integration
def test_run_semgrep_targeted_scan_returns_one_finding() -> None:
    config = make_config()

    findings = run_semgrep(VULN_REPO, config, files=["app/db.py"])

    assert len(findings) == 1
    assert findings[0].path == "app/db.py"


@pytest.mark.integration
def test_run_semgrep_min_severity_error_filters_warning() -> None:
    config = make_config(min_severity="ERROR")

    findings = run_semgrep(VULN_REPO, config)

    assert {f.severity for f in findings} == {"ERROR"}
    assert {f.path for f in findings} == {"app/db.py", "app/yaml_loader.py"}


@pytest.mark.integration
def test_run_semgrep_scans_tests_directory_when_config_targets_it(tmp_path: Path) -> None:
    repo = tmp_path / "vuln_repo"
    shutil.copytree(VULN_REPO, repo)
    (repo / "tests").mkdir()
    (repo / "tests" / "db_test.py").write_text(
        "import sqlite3\n\n"
        "def find_user(conn: sqlite3.Connection, name: str):\n"
        "    cursor = conn.cursor()\n"
        "    cursor.execute(f\"SELECT * FROM users WHERE name = '{name}'\")\n"
    )

    findings = run_semgrep(repo, make_config(), files=["tests/db_test.py"])

    assert [f.path for f in findings] == ["tests/db_test.py"]
