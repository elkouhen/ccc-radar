import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import ccc_radar.embedder as embedder_module
from ccc_radar.cli import DEFAULT_REGISTRY_RULESETS, DEFAULT_RULE_PACKS, _is_exportable_microservice, app
from ccc_radar.indexer import IndexReport
from ccc_radar.models import ArchitectureRelation, Finding, MessageEndpoint, compute_endpoint_id
from ccc_radar.modules import DiscoveredModule
from ccc_radar.store import Store

FIXTURES_DIR = Path(__file__).parent / "fixtures"
VULN_REPO = FIXTURES_DIR / "vuln_repo"
ENDPOINT_INDEX_REPO = FIXTURES_DIR / "endpoint_index_repo"
MAVEN_WORKSPACE = FIXTURES_DIR / "maven_workspace"

runner = CliRunner()


def test_microservice_export_filters_test_and_unresolved_placeholder_names() -> None:
    assert _is_exportable_microservice("orders-service")
    assert not _is_exportable_microservice("orders-test")
    assert not _is_exportable_microservice("${artifactId}")


def test_architecture_command_help_is_short_and_task_oriented() -> None:
    root_help = runner.invoke(app, ["--help"])
    microservices_help = runner.invoke(app, ["microservices", "--help"])
    topics_help = runner.invoke(app, ["topics", "--help"])
    dtos_help = runner.invoke(app, ["dtos", "--help"])
    apis_help = runner.invoke(app, ["apis", "--help"])
    analyze_help = runner.invoke(app, ["analyze", "--help"])
    export_help = runner.invoke(app, ["export", "microservices", "--help"])

    assert root_help.exit_code == 0
    assert "Explorer l'architecture et les constats" in root_help.output
    assert "BACKLOG-" not in root_help.output
    assert "integrations" not in root_help.output
    assert "apis" in root_help.output
    assert "dtos" in root_help.output
    assert "export" in root_help.output
    assert "analyze" in root_help.output
    assert "│ audit" not in root_help.output
    assert "endpoints" not in root_help.output
    assert "resources" not in root_help.output
    assert microservices_help.exit_code == 0
    assert "Explorer les microservices indexés." in microservices_help.output
    assert "mongodb" in microservices_help.output
    assert "│ graph" not in root_help.output
    assert topics_help.exit_code == 0
    assert "show" in topics_help.output
    assert "consumers" in topics_help.output
    assert dtos_help.exit_code == 0
    assert "Explorer les DTOs Java échangés via Kafka." in dtos_help.output
    assert "producers" in dtos_help.output
    assert apis_help.exit_code == 0
    assert "providers" in apis_help.output
    assert analyze_help.exit_code == 0
    assert "audit" in analyze_help.output
    assert "coverage" in analyze_help.output
    assert "microservices" in analyze_help.output
    assert export_help.exit_code == 0
    assert "--drawio" not in export_help.output
    assert "--html" in export_help.output
    assert "--c4" in export_help.output


def test_analyze_coverage_reports_unresolved_inventory_facts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    module = DiscoveredModule(
        name="orders",
        path=tmp_path / "orders",
        build_system="maven",
        version=None,
        kind="library",
        starts_application=True,
        configuration_example="",
    )
    kafka = MessageEndpoint(
        id="kafka-1", role="produce", system="kafka", topic="<dynamic>", topic_dynamic=True,
        source="code", framework="spring-kafka", path="orders/Publisher.java", start_line=12,
        end_line=12, snippet="send(topic, payload)", module="orders",
    )
    call = MessageEndpoint(
        id="rest-1", role="call", system="rest", topic="GET <dynamic>", topic_dynamic=True,
        source="code", framework="resttemplate", path="orders/Client.java", start_line=18,
        end_line=18, snippet="getForObject(url)", module="orders",
    )
    relation = ArchitectureRelation(
        id="relation-1", source_kind="microservice", source_name="orders", relation="publishes",
        target_kind="topic", target_name="<dynamic>", origin="code", confidence="medium",
        module="orders", path="orders/Publisher.java", start_line=12, end_line=12,
    )
    with Store(tmp_path) as store:
        store.replace_modules([module])
        store.replace_endpoints_for_files([kafka.path, call.path], [kafka, call])
        store.replace_architecture_relations([relation])

    result = runner.invoke(app, ["analyze", "coverage", "--json"])

    assert result.exit_code == 0
    coverage = json.loads(result.output)
    assert coverage["relations"]["total"] == 1
    assert coverage["relations"]["by_confidence"] == {"medium": 1}
    assert coverage["unresolved"]["dynamic_kafka_topics"][0]["module"] == "orders"
    assert coverage["unresolved"]["unknown_kafka_message_types"][0]["topic_or_api"] == "<dynamic>"
    assert coverage["unresolved"]["unmatched_http_calls"][0]["topic_or_api"] == "GET <dynamic>"


@pytest.mark.parametrize(
    "command",
    [
        ["topics", "--help"],
        ["dtos", "--help"],
        ["apis", "--help"],
        ["mongodb", "--help"],
        ["analyze", "--help"],
        ["version", "--help"],
        ["doctor", "--help"],
        ["init", "--help"],
        ["index", "--help"],
        ["search", "--help"],
        ["findings", "--help"],
        ["summary", "--help"],
        ["microservices", "--help"],
        ["modules", "--help"],
        ["mcp", "--help"],
        ["export", "--help"],
        ["export", "microservices", "--help"],
        ["export", "modules", "--help"],
    ],
)
def test_visible_command_help_includes_examples(command: list[str]) -> None:
    result = runner.invoke(app, command)

    assert result.exit_code == 0
    assert "Exemple" in result.output


@pytest.mark.parametrize(
    "command",
    [
        ["resources"],
        ["endpoints"],
        ["audit"],
        ["graph"],
        ["integrations"],
        ["export", "microservices", "--drawio", "graph.drawio"],
        ["export", "modules", "--drawio", "modules.drawio"],
        ["microservices", "resources", "orders"],
        ["modules", "endpoints", "orders"],
    ],
)
def test_obsolete_cli_commands_are_not_available(command: list[str]) -> None:
    result = runner.invoke(app, command)

    assert result.exit_code == 2


def install_fake_skill_rules(home: Path, packs: tuple[str, ...] = DEFAULT_RULE_PACKS) -> Path:
    rules_root = home / "ccc-radar-skill" / "skills" / "cccr" / "rules"
    for pack in packs:
        pack_dir = rules_root / pack
        pack_dir.mkdir(parents=True, exist_ok=True)
        (pack_dir / "java.yaml").write_text("rules: []\n")
    return rules_root


@pytest.fixture
def repo_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    dest = tmp_path / "vuln_repo"
    shutil.copytree(VULN_REPO, dest)
    monkeypatch.chdir(dest)
    return dest


def test_init_without_semgrep_config_enables_default_registry_rulesets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    result = runner.invoke(app, ["init"])

    assert result.exit_code == 0
    assert "Java/OWASP/secrets" in result.output
    config_content = (tmp_path / ".cccr" / "config.yml").read_text()
    for ruleset in DEFAULT_REGISTRY_RULESETS:
        assert ruleset in config_content
    assert "p/spring" not in config_content


def test_index_rejects_an_unknown_disabled_type(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["index", "--disable", "unknown"])

    assert result.exit_code == 2
    assert "Type d'indexation inconnu" in result.output
    assert "module-architecture" in result.output
    assert "module-tree-sitter" in result.output


def test_index_accepts_markdown_manifest_as_positional_argument(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".cccr").mkdir()
    (tmp_path / ".cccr" / "config.yml").write_text("rules: ['rules.yml']\n")
    (tmp_path / "TOPICS.md").write_text("### module-a\n")
    captured: dict[str, object] = {}

    def fake_index_repo(*args: object, **kwargs: object) -> IndexReport:
        captured["extra_files"] = kwargs["extra_files"]
        return IndexReport(1, 0, 0, 0, 0, 0, 0)

    monkeypatch.setattr("ccc_radar.cli.resolve_embedding_model", lambda model: (model, None))
    monkeypatch.setattr("ccc_radar.cli.make_embedder", lambda _model: object())
    monkeypatch.setattr("ccc_radar.cli.index_repo", fake_index_repo)

    result = runner.invoke(app, ["index", "TOPICS.md"])

    assert result.exit_code == 0
    assert captured["extra_files"] == ["TOPICS.md"]


def test_index_accepts_topic_manifest_option(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".cccr").mkdir()
    (tmp_path / ".cccr" / "config.yml").write_text("rules: ['rules.yml']\n")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "kafka-flow-graph.json").write_text('{"topics": {}}\n')
    captured: dict[str, object] = {}

    def fake_index_repo(*args: object, **kwargs: object) -> IndexReport:
        captured["extra_files"] = kwargs["extra_files"]
        return IndexReport(1, 0, 0, 0, 0, 0, 0)

    monkeypatch.setattr("ccc_radar.cli.resolve_embedding_model", lambda model: (model, None))
    monkeypatch.setattr("ccc_radar.cli.make_embedder", lambda _model: object())
    monkeypatch.setattr("ccc_radar.cli.index_repo", fake_index_repo)

    result = runner.invoke(app, ["index", "--manifest", "docs/kafka-flow-graph.json"])

    assert result.exit_code == 0
    assert captured["extra_files"] == ["docs/kafka-flow-graph.json"]


def test_index_passes_topic_strategy_to_manual_indexer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".cccr").mkdir()
    (tmp_path / ".cccr" / "config.yml").write_text("rules: ['rules.yml']\n")
    captured: dict[str, object] = {}

    def fake_index_repo(*args: object, **kwargs: object) -> IndexReport:
        captured["topic_strategy"] = kwargs["topic_strategy"]
        return IndexReport(1, 0, 0, 0, 0, 0, 0)

    monkeypatch.setattr("ccc_radar.cli.resolve_embedding_model", lambda model: (model, None))
    monkeypatch.setattr("ccc_radar.cli.make_embedder", lambda _model: object())
    monkeypatch.setattr("ccc_radar.cli.index_repo", fake_index_repo)

    result = runner.invoke(app, ["index", "--topic-strategy", "strategy1"])

    assert result.exit_code == 0
    assert captured["topic_strategy"] == "strategy1"


def test_index_semgrep_option_controls_only_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".cccr").mkdir()
    (tmp_path / ".cccr" / "config.yml").write_text("rules: ['rules.yml']\n")
    calls: list[dict[str, object]] = []

    def fake_index_repo(*args: object, **kwargs: object) -> IndexReport:
        calls.append(kwargs)
        return IndexReport(1, 0, 0, 0, 0, 0, 0)

    monkeypatch.setattr("ccc_radar.cli.resolve_embedding_model", lambda model: (model, None))
    monkeypatch.setattr("ccc_radar.cli.make_embedder", lambda _model: object())
    monkeypatch.setattr("ccc_radar.cli.index_repo", fake_index_repo)

    assert runner.invoke(app, ["index"]).exit_code == 0
    assert calls[-1]["include_semgrep_findings"] is False
    assert "semgrep" not in calls[-1]["disabled"]

    assert runner.invoke(app, ["index", "--semgrep"]).exit_code == 0
    assert calls[-1]["include_semgrep_findings"] is True
    assert "semgrep" not in calls[-1]["disabled"]

    assert runner.invoke(app, ["index", "--disable", "semgrep"]).exit_code == 0
    assert calls[-1]["include_semgrep_findings"] is False
    assert "semgrep" in calls[-1]["disabled"]


def test_init_without_semgrep_config_installs_all_skill_packs_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    install_fake_skill_rules(Path.home())

    result = runner.invoke(app, ["init"])

    assert result.exit_code == 0
    assert "default, liveness, rest, kafka, kafka-security" in result.output
    config_content = (tmp_path / ".cccr" / "config.yml").read_text()
    for pack in DEFAULT_RULE_PACKS:
        assert f".cccr/rules/{pack}" in config_content
        assert (tmp_path / ".cccr" / "rules" / pack / "java.yaml").is_file()
    for ruleset in DEFAULT_REGISTRY_RULESETS:
        assert ruleset in config_content


def test_init_without_semgrep_config_falls_back_when_skill_packs_are_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    install_fake_skill_rules(Path.home(), packs=("default", "liveness"))

    result = runner.invoke(app, ["init"])

    assert result.exit_code == 0
    assert "Java/OWASP/secrets" in result.output
    config_content = (tmp_path / ".cccr" / "config.yml").read_text()
    for ruleset in DEFAULT_REGISTRY_RULESETS:
        assert ruleset in config_content


@pytest.mark.integration
def test_index_with_default_registry_rulesets_succeeds_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    (tmp_path / "app.py").write_text(
        "import sqlite3\n\n\n"
        "def find_user(conn: sqlite3.Connection, name: str):\n"
        "    cursor = conn.cursor()\n"
        "    cursor.execute(f\"SELECT * FROM users WHERE name = '{name}'\")\n"
        "    return cursor.fetchall()\n"
    )

    init_result = runner.invoke(app, ["init"])
    assert init_result.exit_code == 0

    index_result = runner.invoke(app, ["index", "--semgrep"])

    assert index_result.exit_code == 0
    assert "→ Indexation :" in index_result.output
    assert "scanned=" in index_result.output


def test_init_with_explicit_rules_takes_priority_over_default_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init", "--rules", "rules/rules.yml"])

    assert result.exit_code == 0
    config_content = (tmp_path / ".cccr" / "config.yml").read_text()
    assert "rules/rules.yml" in config_content
    assert "p/security-audit" not in config_content


def test_init_detects_local_semgrep_config_over_default_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".semgrep.yml").write_text("rules: []\n")

    result = runner.invoke(app, ["init"])

    assert result.exit_code == 0
    config_content = (tmp_path / ".cccr" / "config.yml").read_text()
    assert ".semgrep.yml" in config_content
    assert "p/security-audit" not in config_content


@pytest.mark.integration
def test_init_with_rules_then_index_reports_correctly(
    repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")

    init_result = runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    assert init_result.exit_code == 0
    assert (repo_copy / ".cccr" / "config.yml").is_file()

    index_result = runner.invoke(app, ["index", "--semgrep"])

    assert index_result.exit_code == 0
    assert "→ Indexation :" in index_result.output
    assert "scanned=" in index_result.output
    assert "+findings=4" in index_result.output
    assert "-findings=0" in index_result.output


@pytest.mark.integration
def test_index_twice_second_run_scans_nothing(
    repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")

    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    runner.invoke(app, ["index"])

    second_result = runner.invoke(app, ["index"])

    assert second_result.exit_code == 0
    assert "scanned=0" in second_result.output


def test_findings_without_index_fails_with_exact_message_and_code_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["findings", "injection sql"])

    assert result.exit_code == 2
    assert "Index absent. Lancez d'abord: cccr index" in result.output


def test_findings_without_query_lists_indexed_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    finding = Finding(
        id="listed-finding", rule_id="custom.listed", severity="ERROR",
        message="Finding de liste", path="app/Main.java", start_line=3, end_line=3,
        snippet="dangerous();", fix=None, cwe=[], owasp=[],
    )
    with Store(tmp_path) as store:
        store.replace_findings_for_files([finding.path], [finding])

    result = runner.invoke(app, ["findings", "--json", "--limit", "10"])

    assert result.exit_code == 0
    assert json.loads(result.output)[0]["rule_id"] == "custom.listed"


@pytest.mark.integration
def test_findings_json_output_matches_contract(
    repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    runner.invoke(app, ["index", "--semgrep"])

    result = runner.invoke(app, ["findings", "injection sql", "--json"])

    assert result.exit_code == 0
    hits = json.loads(result.output)
    # La recherche findings est précision-first : « injection sql » ne doit
    # pas ramener les findings qui ne couvrent qu'un des deux termes.
    assert len(hits) == 1
    assert hits[0]["rule_id"].endswith("custom.sql-fstring")
    expected_keys = {
        "id",
        "rule_id",
        "severity",
        "message",
        "path",
        "start_line",
        "end_line",
        "score",
        "fix",
        "cwe",
        "owasp",
    }
    assert expected_keys <= set(hits[0].keys())


@pytest.mark.integration
def test_findings_invalid_severity_fails_with_exit_code_2(
    repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BACKLOG-16 P4 : `--severity HIGH` (sévérité Semgrep brute, jamais
    stockée telle quelle) échouait auparavant avec un ValueError brut."""
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    runner.invoke(app, ["index", "--semgrep"])

    result = runner.invoke(app, ["findings", "injection sql", "--severity", "HIGH"])

    assert result.exit_code == 2
    assert "HIGH" in result.output


@pytest.mark.integration
def test_findings_context_includes_offending_source_line(
    repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    runner.invoke(app, ["index", "--semgrep"])

    result = runner.invoke(
        app, ["findings", "injection sql", "--path", "app/db.py", "--context", "--json"]
    )

    hits = json.loads(result.output)
    assert "cursor.execute" in hits[0]["context"]


@pytest.mark.integration
def test_findings_hybrid_query_can_match_exact_rule_id(
    repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    runner.invoke(app, ["index", "--semgrep"])

    result = runner.invoke(app, ["findings", "custom.subprocess-shell-true", "--json"])

    assert result.exit_code == 0
    hits = json.loads(result.output)
    assert hits[0]["rule_id"].endswith("custom.subprocess-shell-true")


def test_search_renders_ccc_format_with_findings_blocks(
    fake_ccc_two_results_on_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`cccr search` répond « de la même manière » que ccc : même format de
    résultats, enrichi d'un bloc findings sous les résultats concernés."""
    monkeypatch.chdir(tmp_path)
    from ccc_radar.models import Finding
    from ccc_radar.store import Store

    finding = Finding(
        id="cli-search-finding",
        rule_id="custom.sql-fstring",
        severity="ERROR",
        message="Une requête SQL construite par f-string permet une injection SQL.",
        path="app/db.py",
        start_line=6,
        end_line=6,
        snippet="cursor.execute(query)",
        fix=None,
        cwe=["CWE-89"],
        owasp=[],
    )
    with Store(tmp_path) as store:
        store.replace_findings_for_files(["app/db.py"], [finding])

    result = runner.invoke(app, ["search", "user authentication flow"])

    assert result.exit_code == 0
    assert "--- Result 1 (score: 0.900) ---" in result.output
    assert "File: app/db.py:6-6 [python]" in result.output
    assert "findings (max: ERROR)" in result.output
    assert "custom.sql-fstring" in result.output
    # L'ordre reste celui de ccc : le résultat sans finding n'est pas déplacé.
    assert result.output.index("app/other.py:1-1") < result.output.index("app/db.py:6-6")


def test_search_json_returns_stable_code_search_result_schema(
    fake_ccc_two_results_on_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    from ccc_radar.store import Store

    with Store(tmp_path):
        pass  # index findings vide mais présent

    result = runner.invoke(app, ["search", "auth", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert set(data.keys()) == {"results", "findings_only_fallback", "warning"}
    assert len(data["results"]) == 2
    assert {"path", "start_line", "end_line", "language", "score", "content",
            "findings", "max_severity"} <= set(data["results"][0].keys())


@pytest.mark.integration
def test_search_uses_ccc_even_when_experimental_code_index_is_available(
    fake_ccc_on_path: Path, repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    index_result = runner.invoke(app, ["index", "--semgrep", "--engine", "cocoindex"])
    assert index_result.exit_code == 0
    (repo_copy / ".cocoindex_code").mkdir()
    (repo_copy / ".cocoindex_code" / "target_sqlite.db").write_text("")

    result = runner.invoke(app, ["search", "injection sql", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["results"]
    assert data["warning"] is None
    assert "path" in data["results"][0]


def test_search_without_findings_index_warns_but_shows_code_results(
    fake_ccc_two_results_on_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["search", "auth"])

    assert result.exit_code == 0
    assert "index findings absent" in result.output
    assert "--- Result 1" in result.output


def test_search_without_ccc_nor_index_fails_with_message_and_code_2(
    no_ccc_on_path: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["search", "auth"])

    assert result.exit_code == 2
    assert "ccc introuvable dans le PATH" in result.output


def test_search_without_ccc_code_index_fails_fast(
    fake_ccc_on_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".cocoindex_code" / "target_sqlite.db").unlink()

    result = runner.invoke(app, ["search", "auth"])

    assert result.exit_code == 2
    assert "index code ccc absent" in result.output
    assert "ccc index" in result.output


def test_search_forwards_offset_lang_path_refresh_flags_to_ccc(
    fake_ccc_args_recording_on_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "search", "auth",
            "--limit", "3", "--offset", "2", "--lang", "python", "--path", "app/*", "--refresh",
        ],
    )

    assert result.exit_code == 0
    # cccr transmet la requête telle quelle et ne modifie pas son jeu de résultats.
    assert "ARGS:search auth --limit 3 --offset 2 --lang python --path app/* --refresh" in result.output


def test_search_returns_error_when_ccc_returns_error(
    fake_ccc_error_on_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["search", "auth"])

    assert result.exit_code == 2
    assert "ccc a échoué (code 42)" in result.output
    assert "ccc service failed" in result.output


def test_search_returns_error_when_ccc_times_out(
    fake_ccc_hanging_on_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CCCR_CCC_SEARCH_TIMEOUT_S", "1")

    result = runner.invoke(app, ["search", "auth"])

    assert result.exit_code == 2
    assert "ccc search a expiré après 1s" in result.output


@pytest.mark.integration
def test_summary_json_has_expected_structure(
    repo_copy: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    runner.invoke(app, ["index", "--semgrep"])

    result = runner.invoke(app, ["summary", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["by_severity"] == {"ERROR": 2, "WARNING": 2}


def _make_endpoint(
    role: str,
    topic: str,
    path: str,
    start_line: int,
    end_line: int,
    module: str | None = None,
    message_type: str | None = None,
) -> MessageEndpoint:
    return MessageEndpoint(
        id=compute_endpoint_id(role, topic, path, start_line, end_line),
        role=role,
        system="kafka" if role in ("produce", "consume") else "rest",
        topic=topic,
        topic_dynamic=False,
        source="code",
        framework=None,
        path=path,
        start_line=start_line,
        end_line=end_line,
        snippet="",
        module=module,
        message_type=message_type,
    )


def test_export_microservices_without_index_exits_with_code_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["export", "microservices", "--json"])

    assert result.exit_code == 2
    assert "Index absent" in result.output


def test_graph_json_reports_outbound_call_in_kafka_consumer_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    consumer = _make_endpoint(
        "consume", "orders.created", "app/OrderConsumer.java", 15, 25
    )
    call = _make_endpoint("call", "POST /payments", "app/OrderConsumer.java", 20, 20)
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(["app/OrderConsumer.java"], [consumer, call])

    result = runner.invoke(app, ["export", "microservices", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["services"] == []
    assert data["edges"] == []
    assert len(data["outbound_calls_in_consumers"]) == 1
    hit = data["outbound_calls_in_consumers"][0]
    assert hit["call"]["topic"] == "POST /payments"
    assert hit["consumer"]["topic"] == "orders.created"
    assert "--workspace" in data["note"]


def test_graph_html_writes_interactive_sigma_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    produce = _make_endpoint(
        "produce", "orders.created", "order-service/Producer.java", 10, 10, "order-service", "OrderCreated"
    )
    consume = _make_endpoint(
        "consume", "orders.created", "payment-service/Consumer.java", 5, 7, "payment-service", "OrderCreated"
    )
    with Store(tmp_path) as store:
        store.replace_modules(
            [
                DiscoveredModule(
                    name="order-service",
                    path=tmp_path / "order-service",
                    build_system="maven",
                    version=None,
                    kind="library",
                    starts_application=True,
                    configuration_example="",
                    mongo_collections=("orders",),
                ),
                DiscoveredModule(
                    name="payment-service",
                    path=tmp_path / "payment-service",
                    build_system="maven",
                    version=None,
                    kind="library",
                    starts_application=True,
                    configuration_example="",
                ),
            ]
        )
        store.replace_endpoints_for_files(
            ["order-service/Producer.java", "payment-service/Consumer.java"],
            [produce, consume],
        )
        store.replace_findings_for_files(
            ["order-service/Producer.java"],
            [
                Finding(
                    id="order-producer-finding",
                    rule_id="example-rule",
                    severity="ERROR",
                    message="Example finding",
                    path="order-service/Producer.java",
                    start_line=10,
                    end_line=10,
                    snippet="kafkaTemplate.send(...) ",
                    fix=None,
                    cwe=[],
                    owasp=[],
                    module="order-service",
                )
            ],
        )
    out_file = tmp_path / "graph.html"

    result = runner.invoke(app, ["export", "microservices", "--html", str(out_file)])

    assert result.exit_code == 0
    document = out_file.read_text(encoding="utf-8")
    assert "new Sigma(network" in document
    assert "order-service" in document
    assert "orders.created" in document
    assert '"published_message_types": ["OrderCreated"]' in document
    assert '"consumed_message_types": ["OrderCreated"]' in document
    assert "Types publies" in document
    assert "Types consommes" in document
    assert "function relationText(link)" in document
    assert "function restResourceLabel(link, target)" in document
    assert "link.published_message_types" in document
    assert "publie${types.length" in document
    assert "HTTP · ${source.name} appelle ${target.name}" in document
    assert "MongoDB · ${source.name} stocke dans ${target.name}" in document
    assert "Kafka · ${source.name} publie" in document
    assert "APIs publiees" in document
    assert "APIs REST consommees" in document
    assert "function contractsForPublishedRestResource(node, resource)" in document
    assert "Ouvrir le contrat OpenAPI ${contract.path}" in document
    assert 'Contrat OpenAPI · ${contract.path}' in document
    assert '${source.name} · Contrat OpenAPI · ${contract.path}' in document
    assert "Topics Kafka" in document
    assert "Contrats de messages" in document
    assert "Consommateurs REST detectes" in document
    assert "REST · ${resource}" in document
    assert "${direction} · ${topic.name}" in document
    assert "Collections MongoDB utilisees" in document
    assert "Relations indexees : ${indexedEdges.length}" in document
    assert "Affichees : ${edges.length}" in document
    assert "contrat non indexe" in document
    assert 'appendList("Findings"' not in document
    assert '"findings"' not in document
    assert 'link.source === id ? "vers"' not in document
    assert 'id="path-query"' in document
    assert 'id="path-lock"' in document
    assert 'id="layout-flow"' not in document
    assert 'id="layout-force"' not in document
    assert 'id="layout-forceatlas2"' in document
    assert 'id="layout-noverlap"' in document
    assert 'id="layout-forceatlas2-noverlap"' in document
    assert "function applyLayout(layout)" in document
    assert "graphology-layout-forceatlas2@0.10.1" in document
    assert "graphology-layout-noverlap@0.4.2" in document
    assert "function sugiyamaPositions(nodes, links)" not in document
    assert "function applyLayout(layout, persist = true)" not in document
    assert 'id="issues-tab"' in document
    assert 'id="issues-panel"' in document
    assert 'id="indexing-issues"' in document
    assert "function renderIndexingIssues()" in document
    assert 'id="paths-tab"' in document
    assert 'id="analyzed-paths"' in document
    assert "function rememberAnalyzedPath(stops)" in document
    assert "function replayAnalyzedPath(stops)" in document
    assert "function setPathMicroserviceOrder(path)" in document
    assert "analyzedPaths.splice(30)" not in document
    assert 'placeholder="service-a -> topic-1 -> service-b"' in document
    assert "function parsePathQuery()" in document
    assert "nodesByNormalizedName" in document
    assert "function shortestPath(sourceId, targetId)" in document
    assert "function shortestPathThrough(stops)" in document
    assert 'id="show-simple-paths"' in document
    assert "function allSimplePaths(sourceId, targetId" in document
    assert "const MAX_SIMPLE_PATH_DEPTH = 8;" in document
    assert "const MAX_SIMPLE_PATHS = 8;" in document
    assert "const MAX_SIMPLE_PATH_EXPLORATIONS = 2000;" in document
    assert "Chemins simples disponibles" in document
    assert "function renderSimplePathChoices(paths, limited)" in document
    assert "Chemin le plus court" in document
    assert "Chemin avec noeuds intermediaires" in document
    assert "Parcours" in document
    assert "const pathRelationText = link" not in document
    assert "const pathStepTitle = link" not in document
    assert "Etape ${index + 1}" not in document
    assert 'header.className = "path-details-header"' in document
    assert 'overviewList.className = "path-overview"' in document
    assert "pathStepClass" not in document
    assert "const pathNodeLabel" in document
    assert "node.name} (${types.join" in document
    assert "node.consumed_message_types" in document
    assert "type Java non indexe" in document
    assert "function persistState()" in document
    assert "function restoreState()" in document
    assert "if (!pathLock.checked) clearPathControls();" in document
    assert 'pathQuery.addEventListener("keydown"' in document
    assert "history.replaceState" in document
    assert "mongodb_collection:order-service:orders" in document
    assert '"complexity": {"score": 2' in document
    assert "Complexite elevee" in document

    c4_project = tmp_path / "architecture-likec4"
    c4_export = runner.invoke(app, ["export", "microservices", "--c4", str(c4_project)])
    assert c4_export.exit_code == 0
    assert "npm install && npm run dev" in c4_export.output
    assert (c4_project / "likec4.config.json").is_file()
    assert (c4_project / "package.json").is_file()
    assert (c4_project / ".gitignore").is_file()
    assert (c4_project / "README.md").is_file()
    assert "Microservice colors" in (c4_project / "README.md").read_text(encoding="utf-8")
    c4_document = (c4_project / "architecture.c4").read_text(encoding="utf-8")
    assert "element microservice" in c4_document
    assert "element kafka_topic" in c4_document
    assert "element mongodb_collection" in c4_document
    assert "orders.created" in c4_document
    assert "1 findings (ERROR=1)" in c4_document
    c4_config = json.loads((c4_project / "likec4.config.json").read_text(encoding="utf-8"))
    assert c4_config["$schema"] == "https://likec4.dev/schemas/config.json"
    assert c4_config["implicitViews"] is True
    c4_package = json.loads((c4_project / "package.json").read_text(encoding="utf-8"))
    assert c4_package["scripts"]["dev"] == "likec4 start"
    assert c4_package["scripts"]["build"] == "likec4 build --output dist --base ./"


def test_export_microservices_c4_requires_a_project_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with Store(tmp_path):
        pass

    result = runner.invoke(app, ["export", "microservices", "--c4", "architecture.c4"])

    assert result.exit_code == 2
    assert "attend un répertoire" in result.output


def test_microservices_commands_explore_business_objects_without_source_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    orders = tmp_path / "orders"
    payments = tmp_path / "payments"
    shipping = tmp_path / "shipping"
    orders.mkdir()
    payments.mkdir()
    shipping.mkdir()
    modules = [
        DiscoveredModule(
            name="orders",
            path=orders,
            build_system="maven",
            version="1.0.0",
            kind="library",
            starts_application=True,
            configuration_example="",
            mongo_collections=("orders",),
            openapi_files=("openapi.yml",),
        ),
        DiscoveredModule(
            name="payments",
            path=payments,
            build_system="gradle",
            version=None,
            kind="library",
            starts_application=True,
            configuration_example="",
        ),
        DiscoveredModule(
            name="shipping",
            path=shipping,
            build_system="maven",
            version="1.0.0",
            kind="library",
            starts_application=True,
            configuration_example="",
        ),
    ]

    def endpoint(
        role: str,
        system: str,
        topic: str,
        module: str,
        path: str,
        snippet: str = "",
        message_type: str | None = None,
    ) -> MessageEndpoint:
        return MessageEndpoint(
            id=compute_endpoint_id(role, topic, path),
            role=role,
            system=system,
            topic=topic,
            topic_dynamic=False,
            source="code",
            framework="spring",
            path=path,
            start_line=10,
            end_line=10,
            snippet=snippet,
            module=module,
            message_type=message_type,
        )

    publish = endpoint(
        "produce", "kafka", "orders.created", "orders", "OrderPublisher.java", "send", "OrderCreated"
    )
    consume = endpoint(
        "consume", "kafka", "orders.created", "payments", "PaymentConsumer.java", "listen", "OrderCreated"
    )
    payment_publish = endpoint(
        "produce", "kafka", "payments.accepted", "payments", "PaymentPublisher.java", "send", "PaymentAccepted"
    )
    shipping_consume = endpoint(
        "consume", "kafka", "payments.accepted", "shipping", "ShippingConsumer.java", "listen", "PaymentAccepted"
    )
    call = endpoint("call", "rest", "POST /payments", "orders", "PaymentClient.java", "http://payments/payments")
    serve = endpoint("serve", "rest", "POST /payments", "payments", "PaymentController.java")
    with Store(tmp_path) as store:
        store.replace_modules(modules)
        store.replace_endpoints_for_files(
            [
                publish.path,
                consume.path,
                payment_publish.path,
                shipping_consume.path,
                call.path,
                serve.path,
            ],
            [publish, consume, payment_publish, shipping_consume, call, serve],
        )

    summary = runner.invoke(app, ["microservices", "show", "orders", "--root", str(tmp_path), "--json"])
    assert summary.exit_code == 0
    summary_payload = json.loads(summary.output)
    assert summary_payload["http_apis_exposed"] == []
    assert summary_payload["http_apis_consumed"] == ["POST /payments"]
    assert summary_payload["kafka_topics_published"] == ["orders.created"]
    assert summary_payload["kafka_message_types_published"] == {"orders.created": ["OrderCreated"]}
    assert summary_payload["databases"]["mongodb_collections"] == ["orders"]
    assert summary_payload["openapi_files"] == ["openapi.yml"]

    short_summary = runner.invoke(app, ["microservices", "show", "orders", "--json"])
    assert short_summary.exit_code == 0
    assert json.loads(short_summary.output)["name"] == "orders"
    assert json.loads(short_summary.output)["kafka_topics_published"] == ["orders.created"]

    service_topics = runner.invoke(
        app, ["microservices", "topics", "orders", "--root", str(tmp_path), "--json"]
    )
    assert service_topics.exit_code == 0
    assert json.loads(service_topics.output) == {
        "microservice": "orders",
        "published": ["orders.created"],
        "consumed": [],
        "published_message_types": {"orders.created": ["OrderCreated"]},
        "consumed_message_types": {},
    }

    topic_summary = runner.invoke(
        app, ["topics", "show", "orders.created", "--root", str(tmp_path), "--json"]
    )
    assert topic_summary.exit_code == 0
    assert json.loads(topic_summary.output)["message_types_published"] == ["OrderCreated"]
    assert json.loads(topic_summary.output)["message_types_consumed"] == ["OrderCreated"]

    dto_summary = runner.invoke(
        app, ["dtos", "show", "OrderCreated", "--root", str(tmp_path), "--json"]
    )
    assert dto_summary.exit_code == 0
    assert json.loads(dto_summary.output) == {
        "kind": "dto",
        "name": "OrderCreated",
        "topics": ["orders.created"],
        "producer_microservices": ["orders"],
        "consumer_microservices": ["payments"],
    }

    dto_consumers = runner.invoke(
        app, ["dtos", "consumers", "OrderCreated", "--root", str(tmp_path), "--json"]
    )
    assert dto_consumers.exit_code == 0
    assert json.loads(dto_consumers.output) == {
        "query": "consumers", "dto": "OrderCreated", "microservices": ["payments"]
    }

    service_resources = runner.invoke(
        app, ["microservices", "apis", "orders", "--root", str(tmp_path), "--json"]
    )
    assert service_resources.exit_code == 0
    assert json.loads(service_resources.output) == {
        "microservice": "orders", "exposed": [], "consumed": ["POST /payments"]
    }

    service_mongodb = runner.invoke(
        app, ["microservices", "mongodb", "orders", "--root", str(tmp_path), "--json"]
    )
    assert service_mongodb.exit_code == 0
    assert json.loads(service_mongodb.output) == {
        "microservice": "orders", "collections": ["orders"]
    }

    mongodb_collections = runner.invoke(app, ["mongodb", "--root", str(tmp_path), "--json"])
    assert mongodb_collections.exit_code == 0
    assert json.loads(mongodb_collections.output) == [
        {"kind": "collection", "name": "orders", "modules": ["orders"], "operations": 0}
    ]

    mongodb_services = runner.invoke(
        app, ["mongodb", "services", "orders", "--root", str(tmp_path), "--json"]
    )
    assert mongodb_services.exit_code == 0
    assert json.loads(mongodb_services.output) == {
        "query": "services", "collection": "orders", "microservices": ["orders"]
    }

    mongodb_search = runner.invoke(
        app, ["mongodb", "search", "ord", "--root", str(tmp_path), "--json"]
    )
    assert mongodb_search.exit_code == 0
    assert json.loads(mongodb_search.output)["resolved"] == "orders"

    topic_neighbors = runner.invoke(
        app, ["topics", "neighbors", "orders.created", "--root", str(tmp_path), "--json"]
    )
    assert topic_neighbors.exit_code == 0
    assert json.loads(topic_neighbors.output) == [
        {"kind": "module", "name": "orders", "relation": "producer"},
        {"kind": "module", "name": "payments", "relation": "consumer"},
    ]

    consumers = runner.invoke(
        app, ["topics", "consumers", "orders.created", "--root", str(tmp_path), "--json"]
    )
    assert consumers.exit_code == 0
    assert json.loads(consumers.output)["microservices"] == ["payments"]

    service_path = runner.invoke(
        app, ["analyze", "microservices", "path", "payments", "shipping", "--root", str(tmp_path), "--json"]
    )
    assert service_path.exit_code == 0
    assert json.loads(service_path.output) == {
        "kind": "microservice_paths",
        "source": "payments",
        "target": "shipping",
        "paths": [
            {
                "nodes": [
                    {"kind": "microservice", "name": "payments"},
                    {"kind": "topic", "name": "payments.accepted"},
                    {"kind": "microservice", "name": "shipping"},
                ],
                "relations": [
                    {"kind": "publishes", "label": "payments.accepted"},
                    {"kind": "consumes", "label": "payments.accepted"},
                ],
            }
        ],
        "max_depth": 12,
        "truncated": False,
    }

    trace = runner.invoke(
        app, ["topics", "trace", "orders.created", "--root", str(tmp_path), "--json"]
    )
    assert trace.exit_code == 0
    trace_payload = json.loads(trace.output)
    assert trace_payload["kind"] == "potential_topic_flows"
    assert trace_payload["flows"] == [
        {
            "nodes": [
                {"kind": "topic", "name": "orders.created"},
                {"kind": "microservice", "name": "payments"},
                {"kind": "topic", "name": "payments.accepted"},
                {"kind": "microservice", "name": "shipping"},
            ]
        }
    ]
    assert "hypothèses" in trace_payload["caveat"]

    shallow_trace = runner.invoke(
        app,
        [
            "topics",
            "trace",
            "orders.created",
            "--max-depth",
            "1",
            "--root",
            str(tmp_path),
            "--json",
        ],
    )
    assert shallow_trace.exit_code == 0
    assert json.loads(shallow_trace.output)["flows"] == [
        {
            "nodes": [
                {"kind": "topic", "name": "orders.created"},
                {"kind": "microservice", "name": "payments"},
            ]
        }
    ]

    cycle_publish = endpoint(
        "produce", "kafka", "orders.created", "shipping", "ShippingPublisher.java", "send"
    )
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(
            [cycle_publish.path], [cycle_publish]
        )
    cyclic_trace = runner.invoke(
        app, ["topics", "trace", "orders.created", "--root", str(tmp_path), "--json"]
    )
    assert cyclic_trace.exit_code == 0
    assert json.loads(cyclic_trace.output)["flows"] == [
        {
            "nodes": [
                {"kind": "topic", "name": "orders.created"},
                {"kind": "microservice", "name": "payments"},
                {"kind": "topic", "name": "payments.accepted"},
                {"kind": "microservice", "name": "shipping"},
                {"kind": "topic", "name": "orders.created"},
            ],
            "cycle_detected": True,
        }
    ]

    topic_search = runner.invoke(
        app, ["topics", "search", "created", "--root", str(tmp_path), "--json"]
    )
    assert topic_search.exit_code == 0
    assert json.loads(topic_search.output)["resolved"] == "orders.created"

    resource_search = runner.invoke(
        app, ["apis", "search", "payments", "--root", str(tmp_path), "--json"]
    )
    assert resource_search.exit_code == 0
    assert json.loads(resource_search.output)["resolved"] == "POST /payments"

    api_providers = runner.invoke(
        app, ["apis", "providers", "POST /payments", "--root", str(tmp_path), "--json"]
    )
    assert api_providers.exit_code == 0
    assert json.loads(api_providers.output) == {
        "query": "providers", "api": "POST /payments", "microservices": ["payments"]
    }

    monkeypatch.setattr(
        "ccc_radar.cli.load_config", lambda _root: SimpleNamespace(embedding_model="test-model")
    )
    monkeypatch.setattr("ccc_radar.cli.make_embedder", lambda _model: object())
    monkeypatch.setattr(
        "ccc_radar.cli.resolve_topic_by_similarity",
        lambda _store, _embedder, _query, endpoints: endpoints[0].topic,
    )
    semantic_topic_search = runner.invoke(
        app, ["topics", "search", "publication de commande", "--root", str(tmp_path), "--json"]
    )
    assert semantic_topic_search.exit_code == 0
    assert json.loads(semantic_topic_search.output)["resolved"] == "orders.created"

    semantic_resource_search = runner.invoke(
        app, ["apis", "search", "encaissement", "--root", str(tmp_path), "--json"]
    )
    assert semantic_resource_search.exit_code == 0
    assert json.loads(semantic_resource_search.output)["resolved"] == "POST /payments"

    implementation = runner.invoke(
        app, ["microservices", "implementation", "integration", publish.id, "--root", str(tmp_path), "--json"]
    )
    assert implementation.exit_code == 0
    assert json.loads(implementation.output)["implementation"]["snippet"] == "send"


@pytest.mark.integration
def test_graph_reflects_a_real_cccr_index_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BACKLOG-11 A1 CA4 : cccr export microservices reflète une indexation
    standard (init + index), sans fixture injectée directement dans le
    store — le scénario de OrderConsumer.java (@KafkaListener contenant un
    appel RestTemplate) doit ressortir de bout en bout."""
    dest = tmp_path / "endpoint_index_repo"
    shutil.copytree(ENDPOINT_INDEX_REPO, dest)
    monkeypatch.chdir(dest)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")

    runner.invoke(app, ["init", "--rules", "rules/rules.yml"])
    index_result = runner.invoke(app, ["index", "--semgrep"])
    assert index_result.exit_code == 0

    graph_result = runner.invoke(app, ["export", "microservices", "--json"])
    assert graph_result.exit_code == 0
    data = json.loads(graph_result.output)
    assert data["services"] == []
    assert data["edges"] == []
    assert len(data["outbound_calls_in_consumers"]) == 1
    hit = data["outbound_calls_in_consumers"][0]
    assert hit["consumer"]["topic"] == "orders.created"
    assert hit["call"]["topic"] == "POST /charge"

    # le finding "ordinaire" (System.out.println) est bien resté un finding,
    # pas un endpoint fuité dans la table findings (ni l'inverse) : 1 seul
    # finding au total, les 2 résultats endpoint-inventory n'y apparaissent pas.
    summary_result = runner.invoke(app, ["summary", "--json"])
    assert summary_result.exit_code == 0
    assert sum(json.loads(summary_result.output)["by_severity"].values()) == 1


def test_graph_reflects_kafka_edges_after_a_real_cccr_index_run_without_semgrep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    for service_name, extra_source in (
        (
            "order-service",
            """
import org.springframework.kafka.core.KafkaTemplate;

class OrderPublisher {
    private KafkaTemplate<String, String> kafkaTemplate;
    void publish(String payload) {
        kafkaTemplate.send("orders.created", payload);
    }
}
""".strip(),
        ),
        (
            "payment-service",
            """
import org.springframework.kafka.annotation.KafkaListener;

class PaymentConsumer {
    @KafkaListener(topics = "orders.created")
    void onOrderCreated(String payload) {}
}
""".strip(),
        ),
    ):
        service_dir = tmp_path / service_name
        (service_dir / "src" / "main" / "java").mkdir(parents=True)
        (service_dir / "pom.xml").write_text(
            f"<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
            f"<modelVersion>4.0.0</modelVersion>"
            f"<artifactId>{service_name}</artifactId>"
            "<version>1.0.0</version>"
            "</project>"
        )
        (service_dir / "src" / "main" / "java" / "Application.java").write_text(
            """
import org.springframework.boot.SpringApplication;

class Application {
    static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
""".strip()
        )
        (service_dir / "src" / "main" / "java" / ("Publisher.java" if "order" in service_name else "Consumer.java")).write_text(extra_source)
    (tmp_path / ".cccr").mkdir()
    (tmp_path / ".cccr" / "config.yml").write_text("rules: ['rules/rules.yml']\n")

    index_result = runner.invoke(app, ["index", "--disable", "semgrep"])
    assert index_result.exit_code == 0

    graph_result = runner.invoke(app, ["export", "microservices", "--json"])
    assert graph_result.exit_code == 0
    data = json.loads(graph_result.output)
    assert data["services"] == ["order-service", "payment-service"]
    assert {node["name"] for node in data["nodes"]} == {
        "order-service",
        "payment-service",
        "orders.created",
    }
    assert {(edge["kind"], edge["from_node"], edge["to_node"], edge["label"]) for edge in data["edges"]} == {
        ("kafka_produce", "order-service", "orders.created", "orders.created"),
        ("kafka_consume", "orders.created", "payment-service", "orders.created"),
    }
    assert data["outbound_calls_in_consumers"] == []


def test_export_microservices_keeps_configured_http_dependency_without_target_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    modules = [
        DiscoveredModule(
            name=name,
            path=tmp_path / name,
            build_system="maven",
            version=None,
            kind="library",
            starts_application=True,
            configuration_example="",
        )
        for name in ("caller-service", "domain-annuaire")
    ]
    client = MessageEndpoint(
        id=compute_endpoint_id("call", "ANY <dynamic>", "caller/RestClientConfig.java", 4, 4),
        role="call",
        system="rest",
        topic="ANY <dynamic>",
        topic_dynamic=True,
        source="code",
        framework="configured-api-client-configuration",
        path="caller/RestClientConfig.java",
        start_line=4,
        end_line=4,
        snippet="cccr-api-domain:domain-annuaire",
        module="caller-service",
    )
    with Store(tmp_path) as store:
        store.replace_modules(modules)
        store.replace_endpoints_for_files([client.path], [client])

    result = runner.invoke(app, ["export", "microservices", "--json"])

    assert result.exit_code == 0
    graph = json.loads(result.output)
    assert graph["services"] == ["caller-service", "domain-annuaire"]
    assert graph["edges"] == [
        {
            "kind": "rest",
            "from_node": "caller-service",
            "from_kind": "microservice",
            "to_node": "domain-annuaire",
            "to_kind": "microservice",
            "label": "domain-annuaire: API",
            "from_site": {
                "path": "caller/RestClientConfig.java",
                "start_line": 4,
                "end_line": 4,
                "topic": "ANY <dynamic>",
            },
            "to_site": None,
        }
    ]


def test_microservices_lists_only_runtime_services(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = tmp_path / "maven_workspace"
    shutil.copytree(MAVEN_WORKSPACE, dest)
    with Store(dest / "service-a"):
        pass  # crée .cccr/findings.db, vide

    result = runner.invoke(app, ["microservices", "--root", str(dest), "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    # Les modules Maven sans point d'entrée Spring Boot ne sont pas des
    # microservices, même si une base .cccr locale existe.
    assert data == {"services": [], "warnings": []}


def test_microservices_text_reports_no_modules_for_empty_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()

    result = runner.invoke(app, ["microservices", "--root", str(empty)])

    assert result.exit_code == 0
    assert "Aucun service workspace découvert" in result.output


def test_microservices_defaults_to_current_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = tmp_path / "maven_workspace"
    shutil.copytree(MAVEN_WORKSPACE, dest)
    monkeypatch.chdir(dest)
    with Store(dest / "service-a"):
        pass

    result = runner.invoke(app, ["microservices", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data == {"services": [], "warnings": []}


def test_microservices_discovers_gradle_services(tmp_path: Path) -> None:
    project = tmp_path / "billing-service" / "billing-service-main"
    (project / "build.gradle").parent.mkdir(parents=True)
    (project / "build.gradle").write_text("archivesName = 'billing-service'\n")
    service = project / "src" / "main" / "java"
    service.mkdir(parents=True)
    (service / "BillingServiceMain.java").write_text(
        """
import org.springframework.boot.SpringApplication;

public class BillingServiceMain {
    public static void main(String[] args) {
        SpringApplication.run(BillingServiceMain.class, args);
    }
}
""".strip()
    )
    module = DiscoveredModule(
        name="billing-service",
        path=project,
        build_system="gradle",
        version=None,
        kind="library",
        starts_application=True,
        configuration_example="",
        mongo_collections=("invoices",),
        openapi_files=("src/main/resources/invoices.yaml",),
    )
    endpoints = [
        MessageEndpoint(
            id="serve", role="serve", system="rest", topic="POST /invoices", topic_dynamic=False,
            source="code", framework="spring", path="InvoiceController.java", start_line=1,
            end_line=1, snippet="", module="billing-service",
        ),
        MessageEndpoint(
            id="publish", role="produce", system="kafka", topic="invoices.created", topic_dynamic=False,
            source="code", framework="spring", path="InvoicePublisher.java", start_line=1,
            end_line=1, snippet="", module="billing-service", message_type="InvoiceCreated",
        ),
        MessageEndpoint(
            id="consume", role="consume", system="kafka", topic="payments.received", topic_dynamic=False,
            source="code", framework="spring", path="PaymentConsumer.java", start_line=1,
            end_line=1, snippet="", module="billing-service", message_type="PaymentReceived",
        ),
    ]
    with Store(tmp_path) as store:
        store.replace_modules([module])
        store.replace_endpoints_for_files([endpoint.path for endpoint in endpoints], endpoints)

    result = runner.invoke(app, ["microservices", "--root", str(tmp_path), "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["services"] == [
        {
            "name": "billing-service",
            "kind": "microservice",
            "starts_application": True,
            "indexed": True,
            "integration_count": 3,
            "finding_count": 0,
            "exposes_http_api": True,
            "http_apis_exposed": ["POST /invoices"],
            "http_apis_consumed": [],
            "kafka_topics_published": ["invoices.created"],
            "kafka_topics_consumed": ["payments.received"],
            "kafka_message_types_published": {"invoices.created": ["InvoiceCreated"]},
            "kafka_message_types_consumed": {"payments.received": ["PaymentReceived"]},
            "mongo_collections": ["invoices"],
            "openapi_files": ["src/main/resources/invoices.yaml"],
        }
    ]

    text_result = runner.invoke(app, ["microservices", "--root", str(tmp_path)])
    assert text_result.exit_code == 0
    assert "HTTP exposées: POST /invoices" in text_result.output
    assert "Kafka publiés: invoices.created" in text_result.output
    assert "Kafka consommés: payments.received" in text_result.output
    assert "Mongo: invoices" in text_result.output
    assert "OpenAPI: src/main/resources/invoices.yaml" in text_result.output


def test_microservices_service_subcommands_render_apis_and_properties(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    service_a = workspace / "service-a"
    service_b = workspace / "service-b"
    for service, artifact, class_name in (
        (service_a, "order-service", "OrderApplication"),
        (service_b, "payment-service", "PaymentApplication"),
    ):
        (service / "src" / "main" / "java").mkdir(parents=True)
        (service / "pom.xml").write_text(
            f"<project><artifactId>{artifact}</artifactId></project>"
        )
        (service / "src" / "main" / "java" / f"{class_name}.java").write_text(
            "import org.springframework.boot.SpringApplication;\n"
            f"class {class_name} {{ static void main(String[] args) {{ "
            f"SpringApplication.run({class_name}.class, args); }} }}\n"
        )
    (service_a / "src" / "main" / "resources").mkdir(parents=True)
    (service_a / "src" / "main" / "resources" / "application.yml").write_text("server:\n  port: 8081\n")
    (service_a / "src" / "main" / "resources" / "openapi.yml").write_text("openapi: 3.0.0\npaths: {}\n")
    call = MessageEndpoint(
        id="call", role="call", system="rest", topic="GET /payments", topic_dynamic=False,
        source="code", framework="resttemplate", path="OrderClient.java", start_line=10,
            end_line=10, snippet="restTemplate.getForObject(\"http://payment-service/payments\")", module="order-service",
    )
    serve = MessageEndpoint(
        id="serve", role="serve", system="rest", topic="GET /payments", topic_dynamic=False,
        source="code", framework="spring", path="PaymentController.java", start_line=5,
        end_line=5, snippet="", module="payment-service",
    )
    with Store(service_a) as store:
        store.replace_endpoints_for_files([call.path], [call])
    with Store(service_b) as store:
        store.replace_endpoints_for_files([serve.path], [serve])

    resources_by_service = runner.invoke(
        app, ["microservices", "apis", "order-service", "--root", str(workspace), "--json"]
    )
    assert resources_by_service.exit_code == 0
    assert json.loads(resources_by_service.output) == {
        "microservice": "order-service", "exposed": [], "consumed": ["GET /payments"]
    }

    api_consumers = runner.invoke(
        app, ["apis", "consumers", "GET /payments", "--root", str(workspace), "--json"]
    )
    assert api_consumers.exit_code == 0
    assert json.loads(api_consumers.output) == {
        "query": "consumers", "api": "GET /payments", "microservices": ["order-service"]
    }

    properties = runner.invoke(app, ["microservices", "properties", "order-service", "--root", str(workspace), "--json"])
    assert properties.exit_code == 0
    assert "Aucune propriété Spring détectée" in json.loads(properties.output)["properties_example"]

    openapi = runner.invoke(app, ["microservices", "openapi", "order-service", "--root", str(workspace), "--json"])
    assert openapi.exit_code == 0
    assert json.loads(openapi.output)["contracts"] == [
        {"path": "src/main/resources/openapi.yml", "content": "openapi: 3.0.0\npaths: {}\n"}
    ]


def test_workspace_command_is_no_longer_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["workspace"])

    assert result.exit_code != 0
    assert "No such command 'workspace'" in result.output


def test_index_falls_back_to_local_default_model_when_config_uses_remote_identifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "app.py").write_text("print('hello')\n")
    local_model = tmp_path / "local-model"
    local_model.mkdir()
    monkeypatch.setattr(embedder_module, "DEFAULT_EMBEDDING_MODEL", str(local_model))
    (tmp_path / ".cccr").mkdir()
    (tmp_path / ".cccr" / "config.yml").write_text(
        "rules:\n  - rules/rules.yml\nembedding_model: Snowflake/snowflake-arctic-embed-xs\n"
    )
    (tmp_path / "rules").mkdir()
    (tmp_path / "rules" / "rules.yml").write_text("rules: []\n")

    result = runner.invoke(app, ["index"])

    assert result.exit_code == 0
    assert "Snowflake/snowflake-arctic-embed-xs" in result.output
    with Store(tmp_path) as store:
        assert store.get_meta("embedding_model") == str(local_model)


def test_graph_json_reports_stale_endpoint_inventory_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    endpoint = MessageEndpoint(
        id=compute_endpoint_id("consume", "orders.created", "app/Consumer.java", 5, 7),
        role="consume",
        system="kafka",
        topic="orders.created",
        topic_dynamic=False,
        source="code",
        framework="spring-kafka",
        path="app/Consumer.java",
        start_line=5,
        end_line=7,
        snippet="",
    )
    with Store(tmp_path) as store:
        store.replace_endpoints_for_files(["app/Consumer.java"], [endpoint])
        store.set_meta("endpoint_inventory_signature", "endpoint-inventory-v0")

    result = runner.invoke(app, ["export", "microservices", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "inventaire des intégrations potentiellement obsolète" in payload["note"]


def test_export_microservices_without_semgrep_does_not_report_stale_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CCCR_FAKE_EMBEDDER", "1")
    (tmp_path / "src" / "main" / "java").mkdir(parents=True)
    (tmp_path / "src" / "main" / "java" / "BillingServiceMain.java").write_text(
        """
import org.springframework.boot.SpringApplication;

public class BillingServiceMain {
    public static void main(String[] args) {
        SpringApplication.run(BillingServiceMain.class, args);
    }
}
""".strip()
    )
    (tmp_path / "pom.xml").write_text(
        "<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
        "<modelVersion>4.0.0</modelVersion>"
        "<artifactId>billing-service</artifactId>"
        "<version>1.0.0</version>"
        "</project>"
    )
    (tmp_path / ".cccr").mkdir()
    (tmp_path / ".cccr" / "config.yml").write_text("rules: ['rules/rules.yml']\n")

    index_result = runner.invoke(app, ["index", "--disable", "semgrep"])
    assert index_result.exit_code == 0

    export_result = runner.invoke(app, ["export", "microservices", "--json"])
    assert export_result.exit_code == 0
    payload = json.loads(export_result.output)
    assert "obsolète" not in payload.get("note", "")
