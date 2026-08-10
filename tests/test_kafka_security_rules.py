from pathlib import Path

import pytest

from ccc_radar.config import Config
from ccc_radar.semgrep import run_semgrep

# Le pack de règles vit dans le repo skill (ccc-radar-skill/skills/cccr/
# rules/kafka-security/), pas dans ce repo (ADR-24). Cible d'analyse :
# Java + Spring + Maven uniquement.
#
# Volet sécurité de K8 (BACKLOG-10) : les autres items listés à l'origine
# (producteur non idempotent, enable.auto.commit, handler sans DLQ/retry)
# sont déjà couverts par le pack `default` (skills/cccr/rules/default/
# b-kafka.yaml, règles R7/R10) — pas dupliqués ici.
FIXTURES_DIR = Path(__file__).parent / "fixtures"
KAFKA_SECURITY_REPO = FIXTURES_DIR / "kafka_security_repo"


def make_config(**overrides: object) -> Config:
    defaults: dict = {"rules": ["rules/java.yaml"]}
    defaults.update(overrides)
    return Config(**defaults)


@pytest.mark.integration
def test_sasl_plaintext_credentials_ignores_credentials_built_from_a_variable() -> None:
    findings = run_semgrep(
        KAFKA_SECURITY_REPO, make_config(), files=["app/KafkaConfig.java"]
    )

    hits = [
        f for f in findings if f.rule_id == "rules.cccr.kafka-security.sasl-plaintext-credentials"
    ]
    assert [f.start_line for f in hits] == [10]


@pytest.mark.integration
def test_plaintext_protocol_flags_string_literal_and_constant_key() -> None:
    findings = run_semgrep(
        KAFKA_SECURITY_REPO, make_config(), files=["app/KafkaConfig.java"]
    )

    hits = [f for f in findings if f.rule_id == "rules.cccr.kafka-security.plaintext-protocol"]
    assert {f.start_line for f in hits} == {26, 32}


@pytest.mark.integration
def test_json_deserializer_trusts_all_packages_ignores_specific_package() -> None:
    findings = run_semgrep(
        KAFKA_SECURITY_REPO, make_config(), files=["app/Deserialization.java"]
    )

    hits = [
        f
        for f in findings
        if f.rule_id == "rules.cccr.kafka-security.json-deserializer-trusts-all-packages"
    ]
    assert {f.start_line for f in hits} == {14, 18}


@pytest.mark.integration
def test_unsafe_java_deserialization_ignores_non_deserializing_stream_read() -> None:
    findings = run_semgrep(
        KAFKA_SECURITY_REPO, make_config(), files=["app/Deserialization.java"]
    )

    hits = [
        f for f in findings if f.rule_id == "rules.cccr.kafka-security.unsafe-java-deserialization"
    ]
    assert [f.start_line for f in hits] == [27]


@pytest.mark.integration
def test_kafka_security_pack_runs_standalone_without_other_backlog_tasks() -> None:
    findings = run_semgrep(KAFKA_SECURITY_REPO, make_config())

    assert len(findings) == 6
    assert {f.rule_id for f in findings} == {
        "rules.cccr.kafka-security.sasl-plaintext-credentials",
        "rules.cccr.kafka-security.plaintext-protocol",
        "rules.cccr.kafka-security.json-deserializer-trusts-all-packages",
        "rules.cccr.kafka-security.unsafe-java-deserialization",
    }
