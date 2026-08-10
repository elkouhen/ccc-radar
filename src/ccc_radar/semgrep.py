"""Optional Semgrep integration, isolated from local architecture analysis."""

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from ccc_radar.config import Config
from ccc_radar.models import Finding, MessageEndpoint, compute_endpoint_id, compute_finding_id
from ccc_radar.scanner import (
    SEVERITY_ORDER,
    _extract_rest_path,
    _extract_restclient_path,
    _extract_resttemplate_path,
    _file_uses_restclient,
    _file_uses_resttemplate,
    _java_qualified_name,
    _module_for_path,
    _read_snippet,
    infer_framework_endpoints,
    infer_json_kafka_flow_graph_endpoints,
    infer_kafka_endpoints,
    infer_markdown_topic_manifest_endpoints,
)


class SemgrepError(Exception):
    """Raised when the optional Semgrep analysis cannot produce valid output."""


_SEVERITY_MAP = {
    "INFO": "INFO", "WARNING": "WARNING", "ERROR": "ERROR", "LOW": "INFO",
    "MEDIUM": "WARNING", "HIGH": "ERROR", "CRITICAL": "ERROR",
}


def _semgrep_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault(
        "SEMGREP_LOG_FILE",
        os.environ.get("CCCR_SEMGREP_LOG_FILE", str(Path(tempfile.gettempdir()) / "cccr-semgrep.log")),
    )
    env.setdefault("SEMGREP_SEND_METRICS", "off")
    return env


def _normalize_severity(raw_severity: str) -> str:
    severity = _SEVERITY_MAP.get(str(raw_severity).upper())
    if severity is None:
        raise SemgrepError(f"Sévérité Semgrep inconnue : {raw_severity!r}")
    return severity


def _normalize_str_or_list(value: Any) -> list[str]:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def _relative_path(raw_path: str, repo_root: Path) -> str:
    path = Path(raw_path)
    if path.is_absolute():
        path = path.relative_to(repo_root.resolve())
    return path.as_posix()


def _results(raw: str) -> list[dict]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SemgrepError(f"Sortie Semgrep JSON invalide : {exc}") from exc
    try:
        return data["results"]
    except (KeyError, TypeError) as exc:
        raise SemgrepError(
            f"Sortie Semgrep JSON invalide : champ 'results' manquant ({exc})"
        ) from exc


def parse_semgrep_json(raw: str, repo_root: Path) -> list[Finding]:
    findings: list[Finding] = []
    for result in _results(raw):
        try:
            rule_id = result["check_id"]
            extra = result["extra"]
            severity = _normalize_severity(extra["severity"])
            path = _relative_path(result["path"], repo_root)
            start_line = result["start"]["line"]
            end_line = result["end"]["line"]
        except (KeyError, TypeError) as exc:
            raise SemgrepError(
                f"Sortie Semgrep JSON invalide : champ manquant ({exc})"
            ) from exc
        metadata = extra.get("metadata") or {}
        if metadata.get("category") == "endpoint-inventory":
            continue
        snippet = _read_snippet(repo_root, path, start_line, end_line)
        findings.append(Finding(
            id=compute_finding_id(rule_id, path, snippet, start_line, end_line),
            rule_id=rule_id, severity=severity, message=extra.get("message", ""),
            path=path, start_line=start_line, end_line=end_line, snippet=snippet,
            fix=extra.get("fix"), cwe=_normalize_str_or_list(metadata.get("cwe")),
            owasp=_normalize_str_or_list(metadata.get("owasp")),
            module=_module_for_path(repo_root, path),
            qualified_name=_java_qualified_name(str(repo_root), path),
        ))
    return findings


def parse_semgrep_endpoints(raw: str, repo_root: Path) -> list[MessageEndpoint]:
    endpoints: list[MessageEndpoint] = []
    for result in _results(raw):
        extra = result.get("extra") or {}
        metadata = extra.get("metadata") or {}
        if metadata.get("category") != "endpoint-inventory" or metadata.get("system", "rest") != "rest":
            continue
        try:
            path = _relative_path(result["path"], repo_root)
            start_line = result["start"]["line"]
            end_line = result["end"]["line"]
            role = metadata["role"]
            http_method = metadata["http_method"]
        except (KeyError, TypeError) as exc:
            raise SemgrepError(
                f"Règle d'inventaire d'endpoints mal formée : champ manquant ({exc})"
            ) from exc
        snippet = _read_snippet(repo_root, path, start_line, end_line)
        framework = metadata.get("framework")
        is_restclient = framework == "webclient" and _file_uses_restclient(str(repo_root), path)
        if framework == "resttemplate":
            if not _file_uses_resttemplate(str(repo_root), path):
                continue
            route, dynamic = _extract_resttemplate_path(snippet, repo_root, path) or _extract_rest_path(
                snippet, repo_root, path, start_line
            )
        elif is_restclient:
            route, dynamic = _extract_restclient_path(snippet, repo_root, path) or _extract_rest_path(
                snippet, repo_root, path, start_line
            )
        else:
            route, dynamic = _extract_rest_path(snippet, repo_root, path, start_line)
        topic = f"{http_method} {route}"
        endpoints.append(MessageEndpoint(
            id=compute_endpoint_id(role, topic, path, start_line, end_line),
            role=role, system="rest", topic=topic, topic_dynamic=dynamic, source="code",
            framework="restclient" if is_restclient else framework, path=path,
            start_line=start_line, end_line=end_line, snippet=snippet,
            module=_module_for_path(repo_root, path),
            qualified_name=_java_qualified_name(str(repo_root), path),
        ))
    return endpoints


def invoke_semgrep_raw(repo_root: Path, config: Config, files: list[str] | None = None) -> str:
    cmd = [
        "semgrep", "scan", "--json", "--quiet", "--disable-version-check", "--metrics=off",
        "--x-ignore-semgrepignore-files", "--timeout", str(config.semgrep_timeout_s),
    ]
    for rule in config.rules:
        cmd += ["--config", rule]
    cmd += files if files else ["."]
    proc = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True, check=False, env=_semgrep_env())
    if proc.returncode not in (0, 1):
        raise SemgrepError(f"Semgrep a échoué (code {proc.returncode}) : {proc.stderr.strip()}")
    return proc.stdout


def run_semgrep(repo_root: Path, config: Config, files: list[str] | None = None) -> list[Finding]:
    min_index = SEVERITY_ORDER.index(config.min_severity)
    return [
        finding for finding in parse_semgrep_json(invoke_semgrep_raw(repo_root, config, files), repo_root)
        if SEVERITY_ORDER.index(finding.severity) >= min_index
    ]


def run_semgrep_endpoints(
    repo_root: Path, config: Config, files: list[str] | None = None, *,
    configured_api_client_strategy1: bool = False,
) -> list[MessageEndpoint]:
    endpoints = parse_semgrep_endpoints(invoke_semgrep_raw(repo_root, config, files), repo_root)
    endpoints = [
        endpoint for endpoint in endpoints
        if not (endpoint.path.endswith(".java") and endpoint.framework in {"spring", "feign", "resttemplate", "webclient"})
    ]
    endpoints.extend(infer_framework_endpoints(repo_root, files, configured_api_client_strategy1=configured_api_client_strategy1))
    endpoints.extend(infer_kafka_endpoints(repo_root, files))
    endpoints.extend(infer_markdown_topic_manifest_endpoints(repo_root, files))
    endpoints.extend(infer_json_kafka_flow_graph_endpoints(repo_root, files))
    return endpoints
