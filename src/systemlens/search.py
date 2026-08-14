from dataclasses import dataclass
from pathlib import Path
import re

from systemlens.config import VALID_SEVERITIES
from systemlens.models import Finding
from systemlens.store import Store

_WORD_RE = re.compile(r"\w+")


class SearchError(Exception):
    """A findings search parameter is invalid."""


@dataclass
class SearchHit:
    finding: Finding
    score: float


@dataclass
class Summary:
    by_severity: dict[str, int]
    top_rules: list[tuple[str, int]]
    by_top_level_dir: dict[str, int]


def _tokenize(text: str) -> list[str]:
    return _WORD_RE.findall(text.casefold())


def _normalize_for_match(text: str) -> str:
    return " ".join(_tokenize(text))


def _severity_rank(severity: str) -> int:
    return VALID_SEVERITIES.index(severity)


def _finding_sort_key(finding: Finding) -> tuple[int, str, int, int, str]:
    return (-_severity_rank(finding.severity), finding.path, finding.start_line, finding.end_line, finding.id)


def _keyword_fields(finding: Finding) -> list[tuple[str, float]]:
    return [
        (finding.rule_id, 6.0),
        (finding.message, 4.0),
        (finding.path, 3.0),
        (" ".join(finding.cwe), 5.0),
        (" ".join(finding.owasp), 5.0),
        (finding.snippet, 2.0),
        (finding.severity, 1.0),
    ]


def _keyword_score(finding: Finding, query: str) -> float:
    query_casefold = query.strip().casefold()
    query_tokens = set(_tokenize(query))
    normalized_query = _normalize_for_match(query)
    if not query_casefold and not query_tokens:
        return 0.0

    fields = _keyword_fields(finding)
    searchable = " ".join(value for value, _ in fields).casefold()
    searchable_tokens = set(_tokenize(searchable))
    # A findings result must cover every query token. This deliberately avoids
    # returning a result merely because it shares one generic word such as
    # "security" or "injection" with the query.
    if query_tokens and not query_tokens.issubset(searchable_tokens):
        return 0.0

    score = 0.0
    for value, weight in fields:
        if not value:
            continue
        raw_value = value.casefold()
        normalized_value = _normalize_for_match(value)
        if query_casefold:
            if raw_value == query_casefold:
                score += weight * 8.0
            elif query_casefold in raw_value:
                score += weight * 4.0
        if normalized_query:
            if normalized_value == normalized_query:
                score += weight * 6.0
            elif normalized_query in normalized_value:
                score += weight * 3.0
        if query_tokens:
            field_tokens = set(_tokenize(value))
            score += weight * sum(1.0 for token in query_tokens if token in field_tokens)
    return score


def _keyword_hits(candidates: list[Finding], query: str) -> list[SearchHit]:
    hits = [
        SearchHit(finding=finding, score=score)
        for finding in candidates
        if (score := _keyword_score(finding, query)) > 0.0
    ]
    hits.sort(key=lambda hit: (-hit.score, *_finding_sort_key(hit.finding)))
    return hits


def search_findings(
    store: Store,
    query: str | None,
    severity: str | None = None,
    rule: str | None = None,
    path_glob: str | None = None,
    limit: int = 5,
    offset: int = 0,
) -> list[SearchHit]:
    """Precision-first lexical findings search."""
    if severity is not None and severity not in VALID_SEVERITIES:
        raise SearchError(
            f"Sévérité invalide : {severity!r}. Valeurs autorisées : {VALID_SEVERITIES}."
        )

    candidates = store.all_findings(
        severity_at_least=severity, rule_id=rule, path_glob=path_glob
    )
    if not candidates:
        return []

    # `systemlens findings` without a query is an inventory view. Keep the same
    # deterministic severity/location ordering as a searched result while
    # preserving all filters and pagination.
    if query is None or not query.strip():
        hits = [SearchHit(finding=finding, score=0.0) for finding in candidates]
        hits.sort(key=lambda hit: _finding_sort_key(hit.finding))
        return hits[offset : offset + limit]

    keyword_hits = _keyword_hits(candidates, query)
    return keyword_hits[offset : offset + limit]


def summary(store: Store) -> Summary:
    by_severity = store.counts_by("severity")
    rule_counts = store.counts_by("rule_id")
    top_rules = sorted(rule_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]

    dir_counts: dict[str, int] = {}
    for finding in store.all_findings():
        top_dir = finding.path.split("/", 1)[0]
        dir_counts[top_dir] = dir_counts.get(top_dir, 0) + 1

    return Summary(by_severity=by_severity, top_rules=top_rules, by_top_level_dir=dir_counts)


def get_context(repo_root: Path, finding: Finding, before: int = 5, after: int = 5) -> str:
    lines = (repo_root / finding.path).read_text(
        encoding="utf-8", errors="replace"
    ).splitlines()
    start_line = max(finding.start_line - before, 1)
    end_line = min(finding.end_line + after, len(lines))
    return "\n".join(f"{n:>5}| {lines[n - 1]}" for n in range(start_line, end_line + 1))
