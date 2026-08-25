"""Bounded, unpersisted APM overlay for the static microservice HTML graph.

This module never writes to SQLite, ``architecture_relations``, or MCP. It
only combines the existing bounded ``service_destination`` and
``service_transaction`` aggregate adapters with the microservice names already
held in memory by an HTML export, per ADR-16 (docs/ADR.md).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from systemlens.apm import (
    ApmError,
    ElasticApmClient,
    _aggregate_relations,
    _as_number,
    _iso8601,
    _latency_query,
    _rank_latency_buckets,
    _read_composite_buckets,
    _read_relation_buckets,
    _view_coverage,
    parse_since,
)

DEFAULT_MAX_RELATIONS = 80
DEFAULT_MAX_BUCKETS = 2_000


@dataclass(frozen=True)
class NameCorrelation:
    """The outcome of correlating one observed APM name to indexed services."""

    status: str  # "matched" | "heuristic" | "ambiguous" | "unmapped"
    node_name: str | None
    candidates: list[str] = field(default_factory=list)


def _normalize(name: str) -> str:
    normalized = name.strip().casefold()
    for separator in ("-", "_", "."):
        normalized = normalized.replace(separator, "")
    return normalized


def correlate_service_name(
    observed_name: str, indexed_names: list[str]
) -> NameCorrelation:
    """Correlate ``observed_name`` to an indexed microservice, conservatively.

    An exact, normalized match is ``matched``. Otherwise, an indexed name is a
    heuristic candidate only when one normalized name fully contains the
    other; the single highest-scoring candidate becomes ``heuristic``. A
    strict tie between two or more equally-scored candidates is ``ambiguous``
    and is never attached to the graph. No candidate at all is ``unmapped``.
    """
    normalized_observed = _normalize(observed_name)
    if not normalized_observed:
        return NameCorrelation("unmapped", None, [])
    for indexed_name in indexed_names:
        if _normalize(indexed_name) == normalized_observed:
            return NameCorrelation("matched", indexed_name, [indexed_name])
    scored: list[tuple[float, str]] = []
    for indexed_name in indexed_names:
        normalized_indexed = _normalize(indexed_name)
        if not normalized_indexed:
            continue
        if (
            normalized_observed in normalized_indexed
            or normalized_indexed in normalized_observed
        ):
            shorter = min(len(normalized_observed), len(normalized_indexed))
            longer = max(len(normalized_observed), len(normalized_indexed))
            scored.append((shorter / longer, indexed_name))
    if not scored:
        return NameCorrelation("unmapped", None, [])
    best_score = max(score for score, _ in scored)
    all_candidates = sorted({name for _, name in scored})
    best_candidates = sorted({name for score, name in scored if score == best_score})
    if len(best_candidates) == 1:
        return NameCorrelation("heuristic", best_candidates[0], all_candidates)
    return NameCorrelation("ambiguous", None, all_candidates)


def _assign_call_volume_levels(edges: dict[str, dict[str, object]]) -> None:
    """Compute a low/medium/high tercile over overlaid edges only.

    This tercile is intentionally independent from the static complexity
    tercile computed for the exported graph (see ADR-16).
    """
    volumes = sorted(
        _as_number(metrics.get("calls"))
        for metrics in edges.values()
        if isinstance(metrics.get("calls"), (int, float))
    )
    if not volumes:
        return
    population = len(volumes)
    third = max(1, population / 3)

    def level_for(value: float) -> str:
        rank = sum(1 for candidate in volumes if candidate <= value)
        if rank <= third:
            return "low"
        if rank <= 2 * third:
            return "medium"
        return "high"

    for metrics in edges.values():
        calls = metrics.get("calls")
        if isinstance(calls, (int, float)):
            metrics["call_volume_level"] = level_for(_as_number(calls))


def _merge_unresolved(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    merged: dict[tuple[str, str], dict[str, object]] = {}
    for entry in entries:
        key = (str(entry["observed_name"]), str(entry["role"]))
        existing = merged.get(key)
        if existing is None:
            merged[key] = dict(entry)
            continue
        existing_calls = existing.get("calls")
        new_calls = entry.get("calls")
        if isinstance(existing_calls, (int, float)) and isinstance(
            new_calls, (int, float)
        ):
            existing["calls"] = existing_calls + new_calls
        existing_candidates = existing.get("candidates")
        new_candidates = entry.get("candidates")
        candidates: set[str] = set(existing_candidates) if isinstance(existing_candidates, list) else set()
        if isinstance(new_candidates, list):
            candidates.update(str(candidate) for candidate in new_candidates)
        existing["candidates"] = sorted(candidates)
    return sorted(
        merged.values(),
        key=lambda item: (
            str(item["status"]),
            -_as_number(item.get("calls")),
            str(item["observed_name"]),
        ),
    )


def build_microservice_overlay(
    client: ElasticApmClient,
    *,
    since: str,
    environment: str | None,
    indexed_service_names: list[str],
    max_relations: int = DEFAULT_MAX_RELATIONS,
    max_buckets: int = DEFAULT_MAX_BUCKETS,
    now: datetime | None = None,
) -> dict[str, object]:
    """Build a bounded, in-memory ``apm-microservice-overlay-v1`` projection.

    Edge metrics (call volume, failures, error rate) come from the same
    bounded ``service_destination`` reader used by ``apm export``. Node
    latency comes from the same bounded ``service_transaction`` reader.
    Observed names are joined to ``indexed_service_names``
    only through :func:`correlate_service_name`; ambiguous and unmapped
    observations never enrich a node or edge.
    """
    if max_relations < 1:
        raise ApmError("`--max-relations` doit être supérieur à zéro.")
    if max_buckets < 1:
        raise ApmError("`--max-buckets` doit être supérieur à zéro.")
    start, end = parse_since(since, now=now)

    dependency_buckets, dependency_truncated, target_field = _read_relation_buckets(
        client, start, end, environment, max_buckets
    )
    relations = _aggregate_relations(dependency_buckets)
    relations_seen = len(relations)
    relations.sort(
        key=lambda item: (
            -_as_number(item.get("calls")),
            str(item["source"]),
            str(item["target"]),
        )
    )
    relations = relations[:max_relations]

    service_buckets, service_truncated = _read_composite_buckets(
        client, _latency_query("service", start, end, environment), max_buckets
    )
    services = _rank_latency_buckets(service_buckets, kind="service")

    unresolved: list[dict[str, object]] = []
    node_metrics: dict[str, dict[str, object]] = {}
    service_correlations = [
        (service, correlate_service_name(str(service["service"]), indexed_service_names))
        for service in services
    ]
    # Exact matches claim their node before any heuristic candidate is
    # considered, so processing order never lets a weaker guess win a node
    # over an exact identity.
    for service, correlation in service_correlations:
        if correlation.status != "matched":
            continue
        node_id = f"microservice:{correlation.node_name}"
        node_metrics[node_id] = {
            "average_ms": service.get("average_ms"),
            "p95_ms": service.get("p95_ms"),
            "calls": service.get("calls"),
            "error_rate": service.get("error_rate"),
            "match": "matched",
            "observed_name": str(service["service"]),
        }
    for service, correlation in service_correlations:
        name = str(service["service"])
        if correlation.status != "heuristic":
            continue
        node_id = f"microservice:{correlation.node_name}"
        if node_id in node_metrics:
            # The node is already claimed by an exact match; do not silently
            # overwrite it with a weaker, differently named guess.
            unresolved.append(
                {
                    "observed_name": name,
                    "role": "service",
                    "calls": service.get("calls"),
                    "error_rate": service.get("error_rate"),
                    "status": "ambiguous",
                    "candidates": [str(correlation.node_name)],
                }
            )
            continue
        node_metrics[node_id] = {
            "average_ms": service.get("average_ms"),
            "p95_ms": service.get("p95_ms"),
            "calls": service.get("calls"),
            "error_rate": service.get("error_rate"),
            "match": correlation.status,
            "observed_name": name,
        }
    for service, correlation in service_correlations:
        if correlation.status in ("matched", "heuristic"):
            continue
        unresolved.append(
            {
                "observed_name": str(service["service"]),
                "role": "service",
                "calls": service.get("calls"),
                "error_rate": service.get("error_rate"),
                "status": correlation.status,
                "candidates": correlation.candidates,
            }
        )

    edge_metrics: dict[str, dict[str, object]] = {}
    relation_correlations = [
        (
            relation,
            correlate_service_name(str(relation["source"]), indexed_service_names),
            correlate_service_name(str(relation["target"]), indexed_service_names),
        )
        for relation in relations
    ]

    def _edge_metrics(relation: dict[str, object], match: str) -> dict[str, object]:
        return {
            "calls": relation.get("calls"),
            "failure_calls": relation.get("failure_calls"),
            "error_rate": relation.get("error_rate"),
            "average_ms": relation.get("average_ms"),
            "match": match,
        }

    # Pass 1: both ends exactly matched.
    for relation, source_correlation, target_correlation in relation_correlations:
        if source_correlation.status == "matched" and target_correlation.status == "matched":
            key = (
                f"microservice:{source_correlation.node_name}"
                f"->microservice:{target_correlation.node_name}"
            )
            edge_metrics[key] = _edge_metrics(relation, "matched")
    # Pass 2: both ends resolved (matched or heuristic), edge not yet claimed.
    for relation, source_correlation, target_correlation in relation_correlations:
        source_ok = source_correlation.status in ("matched", "heuristic")
        target_ok = target_correlation.status in ("matched", "heuristic")
        if not (source_ok and target_ok):
            continue
        key = (
            f"microservice:{source_correlation.node_name}"
            f"->microservice:{target_correlation.node_name}"
        )
        if key in edge_metrics:
            continue
        edge_metrics[key] = _edge_metrics(relation, "heuristic")
    # Pass 3: whatever could not be attached to an edge is reported unresolved.
    for relation, source_correlation, target_correlation in relation_correlations:
        source_name = str(relation["source"])
        target_name = str(relation["target"])
        source_ok = source_correlation.status in ("matched", "heuristic")
        target_ok = target_correlation.status in ("matched", "heuristic")
        loser_key: str | None = (
            f"microservice:{source_correlation.node_name}"
            f"->microservice:{target_correlation.node_name}"
            if source_ok and target_ok
            else None
        )
        if source_ok and target_ok and loser_key in edge_metrics:
            continue
        if not source_ok:
            unresolved.append(
                {
                    "observed_name": source_name,
                    "role": "source",
                    "calls": relation.get("calls"),
                    "error_rate": relation.get("error_rate"),
                    "status": source_correlation.status,
                    "candidates": source_correlation.candidates,
                }
            )
        if not target_ok:
            unresolved.append(
                {
                    "observed_name": target_name,
                    "role": "destination",
                    "calls": relation.get("calls"),
                    "error_rate": relation.get("error_rate"),
                    "status": target_correlation.status,
                    "candidates": target_correlation.candidates,
                }
            )
        if source_ok and target_ok and loser_key not in edge_metrics:
            # Both ends resolve, but a stronger relation already claimed this
            # exact node pair; report the loser instead of silently dropping it.
            unresolved.append(
                {
                    "observed_name": f"{source_name}->{target_name}",
                    "role": "dependency",
                    "calls": relation.get("calls"),
                    "error_rate": relation.get("error_rate"),
                    "status": "ambiguous",
                    "candidates": [loser_key] if loser_key else [],
                }
            )

    _assign_call_volume_levels(edge_metrics)

    return {
        "schema_version": "apm-microservice-overlay-v1",
        "source": {
            "kind": "elastic_apm_metric_aggregates",
            "raw_events_exported": False,
            "service_metricset": "service_transaction",
            "dependency_metricset": "service_destination",
            "dependency_target_field": target_field,
        },
        "window": {"from": _iso8601(start), "to": _iso8601(end)},
        "environment": environment,
        "nodes": node_metrics,
        "edges": edge_metrics,
        "unresolved": _merge_unresolved(unresolved),
        "coverage": {
            "services": _view_coverage(
                len(services), len(services), max_buckets, service_truncated
            ),
            "dependencies": _view_coverage(
                relations_seen, max_relations, max_buckets, dependency_truncated
            ),
        },
    }
