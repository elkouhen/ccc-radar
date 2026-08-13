"""Snapshot-only view-model builders shared by architecture renderers."""

from __future__ import annotations

from systemlens.models import MessageEndpoint


def kafka_dto_views(
    endpoints_by_service: dict[str, list[MessageEndpoint]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Build the Kafka DTO view from indexed endpoint facts only."""
    by_type: dict[str, list[tuple[str, MessageEndpoint]]] = {}
    for service, endpoints in endpoints_by_service.items():
        for endpoint in endpoints:
            if endpoint.system == "kafka" and endpoint.message_type:
                by_type.setdefault(endpoint.message_type, []).append((service, endpoint))
    return (
        [
            {
                "id": message_type,
                "name": message_type.rsplit(".", 1)[-1],
                "qualified_name": message_type if "." in message_type else None,
                "fields": [],
                "source": None,
                "producers": sorted({service for service, endpoint in matches if endpoint.role == "produce"}),
                "consumers": sorted({service for service, endpoint in matches if endpoint.role == "consume"}),
                "topics": sorted({endpoint.topic for _service, endpoint in matches}),
            }
            for message_type, matches in sorted(by_type.items())
        ],
        [],
    )
