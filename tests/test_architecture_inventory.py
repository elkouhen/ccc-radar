from pathlib import Path

import pytest

from ccc_radar.architecture_inventory import (
    ArchitectureInventoryError,
    load_architecture_inventory,
)
from ccc_radar.models import MessageEndpoint, compute_endpoint_id
from ccc_radar.modules import DiscoveredModule
from ccc_radar.store import Store


def test_loads_a_single_current_indexed_inventory(tmp_path: Path) -> None:
    endpoint = MessageEndpoint(
        id=compute_endpoint_id("serve", "GET /orders", "orders/Orders.java", 10, 10),
        role="serve",
        system="rest",
        topic="GET /orders",
        topic_dynamic=False,
        source="code",
        framework="spring",
        path="orders/Orders.java",
        start_line=10,
        end_line=10,
        snippet="",
        module="orders",
    )
    module = DiscoveredModule(
        "orders", tmp_path / "orders", "maven", None, "library", True, ""
    )
    with Store(tmp_path) as store:
        store.replace_modules([module])
        store.replace_endpoints_for_files([endpoint.path], [endpoint])
        store.set_meta("endpoint_inventory_indexed", "1")

    inventory = load_architecture_inventory(tmp_path)

    assert inventory.endpoints == [endpoint]
    assert inventory.endpoints_by_service == {"orders": [endpoint]}
    assert inventory.modules_by_service == {"orders": module}
    assert inventory.source_roots == [tmp_path.resolve()]


def test_rejects_an_unindexed_repository(tmp_path: Path) -> None:
    with pytest.raises(ArchitectureInventoryError, match="Index absent"):
        load_architecture_inventory(tmp_path)
