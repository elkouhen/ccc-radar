import json
from pathlib import Path


def test_primary_cluster_dashboard_is_a_complete_kibana_ndjson_export() -> None:
    dashboard = Path(__file__).parents[1] / "docs" / "kibana-primary-cluster-dashboard.ndjson"
    objects = [json.loads(line) for line in dashboard.read_text(encoding="utf-8").splitlines()]

    by_id = {(item["type"], item["id"]): item for item in objects}
    data_view = by_id[("index-pattern", "systemlens-elastic-cluster-monitoring")]
    assert "metrics-elasticsearch.stack_monitoring.cluster_stats-*" in data_view["attributes"]["title"]
    assert "metrics-elasticsearch.stack_monitoring.node-*" in data_view["attributes"]["title"]

    primary = by_id[("search", "systemlens-cluster-primary-instance")]
    source = primary["attributes"]["kibanaSavedObjectMeta"]["searchSourceJSON"]
    assert "elasticsearch.node.master" in source

    exported_dashboard = by_id[("dashboard", "systemlens-primary-cluster-overview")]
    references = {(item["type"], item["id"]) for item in exported_dashboard["references"]}
    assert ("search", "systemlens-cluster-primary-instance") in references
    assert len(references) == 4
