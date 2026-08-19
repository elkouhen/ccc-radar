# Primary Elasticsearch cluster dashboard

[`kibana-primary-cluster-dashboard.ndjson`](kibana-primary-cluster-dashboard.ndjson)
is an importable Kibana 8.x saved-object export. It is intentionally a local
template: it contains no endpoint, credentials, cluster name, or data.

The dashboard targets Elastic Agent Stack Monitoring data streams:

- `metrics-elasticsearch.stack_monitoring.cluster_stats-*` for cluster status,
  node count, primary-shard count, and store size;
- `metrics-elasticsearch.stack_monitoring.node-*` for the elected primary
  Elasticsearch instance (`elasticsearch.node.master: true`).

In Kibana, open **Stack Management → Saved Objects → Import**, select the
NDJSON file, and use **Create new objects with random IDs** when importing it
into a space that might already have a SystemLens dashboard. Kibana requires
Dashboard and Saved Objects Management privileges for this operation.

The export labels the elected cluster-manager/master node as the “primary
instance”. It does not mean a primary shard: primary-shard count is shown in a
separate metric panel.

The dashboard requires Elastic Stack Monitoring data to be present. If your
deployment uses legacy `.monitoring-es-*` indices or custom field mappings,
create a data view for those sources and adapt the panel queries and fields in
Kibana after import.
