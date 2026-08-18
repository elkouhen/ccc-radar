"""Rendering layer: turns indexed facts into CLI text/JSON and HTML/LikeC4 exports.

This package used to be a single 5000+ line ``render.py`` module. It is now
split by rendering concern (see each submodule's docstring), but every name
that used to be importable as ``systemlens.render.<name>`` remains importable
from here unchanged — this ``__init__`` is the single public surface other
modules (``cli``, ``mcp_server``, ``indexer``, tests) depend on.
"""

from systemlens.render.search import (
    FindingHit,
    FindingsSummary,
    RuleCount,
    render_search_json,
    render_search_text,
    render_summary_json,
    render_summary_text,
)
from systemlens.render.graph_json import (
    GraphEdgeInfo,
    GraphNodeInfo,
    GraphResult,
    GraphSite,
    OutboundCallHit,
    render_graph_json,
    render_graph_text,
)
from systemlens.render.html_export import _live_kafka_dto_views, render_graph_html
from systemlens.render.likec4_export import (
    ComplexityRanking,
    render_graph_likec4,
    render_request_reply_html,
)
from systemlens.render.module_graph import (
    EndpointHit,
    FlowResultInfo,
    FlowSiteInfo,
    ModuleDetail,
    ModuleGraphDependency,
    ModuleGraphResult,
    ModuleSummary,
    WorkspaceResult,
    WorkspaceServiceInfo,
    render_endpoints_json,
    render_endpoints_text,
    render_flow_json,
    render_flow_text,
    render_module_detail_json,
    render_module_detail_text,
    render_module_graph_html,
    render_module_graph_json,
    render_module_graph_text,
    render_modules_list_json,
    render_modules_list_text,
    render_workspace_json,
    render_workspace_text,
)
# Kept importable for the test suite, which exercises this internal helper directly.
from systemlens.render._graph_view_helpers import _vscode_file_uri

__all__ = [
    "FindingHit",
    "FindingsSummary",
    "RuleCount",
    "render_search_json",
    "render_search_text",
    "render_summary_json",
    "render_summary_text",
    "GraphEdgeInfo",
    "GraphNodeInfo",
    "GraphResult",
    "GraphSite",
    "OutboundCallHit",
    "render_graph_json",
    "render_graph_text",
    "render_graph_html",
    "_live_kafka_dto_views",
    "ComplexityRanking",
    "render_graph_likec4",
    "render_request_reply_html",
    "EndpointHit",
    "FlowResultInfo",
    "FlowSiteInfo",
    "ModuleDetail",
    "ModuleGraphDependency",
    "ModuleGraphResult",
    "ModuleSummary",
    "WorkspaceResult",
    "WorkspaceServiceInfo",
    "render_endpoints_json",
    "render_endpoints_text",
    "render_flow_json",
    "render_flow_text",
    "render_module_detail_json",
    "render_module_detail_text",
    "render_module_graph_html",
    "render_module_graph_json",
    "render_module_graph_text",
    "render_modules_list_json",
    "render_modules_list_text",
    "render_workspace_json",
    "render_workspace_text",
    "_vscode_file_uri",
]
