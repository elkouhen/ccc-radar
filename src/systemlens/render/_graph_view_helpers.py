"""Low-level graph rendering helpers shared by the HTML and LikeC4 exports.

Deep-link URI building and MongoDB/REST evidence lookups are common to both
`render_graph_html` and `render_graph_likec4`; keeping a single shared module
avoids duplicating this logic across the two large renderers.
"""

import re
from pathlib import Path
from urllib.parse import quote

from systemlens.graph import GraphEdge, graph_edge_rest_resource
from systemlens.models import Finding, MessageEndpoint
from systemlens.modules import DiscoveredModule


def _rest_resources_served(endpoints: list[MessageEndpoint]) -> list[str]:
    return sorted(
        {
            endpoint.topic
            for endpoint in endpoints
            if endpoint.system == "rest" and endpoint.role == "serve"
        }
    )


def _endpoint_vscode_uri(
    endpoint: MessageEndpoint,
    modules: list[DiscoveredModule],
    source_roots: list[Path] | None,
    root_path: Path | None = None,
) -> str:
    """Build a deep link to an endpoint source location when reporting an issue."""
    module_roots = [module.path for module in modules if module.name == endpoint.module]
    roots = module_roots + list(source_roots or []) + [module.path for module in modules]
    for root in dict.fromkeys(roots):
        candidate = (root / endpoint.path).resolve()
        if candidate.is_file():
            return _vscode_file_uri(candidate, root_path, source_roots, endpoint.start_line)
    root = roots[0] if roots else Path.cwd()
    candidate = (root / endpoint.path).resolve()
    return _vscode_file_uri(candidate, root_path, source_roots, endpoint.start_line)


def _vscode_uri(finding: Finding, module: DiscoveredModule | None, source_roots: list[Path] | None, root_path: Path | None = None) -> str:
    """Build a VS Code deep link for an evidenced finding location."""
    candidates = ([module.path] if module is not None else []) + list(source_roots or [])
    for root in candidates:
        candidate = (root / finding.path).resolve()
        if candidate.is_file():
            return _vscode_file_uri(candidate, root_path, source_roots, finding.start_line)
    root = candidates[0] if candidates else Path.cwd()
    candidate = (root / finding.path).resolve()
    return _vscode_file_uri(candidate, root_path, source_roots, finding.start_line)


def _vscode_file_uri(
    path: Path,
    root_path: Path | None = None,
    source_roots: list[Path] | None = None,
    line: int | None = None,
) -> str:
    """Build a VS Code URI for a module directory or a Java source location."""
    resolved = path.resolve()
    if root_path is not None:
        for source_root in source_roots or []:
            try:
                resolved = root_path.resolve() / resolved.relative_to(source_root.resolve())
                break
            except ValueError:
                continue
    location = f":{line}" if line is not None else ""
    return f"vscode://file/{quote(resolved.as_posix(), safe='/')}{location}"


def _openapi_contract_evidence_path(endpoint: MessageEndpoint) -> str:
    """Return the physical contract path carried by a Strategy1 declaration."""
    for line in endpoint.snippet.splitlines():
        if line.startswith("systemlens-openapi-contract:"):
            return line.removeprefix("systemlens-openapi-contract:")
    return endpoint.path


def _resolve_openapi_contract_owner(
    contract_path: str,
    modules: list[DiscoveredModule],
) -> tuple[DiscoveredModule | None, str]:
    """Find which known module physically encloses a repository-relative contract.

    A Strategy1 declaration (``systemlens-openapi-contract:``) can point to a
    contract living in a *different* module than the one publishing it (for
    example a shared ``model-*`` module). Naively stripping only the
    publishing module's own directory *name* (a single path segment) from
    that evidence path is both too narrow (it silently fails for modules
    nested two or more levels below the repository root) and too permissive
    (it never notices when the evidence actually belongs to a sibling
    module). Either failure means ``openapi_contracts``, which is indexed by
    ``(owning module identity, module-relative path)``, cannot be found for
    the mis-normalized path: the contract renders as "contrat non detecte"
    and, once unioned with ``module.openapi_files``, the same physical file
    can appear a second time under an unrelated path string.

    This matches ``contract_path``'s leading segments against the trailing
    segments of each known module's directory (a purely lexical comparison,
    independent of the filesystem or of any particular repository root), and
    returns the *deepest* enclosing module together with the contract path
    expressed relative to it. Returns ``(None, contract_path)`` when no known
    module encloses the file.
    """
    path = Path(contract_path)
    if path.is_absolute():
        for module in modules:
            try:
                return module, path.relative_to(module.path).as_posix()
            except ValueError:
                continue
        return None, contract_path
    parts = path.parts
    best: tuple[DiscoveredModule, int] | None = None
    for module in modules:
        module_parts = module.path.parts
        for k in range(min(len(parts), len(module_parts)), 0, -1):
            if parts[:k] == module_parts[-k:]:
                if best is None or k > best[1]:
                    best = (module, k)
                break
    if best is not None:
        owner, k = best
        return owner, Path(*parts[k:]).as_posix()
    return None, contract_path


def _mongodb_collection_nodes(
    collections_by_service: dict[str, list[str]] | None,
) -> list[tuple[str, str, str]]:
    """Returns a distinct graph identity for each service/collection pair.

    Collection names alone are not globally unique: two microservices can both
    use `orders` in independent Mongo databases. Keeping the service in the
    node identity prevents the visual graph from inventing a shared store.
    """
    return [
        (service, collection, f"{service}:{collection}")
        for service in sorted(collections_by_service or {})
        for collection in sorted(set((collections_by_service or {})[service]))
        if collection
    ]


def _mongodb_visual_graph_edges(
    collections_by_service: dict[str, list[str]] | None,
) -> list[tuple[str, str, str, str, str, str]]:
    return [
        ("microservice", service, "mongodb_collection", identity, "stocke", "mongodb")
        for service, _collection, identity in _mongodb_collection_nodes(collections_by_service)
    ]


def _visual_graph_edges(
    edges: list[GraphEdge],
) -> list[tuple[str, str, str, str, str, str]]:
    """Projette les `GraphEdge` vers les arêtes réellement dessinées, en
    supprimant les doublons ayant la même source, destination et label.

    Retourne `(source_kind, source, target_kind, target, label, kind)`, où les
    types de nœuds évitent toute ambiguïté quand un service porte le même nom
    qu'un topic Kafka."""
    projected: dict[tuple[str, str, str, str, str], str] = {}
    order: list[tuple[str, str, str, str, str]] = []
    for edge in edges:
        visual_edges: list[tuple[str, str, str, str, str]] = []
        if edge.kind == "rest":
            label = graph_edge_rest_resource(edge)
            if edge.from_endpoint.framework == "spring-cloud-gateway":
                match = re.search(r"Path=([^;]+)", edge.from_endpoint.snippet)
                if match is not None:
                    label = f"ANY {match.group(1)}"
            visual_edges.append(
                ("microservice", edge.from_service, "microservice", edge.to_service, label)
            )
        else:
            topic = edge.from_endpoint.topic
            visual_edges.append(("microservice", edge.from_service, "kafka_topic", topic, topic))
            visual_edges.append(("kafka_topic", topic, "microservice", edge.to_service, topic))

        for source_kind, source_name, target_kind, target_name, label in visual_edges:
            key = (source_kind, source_name, target_kind, target_name, label)
            if key not in projected:
                projected[key] = edge.kind
                order.append(key)

    return [
        (*key, projected[key])
        for key in order
    ]


def _visual_link_evidence(
    kind: str, source_kind: str, source_name: str, target_kind: str, target_name: str,
    edges: list[GraphEdge],
) -> tuple[str, str]:
    """Return the confidence level and origin displayed by the HTML explorer."""
    candidates = [
        edge
        for edge in edges
        if edge.kind == kind
        and (
            (kind == "rest" and edge.from_service == source_name and edge.to_service == target_name)
            or (kind == "kafka" and source_kind == "microservice" and edge.from_service == source_name and edge.from_endpoint.topic == target_name)
            or (kind == "kafka" and target_kind == "microservice" and edge.to_service == target_name and edge.from_endpoint.topic == source_name)
        )
    ]
    if not candidates:
        return "inferred", "inference"
    endpoints = [endpoint for edge in candidates for endpoint in (edge.from_endpoint, edge.to_endpoint) if endpoint]
    if any(endpoint.framework == "kafka-topic-strategy1" for endpoint in endpoints):
        return "conventional", "Strategy1"
    if any(endpoint.framework == "openapi" for endpoint in endpoints):
        return "inferred", "OpenAPI"
    if any(endpoint.topic_dynamic for endpoint in endpoints):
        return "inferred", "code (dynamic)"
    if any(endpoint.source == "manifest" for endpoint in endpoints):
        return "proved", "manifest"
    return "proved", "code"


