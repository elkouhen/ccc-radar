"""Rendering of the indexed namespace inventory."""

import json
from pathlib import Path

from systemlens.modules import DiscoveredModule, module_identity

_NAMESPACE_HTML_TEMPLATE = (
    Path(__file__).parent / "assets" / "namespaces.html"
).read_text(encoding="utf-8")


def project_namespace(module: DiscoveredModule, root_path: Path | None = None) -> str:
    """Return the parent-project namespace, assigning root projects to root."""
    parent = module.path.resolve().parent
    if root_path is not None and parent == root_path.resolve():
        return "root"
    return parent.name or "root"


def render_namespaces_html(
    modules: list[DiscoveredModule], root_path: Path | None = None
) -> str:
    """Render namespaces as containers containing their indexed modules."""
    by_namespace: dict[str, list[DiscoveredModule]] = {}
    for module in modules:
        namespace = project_namespace(module, root_path)
        by_namespace.setdefault(namespace, []).append(module)

    namespace_names = sorted(by_namespace)
    data = {
        "namespaces": [
            {
                "name": namespace,
                "parent": namespace.rsplit("/", 1)[0] if "/" in namespace else None,
                "modules": [
                    {
                        "id": module_identity(module),
                        "name": module.name,
                        "kind": module.kind,
                        "deployable": module.starts_application,
                    }
                    for module in sorted(
                        by_namespace[namespace], key=lambda item: module_identity(item)
                    )
                ],
            }
            for namespace in namespace_names
        ]
    }
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    return _NAMESPACE_HTML_TEMPLATE.replace("__NAMESPACE_DATA__", payload)
