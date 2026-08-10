"""Load one consistent architecture inventory for CLI and MCP queries.

The index of the current repository and a read-only workspace federation are
two transport details for the same application use case.  Keeping their
normalisation here prevents graph, audit and MCP tools from each rebuilding a
slightly different view of modules, endpoints and warnings.
"""

from dataclasses import dataclass
from pathlib import Path

from ccc_radar.graph import group_endpoints_by_module
from ccc_radar.inventory_freshness import endpoint_inventory_warning
from ccc_radar.models import Finding, MessageEndpoint
from ccc_radar.modules import DiscoveredModule, ModuleDependency
from ccc_radar.paths import db_path
from ccc_radar.store import Store
from ccc_radar.workspace import (
    dependency_federation_warning,
    discover_workspace_services,
    load_federation,
)


class ArchitectureInventoryError(RuntimeError):
    """The requested repository has not been indexed yet."""


@dataclass(frozen=True)
class ArchitectureInventory:
    """Normalised read-only facts used by architecture query use cases."""

    endpoints_by_service: dict[str, list[MessageEndpoint]]
    endpoints_by_module: dict[str, list[MessageEndpoint]]
    findings_by_service: dict[str, list[Finding]]
    endpoints: list[MessageEndpoint]
    findings: list[Finding]
    modules: list[DiscoveredModule]
    modules_by_service: dict[str, DiscoveredModule]
    module_dependencies: list[ModuleDependency]
    warnings: list[str]
    source_roots: list[Path]

def load_architecture_inventory(
    repo_root: Path,
    workspace_root: Path | None = None,
    *,
    include_runtime_services_without_endpoints: bool = False,
) -> ArchitectureInventory:
    """Load indexed facts from one repository or a federated workspace.

    ``workspace_root`` keeps the existing federation behaviour: it is a
    read-only view of separately indexed services, not a merge with
    ``repo_root``.  The optional empty runtime services are useful to exports
    that must show a deployable service even before an endpoint is detected.
    """
    repo_root = repo_root.resolve()
    if workspace_root is not None:
        workspace_root = workspace_root.resolve()
        services = discover_workspace_services(workspace_root)
        federation = load_federation(services)
        warnings = list(federation.warnings)
        if warning := dependency_federation_warning(services, federation):
            warnings.append(warning)
        endpoints_by_service = dict(federation.endpoints_by_service)
        modules_by_service = {
            service: module
            for service, module in federation.modules_by_service.items()
            if service in endpoints_by_service
        }
        if include_runtime_services_without_endpoints:
            for service in services:
                if service.kind != "microservice":
                    continue
                module = federation.modules_by_service.get(service.name)
                if module is not None:
                    endpoints_by_service.setdefault(service.name, [])
                    modules_by_service.setdefault(service.name, module)
        return ArchitectureInventory(
            endpoints_by_service=endpoints_by_service,
            endpoints_by_module=dict(federation.endpoints_by_module),
            findings_by_service=dict(federation.findings_by_service),
            endpoints=[
                endpoint
                for endpoints in federation.endpoints_by_module.values()
                for endpoint in endpoints
            ],
            findings=[
                finding
                for findings in federation.findings_by_service.values()
                for finding in findings
            ],
            modules=list(federation.modules.values()),
            modules_by_service=modules_by_service,
            module_dependencies=federation.module_dependencies,
            warnings=warnings,
            source_roots=[workspace_root, *(service.path.resolve() for service in services)],
        )

    if not db_path(repo_root).is_file():
        raise ArchitectureInventoryError("Index absent. Lancez d'abord: cccr index")
    with Store(repo_root, readonly=True) as store:
        endpoints = store.all_endpoints()
        findings = store.all_findings()
        modules = store.all_modules()
        dependencies = store.all_module_dependencies()
        warning = endpoint_inventory_warning(
            store.get_meta("endpoint_inventory_signature"),
            scope="ce projet",
            inventory_indexed=store.get_meta("endpoint_inventory_indexed") == "1",
        )
    endpoints_by_module = group_endpoints_by_module(endpoints)
    endpoints_by_service = dict(endpoints_by_module)
    modules_by_service = {
        module.name: module for module in modules if module.name in endpoints_by_service
    }
    if include_runtime_services_without_endpoints:
        for module in modules:
            if module.starts_application:
                endpoints_by_service.setdefault(module.name, [])
                modules_by_service.setdefault(module.name, module)
    findings_by_service = {
        service: [finding for finding in findings if finding.module == service]
        for service in endpoints_by_service
    }
    return ArchitectureInventory(
        endpoints_by_service=endpoints_by_service,
        endpoints_by_module=endpoints_by_module,
        findings_by_service=findings_by_service,
        endpoints=endpoints,
        findings=findings,
        modules=modules,
        modules_by_service=modules_by_service,
        module_dependencies=dependencies,
        warnings=[warning] if warning else [],
        source_roots=[repo_root],
    )
