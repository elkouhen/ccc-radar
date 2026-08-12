"""Fédération read-only d'un répertoire multi-services Maven/Gradle (BACKLOG-11 A2).

Découvre les services d'un répertoire parent : modules Maven (`pom.xml`) ou
microservices Gradle détectés via leur classe Spring Boot principale. Puis
lit — en lecture seule, jamais d'écriture (ADR-30) — les `.archlens/findings.db`
déjà indexés pour construire une vue fédérée
(`endpoints_by_service`/`findings_by_service`) consommable par `graph.py`.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

from archlens.inventory_freshness import endpoint_inventory_warning
from archlens.models import Finding, MessageEndpoint
from archlens.modules import DiscoveredModule, ModuleDependency, discover_modules
from archlens.paths import db_path
from archlens.store import Store, StoreError

_ItemT = TypeVar("_ItemT", Finding, MessageEndpoint)


@dataclass(frozen=True)
class DiscoveredService:
    name: str
    path: Path
    kind: str  # "microservice" | "shared-module"
    indexed: bool
    index_root: Path


@dataclass(frozen=True)
class FederationResult:
    endpoints_by_service: dict[str, list[MessageEndpoint]]
    findings_by_service: dict[str, list[Finding]]
    warnings: list[str]
    modules_by_service: dict[str, DiscoveredModule] = field(default_factory=dict)
    endpoints_by_module: dict[str, list[MessageEndpoint]] = field(default_factory=dict)
    modules: dict[str, DiscoveredModule] = field(default_factory=dict)
    module_dependencies: list[ModuleDependency] = field(default_factory=list)


def missing_indexed_microservices(
    services: list[DiscoveredService], federation: FederationResult
) -> list[str]:
    """Return runtime services whose inventory is absent from a federation.

    Endpoint facts are indexed locally, one service at a time.  Runtime
    dependencies must only be derived once every discovered microservice has
    contributed its inventory; deriving an edge from a partial federation
    would otherwise make REST and Kafka topology depend on indexing order.
    """
    return sorted({
        service.name
        for service in services
        if service.kind == "microservice" and service.name not in federation.endpoints_by_service
    })


def dependency_federation_warning(
    services: list[DiscoveredService], federation: FederationResult
) -> str | None:
    """Explain that inter-service dependencies come from a partial inventory."""
    missing = missing_indexed_microservices(services, federation)
    if not missing:
        return None
    return (
        "Dépendances inter-microservices partielles : seules les informations "
        "des services déjà indexés sont prises en compte "
        f"(manquants : {', '.join(missing)})."
    )


def _dedupe_by_id(items: list[_ItemT]) -> list[_ItemT]:
    deduped: list[_ItemT] = []
    seen: set[str] = set()
    for item in items:
        if item.id in seen:
            continue
        seen.add(item.id)
        deduped.append(item)
    return deduped


def _service_index_state(root: Path, service_dir: Path) -> tuple[bool, Path]:
    root = root.resolve()
    root_indexed = db_path(root).is_file()
    direct_indexed = db_path(service_dir).is_file()
    parent_indexed = root_indexed and service_dir != root
    indexed = direct_indexed or parent_indexed
    index_root = service_dir if direct_indexed or service_dir == root else root
    return indexed, index_root


def discover_workspace_services(root: Path) -> list[DiscoveredService]:
    """Projette les modules build sur la vue historique du graphe.

    Toute la découverte et l'analyse appartiennent à ``modules``. Un
    microservice n'est plus une entité découverte séparément : c'est un module
    dont ``starts_application`` est vrai. Cette projection conserve le contrat
    ``DiscoveredService`` consommé par le graphe et la fédération.
    """
    root = root.resolve()
    services: list[DiscoveredService] = []
    for module in discover_modules(root):
        if module.kind == "aggregator":
            continue
        indexed, index_root = _service_index_state(root, module.path)
        services.append(DiscoveredService(
            name=module.name,
            path=module.path,
            kind="microservice" if module.starts_application else "shared-module",
            indexed=indexed,
            index_root=index_root,
        ))
    return sorted(services, key=lambda service: str(service.path))


def discover_maven_services(root: Path) -> list[DiscoveredService]:
    """Compatibilité historique : alias vers `discover_workspace_services`."""
    return discover_workspace_services(root)


def load_federation(services: list[DiscoveredService]) -> FederationResult:
    """Lit, en lecture seule, les bases déjà indexées des services
    découverts. Un service non indexé, dont la base est introuvable ou
    incompatible, génère un avertissement (`warnings`) plutôt que de faire
    échouer la fédération entière (K7 CA2). Les modules partagés
    (`shared-module`) contribuent leurs findings, mais pas leurs endpoints :
    ce ne sont pas des producteurs/consommateurs runtime (A2 CA5)."""
    endpoints_by_service: dict[str, list[MessageEndpoint]] = {}
    findings_by_service: dict[str, list[Finding]] = {}
    modules_by_service: dict[str, DiscoveredModule] = {}
    endpoints_by_module: dict[str, list[MessageEndpoint]] = {}
    modules: dict[str, DiscoveredModule] = {}
    module_dependencies: set[ModuleDependency] = set()
    warnings: list[str] = []

    for service in services:
        if not service.indexed:
            warnings.append(
                f"{service.name} : non indexé, ignoré "
                "(lancez archlens index sur ce projet)."
            )
            continue
        try:
            with Store(service.index_root, readonly=True) as store:
                indexed_modules = {module.name: module for module in store.all_modules()}
                modules.update(indexed_modules)
                module_dependencies.update(store.all_module_dependencies())
                if module := indexed_modules.get(service.name):
                    modules_by_service[service.name] = module
                if service.index_root == service.path:
                    findings = store.all_findings()
                    endpoints = store.all_endpoints()
                else:
                    findings = [f for f in store.all_findings() if f.module == service.name]
                    endpoints = [e for e in store.all_endpoints() if e.module == service.name]
                findings = _dedupe_by_id(findings)
                endpoints = _dedupe_by_id(endpoints)
                findings_by_service[service.name] = findings
                endpoints_by_module[service.name] = endpoints
                if service.kind == "microservice":
                    endpoints_by_service[service.name] = endpoints
                stale_warning = endpoint_inventory_warning(
                    store.get_meta("endpoint_inventory_signature"),
                    scope=service.name,
                    inventory_indexed=store.get_meta("endpoint_inventory_indexed") == "1",
                )
                if stale_warning is not None:
                    warnings.append(stale_warning)
        except StoreError as exc:
            warnings.append(f"{service.name} : {exc}")

    return FederationResult(
        endpoints_by_service,
        findings_by_service,
        warnings,
        modules_by_service,
        endpoints_by_module,
        modules,
        sorted(module_dependencies, key=lambda dependency: (dependency.source, dependency.target)),
    )
