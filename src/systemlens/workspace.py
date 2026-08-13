"""Fédération read-only d'un répertoire multi-services Maven/Gradle (BACKLOG-11 A2).

Découvre les services d'un répertoire parent : modules Maven (`pom.xml`) ou
microservices Gradle détectés via leur classe Spring Boot principale. Puis
lit — en lecture seule, jamais d'écriture (ADR-30) — les `.systemlens/findings.db`
déjà indexés pour construire une vue fédérée
(`endpoints_by_service`/`findings_by_service`) consommable par `graph.py`.
"""

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TypeVar

from systemlens.inventory_freshness import endpoint_inventory_warning
from systemlens.models import ArchitectureRelation, Finding, MessageEndpoint
from systemlens.modules import DiscoveredModule, ModuleDependency, discover_modules, module_identity
from systemlens.paths import db_path
from systemlens.store import Store, StoreError

_ItemT = TypeVar("_ItemT", ArchitectureRelation, Finding, MessageEndpoint)


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
    relations: list[ArchitectureRelation] = field(default_factory=list)
    topic_strategies: tuple[str, ...] = ()


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


def _federated_endpoint(
    endpoint: MessageEndpoint, service: DiscoveredService, local_identity: str
) -> MessageEndpoint:
    """Namespace a directly indexed service fact in a parent federation."""
    return replace(
        endpoint,
        id=f"{service.name}:{endpoint.id}",
        module=service.name if endpoint.module == local_identity else endpoint.module,
    )


def _federated_finding(
    finding: Finding, service: DiscoveredService, local_identity: str
) -> Finding:
    return replace(
        finding,
        id=f"{service.name}:{finding.id}",
        module=service.name if finding.module == local_identity else finding.module,
    )


def _federated_relation(
    relation: ArchitectureRelation, service: DiscoveredService, local_identity: str
) -> ArchitectureRelation:
    """Retain local evidence while rewriting its service identity at the boundary."""
    return replace(
        relation,
        id=f"{service.name}:{relation.id}",
        source_name=(
            service.name
            if relation.source_kind == "microservice" and relation.source_name == local_identity
            else relation.source_name
        ),
        target_name=(
            service.name
            if relation.target_kind == "microservice" and relation.target_name == local_identity
            else relation.target_name
        ),
        module=service.name if relation.module == local_identity else relation.module,
    )


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
            name=module_identity(module),
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
    relations: list[ArchitectureRelation] = []
    topic_strategies: set[str] = set()
    warnings: list[str] = []

    for service in services:
        if not service.indexed:
            warnings.append(
                f"{service.name} : non indexé, ignoré "
                "(lancez systemlens index sur ce projet)."
            )
            continue
        try:
            with Store(service.index_root, readonly=True) as store:
                indexed_modules = {
                    module_identity(module): module for module in store.all_modules()
                }
                indexed_relations = store.all_architecture_relations()
                local_module = next(
                    (
                        module
                        for module in indexed_modules.values()
                        if module.path.resolve() == service.path.resolve()
                    ),
                    indexed_modules.get(service.name),
                )
                if service.index_root == service.path:
                    local_identity = module_identity(local_module) if local_module else service.name
                    def federated_module_identity(identity: str) -> str:
                        return service.name if identity == local_identity else f"{service.name}/{identity}"

                    module_dependencies.update(
                        ModuleDependency(
                            federated_module_identity(dependency.source),
                            federated_module_identity(dependency.target),
                        )
                        for dependency in store.all_module_dependencies()
                    )
                    modules.update({
                        federated_module_identity(identity): replace(
                            module,
                            identity=federated_module_identity(identity),
                        )
                        for identity, module in indexed_modules.items()
                    })
                    if local_module is not None:
                        modules_by_service[service.name] = replace(
                            local_module, identity=service.name
                        )
                    findings = [
                        _federated_finding(finding, service, local_identity)
                        for finding in store.all_findings()
                    ]
                    endpoints = [
                        _federated_endpoint(endpoint, service, local_identity)
                        for endpoint in store.all_endpoints()
                    ]
                    relations.extend(
                        _federated_relation(relation, service, local_identity)
                        for relation in indexed_relations
                    )
                else:
                    modules.update(indexed_modules)
                    module_dependencies.update(store.all_module_dependencies())
                    if module := indexed_modules.get(service.name):
                        modules_by_service[service.name] = module
                    findings = [f for f in store.all_findings() if f.module == service.name]
                    endpoints = [e for e in store.all_endpoints() if e.module == service.name]
                    relations.extend(
                        relation
                        for relation in indexed_relations
                        if relation.module == service.name
                        or relation.source_name == service.name
                        or relation.target_name == service.name
                    )
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
                topic_strategies.add(store.get_meta("topic_strategy") or "default")
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
        _dedupe_by_id(relations),
        tuple(sorted(topic_strategies)),
    )
