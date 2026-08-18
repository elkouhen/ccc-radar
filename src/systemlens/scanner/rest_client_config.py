"""Découverte des configurations client REST « configured API client » (BACKLOG-16).

Détecte les classes `@Configuration` de type `Rest*Config*` (convention
Strategy1) et en extrait les domaines/services externes dépendants
(constantes majuscules `SCREAMING_SNAKE_CASE`, factories de bean typées).
Fournit aussi `_trace`/`_trace_rest_client`, les traces de debug best-effort
(activées via `SYSTEMLENS_TRACE`/`SYSTEMLENS_TRACE_REST_CLIENT`) utilisées
uniquement par ce flux d'inférence."""

from __future__ import annotations

import os
import sys
import time
from functools import lru_cache
from pathlib import Path

from systemlens import java_parser
from systemlens import maven as maven_module

def _trace(stage: str, **fields: object) -> None:
    """Émet des traces opt-in de l'inventaire REST (`SYSTEMLENS_TRACE=1`)."""
    if os.environ.get("SYSTEMLENS_TRACE") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(
        f"SYSTEMLENS_TRACE ts={time.monotonic():.6f} stage={stage} {details}".rstrip(),
        file=sys.stderr,
        flush=True,
    )
def _trace_rest_client(stage: str, **fields: object) -> None:
    """Trace exhaustive de la recherche de clients API.

    Activée séparément avec `SYSTEMLENS_TRACE_REST_CLIENTS=1`, afin d'éviter le
    volume des fichiers Java parcourus dans la trace générale `SYSTEMLENS_TRACE`.
    """
    if os.environ.get("SYSTEMLENS_TRACE_REST_CLIENTS") != "1":
        return
    details = " ".join(f"{name}={value}" for name, value in fields.items())
    print(
        f"SYSTEMLENS_TRACE_REST_CLIENTS ts={time.monotonic():.6f} stage={stage} {details}".rstrip(),
        file=sys.stderr,
        flush=True,
    )
def _is_rest_client_configuration(class_name: str | None) -> bool:
    """Whether a Java configuration name follows the ``Rest*Config*`` convention."""
    return class_name is not None and class_name.startswith("Rest") and "Config" in class_name
def _is_uppercase_underscore_constant(name: str) -> bool:
    return (
        "_" in name
        and name[0].isupper()
        and all(character.isupper() or character.isdigit() or character == "_" for character in name)
    )
def _rest_configuration_domains(type_node, source: bytes) -> list[tuple[str, int]]:
    """Return uppercase constants of a REST configuration from its Java AST.

    Only identifier nodes are considered.  A constant mentioned in a comment,
    a string literal or an annotation value therefore cannot create a false
    microservice dependency.
    """
    domains = {
        (name.lower().replace("_", "-"), node.start_point.row + 1)
        for node in java_parser.walk(type_node)
        if node.type in {"identifier", "field_identifier"}
        if _is_uppercase_underscore_constant(name := java_parser.node_text(source, node))
    }
    return sorted(domains)
def _rest_configuration_external_services(type_node, source: bytes) -> list[tuple[str, int]]:
    """Return external services referenced through ``getRest().get(...)``.

    Strategy1 treats ``getRest().get("partner")`` in a ``Rest*Config*`` class
    as an explicit HTTP dependency on the external microservice ``partner``.
    The AST shape is checked end-to-end so a matching string in a comment, an
    unrelated ``get`` chain, or another type declaration cannot manufacture an
    architecture relation.
    """
    services: set[tuple[str, int]] = set()
    for invocation in java_parser.walk(type_node):
        if invocation.type != "method_invocation":
            continue
        if java_parser.enclosing(invocation, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration") != type_node:
            continue
        receiver, method_name, arguments = java_parser.invocation_parts(invocation, source)
        if method_name != "get" or len(arguments) != 1 or receiver is None:
            continue
        service = _normalize_api_domain(java_parser.string_value(arguments[0], source) or "")
        if service is None or receiver.type != "method_invocation":
            continue
        rest_receiver, rest_method, rest_arguments = java_parser.invocation_parts(receiver, source)
        if rest_method != "getRest" or rest_arguments:
            continue
        services.add((service, invocation.start_point.row + 1))
    return sorted(services)
def _is_configured_api_client_factory(method_name: str) -> bool:
    """Whether a helper name follows the ``create*ClientApi`` convention."""
    return method_name.startswith("create") and method_name.endswith("ClientApi")
def _simple_java_type(value: str) -> str:
    """Nom simple d'un type Java, sans génériques ni tableau."""
    value = value.strip().rsplit(".", 1)[-1]
    return value.split("<", 1)[0].strip().removesuffix("[]")
def _normalize_api_domain(value: str) -> str | None:
    """Normalise une valeur de domaine vers le nom de microservice attendu."""
    if (
        not value
        or not value[0].isalpha()
        or any(not (character.isalnum() or character in {"_", "-"}) for character in value)
    ):
        return None
    return value.lower().replace("_", "-")
def _api_domain_from_key_invocation(node, source: bytes) -> str | None:
    """Extrait ``DOMAIN_FOO`` de l'expression ``DOMAIN_FOO.getKey()``."""
    if node.type != "method_invocation":
        return None
    receiver, method_name, args = java_parser.invocation_parts(node, source)
    if method_name != "getKey" or args or receiver is None:
        return None
    receiver_text = java_parser.node_text(source, receiver)
    return _normalize_api_domain(receiver_text.rsplit(".", 1)[-1])
def _api_domain_argument(node, source: bytes) -> str | None:
    """Extrait le domaine logique donné à un factory de client HTTP.

    Outre un littéral et la constante terminale de ``XXX.NAME``, la
    convention ``getUriPath(..., DOMAIN_FOO.getKey())`` est acceptée. Les URLs
    et chemins restent exclus : ils ne désignent pas le microservice cible.
    """
    literal = java_parser.string_value(node, source)
    if literal is not None:
        return _normalize_api_domain(literal)

    if node.type == "method_invocation":
        receiver, method_name, args = java_parser.invocation_parts(node, source)
        if method_name == "getKey" and not args and receiver is not None:
            receiver_text = java_parser.node_text(source, receiver)
            return _normalize_api_domain(receiver_text.rsplit(".", 1)[-1])
        if method_name == "getUriPath":
            domains = {
                domain
                for argument in args
                if (domain := _api_domain_from_key_invocation(argument, source)) is not None
            }
            return next(iter(domains)) if len(domains) == 1 else None

    if node.type not in {"identifier", "field_access", "scoped_identifier"}:
        return None
    text = java_parser.node_text(source, node)
    name = text.rsplit(".", 1)[-1]
    return _normalize_api_domain(name)
def _bean_api_domain(method_node, source: bytes, microservice: str) -> str | None:
    """Domaine d'un appel `create*ClientApi(...)` dans un bean.

    La convention applicative est précise : le premier argument est une
    constante de domaine (`YYY.DOMAIN_ANNUAIRE`) et le second l'interface
    d'API. La constante devient `domain-annuaire`, qui correspond au nom du
    microservice. Plusieurs appels de ce type dans un même bean sont ambigus.
    """
    domains: set[str] = set()
    for invocation in java_parser.walk(method_node):
        if invocation.type != "method_invocation":
            continue
        _, method_name, args = java_parser.invocation_parts(invocation, source)
        if not _is_configured_api_client_factory(method_name) or not args:
            continue
        _trace_rest_client(
            "rest_client.search.helper",
            microservice=microservice,
            helper=method_name,
            first_argument=java_parser.node_text(source, args[0]),
            argument_count=len(args),
        )
        domain = _api_domain_argument(args[0], source)
        if domain is not None:
            domains.add(domain)
            _trace_rest_client(
                "rest_client.search.domain", microservice=microservice, domain=domain
            )
        else:
            _trace_rest_client(
                "rest_client.search.domain_ignored",
                microservice=microservice,
                argument=java_parser.node_text(source, args[0]),
            )
    if len(domains) == 1:
        return next(iter(domains))
    if domains:
        _trace_rest_client(
            "rest_client.search.domain_ambiguous",
            microservice=microservice,
            domains=sorted(domains),
        )
    return None
def _rest_client_microservice_name(module_root: Path) -> str:
    """Nom du microservice porté par le POM, ou nom du répertoire en repli."""
    pom_path = module_root / "pom.xml"
    if pom_path.is_file():
        artifact_id, _, _ = maven_module.parse_pom(pom_path)
        if artifact_id:
            return artifact_id
    return module_root.name
def _rest_configuration_module_root(repo_root: Path, source_path: str) -> Path:
    """Racine du microservice contenant `source_path`."""
    caller_path = repo_root / source_path
    for parent in (caller_path.parent, *caller_path.parents):
        if parent == repo_root.parent:
            break
        if (parent / "pom.xml").is_file() or (parent / "build.gradle").is_file() or (parent / "build.gradle.kts").is_file():
            _trace_rest_client(
                "rest_client.search.module",
                caller=source_path,
                module=parent.relative_to(repo_root),
                microservice=_rest_client_microservice_name(parent),
            )
            return parent
    _trace_rest_client(
        "rest_client.search.module_fallback",
        caller=source_path,
        module=".",
        microservice=_rest_client_microservice_name(repo_root),
    )
    return repo_root


@lru_cache(maxsize=512)
def _rest_configuration_client_domains_in_module(
    repo_root_str: str, module_root_rel: str
) -> tuple[tuple[str, str, str], ...]:
    """Retourne `(type_client, nom_bean, domaine)` d'un microservice.

    Le `pom.xml` détermine la frontière et le nom (`artifactId`) du
    microservice Maven. Ainsi, chaque configuration `Rest*Config*` n'est parcourue qu'une fois
    pour ce microservice, sans mélanger les services voisins d'un workspace.
    """
    repo_root = Path(repo_root_str)
    module_root = repo_root / module_root_rel
    service_name = _rest_client_microservice_name(module_root)

    _trace(
        "rest_client.configuration.scan.begin",
        microservice=service_name,
        module=module_root_rel,
    )
    _trace_rest_client(
        "rest_client.search.scan_begin",
        microservice=service_name,
        module=module_root_rel,
    )
    clients: list[tuple[str, str, str]] = []
    for candidate in module_root.rglob("*.java"):
        candidate_rel = candidate.relative_to(repo_root).as_posix()
        _trace_rest_client(
            "rest_client.search.source", microservice=service_name, path=candidate_rel
        )
        parsed = java_parser.parse_java(
            repo_root_str, candidate_rel
        )
        if parsed is None:
            _trace_rest_client(
                "rest_client.search.source_unparsed",
                microservice=service_name,
                path=candidate_rel,
            )
            continue
        source, root = parsed
        for type_node in java_parser.type_declarations(root):
            if not _is_rest_client_configuration(
                java_parser.declaration_name(type_node, source)
            ):
                continue
            _trace_rest_client(
                "rest_client.search.configuration",
                microservice=service_name,
                path=candidate_rel,
            )
            for method_node in java_parser.walk(type_node):
                if method_node.type != "method_declaration":
                    continue
                name_node = method_node.child_by_field_name("name")
                method_name = java_parser.node_text(source, name_node) if name_node is not None else "<anonymous>"
                if not any(
                    java_parser.annotation_name(annotation, source) == "Bean"
                    for annotation in java_parser.annotations_of(method_node)
                ):
                    continue
                _trace_rest_client(
                    "rest_client.search.bean",
                    microservice=service_name,
                    path=candidate_rel,
                    bean=method_name,
                )
                type_node_return = method_node.child_by_field_name("type")
                if type_node_return is None or name_node is None:
                    _trace_rest_client(
                        "rest_client.search.bean_ignored",
                        microservice=service_name,
                        path=candidate_rel,
                        bean=method_name,
                        reason="missing_type_or_name",
                    )
                    continue
                domain = _bean_api_domain(method_node, source, service_name)
                if domain is None:
                    _trace_rest_client(
                        "rest_client.search.bean_ignored",
                        microservice=service_name,
                        path=candidate_rel,
                        bean=method_name,
                        reason="no_unique_create_client_api_domain",
                    )
                    continue
                clients.append(
                    (
                        _simple_java_type(java_parser.node_text(source, type_node_return)),
                        java_parser.node_text(source, name_node),
                        domain,
                    )
                )
                _trace(
                    "rest_client.configuration.bean",
                    configuration=candidate.relative_to(repo_root),
                    bean=java_parser.node_text(source, name_node),
                    api_type=_simple_java_type(java_parser.node_text(source, type_node_return)),
                    domain=domain,
                )
                _trace_rest_client(
                    "rest_client.search.bean_registered",
                    microservice=service_name,
                    bean=java_parser.node_text(source, name_node),
                    api_type=_simple_java_type(java_parser.node_text(source, type_node_return)),
                    domain=domain,
                )
    _trace(
        "rest_client.configuration.scan.end",
        microservice=service_name,
        clients=len(clients),
    )
    _trace_rest_client(
        "rest_client.search.scan_end", microservice=service_name, clients=len(clients)
    )
    return tuple(clients)
def _rest_configuration_client_domains(
    repo_root_str: str, source_path: str
) -> tuple[tuple[str, str, str], ...]:
    """Clients configurés du microservice qui contient `source_path`."""
    repo_root = Path(repo_root_str)
    module_root = _rest_configuration_module_root(repo_root, source_path)
    return _rest_configuration_client_domains_in_module(
        repo_root_str, module_root.relative_to(repo_root).as_posix()
    )
def discover_rest_api_client_configurations(repo_root: Path) -> None:
    """Parcourt proactivement les configurations clients de chaque module.

    Cette phase précède l'analyse des appels : les interfaces d'API générées
    ne donnent pas toujours un résultat REST exploitable, mais leur
    toute configuration `Rest*Config*` doit tout de même être cherchée dans chaque
    microservice Maven du workspace.
    """
    module_roots = sorted({pom_path.parent for pom_path in repo_root.rglob("pom.xml")})
    if not module_roots:
        module_roots = [repo_root]
    _trace_rest_client(
        "rest_client.search.workspace_begin", modules=len(module_roots), root=repo_root
    )
    for module_root in module_roots:
        module_root_rel = module_root.relative_to(repo_root).as_posix()
        _trace_rest_client(
            "rest_client.search.workspace_module",
            microservice=_rest_client_microservice_name(module_root),
            module=module_root_rel,
        )
        _rest_configuration_client_domains_in_module(str(repo_root), module_root_rel)
    _trace_rest_client("rest_client.search.workspace_end", modules=len(module_roots))
