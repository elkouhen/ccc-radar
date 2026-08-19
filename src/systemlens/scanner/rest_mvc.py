"""Inférence des endpoints REST (MVC, Feign, Spring Data REST, OpenAPI,
Gateway, RestTemplate/RestClient, WebClient, WebFlux) (BACKLOG-16).

Point d'entrée public : `infer_framework_endpoints`, l'orchestrateur qui
agrège toutes les stratégies de détection REST pour un module donné."""

from __future__ import annotations

import json
import re
from dataclasses import replace
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from systemlens import java_parser
from systemlens import maven as maven_module
from systemlens.models import MessageEndpoint, compute_endpoint_id
from systemlens.modules import discover_rest_controllers, maven_module_dependencies
from systemlens.scanner._core import (
    _build_endpoint,
    _find_first_literal,
    _invocation_receiver,
    _module_for_path,
    _read_snippet,
)
from systemlens.scanner._spring_properties import (
    _load_flat_spring_properties,
    _resolve_value_annotated_variable,
    resolve_spring_property,
)
from systemlens.scanner.rest_client_config import (
    _is_rest_client_configuration,
    _rest_configuration_domains,
    _rest_configuration_external_services,
    _trace_rest_client,
)

_PROPERTY_PLACEHOLDER_RE = re.compile(r"^\$\{([^}]+)\}$")
_MULTI_SLASH_RE = re.compile(r"/{2,}")

# BACKLOG-10 K2 (reliquat) : `@KafkaListener(topics = someVar)` ou
# `kafkaTemplate.send(someVar, ...)` où `someVar` n'est pas un littéral mais
# une variable alimentée ailleurs dans la classe par `@Value("${...}")` —
# retrouver le nom de variable en jeu (pas son contenu, absent du snippet)
# avant de la résoudre contre les champs `@Value` du fichier source.
_MAPPING_ANNOTATION_RE = re.compile(r"@\w+Mapping\s*(?:\(([^)]*)\))?")
_MAPPING_ANNOTATION_BLOCK_RE = re.compile(r"@\w+Mapping\s*(?:\((.*?)\))?", re.DOTALL)
_REQUEST_MAPPING_BLOCK_RE = re.compile(r"@RequestMapping\s*(?:\((.*?)\))?", re.DOTALL)
_REQUEST_PARAM_RE = re.compile(
    r"@RequestParam\s*(?:\((.*?)\))?\s+[\w<>\[\], ?]+\s+(\w+)", re.DOTALL
)
_NON_PATH_MAPPING_ATTRS = {"method", "produces", "consumes", "headers", "params", "name"}
_FEIGN_CLIENT_RE = re.compile(r"@FeignClient\s*\((.*?)\)", re.DOTALL)
_NAMED_STRING_ARG_RE = re.compile(r'(\w+)\s*=\s*"([^"]*)"')
_OPENAPI_HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})
_REST_TEMPLATE_CALL_RE = re.compile(
    r"\.(getForObject|getForEntity|postForObject|postForEntity|put|delete)\(\s*(.+?)\s*(?:,|\))",
    re.DOTALL,
)
_REST_TEMPLATE_EXCHANGE_RE = re.compile(
    r"\.exchange\(\s*(.+?)\s*,\s*(?:HttpMethod\.)?([A-Z]+)\s*,",
    re.DOTALL,
)
_URI_CALL_RE = re.compile(r"\.uri\s*\(")
_GATEWAY_ROUTE_PATH_RE = re.compile(r'\.path\(\s*"([^"]+)"\s*\)')
_GATEWAY_ROUTE_METHOD_RE = re.compile(r'\.method\(\s*(?:"([A-Z]+)"|HttpMethod\.([A-Z]+))\s*\)')
_GATEWAY_ROUTE_URI_RE = re.compile(r"\.uri\(\s*([^)]+?)\s*\)", re.DOTALL)
_ROUTER_FUNCTION_ROUTE_RE = re.compile(
    r"(?:RouterFunctions\.)?route\(\s*(?:RequestPredicates\.)?([A-Z]+)\(\s*\"([^\"]+)\"\s*\)",
    re.DOTALL,
)
_ROUTER_FUNCTION_AND_ROUTE_RE = re.compile(
    r"\.andRoute\(\s*(?:RequestPredicates\.)?([A-Z]+)\(\s*\"([^\"]+)\"\s*\)",
    re.DOTALL,
)
def _mapping_args_have_only_non_path_attrs(args: str) -> bool:
    """`True` si les arguments d'une annotation `@XMapping(...)` ne portent
    aucun chemin explicite (vide, ou uniquement `method=`/`produces=`/...) —
    dans ce cas le chemin effectif de la méthode est vide (hérite du préfixe
    de classe), pas inconnu."""
    args = args.strip()
    if not args:
        return True
    for part in args.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            return False  # argument positionnel : c'est le chemin lui-même
        key = part.split("=", 1)[0].strip()
        if key not in _NON_PATH_MAPPING_ATTRS:
            return False
    return True
def _next_declaration_line(lines: list[str], start_idx: int) -> int | None:
    for idx in range(start_idx, len(lines)):
        stripped = lines[idx].strip()
        if not stripped or stripped.startswith("@"):
            continue
        return idx
    return None
def _named_string_arg(args: str, key: str) -> str | None:
    for match in _NAMED_STRING_ARG_RE.finditer(args):
        if match.group(1) == key:
            return match.group(2)
    return None
def _split_java_concat(expr: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    escaped = False
    for ch in expr:
        if quote is not None:
            current.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in ('"', "'"):
            quote = ch
            current.append(ch)
            continue
        if ch == "+":
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts
def _resolve_rest_path_expression(
    expr: str, repo_root: Path, source_path: str, *, preserve_dynamic_segments: bool = False
) -> tuple[str, bool]:
    resolved_parts: list[str] = []
    dynamic = False
    for part in _split_java_concat(expr.strip()):
        if len(part) >= 2 and part[0] == part[-1] == '"':
            literal = part[1:-1]
            placeholder = _PROPERTY_PLACEHOLDER_RE.match(literal)
            if placeholder is not None:
                resolved = resolve_spring_property(repo_root, placeholder.group(1), source_path)
                if resolved is None:
                    dynamic = True
                    continue
                resolved_parts.append(resolved)
                continue
            resolved_parts.append(literal)
            continue
        if re.fullmatch(r"[A-Za-z_]\w*", part):
            resolved = _resolve_value_annotated_variable(repo_root, source_path, part)
            if resolved is None:
                dynamic = True
                if preserve_dynamic_segments and resolved_parts:
                    resolved_parts.append(f"{{{part}}}")
                continue
            resolved_parts.append(resolved)
            continue
        dynamic = True
        if preserve_dynamic_segments and resolved_parts:
            resolved_parts.append("{dynamic}")
    raw = "".join(resolved_parts).strip()
    if not raw:
        return "<dynamic>", True
    return _normalize_rest_path(raw), dynamic


@lru_cache(maxsize=1024)
def _class_base_path(repo_root_str: str, source_path: str, start_line: int) -> tuple[str, bool]:
    """Chemin `@RequestMapping` de la classe/interface qui englobe la
    méthode trouvée à `start_line` — renvoie (préfixe, dynamique) ;
    (`""`, `False`) si aucune classe englobante ou aucun `@RequestMapping`
    de classe (rien à préfixer, pas une valeur inconnue)."""
    parsed = java_parser.parse_java(repo_root_str, source_path)
    if parsed is None:
        return "", False
    source, root = parsed
    method = next(
        (
            node for node in java_parser.walk(root)
            if node.type == "method_declaration"
            and any(
                ann.start_point.row + 1 <= start_line <= node.end_point.row + 1
                for ann in java_parser.annotations_of(node)
            )
        ),
        None,
    )
    if method is None:
        return "", False
    owner = java_parser.enclosing(
        method, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration"
    )
    if owner is None:
        return "", False
    mapping = next(
        (ann for ann in java_parser.annotations_of(owner)
         if java_parser.annotation_name(ann, source) == "RequestMapping"),
        None,
    )
    if mapping is not None:
        return _ast_mapping_value(mapping, source, Path(repo_root_str), source_path)
    feign = next(
        (ann for ann in java_parser.annotations_of(owner)
         if java_parser.annotation_name(ann, source) == "FeignClient"),
        None,
    )
    if feign is not None:
        return _ast_feign_base(feign, source, Path(repo_root_str), source_path)
    return "", False
def _extract_rest_path(
    snippet: str,
    repo_root: Path | None = None,
    source_path: str | None = None,
    start_line: int | None = None,
) -> tuple[str, bool]:
    """Renvoie (chemin, dynamique) — jamais résolu silencieusement (même
    esprit que `topic_dynamic` en K2). Fusionne le préfixe `@RequestMapping`
    de la classe englobante (Q24) quand `repo_root`/`source_path`/
    `start_line` sont fournis."""
    prefix, prefix_dynamic = "", False
    if repo_root is not None and source_path is not None and start_line is not None:
        prefix, prefix_dynamic = _class_base_path(str(repo_root), source_path, start_line)

    lines = snippet.splitlines()
    decl_idx = _next_declaration_line(lines, 0)
    annotation_block = "\n".join(lines[:decl_idx]) if decl_idx is not None else snippet
    match = _MAPPING_ANNOTATION_BLOCK_RE.search(annotation_block)
    mapping_args = (match.group(1) or "") if match is not None else None

    if mapping_args is not None:
        literal, method_dynamic = _find_first_literal(mapping_args)
        if literal is None:
            if not _mapping_args_have_only_non_path_attrs(mapping_args or ""):
                return "<dynamic>", True
            literal, method_dynamic = "", False
    else:
        literal, method_dynamic = _find_first_literal(snippet)
        if literal is None:
            return "<dynamic>", True

    if prefix_dynamic:
        return "<dynamic>", True

    method_path = _normalize_rest_path(literal) if literal else "/"
    route = method_path if not prefix else _join_rest_paths(_normalize_rest_path(prefix), method_path)
    query_params = _extract_request_param_names(snippet)
    if query_params:
        route = _with_query_params(route, query_params)
    return route, method_dynamic
def _extract_request_param_names(snippet: str) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for match in _REQUEST_PARAM_RE.finditer(snippet):
        args = match.group(1) or ""
        name = (
            _named_string_arg(args, "name")
            or _named_string_arg(args, "value")
            or _find_first_literal(args)[0]
            or match.group(2)
        )
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names
def _with_query_params(route: str, params: list[str]) -> str:
    if not params or route == "<dynamic>":
        return route
    return f"{route}?{'&'.join(params)}"
def _join_rest_paths(prefix: str, suffix: str) -> str:
    """Assemble deux chemins déjà normalisés (slash de tête unique) sans
    jamais repasser par l'heuristique d'URL protocole-relatif de
    `_normalize_rest_path` : une simple concaténation `"" + "/" +
    "/orders/{id}"` produit `"//orders/{id}"`, que `urlsplit` interprète à
    tort comme `http://orders/{id}` (`orders` avalé comme nom d'hôte)."""
    segments = [s for s in (prefix.strip("/"), suffix.strip("/")) if s]
    return "/" + "/".join(segments) if segments else "/"
def _normalize_rest_path(literal: str) -> str:
    normalized = literal.strip()
    if not normalized:
        return "/"
    if normalized.startswith("//"):
        normalized = urlsplit(f"http:{normalized}").path or "/"
    elif "://" in normalized:
        normalized = urlsplit(normalized).path or "/"
    normalized = normalized.split("?", 1)[0].split("#", 1)[0]
    normalized = _MULTI_SLASH_RE.sub("/", normalized)
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return normalized or "/"
def _infer_generic_request_mapping_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Infère les annotations de routes Spring et Feign depuis l'AST Java.

    Les anciens parcours ligne par ligne échouaient dès qu'une annotation ou
    une signature était répartie sur plusieurs lignes, ou qu'un commentaire
    imitait une annotation. Ici Tree-sitter associe directement l'annotation
    à sa ``method_declaration``.
    """
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    endpoints: list[MessageEndpoint] = []
    for method in java_parser.walk(root):
        if method.type != "method_declaration":
            continue
        owner = java_parser.enclosing(
            method, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration"
        )
        if owner is None:
            continue
        feign = next(
            (ann for ann in java_parser.annotations_of(owner)
             if java_parser.annotation_name(ann, source) == "FeignClient"),
            None,
        )
        for annotation in java_parser.annotations_of(method):
            methods = _ast_mapping_methods(annotation, source)
            if not methods:
                continue
            route, dynamic = _ast_mapping_route(
                annotation, method, source, repo_root, rel_path, feign
            )
            for http_method in methods:
                endpoints.append(
                    _build_endpoint(
                        repo_root, rel_path, annotation.start_point.row + 1,
                        method.end_point.row + 1,
                        "call" if feign is not None else "serve", "rest",
                        f"{http_method} {route}", "feign" if feign is not None else "spring",
                        java_parser.node_text(source, method), topic_dynamic=dynamic,
                    )
                )
    return endpoints
def _ast_mapping_value(
    annotation, source: bytes, repo_root: Path | None = None, rel_path: str | None = None
) -> tuple[str, bool]:
    """Return a mapping annotation's explicit path, if it has one.

    An annotation with only non-path attributes has the meaningful empty path
    (it inherits its class route); an expression that cannot be resolved is
    dynamic.
    """
    value = (
        java_parser.annotation_argument(annotation, source, key="path")
        or java_parser.annotation_argument(annotation, source, key="value")
        or java_parser.annotation_argument(annotation, source)
    )
    if value is None:
        return "", False
    if value.type != "string_literal":
        return "", True
    if repo_root is not None and rel_path is not None:
        return _resolve_rest_path_expression(
            java_parser.node_text(source, value), repo_root, rel_path, preserve_dynamic_segments=True
        )
    literal = java_parser.string_value(value, source)
    return literal or "", False
def _ast_mapping_methods(annotation, source: bytes) -> list[str]:
    """HTTP methods represented by a Spring mapping annotation."""
    name = java_parser.annotation_name(annotation, source)
    dedicated = {
        "GetMapping": "GET", "PostMapping": "POST", "PutMapping": "PUT",
        "DeleteMapping": "DELETE", "PatchMapping": "PATCH",
    }
    if name in dedicated:
        return [dedicated[name]]
    if name != "RequestMapping":
        return []
    value = java_parser.annotation_argument(annotation, source, key="method")
    if value is None:
        return ["ANY"]
    methods: list[str] = []
    for node in java_parser.walk(value):
        if node.type in {"identifier", "scoped_identifier", "field_access"}:
            candidate = java_parser.node_text(source, node).rsplit(".", 1)[-1]
            if candidate in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
                methods.append(candidate)
    return list(dict.fromkeys(methods))
def _ast_mapping_route(
    annotation, method, source: bytes, repo_root: Path, rel_path: str, feign=None
) -> tuple[str, bool]:
    path, dynamic = _ast_mapping_value(annotation, source, repo_root, rel_path)
    owner = java_parser.enclosing(
        method, "class_declaration", "interface_declaration", "record_declaration", "enum_declaration"
    )
    prefix, prefix_dynamic = "", False
    if owner is not None:
        class_mapping = next(
            (
                ann for ann in java_parser.annotations_of(owner)
                if java_parser.annotation_name(ann, source) == "RequestMapping"
            ),
            None,
        )
        if class_mapping is not None:
            prefix, prefix_dynamic = _ast_mapping_value(class_mapping, source, repo_root, rel_path)
        elif feign is not None:
            prefix, prefix_dynamic = _ast_feign_base(feign, source, repo_root, rel_path)
    if dynamic or prefix_dynamic:
        return "<dynamic>", True
    route = _join_rest_paths(_normalize_rest_path(prefix), _normalize_rest_path(path))
    params = _ast_request_param_names(method, source)
    return _with_query_params(route, params), False
def _ast_feign_base(annotation, source: bytes, repo_root: Path, rel_path: str) -> tuple[str, bool]:
    """Resolve Feign's optional URL/path base without treating ``name`` as a route."""
    value = (
        java_parser.annotation_argument(annotation, source, key="url")
        or java_parser.annotation_argument(annotation, source, key="path")
    )
    if value is None:
        return "", False
    if value.type != "string_literal":
        return "", True
    return _resolve_rest_path_expression(
        java_parser.node_text(source, value), repo_root, rel_path, preserve_dynamic_segments=True
    )
def _ast_request_param_names(method, source: bytes) -> list[str]:
    names: list[str] = []
    params = method.child_by_field_name("parameters")
    if params is None:
        return names
    for parameter in params.children:
        if parameter.type != "formal_parameter":
            continue
        annotation = next(
            (
                ann for ann in java_parser.annotations_of(parameter)
                if java_parser.annotation_name(ann, source) == "RequestParam"
            ),
            None,
        )
        if annotation is None:
            continue
        value = (
            java_parser.annotation_argument(annotation, source, key="name")
            or java_parser.annotation_argument(annotation, source, key="value")
            or java_parser.annotation_argument(annotation, source)
        )
        name = java_parser.string_value(value, source)
        if name is None:
            name_node = parameter.child_by_field_name("name")
            name = java_parser.node_text(source, name_node) if name_node is not None else None
        if name and name not in names:
            names.append(name)
    return names
def _simple_type_name(qualified: str) -> str:
    """`a.b.Foo<Bar>` -> `Foo`."""
    cleaned = qualified.strip().split("<", 1)[0]
    return cleaned.rsplit(".", 1)[-1]
def _pluralize(word: str) -> str:
    """Pluralisation anglaise best-effort, alignée sur Spring Data REST."""
    word = word.strip()
    if not word:
        return word
    if word.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    if word.endswith("y") and len(word) > 1 and word[-2].lower() not in "aeiou":
        return word[:-1] + "ies"
    return word + "s"


@lru_cache(maxsize=256)
def _module_has_spring_data_rest(repo_root_str: str, rel_path: str) -> bool:
    """True si le module Maven contenant ce fichier déclare data-rest.

    Spring Data REST n'auto-expose les repositories qu'avec la dépendance
    `spring-boot-starter-data-rest`. Ce portillon évite les faux positifs sur
    les modules JPA purs (ex. `invoicing` ici, dont `InvoiceRepository` n'est
    pas exposé). On remonte au `pom.xml` du module le plus proche.
    """
    repo_root = Path(repo_root_str)
    current = (repo_root / rel_path).parent
    pom: Path | None = None
    while True:
        candidate = current / "pom.xml"
        if candidate.exists():
            pom = candidate
            break
        if current == repo_root or current.parent == current:
            break
        current = current.parent
    if pom is None:
        return False
    return any("data-rest" in dependency for dependency in maven_module_dependencies(pom))
def _infer_spring_data_rest_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Inventorie les endpoints Spring Data REST d'un repository.

    Deux cas :
    - `@RepositoryRestResource(path = "...")` : chemin explicite (l'annotation
      implique la présence de data-rest, pas de portillon classpath) ;
    - interface Spring Data sans `exported=false` et sans `path` : chemin par
      défaut `/<entité-pluriel>`, uniquement si le module déclare data-rest.
      C'est le cas d'un `UserRepository extends JpaRepository<User, ...>` sans
      annotation, qu'aucun littéral de chemin ne permettait jusque-là de lier.
    """
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    endpoints: list[MessageEndpoint] = []
    for declaration in java_parser.type_declarations(root):
        if declaration.type != "interface_declaration":
            continue
        entity = _repository_entity_type(declaration, source)
        if entity is None:
            continue
        annotation = next(
            (
                ann for ann in java_parser.annotations_of(declaration)
                if java_parser.annotation_name(ann, source) == "RepositoryRestResource"
            ),
            None,
        )
        exported = java_parser.annotation_argument(annotation, source, key="exported") if annotation else None
        if exported is not None and java_parser.node_text(source, exported) == "false":
            continue
        rest_path = java_parser.string_value(
            java_parser.annotation_argument(annotation, source, key="path") if annotation else None,
            source,
        )
        if rest_path is not None:
            assert annotation is not None
            base_path = _normalize_rest_path(rest_path)
            snippet = java_parser.node_text(source, annotation)
            decl_line = annotation.start_point.row + 1
        else:
            if not _module_has_spring_data_rest(str(repo_root), rel_path):
                continue
            base_path = "/" + _pluralize(entity.lower())
            snippet = java_parser.node_text(source, declaration)
            decl_line = declaration.start_point.row + 1

        for topic in (
            f"GET {base_path}",
            f"POST {base_path}",
            f"GET {base_path}/{{id}}",
            f"PUT {base_path}/{{id}}",
            f"PATCH {base_path}/{{id}}",
            f"DELETE {base_path}/{{id}}",
        ):
            endpoints.append(
                _build_endpoint(
                    repo_root,
                    rel_path,
                    decl_line,
                    decl_line,
                    "serve",
                    "rest",
                    topic,
                    "spring-data-rest",
                    snippet,
                )
            )
        endpoints.extend(
            _infer_spring_data_rest_search_endpoints(repo_root, rel_path, source, declaration, base_path)
        )
    return endpoints
def _infer_spring_data_rest_search_endpoints(
    repo_root: Path, rel_path: str, source: bytes, declaration, base_path: str
) -> list[MessageEndpoint]:
    """Ressources de recherche Spring Data REST (`GET <base>/search/<méthode>`).

    Toute méthode de requête déclarée directement sur le repository (ex.
    `@Query`, dérivée `findBy...`) est exposée par défaut sous
    `/search/<nom-de-méthode>`, sauf `@RestResource(exported = false)`. Le nom
    de recherche est le nom de méthode Java, sauf `@RestResource(path = ...)`
    explicite.
    """
    body = java_parser.child_by_type(declaration, "interface_body")
    if body is None:
        return []
    endpoints: list[MessageEndpoint] = []
    for method in body.children:
        if method.type != "method_declaration":
            continue
        rest_resource = next(
            (
                ann for ann in java_parser.annotations_of(method)
                if java_parser.annotation_name(ann, source) == "RestResource"
            ),
            None,
        )
        exported = (
            java_parser.annotation_argument(rest_resource, source, key="exported")
            if rest_resource is not None
            else None
        )
        if exported is not None and java_parser.node_text(source, exported) == "false":
            continue
        search_name = java_parser.string_value(
            java_parser.annotation_argument(rest_resource, source, key="path")
            if rest_resource is not None
            else None,
            source,
        )
        if search_name is None:
            search_name = java_parser.declaration_name(method, source)
        if not search_name:
            continue
        decl_line = method.start_point.row + 1
        endpoints.append(
            _build_endpoint(
                repo_root,
                rel_path,
                decl_line,
                method.end_point.row + 1,
                "serve",
                "rest",
                f"GET {base_path}/search/{search_name}",
                "spring-data-rest",
                java_parser.node_text(source, method),
            )
        )
    return endpoints
def _repository_entity_type(declaration, source: bytes) -> str | None:
    """Entity argument of a Spring Data repository interface, via AST types."""
    for node in java_parser.walk(declaration):
        if node.type != "generic_type":
            continue
        type_node = next(
            (child for child in node.children if child.type in {"type_identifier", "scoped_type_identifier"}),
            None,
        )
        type_name = java_parser.node_text(source, type_node) if type_node is not None else ""
        if not type_name.endswith("Repository"):
            continue
        arguments = next((child for child in node.children if child.type == "type_arguments"), None)
        if arguments is None:
            continue
        for child in arguments.children:
            if child.type not in {"<", ",", ">"}:
                return _simple_type_name(java_parser.node_text(source, child))
    return None
def _infer_swagger_endpoint(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    for declaration in java_parser.type_declarations(root):
        annotation = next(
            (
                ann for ann in java_parser.annotations_of(declaration)
                if java_parser.annotation_name(ann, source) == "EnableSwagger2"
            ),
            None,
        )
        if annotation is not None:
            return [
                _build_endpoint(
                    repo_root,
                    rel_path,
                    annotation.start_point.row + 1,
                    annotation.end_point.row + 1,
                    "serve",
                    "rest",
                    "GET /swagger-ui.html",
                    "swagger-ui",
                    java_parser.node_text(source, annotation),
                )
            ]
    return []


@lru_cache(maxsize=64)
def _openapi_generator_contract_owners(repo_root_str: str) -> dict[str, str | None]:
    """Map each plugin-referenced contract path to its implementing module.

    The contract file frequently lives in a different (often shared
    ``model-*``) Maven module than the one whose ``pom.xml`` configures
    ``openapi-generator-maven-plugin`` and whose ``@RestController``s
    actually serve it. Scanning the contract file directly (as a plain
    ``*.yaml``/``*.json`` candidate) must still attribute its endpoints to
    that implementing module, not to whichever module physically encloses
    the file, or the operations silently vanish under a non-runtime
    library module that is never rendered as a service.
    """
    repo_root = Path(repo_root_str).resolve()
    owners: dict[str, str | None] = {}
    for pom_path in sorted(repo_root.rglob("pom.xml")):
        try:
            module_dir = pom_path.parent
            if not discover_rest_controllers(module_dir, set()):
                continue
            implementing_module = _module_for_path(
                repo_root, pom_path.relative_to(repo_root).as_posix()
            )
            for module_relative in maven_module.detect_openapi_generator_input_specs(pom_path):
                contract_rel_path = (
                    (module_dir / module_relative).resolve().relative_to(repo_root).as_posix()
                )
                owners[contract_rel_path] = implementing_module
        except ValueError:
            continue
    return owners
def _openapi_generator_contract_paths(repo_root_str: str) -> tuple[str, ...]:
    return tuple(sorted(_openapi_generator_contract_owners(repo_root_str)))
def _is_strategy1_openapi_declaration_path(rel_path: str) -> bool:
    """Recognize a Strategy1 microservice API publication declaration."""
    path = Path(rel_path)
    parts = path.parts
    return path.suffix.casefold() == ".rest" and any(
        parts[index:index + 4] == ("src", "main", "resources", "openapi")
        for index in range(max(0, len(parts) - 3))
    )
def _is_openapi_contract_path(repo_root: Path, rel_path: str) -> bool:
    path = repo_root / rel_path
    return path.name in {
        "openapi.yaml", "openapi.yml", "openapi.json",
        "swagger.yaml", "swagger.yml", "swagger.json",
    } or rel_path in _openapi_generator_contract_paths(str(repo_root))
def _infer_openapi_generator_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Inventory operations from a plugin-generated contract, attributed to
    the implementing service even when the contract file itself physically
    lives in a different (often shared ``model-*``) Maven module.

    Grouping endpoints by ``module`` (``graph.group_endpoints_by_module``)
    would otherwise silently move these operations onto the contract's own
    module -- which is frequently a non-runtime library never rendered as a
    service -- making them vanish instead of showing under the service that
    actually serves them.
    """
    path = repo_root / rel_path
    if path.name != "pom.xml":
        return []
    if not discover_rest_controllers(path.parent, set()):
        return []
    implementing_module = _module_for_path(repo_root, rel_path)
    endpoints: list[MessageEndpoint] = []
    for module_relative in maven_module.detect_openapi_generator_input_specs(path):
        try:
            contract_rel_path = (
                (path.parent / module_relative).resolve().relative_to(repo_root.resolve()).as_posix()
            )
        except ValueError:
            continue
        endpoints.extend(
            replace(endpoint, module=implementing_module) if endpoint.module != implementing_module else endpoint
            for endpoint in _infer_openapi_endpoints(repo_root, contract_rel_path)
        )
    return endpoints
def _infer_openapi_endpoints(
    repo_root: Path, rel_path: str, *, force_contract: bool = False
) -> list[MessageEndpoint]:
    """Inventory literal operations declared by a production OpenAPI contract.

    Some Spring projects generate their controller interfaces from OpenAPI and
    only implement those interfaces in ``src/main``.  In that layout there is
    no method-level Spring annotation to scan in the checked-in Java sources,
    while the contract remains the authoritative local evidence.  Keep the
    contract file and the operation line as evidence rather than attributing
    the route to an implementation method that does not declare it.
    """
    path = repo_root / rel_path
    if not force_contract and not _is_openapi_contract_path(repo_root, rel_path):
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        document = json.loads(text) if path.suffix == ".json" else yaml.safe_load(text)
    except (OSError, ValueError, yaml.YAMLError):
        return []
    if not isinstance(document, dict) or not isinstance(document.get("paths"), dict):
        return []

    endpoints: list[MessageEndpoint] = []
    lines = text.splitlines()
    for raw_route, operations in document["paths"].items():
        if not isinstance(raw_route, str) or not isinstance(operations, dict):
            continue
        route = _normalize_rest_path(raw_route)
        route_line = next(
            (
                index + 1
                for index, line in enumerate(lines)
                if line.lstrip().startswith(f"{raw_route}:")
                or line.lstrip().startswith(f'"{raw_route}":')
            ),
            1,
        )
        for raw_method in operations:
            method = str(raw_method).lower()
            if method not in _OPENAPI_HTTP_METHODS:
                continue
            method_line = next(
                (
                    index + 1
                    for index in range(route_line, len(lines))
                    if lines[index].strip() in (f"{method}:", f'"{method}":')
                ),
                route_line,
            )
            snippet = _read_snippet(repo_root, rel_path, method_line, method_line)
            endpoints.append(
                _build_endpoint(
                    repo_root, rel_path, method_line, method_line, "serve", "rest",
                    f"{method.upper()} {route}", "openapi", snippet,
                )
            )
    return endpoints
def _infer_openapi_endpoints_attributed(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Like `_infer_openapi_endpoints`, reattributed to the module that
    configures the contract via the generator plugin when it differs from
    the module that physically encloses the contract file.

    A contract file discovered by direct file scan (as a plain ``*.yaml``/
    ``*.json`` candidate, not through the referencing ``pom.xml``) would
    otherwise keep ``_build_endpoint``'s default module attribution -- the
    contract's own enclosing module -- even when a *different* module's
    ``openapi-generator-maven-plugin`` configuration is what actually
    serves it. That silently moves the endpoints onto a module that is
    frequently a non-runtime shared library, never rendered as a service.
    """
    endpoints = _infer_openapi_endpoints(repo_root, rel_path)
    if not endpoints:
        return endpoints
    try:
        resolved_rel_path = (
            (repo_root / rel_path).resolve().relative_to(repo_root.resolve()).as_posix()
        )
    except ValueError:
        return endpoints
    owner = _openapi_generator_contract_owners(str(repo_root.resolve())).get(resolved_rel_path)
    if owner is None:
        return endpoints
    return [
        replace(endpoint, module=owner) if endpoint.module != owner else endpoint
        for endpoint in endpoints
    ]
def _strategy1_model_openapi_contracts(repo_root: Path, api_name: str) -> tuple[str, ...]:
    """Find the ``<api>.yaml``-style contract configured by a ``model-*`` module."""
    contracts: set[str] = set()
    for pom_path in sorted(repo_root.rglob("pom.xml")):
        try:
            artifact_id, _, _ = maven_module.parse_pom(pom_path)
            module_names = {pom_path.parent.name.casefold()}
            if artifact_id:
                module_names.add(artifact_id.casefold())
            if not any(name.startswith("model-") for name in module_names):
                continue
            for module_relative in maven_module.detect_openapi_generator_input_specs(pom_path):
                contract_name = Path(module_relative).stem.casefold().replace("_", "-")
                if contract_name != api_name:
                    continue
                contracts.add(
                    (pom_path.parent / module_relative).resolve().relative_to(repo_root.resolve()).as_posix()
                )
        except (OSError, ValueError):
            continue
    return tuple(sorted(contracts))
def _infer_strategy1_declared_openapi_publications(
    repo_root: Path, declaration_rel_path: str
) -> list[MessageEndpoint]:
    """Attribute a ``*.rest`` declaration to its contract in a ``model-*`` module.

    A Strategy1 declaration is deliberately not parsed as OpenAPI.  It tells
    us which microservice publishes the contract with the same base name in a
    ``model-*`` Maven module. Endpoints keep the declaration path so their
    lifecycle follows the publishing service during incremental indexing.
    """
    api_name = Path(declaration_rel_path).stem.casefold().replace("_", "-")
    publisher_module = _module_for_path(repo_root, declaration_rel_path)
    endpoints: list[MessageEndpoint] = []
    for contract_rel_path in _strategy1_model_openapi_contracts(repo_root, api_name):
        for contract_endpoint in _infer_openapi_endpoints(
            repo_root, contract_rel_path, force_contract=True
        ):
            endpoints.append(
                replace(
                    contract_endpoint,
                    id=compute_endpoint_id(
                        contract_endpoint.role,
                        contract_endpoint.topic,
                        declaration_rel_path,
                        1,
                        1,
                    ),
                    path=declaration_rel_path,
                    start_line=1,
                    end_line=1,
                    snippet=(
                        f"Publication OpenAPI declaree par {declaration_rel_path}\n"
                        f"systemlens-openapi-contract:{contract_rel_path}\n"
                        f"{contract_endpoint.snippet}"
                    ),
                    module=publisher_module,
                    qualified_name=None,
                )
            )
    return endpoints
def _infer_actuator_endpoint(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    path = repo_root / rel_path
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    value = _load_flat_spring_properties(str(path)).get("management.endpoints.web.exposure.include")
    if value is None or "*" not in value:
        return []
    start_line = 1
    for idx, line in enumerate(text.splitlines(), start=1):
        if "management.endpoints.web.exposure.include" in line:
            start_line = idx
            break
    return [
        _build_endpoint(
            repo_root,
            rel_path,
            start_line,
            start_line,
            "serve",
            "rest",
            "GET /actuator/**",
            "spring-actuator",
            "management.endpoints.web.exposure.include=*",
        )
    ]


@lru_cache(maxsize=512)
def _file_uses_resttemplate(repo_root_str: str, rel_path: str) -> bool:
    path = Path(repo_root_str) / rel_path
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return (
        "org.springframework.web.client.RestTemplate" in text
        or "new RestTemplate(" in text
        or " RestTemplate " in text
    )


@lru_cache(maxsize=512)
def _file_uses_restclient(repo_root_str: str, rel_path: str) -> bool:
    path = Path(repo_root_str) / rel_path
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return "org.springframework.web.client.RestClient" in text or "RestClient " in text
def _uri_argument(snippet: str) -> str | None:
    """Extrait l'argument de `.uri(...)` en tenant compte des appels imbriqués."""
    match = _URI_CALL_RE.search(snippet)
    if match is None:
        return None
    start = match.end()
    depth = 1
    quote: str | None = None
    escaped = False
    for index in range(start, len(snippet)):
        char = snippet[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ('"', "'"):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return snippet[start:index]
    return None
def _extract_restclient_path(
    snippet: str, repo_root: Path, source_path: str
) -> tuple[str, bool] | None:
    expr = _uri_argument(snippet)
    if expr is None:
        return None
    return _resolve_rest_path_expression(
        expr, repo_root, source_path, preserve_dynamic_segments=True
    )
def _extract_resttemplate_path(
    snippet: str, repo_root: Path, source_path: str
) -> tuple[str, bool] | None:
    match = _REST_TEMPLATE_CALL_RE.search(snippet)
    if match is not None:
        return _resolve_rest_path_expression(match.group(2), repo_root, source_path)
    exchange_match = _REST_TEMPLATE_EXCHANGE_RE.search(snippet)
    if exchange_match is not None:
        return _resolve_rest_path_expression(exchange_match.group(1), repo_root, source_path)
    return None
def _infer_resttemplate_exchange_endpoints(
    repo_root: Path, rel_path: str
) -> list[MessageEndpoint]:
    """Infer RestTemplate calls from ``method_invocation`` AST nodes.

    This covers the regular convenience methods as well as ``exchange``. The
    former used to depend on a rule-engine match; taking their first
    argument from the invocation node means nested calls and line wrapping no
    longer affect which expression is considered the URL.
    """
    if not _file_uses_resttemplate(str(repo_root), rel_path):
        return []
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    direct_methods = {
        "getForObject": "GET", "getForEntity": "GET",
        "postForObject": "POST", "postForEntity": "POST",
        "put": "PUT", "delete": "DELETE",
    }
    inferred: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        receiver, method_name, args = java_parser.invocation_parts(invocation, source)
        if method_name not in {*direct_methods, "exchange"} or receiver is None or not args:
            continue
        receiver_name = _invocation_receiver(source, receiver)
        if receiver_name is None or "resttemplate" not in receiver_name.lower():
            continue
        if method_name == "exchange":
            if len(args) < 2:
                continue
            http_method = java_parser.node_text(source, args[1]).rsplit(".", 1)[-1]
            if http_method not in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
                continue
        else:
            http_method = direct_methods[method_name]
        route, dynamic = _resolve_rest_path_expression(
            java_parser.node_text(source, args[0]), repo_root, rel_path
        )
        inferred.append(
            _build_endpoint(
                repo_root, rel_path, invocation.start_point.row + 1,
                invocation.end_point.row + 1, "call", "rest", f"{http_method} {route}",
                "resttemplate", java_parser.node_text(source, invocation), topic_dynamic=dynamic,
            )
        )
    return inferred
def _infer_webclient_endpoints(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    """Infer fluent WebClient calls by linking ``.uri`` to its AST receiver."""
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    if b"WebClient" not in source:
        return []
    endpoints: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        receiver, name, args = java_parser.invocation_parts(invocation, source)
        if name != "uri" or receiver is None or not args:
            continue
        verb_call = _webclient_verb_invocation(receiver, source)
        if verb_call is None:
            continue
        http_method, anchor = verb_call
        route, dynamic = _resolve_rest_path_expression(
            java_parser.node_text(source, args[0]), repo_root, rel_path,
            preserve_dynamic_segments=True,
        )
        endpoints.append(
            _build_endpoint(
                repo_root, rel_path, anchor.start_point.row + 1, invocation.end_point.row + 1,
                "call", "rest", f"{http_method} {route}", "webclient",
                java_parser.node_text(source, invocation), topic_dynamic=dynamic,
            )
        )
    return endpoints
def _webclient_verb_invocation(node, source: bytes):
    """Nearest request verb in a fluent receiver chain, with its AST node."""
    if node.type != "method_invocation":
        return None
    receiver, name, _args = java_parser.invocation_parts(node, source)
    verbs = {"get", "post", "put", "delete", "patch", "head", "options"}
    if name in verbs:
        return name.upper(), node
    return _webclient_verb_invocation(receiver, source) if receiver is not None else None
def _infer_configured_api_client_endpoints(
    repo_root: Path, rel_path: str
) -> list[MessageEndpoint]:
    """Émet les dépendances configurées d'une configuration REST Strategy1.

    La convention strategy1 ne dépend ni du nom de factory ni de la forme du
    bean : toute constante majuscule avec underscore présente dans une classe
    ``Rest*Config*`` désigne le microservice homologue en kebab-case.
    """
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    endpoints: dict[str, MessageEndpoint] = {}
    for type_node in java_parser.type_declarations(root):
        if not _is_rest_client_configuration(
            java_parser.declaration_name(type_node, source)
        ):
            continue
        configuration = java_parser.node_text(source, type_node)
        for domain, line in _rest_configuration_domains(type_node, source):
            endpoint = _build_endpoint(
                repo_root,
                rel_path,
                line,
                line,
                "call",
                "rest",
                "ANY <dynamic>",
                "configured-api-client-configuration",
                f"{configuration}\nsystemlens-api-domain:{domain}",
                topic_dynamic=True,
            )
            endpoints[endpoint.id] = endpoint
            _trace_rest_client(
                "rest_client.search.configuration_dependency",
                path=rel_path,
                line=line,
                domain=domain,
            )
        for service, line in _rest_configuration_external_services(type_node, source):
            endpoint = _build_endpoint(
                repo_root,
                rel_path,
                line,
                line,
                "call",
                "rest",
                "ANY <dynamic>",
                "configured-external-rest-api-properties",
                f"{configuration}\nsystemlens-external-microservice:{service}",
                topic_dynamic=True,
            )
            endpoints[endpoint.id] = endpoint
            _trace_rest_client(
                "rest_client.search.external_configuration_dependency",
                path=rel_path,
                line=line,
                service=service,
            )
    return list(endpoints.values())
def _infer_spring_cloud_gateway_routes(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    inferred: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        _receiver, name, args = java_parser.invocation_parts(invocation, source)
        if name != "route" or not args or args[0].type != "lambda_expression":
            continue
        path_value: str | None = None
        http_method: str | None = None
        has_uri = False
        for call in java_parser.walk(args[0]):
            if call.type != "method_invocation":
                continue
            _call_receiver, call_name, call_args = java_parser.invocation_parts(call, source)
            if call_name == "path" and call_args and call_args[0].type == "string_literal":
                path_value = java_parser.string_value(call_args[0], source)
            elif call_name == "method" and call_args:
                candidate = java_parser.string_value(call_args[0], source)
                if candidate is None:
                    candidate = java_parser.node_text(source, call_args[0]).rsplit(".", 1)[-1]
                if candidate in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
                    http_method = candidate
            elif call_name == "uri":
                has_uri = True
        if path_value is None or http_method is None or not has_uri:
            continue
        route = _normalize_rest_path(path_value)
        for role in ("serve", "call"):
            inferred.append(
                _build_endpoint(
                    repo_root, rel_path, invocation.start_point.row + 1,
                    invocation.end_point.row + 1, role, "rest", f"{http_method} {route}",
                    "spring-cloud-gateway", java_parser.node_text(source, invocation),
                )
            )
    return inferred
def _gateway_route_entries(data: object) -> list[dict[str, object]]:
    if not isinstance(data, dict):
        return []
    spring = data.get("spring")
    if not isinstance(spring, dict):
        return []
    cloud = spring.get("cloud")
    if not isinstance(cloud, dict):
        return []
    gateway = cloud.get("gateway")
    if not isinstance(gateway, dict):
        return []
    routes = gateway.get("routes")
    if not isinstance(routes, list):
        server = gateway.get("server")
        webflux = server.get("webflux") if isinstance(server, dict) else None
        routes = webflux.get("routes") if isinstance(webflux, dict) else None
    return [route for route in routes if isinstance(route, dict)] if isinstance(routes, list) else []
def _gateway_paths(route: dict[str, object]) -> list[str]:
    predicates = route.get("predicates")
    if not isinstance(predicates, list):
        return []
    paths: list[str] = []
    for predicate in predicates:
        if isinstance(predicate, str) and predicate.startswith("Path="):
            paths.extend(path.strip() for path in predicate[5:].split(",") if path.strip())
        elif isinstance(predicate, dict) and predicate.get("name") == "Path":
            args = predicate.get("args")
            if isinstance(args, dict):
                value = args.get("_genkey_0") or args.get("patterns")
                if isinstance(value, str):
                    paths.append(value)
    return paths
def _gateway_strip_prefix(route: dict[str, object]) -> int:
    filters = route.get("filters")
    if not isinstance(filters, list):
        return 0
    for item in filters:
        if isinstance(item, str) and item.startswith("StripPrefix="):
            try:
                return int(item.partition("=")[2])
            except ValueError:
                return 0
    return 0
def _strip_gateway_path(route: str, prefix_count: int) -> str:
    if prefix_count <= 0:
        return _normalize_rest_path(route)
    parts = [part for part in route.split("/") if part]
    remaining = parts[prefix_count:]
    return "/" + "/".join(remaining) if remaining else "/"
def _infer_spring_cloud_gateway_yaml_routes(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    path = repo_root / rel_path
    try:
        documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8", errors="replace")))
    except (OSError, yaml.YAMLError):
        return []

    inferred: list[MessageEndpoint] = []
    for document in documents:
        for route in _gateway_route_entries(document):
            uri = route.get("uri")
            if not isinstance(uri, str) or not uri.startswith("lb://"):
                continue
            strip_prefix = _gateway_strip_prefix(route)
            line_no = 1
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
                line_no = next(
                    index
                    for index, line in enumerate(text.splitlines(), start=1)
                    if f"uri: {uri}" in line or f"uri:{uri}" in line
                )
            except (OSError, StopIteration):
                pass
            for public_path in _gateway_paths(route):
                public_route = _normalize_rest_path(public_path)
                target_path = _strip_gateway_path(public_route, strip_prefix)
                snippet = f"Path={public_route}; StripPrefix={strip_prefix}; uri={uri}"
                inferred.append(
                    _build_endpoint(
                        repo_root,
                        rel_path,
                        line_no,
                        line_no,
                        "serve",
                        "rest",
                        f"ANY {public_route}",
                        "spring-cloud-gateway",
                        snippet,
                    )
                )
                inferred.append(
                    _build_endpoint(
                        repo_root,
                        rel_path,
                        line_no,
                        line_no,
                        "call",
                        "rest",
                        f"ANY {target_path}",
                        "spring-cloud-gateway",
                        snippet,
                    )
                )
    return inferred
def _infer_spring_webflux_routes(repo_root: Path, rel_path: str) -> list[MessageEndpoint]:
    parsed = java_parser.parse_java(str(repo_root), rel_path)
    if parsed is None:
        return []
    source, root = parsed
    inferred: list[MessageEndpoint] = []
    for invocation in java_parser.walk(root):
        if invocation.type != "method_invocation":
            continue
        _receiver, name, args = java_parser.invocation_parts(invocation, source)
        if name not in {"route", "andRoute"} or not args or args[0].type != "method_invocation":
            continue
        _predicate_receiver, predicate, predicate_args = java_parser.invocation_parts(args[0], source)
        if predicate not in {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}:
            continue
        if not predicate_args or predicate_args[0].type != "string_literal":
            continue
        path = java_parser.string_value(predicate_args[0], source)
        if path is None:
            continue
        inferred.append(
            _build_endpoint(
                repo_root, rel_path, invocation.start_point.row + 1,
                invocation.end_point.row + 1, "serve", "rest",
                f"{predicate} {_normalize_rest_path(path)}", "spring-webflux",
                java_parser.node_text(source, invocation),
            )
        )
    return inferred
def infer_framework_endpoints(
    repo_root: Path,
    files: list[str] | None = None,
    *,
    configured_api_client_strategy1: bool = False,
) -> list[MessageEndpoint]:
    """Infère les endpoints des frameworks connus.

    Les conventions applicatives de constantes majuscules avec underscore des
    classes ``Rest*Config*`` ne sont actives qu'avec ``strategy1``.
    """
    if files is None:
        candidate_files = [
            path.relative_to(repo_root).as_posix() for path in repo_root.rglob("*") if path.is_file()
        ]
    else:
        candidate_files = sorted(files)

    inferred: dict[str, MessageEndpoint] = {}
    for rel_path in candidate_files:
        if rel_path.endswith(".java"):
            for endpoint in (
                _infer_generic_request_mapping_endpoints(repo_root, rel_path)
                + _infer_spring_data_rest_endpoints(repo_root, rel_path)
                + _infer_swagger_endpoint(repo_root, rel_path)
                + _infer_resttemplate_exchange_endpoints(repo_root, rel_path)
                + _infer_webclient_endpoints(repo_root, rel_path)
                + _infer_spring_cloud_gateway_routes(repo_root, rel_path)
                + _infer_spring_webflux_routes(repo_root, rel_path)
                + (
                    _infer_configured_api_client_endpoints(repo_root, rel_path)
                    if configured_api_client_strategy1
                    else []
                )
            ):
                inferred[endpoint.id] = endpoint
        elif rel_path.endswith("pom.xml"):
            for endpoint in _infer_openapi_generator_endpoints(repo_root, rel_path):
                inferred[endpoint.id] = endpoint
        elif rel_path.endswith((".properties", ".yml", ".yaml")):
            for endpoint in (
                _infer_actuator_endpoint(repo_root, rel_path)
                + _infer_spring_cloud_gateway_yaml_routes(repo_root, rel_path)
                + _infer_openapi_endpoints_attributed(repo_root, rel_path)
            ):
                inferred[endpoint.id] = endpoint
        elif rel_path.endswith(".json"):
            for endpoint in _infer_openapi_endpoints_attributed(repo_root, rel_path):
                inferred[endpoint.id] = endpoint
        elif configured_api_client_strategy1 and _is_strategy1_openapi_declaration_path(rel_path):
            for endpoint in _infer_strategy1_declared_openapi_publications(repo_root, rel_path):
                inferred[endpoint.id] = endpoint
    return list(inferred.values())
