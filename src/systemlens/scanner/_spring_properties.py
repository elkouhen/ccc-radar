"""Résolution de configuration Spring (properties/YAML/`@Value`) (BACKLOG-16).

Découvre et aplatit les fichiers `application*.yml/properties` (y compris
profils et config-server local), résout une clé de propriété Spring
(``${...}``) via ces fichiers ou une valeur par défaut, et résout la valeur
d'un champ Java annoté ``@Value`` en remontant vers la même résolution de
propriétés. Utilisé par l'inférence REST (chemins) comme par l'inférence
Kafka (topics)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from systemlens import java_parser

_SPRING_BASE_FILENAMES = (
    "application.yml",
    "application.yaml",
    "application.properties",
    "bootstrap.yml",
    "bootstrap.yaml",
    "bootstrap.properties",
)
_SPRING_PROFILE_PATTERNS = (
    "application-*.yml",
    "application-*.yaml",
    "application-*.properties",
    "bootstrap-*.yml",
    "bootstrap-*.yaml",
    "bootstrap-*.properties",
)
_SPRING_CLOUD_CONFIG_DIR_PATTERNS = (
    "src/main/resources/configurations",
    "configurations",
)
def _flatten_properties(data: object, prefix: str = "") -> dict[str, str]:
    flat: dict[str, str] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else str(key)
            flat.update(_flatten_properties(value, full_key))
    elif isinstance(data, (str, int, float, bool)):
        flat[prefix] = str(data)
    return flat
def _parse_dotted_properties_file(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("!"):
            continue
        sep_index = min(
            (i for i in (stripped.find("="), stripped.find(":")) if i != -1), default=-1
        )
        if sep_index == -1:
            continue
        key, value = stripped[:sep_index], stripped[sep_index + 1 :]
        result[key.strip()] = value.strip()
    return result
def _candidate_spring_roots(repo_root: Path, source_path: str | None) -> list[Path]:
    if source_path is None:
        return [repo_root]
    source_abs = (repo_root / source_path).resolve()
    roots: list[Path] = []
    for candidate in [source_abs.parent, *source_abs.parents]:
        if candidate == repo_root or repo_root in candidate.parents:
            roots.append(candidate)
        if candidate == repo_root:
            break
    if repo_root not in roots:
        roots.append(repo_root)
    return roots
def _discover_spring_property_files(
    repo_root_str: str, source_path: str | None
) -> tuple[str, ...]:
    repo_root = Path(repo_root_str)
    discovered: list[str] = []
    seen: set[Path] = set()

    for root in _candidate_spring_roots(repo_root, source_path):
        for config_dir in (root / "src" / "main" / "resources", root):
            for filename in _SPRING_BASE_FILENAMES:
                candidate = config_dir / filename
                if candidate.is_file() and candidate not in seen:
                    seen.add(candidate)
                    discovered.append(str(candidate))
            for pattern in _SPRING_PROFILE_PATTERNS:
                for candidate in sorted(config_dir.glob(pattern)):
                    if candidate.is_file() and candidate not in seen:
                        seen.add(candidate)
                        discovered.append(str(candidate))
    for candidate in _discover_spring_cloud_config_files(repo_root, source_path):
        if candidate not in seen:
            seen.add(candidate)
            discovered.append(str(candidate))
    return tuple(discovered)
def _local_spring_application_names(repo_root: Path, source_path: str | None) -> tuple[str, ...]:
    names: list[str] = []
    seen: set[str] = set()
    for root in _candidate_spring_roots(repo_root, source_path):
        for config_dir in (root / "src" / "main" / "resources", root):
            for filename in _SPRING_BASE_FILENAMES:
                candidate = config_dir / filename
                if not candidate.is_file():
                    continue
                name = _load_flat_spring_properties(str(candidate)).get("spring.application.name")
                if name and name not in seen:
                    seen.add(name)
                    names.append(name)
            for pattern in _SPRING_PROFILE_PATTERNS:
                for candidate in sorted(config_dir.glob(pattern)):
                    if not candidate.is_file():
                        continue
                    name = _load_flat_spring_properties(str(candidate)).get("spring.application.name")
                    if name and name not in seen:
                        seen.add(name)
                        names.append(name)
    return tuple(names)
def _discover_spring_cloud_config_files(
    repo_root: Path, source_path: str | None
) -> tuple[Path, ...]:
    discovered: list[Path] = []
    seen: set[Path] = set()
    for app_name in _local_spring_application_names(repo_root, source_path):
        for config_dir_pattern in _SPRING_CLOUD_CONFIG_DIR_PATTERNS:
            for suffix in (".yml", ".yaml", ".properties"):
                for candidate in sorted(
                    repo_root.glob(f"**/{config_dir_pattern}/{app_name}{suffix}")
                ):
                    if candidate.is_file() and candidate not in seen:
                        seen.add(candidate)
                        discovered.append(candidate)
    return tuple(discovered)


@lru_cache(maxsize=512)
def _load_flat_spring_properties(path_str: str) -> dict[str, str]:
    path = Path(path_str)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}

    if path.suffix == ".properties":
        return _parse_dotted_properties_file(text)
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError:
        return {}
    return _flatten_properties(data or {})
def resolve_spring_property(
    repo_root: Path, property_key: str, source_path: str | None = None
) -> str | None:
    """Cherche `property_key` (ex. `app.kafka.topics.orders`, ou
    `prop:default` — syntaxe de valeur par défaut Spring) dans les fichiers
    de configuration Spring Boot conventionnels du repo. La recherche est
    best-effort mais orientée microservice : on essaie d'abord les configs du
    module contenant `source_path`, puis celles du repo parent ; les fichiers
    sont parsés une seule fois par process via cache."""
    key, _, default = property_key.partition(":")
    for path_str in _discover_spring_property_files(str(repo_root), source_path):
        flat = _load_flat_spring_properties(path_str)
        if key in flat:
            return flat[key]
    return default or None


@lru_cache(maxsize=512)
def _load_value_annotated_fields(path_str: str) -> dict[str, str]:
    """Champs `@Value("${clé}")` d'un fichier source Java — variable ->
    clé de propriété (avec éventuel `:défaut`, laissé tel quel pour
    `resolve_spring_property`). Extrait via l'AST tree-sitter : pour chaque
    `field_declaration` annotée `@Value("${...}")`, on associe le nom du
    champ à la clé de propriété (sans les `${ }`)."""
    path = Path(path_str)
    try:
        source = path.read_bytes()
    except OSError:
        return {}
    root = java_parser.java_parser("value_fields").parse(source).root_node
    if root.has_error:
        return {}
    fields: dict[str, str] = {}
    for node in java_parser.walk(root):
        if node.type != "field_declaration":
            continue
        value_ann = next(
            (
                ann
                for ann in java_parser.annotations_of(node)
                if java_parser.annotation_name(ann, source) == "Value"
            ),
            None,
        )
        if value_ann is None:
            continue
        raw = java_parser.first_string_argument(value_ann, source)
        if raw is None or not (raw.startswith("${") and raw.endswith("}")):
            continue
        property_key = raw[2:-1]
        for declarator in node.children:
            if declarator.type != "variable_declarator":
                continue
            name_node = declarator.child_by_field_name("name")
            if name_node is None:
                continue
            fields[java_parser.node_text(source, name_node)] = property_key
    return fields
def _resolve_value_annotated_variable(
    repo_root: Path, source_path: str, var_name: str
) -> str | None:
    fields = _load_value_annotated_fields(str(repo_root / source_path))
    property_key = fields.get(var_name)
    if property_key is None:
        return None
    return resolve_spring_property(repo_root, property_key, source_path)


# Certains microservices ne portent pas l'URL de destination au site d'appel :
# le client HTTP est injecté et construit dans une configuration `Rest*Config*`.
# Dans cette
# convention, un `@Bean` délègue à un helper auquel est passé le domaine qui
# publie l'API. On conserve ce domaine dans l'évidence de l'endpoint pour que
# le graphe puisse restreindre la cible, sans prétendre résoudre une URL.
