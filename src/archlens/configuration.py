"""Construction d'un modèle de propriétés Spring depuis le code source."""

from pathlib import Path
import re

import yaml

_IGNORED_PARTS = {".git", ".archlens", "build", "target"}
_SENSITIVE_KEY_RE = re.compile(
    r"(?:password|passwd|secret|token|credential|api[-_.]?key|private[-_.]?key)", re.IGNORECASE
)
_SECRET_EXAMPLE = "<secret>"
_PLACEHOLDER_RE = re.compile(r"\$\{\s*([A-Za-z0-9_.-]+)(?:\s*:[^}]*)?\}")
_GET_PROPERTY_RE = re.compile(
    r"\b(?:getProperty|getRequiredProperty)\s*\(\s*['\"]([A-Za-z0-9_.-]+)['\"]"
)
_CONDITIONAL_PROPERTY_RE = re.compile(
    r"@ConditionalOnProperty\s*\([^)]*?\b(?:name|value)\s*=\s*['\"]([A-Za-z0-9_.-]+)['\"]",
    re.DOTALL,
)
_CONFIGURATION_PROPERTIES_RE = re.compile(
    r"@ConfigurationProperties\s*\(\s*(?:prefix\s*=\s*)?['\"]([A-Za-z0-9_.-]+)['\"]"
)
# Spring Boot beans often expose the complete prefix with a field name.  The
# field-level shape is deliberately not guessed: a prefix is still useful in
# a template and avoids inventing configuration keys.
_BOOLEAN_KEY_RE = re.compile(r"(?:enabled?|ssl|secure|debug)$", re.IGNORECASE)
_NUMBER_KEY_RE = re.compile(r"(?:port|timeout|retries|count|size|interval|duration)$", re.IGNORECASE)


def _is_relevant_source_file(service_root: Path, path: Path) -> bool:
    try:
        relative_parts = path.relative_to(service_root).parts
    except ValueError:
        return False
    relative = Path(*relative_parts).as_posix()
    return "src/test/" not in f"{relative}/" and not any(
        part in _IGNORED_PARTS for part in relative_parts
    )


def _example_value(key: str) -> object:
    """Choose a synthetic value from the key name, never from configuration."""
    if _SENSITIVE_KEY_RE.search(key):
        return _SECRET_EXAMPLE
    if _BOOLEAN_KEY_RE.search(key):
        return False
    if _NUMBER_KEY_RE.search(key):
        return 0
    return "<string>"


def _property_keys_from_file(path: Path) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return set()
    return {
        *(_PLACEHOLDER_RE.findall(text)),
        *(_GET_PROPERTY_RE.findall(text)),
        *(_CONDITIONAL_PROPERTY_RE.findall(text)),
        *(_CONFIGURATION_PROPERTIES_RE.findall(text)),
    }


def _properties_tree(keys: set[str]) -> dict[str, object]:
    tree: dict[str, object] = {}
    for key in sorted(keys):
        cursor = tree
        parts = [part for part in key.split(".") if part]
        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child
        if parts:
            cursor[parts[-1]] = _example_value(parts[-1])
    return tree


def service_configuration_example(service_root: Path) -> str:
    """Return a YAML property template inferred only from production code."""
    source_root = service_root / "src" / "main"
    files = (
        sorted(path for path in source_root.rglob("*.java") if _is_relevant_source_file(service_root, path))
        + sorted(path for path in source_root.rglob("*.kt") if _is_relevant_source_file(service_root, path))
    ) if source_root.is_dir() else []
    keys: set[str] = set()
    for path in files:
        keys.update(_property_keys_from_file(path))
    if not keys:
        return "# Aucune propriété Spring détectée dans le code de production.\n"
    return yaml.safe_dump(_properties_tree(keys), allow_unicode=True, sort_keys=False)
