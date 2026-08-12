"""Détection de microservices Gradle Spring Boot (BACKLOG-15 H1, ADR-33)."""

import re
from functools import lru_cache
from pathlib import Path

from systemlens import java_parser

_GRADLE_ARTIFACT_RE = re.compile(
    r"(?:archiveBaseName|archivesBaseName|archivesName)\s*(?:\.set\s*\()?\s*=\s*['\"]([^'\"]+)['\"]"
)
_GRADLE_ARTIFACT_SET_RE = re.compile(
    r"(?:archiveBaseName|archivesBaseName|archivesName)\s*\.set\s*\(\s*['\"]([^'\"]+)['\"]\s*\)"
)
_ROOT_PROJECT_NAME_RE = re.compile(r"rootProject\.name\s*=\s*['\"]([^'\"]+)['\"]")
_GRADLE_VERSION_RE = re.compile(r"\bversion\s*(?:=|\.set\()\s*['\"]([^'\"]+)['\"]")
_GRADLE_BUILD_FILENAMES = ("build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts")
_MAX_NESTED_MODULE_DEPTH = 5


def _is_module_within_depth(root: Path, module_dir: Path) -> bool:
    return len(module_dir.relative_to(root).parts) <= _MAX_NESTED_MODULE_DEPTH


def _is_spring_boot_main_class(source: bytes) -> bool:
    return java_parser.has_spring_boot_main_class(source)


def _nearest_gradle_project(repo_root: Path, java_file: Path) -> Path | None:
    """Return the owning Gradle project, never a source-directory surrogate."""
    for candidate in [java_file.parent, *java_file.parent.parents]:
        if candidate != repo_root and repo_root not in candidate.parents:
            break
        if any((candidate / filename).is_file() for filename in _GRADLE_BUILD_FILENAMES):
            return candidate
        if candidate == repo_root:
            break
    return None


def _first_gradle_match(paths: list[Path], patterns: tuple[re.Pattern[str], ...]) -> str | None:
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for pattern in patterns:
            match = pattern.search(text)
            if match is not None:
                return match.group(1)
    return None


@lru_cache(maxsize=128)
def _gradle_artifact_name(service_dir_str: str, fallback: str) -> str:
    """Nom d'artefact Gradle, ou nom de projet si Gradle utilise son défaut."""
    service_dir = Path(service_dir_str)
    build_files = [service_dir / "build.gradle", service_dir / "build.gradle.kts"]
    artifact_name = _first_gradle_match(
        build_files, (_GRADLE_ARTIFACT_RE, _GRADLE_ARTIFACT_SET_RE)
    )
    if artifact_name is not None:
        return artifact_name

    settings_files = [service_dir / "settings.gradle", service_dir / "settings.gradle.kts"]
    project_name = _first_gradle_match(settings_files, (_ROOT_PROJECT_NAME_RE,))
    return project_name or fallback


@lru_cache(maxsize=8)
def _service_root_artifacts(repo_root_str: str) -> tuple[tuple[str, str], ...]:
    """Paires ``(racine relative, nom d'artefact)`` des services détectés."""
    repo_root = Path(repo_root_str)
    roots: dict[str, str] = {}
    for java_file in repo_root.rglob("*.java"):
        try:
            source = java_file.read_bytes()
        except OSError:
            continue
        if not _is_spring_boot_main_class(source):
            continue

        service_dir = _nearest_gradle_project(repo_root, java_file)
        if service_dir is None or not _is_module_within_depth(repo_root, service_dir):
            continue
        relative_root = "" if service_dir == repo_root else service_dir.relative_to(repo_root).as_posix()
        fallback = service_dir.name
        roots[relative_root] = _gradle_artifact_name(str(service_dir), fallback)
    return tuple(sorted(roots.items()))


def clear_caches() -> None:
    """Vide les caches de détection Gradle avant une nouvelle indexation."""
    _gradle_artifact_name.cache_clear()
    _service_root_artifacts.cache_clear()


def discover_gradle_services(repo_root: Path) -> list[tuple[str, Path]]:
    """Services Gradle détectés sous la forme ``(artefact, répertoire)``."""
    root = repo_root.resolve()
    return [
        (artifact, root if not relative_root else root / relative_root)
        for relative_root, artifact in _service_root_artifacts(str(root))
    ]


def discover_gradle_service_roots(repo_root: Path) -> list[str]:
    """Noms d'artefacts des microservices Gradle détectés, triés."""
    return sorted(artifact for artifact, _ in discover_gradle_services(repo_root))


def discover_gradle_modules(repo_root: Path) -> list[tuple[str, Path, str | None]]:
    """Return every Gradle project, including settings-only aggregators."""
    root = repo_root.resolve()
    module_dirs = {
        path.parent
        for filename in ("build.gradle", "build.gradle.kts")
        for path in root.rglob(filename)
        if _is_module_within_depth(root, path.parent)
    }
    if any((root / filename).is_file() for filename in ("settings.gradle", "settings.gradle.kts")):
        module_dirs.add(root)
    modules = []
    for module_dir in sorted(module_dirs):
        name = _gradle_artifact_name(str(module_dir), module_dir.name)
        version = _first_gradle_match(
            [module_dir / "build.gradle", module_dir / "build.gradle.kts"], (_GRADLE_VERSION_RE,)
        )
        modules.append((name, module_dir, version))
    return modules


def gradle_service_for_path(repo_root: Path, rel_path: str) -> str | None:
    """Nom d'artefact du service Gradle auquel appartient ``rel_path``.

    Un projet mono-service avec ``src/main/java`` à la racine est rattaché à
    son propre artefact. Pour un workspace, tous les sous-projets du même
    répertoire de premier niveau reçoivent le nom d'artefact déclaré par le
    projet Gradle de ce service.
    """
    parts = Path(rel_path).parts
    if not parts:
        return None
    for root, artifact in _service_root_artifacts(str(repo_root.resolve())):
        if not root or rel_path == root or rel_path.startswith(f"{root}/"):
            return artifact
    return None
