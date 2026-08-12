import hashlib
from dataclasses import dataclass


def compute_finding_id(
    rule_id: str,
    path: str,
    snippet: str,
    start_line: int | None = None,
    end_line: int | None = None,
) -> str:
    normalized_snippet = " ".join(snippet.split())
    location = "" if start_line is None else f"|{start_line}:{end_line or start_line}"
    digest = hashlib.sha256(
        f"{rule_id}|{path}{location}|{normalized_snippet}".encode()
    ).hexdigest()
    return digest[:16]


@dataclass(frozen=True)
class Finding:
    id: str
    rule_id: str
    severity: str
    message: str
    path: str
    start_line: int
    end_line: int
    snippet: str
    fix: str | None
    cwe: list[str]
    owasp: list[str]
    # BACKLOG-13 M1 : module Maven (artifactId du pom.xml le plus proche) et
    # nom qualifié Java (package + classe) du fichier — None si non
    # applicable (repo non-Maven, fichier non-Java). Permet de grouper par
    # module sans fédération multi-dépôts (voir graph.py).
    module: str | None = None
    qualified_name: str | None = None


def compute_endpoint_id(
    role: str,
    topic: str,
    path: str,
    start_line: int | None = None,
    end_line: int | None = None,
) -> str:
    location = "" if start_line is None else f"|{start_line}:{end_line or start_line}"
    digest = hashlib.sha256(f"{role}|{topic}|{path}{location}".encode()).hexdigest()
    return digest[:16]


def compute_architecture_relation_id(
    source_kind: str,
    source_name: str,
    relation: str,
    target_kind: str,
    target_name: str,
    path: str | None = None,
    start_line: int | None = None,
) -> str:
    """Return a stable identifier for one evidenced architecture fact."""
    location = f"|{path}:{start_line}" if path is not None and start_line is not None else ""
    digest = hashlib.sha256(
        f"{source_kind}|{source_name}|{relation}|{target_kind}|{target_name}{location}".encode()
    ).hexdigest()
    return digest[:16]


@dataclass(frozen=True)
class MessageEndpoint:
    """Un site statique d'échange entre services — production/consommation
    d'un topic Kafka, ou exposition/appel d'une route REST (BACKLOG-10 K1).

    `topic` porte le nom du topic Kafka, ou "METHODE /chemin" pour REST (ex.
    "GET /orders/{id}"). `path`/`start_line`/`end_line` localisent le site :
    pour `source="manifest"`, `path` est le chemin du manifeste (`TOPICS.md`
    ou `kafka-flow-graph.json`) et `start_line`/`end_line` pointent l'entrée déclarative, pas un
    site de code.
    """

    id: str
    role: str  # produce | consume (kafka) ; serve | call (rest)
    system: str  # kafka | rest
    topic: str
    topic_dynamic: bool
    source: str  # code | manifest
    framework: str | None
    path: str
    start_line: int
    end_line: int
    snippet: str
    # BACKLOG-13 M1 : voir Finding.module/qualified_name — même principe.
    module: str | None = None
    qualified_name: str | None = None
    # Type Java du payload Kafka lorsqu'il est déductible statiquement. Les
    # manifestes et les appels sans signature exploitable restent à `None`.
    message_type: str | None = None


@dataclass(frozen=True)
class ArchitectureRelation:
    """A typed, evidenced relation between two indexed architecture objects."""

    id: str
    source_kind: str
    source_name: str
    relation: str
    target_kind: str
    target_name: str
    origin: str  # code | manifest | derived
    confidence: str  # high | medium
    module: str | None = None
    path: str | None = None
    start_line: int | None = None
    end_line: int | None = None
    qualified_name: str | None = None
