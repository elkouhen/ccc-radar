import fnmatch
import json
import sqlite3
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

import numpy as np
import sqlite_vec

from ccc_radar.models import ArchitectureRelation, Finding, MessageEndpoint
from ccc_radar.modules import (
    BlockingPoint,
    DiscoveredModule,
    KafkaMethod,
    ModuleDependency,
    MongoMethod,
    SourceEvidence,
)
from ccc_radar.paths import db_path

SCHEMA_VERSION = "15"
SEVERITY_ORDER = ["INFO", "WARNING", "ERROR"]
_COUNTABLE_DIMENSIONS = ("rule_id", "severity")
_SQLITE_BIND_LIMIT = 900
_CODE_CHUNK_OVERFETCH_FACTOR = 3
_CODE_CHUNK_OVERFETCH_CAP = 200


def _chunked(items: list[str], size: int = _SQLITE_BIND_LIMIT) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _glob_to_sqlite(pattern: str) -> str:
    return pattern


class StoreError(Exception):
    pass


@dataclass(frozen=True)
class CodeChunk:
    id: str
    path: str
    start_line: int
    end_line: int
    language: str
    content: str


def _method_to_json(item: object) -> dict[str, object]:
    data = dict(item.__dict__)
    evidence = data.get("evidence")
    if evidence is not None:
        data["evidence"] = evidence.__dict__
    return data


def _evidence_from_json(data: dict[str, object]) -> SourceEvidence | None:
    evidence = data.pop("evidence", None)
    return SourceEvidence(**evidence) if evidence else None


def _mongo_method_from_json(data: dict[str, object]) -> MongoMethod:
    data = dict(data)
    evidence = _evidence_from_json(data)
    return MongoMethod(**data, evidence=evidence)


def _kafka_method_from_json(data: dict[str, object]) -> KafkaMethod:
    data = dict(data)
    evidence = _evidence_from_json(data)
    return KafkaMethod(**data, evidence=evidence)


def _blocking_point_from_json(data: dict[str, object]) -> BlockingPoint:
    data = dict(data)
    evidence = _evidence_from_json(data)
    return BlockingPoint(**data, evidence=evidence)


class Store:
    def __init__(self, repo_root: Path, readonly: bool = False) -> None:
        self._repo_root = Path(repo_root).resolve()
        self._db_path = db_path(self._repo_root)
        self._conn: sqlite3.Connection | None = None
        self._readonly = readonly

    def __enter__(self) -> "Store":
        if self._readonly:
            # BACKLOG-11 A2 : fédération d'un autre projet, jamais d'écriture
            # dans sa base (ni schéma, ni migration, ni commit) — voir ADR-30.
            if not self._db_path.is_file():
                raise StoreError(f"Base introuvable : {self._db_path}")
            try:
                self._conn = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
            except sqlite3.OperationalError as exc:
                raise StoreError(f"Impossible d'ouvrir {self._db_path} : {exc}") from exc
            self._conn.row_factory = sqlite3.Row
            self._load_vec_extension()
            self._check_schema_compatible()
            return self

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.row_factory = sqlite3.Row
        self._load_vec_extension()
        self._create_schema()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        assert self._conn is not None
        if exc_type is None and not self._readonly:
            self._conn.commit()
        self._conn.close()
        self._conn = None

    def _check_schema_compatible(self) -> None:
        try:
            version = self.get_meta("schema_version")
        except sqlite3.OperationalError as exc:
            raise StoreError(
                f"Base incompatible ({self._db_path}) : {exc}"
            ) from exc
        if version != SCHEMA_VERSION:
            raise StoreError(
                f"Schéma incompatible ({self._db_path}) : version {version!r}, "
                f"attendu {SCHEMA_VERSION!r} — relancez cccr index sur ce projet."
            )

    @property
    def conn(self) -> sqlite3.Connection:
        assert self._conn is not None, "Store doit être utilisé comme context manager"
        return self._conn

    def _load_vec_extension(self) -> None:
        self.conn.enable_load_extension(True)
        sqlite_vec.load(self.conn)
        self.conn.enable_load_extension(False)

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL,
                indexed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS findings (
                id TEXT PRIMARY KEY,
                rule_id TEXT,
                severity TEXT,
                message TEXT,
                path TEXT,
                start_line INTEGER,
                end_line INTEGER,
                snippet TEXT,
                fix TEXT,
                cwe TEXT,
                owasp TEXT,
                module TEXT,
                qualified_name TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_findings_path ON findings(path);
            CREATE INDEX IF NOT EXISTS idx_findings_severity ON findings(severity);
            CREATE TABLE IF NOT EXISTS code_chunks (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                language TEXT NOT NULL,
                content TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_code_chunks_path ON code_chunks(path);
            CREATE TABLE IF NOT EXISTS endpoints (
                id TEXT PRIMARY KEY,
                role TEXT NOT NULL,
                system TEXT NOT NULL,
                topic TEXT NOT NULL,
                topic_dynamic INTEGER NOT NULL,
                source TEXT NOT NULL,
                framework TEXT,
                path TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                snippet TEXT NOT NULL,
                module TEXT,
                qualified_name TEXT,
                message_type TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_endpoints_path ON endpoints(path);
            CREATE INDEX IF NOT EXISTS idx_endpoints_topic ON endpoints(topic);
            CREATE TABLE IF NOT EXISTS modules (
                path TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                build_system TEXT NOT NULL,
                version TEXT,
                kind TEXT NOT NULL,
                starts_application INTEGER NOT NULL DEFAULT 0,
                configuration_example TEXT NOT NULL,
                application_entrypoint TEXT,
                mongo_collections TEXT NOT NULL DEFAULT '[]',
                mongo_methods TEXT NOT NULL DEFAULT '[]',
                openapi_files TEXT NOT NULL DEFAULT '[]',
                kafka_methods TEXT NOT NULL DEFAULT '[]',
                blocking_points TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS module_dependencies (
                source TEXT NOT NULL,
                target TEXT NOT NULL,
                PRIMARY KEY (source, target)
            );
            CREATE TABLE IF NOT EXISTS architecture_relations (
                id TEXT PRIMARY KEY,
                source_kind TEXT NOT NULL,
                source_name TEXT NOT NULL,
                relation TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_name TEXT NOT NULL,
                origin TEXT NOT NULL,
                confidence TEXT NOT NULL,
                module TEXT,
                path TEXT,
                start_line INTEGER,
                end_line INTEGER,
                qualified_name TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_architecture_relations_source
                ON architecture_relations(source_kind, source_name);
            CREATE INDEX IF NOT EXISTS idx_architecture_relations_target
                ON architecture_relations(target_kind, target_name);
            """
        )
        self._migrate_legacy_embeddings()
        self._migrate_module_columns()
        self._migrate_endpoint_message_type()
        self._migrate_module_architecture_columns()
        if self.get_meta("schema_version") != SCHEMA_VERSION:
            self.set_meta("schema_version", SCHEMA_VERSION)
        self.conn.commit()

    def _migrate_legacy_embeddings(self) -> None:
        """Schema v1 stored embeddings as a BLOB column on `findings` (brute-force
        cosine in Python). v2 moves them to a `vec0` virtual table (sqlite-vec,
        SIMD-accelerated KNN) — same store ccc/cocoindex-code already uses for its
        own index. Dropping the old column forces a transparent full re-embed on
        the next `cccr index`, since embedding_signature no longer matches.
        """
        cols = {row["name"] for row in self.conn.execute("PRAGMA table_info(findings)")}
        if "embedding" not in cols:
            return
        self.conn.execute("ALTER TABLE findings DROP COLUMN embedding")
        self.conn.execute(
            "DELETE FROM meta WHERE key IN ('embedding_signature', 'embedding_dim')"
        )
        self.set_meta("schema_version", SCHEMA_VERSION)

    def _migrate_module_columns(self) -> None:
        """Schema v4 -> v5 (BACKLOG-13 M1) : `module`/`qualified_name`
        ajoutés à `findings`/`endpoints`, purement additifs (`NULL` pour les
        lignes existantes jusqu'au prochain `cccr index` qui les
        recalculera) — pas de ré-embedding forcé, contrairement à la
        migration v1 -> v2."""
        for table in ("findings", "endpoints"):
            cols = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
            if "module" not in cols:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN module TEXT")
            if "qualified_name" not in cols:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN qualified_name TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_findings_module ON findings(module)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_endpoints_module ON endpoints(module)")

    def _migrate_module_architecture_columns(self) -> None:
        cols = {row["name"] for row in self.conn.execute("PRAGMA table_info(modules)")}
        for name in ("starts_application", "application_entrypoint", "mongo_collections", "mongo_methods", "openapi_files", "kafka_methods", "blocking_points", "rest_controllers", "openapi_generated_clients"):
            if name not in cols:
                if name == "application_entrypoint":
                    self.conn.execute("ALTER TABLE modules ADD COLUMN application_entrypoint TEXT")
                    continue
                default = "0" if name == "starts_application" else "'[]'"
                column_type = "INTEGER" if name == "starts_application" else "TEXT"
                self.conn.execute(f"ALTER TABLE modules ADD COLUMN {name} {column_type} NOT NULL DEFAULT {default}")

    def _migrate_endpoint_message_type(self) -> None:
        """Schema v12 -> v13: payload Java type on Kafka integration sites."""
        cols = {row["name"] for row in self.conn.execute("PRAGMA table_info(endpoints)")}
        if "message_type" not in cols:
            self.conn.execute("ALTER TABLE endpoints ADD COLUMN message_type TEXT")

    # -- meta --

    def get_meta(self, key: str) -> str | None:
        cur = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    # -- modules --

    def replace_modules(self, modules: list[DiscoveredModule]) -> None:
        """Persist the build inventory produced during `cccr index`."""
        self.conn.execute("DELETE FROM modules")
        self.conn.execute("DELETE FROM module_dependencies")
        for module in modules:
            relative_path = module.path.resolve().relative_to(self._repo_root).as_posix()
            self.conn.execute(
                """
                INSERT INTO modules (path, name, build_system, version, kind, starts_application, configuration_example, application_entrypoint,
                                     mongo_collections, mongo_methods, openapi_files, kafka_methods, blocking_points, rest_controllers, openapi_generated_clients)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    relative_path,
                    module.name,
                    module.build_system,
                    module.version,
                    module.kind,
                    int(module.starts_application),
                    module.configuration_example,
                    json.dumps(module.application_entrypoint.__dict__) if module.application_entrypoint else None,
                    json.dumps(module.mongo_collections),
                    json.dumps([_method_to_json(method) for method in module.mongo_methods]),
                    json.dumps(module.openapi_files),
                    json.dumps([_method_to_json(method) for method in module.kafka_methods]),
                    json.dumps([_method_to_json(point) for point in module.blocking_points]),
                    json.dumps(module.rest_controllers),
                    json.dumps(module.openapi_generated_clients),
                ),
            )

    def all_modules(self) -> list[DiscoveredModule]:
        rows = self.conn.execute(
            "SELECT path, name, build_system, version, kind, starts_application, configuration_example, application_entrypoint, mongo_collections, mongo_methods, openapi_files, kafka_methods, blocking_points, rest_controllers, openapi_generated_clients "
            "FROM modules ORDER BY path"
        ).fetchall()
        return [
            DiscoveredModule(
                name=row["name"],
                path=self._repo_root / row["path"],
                build_system=row["build_system"],
                version=row["version"],
                kind=row["kind"],
                starts_application=bool(row["starts_application"]),
                configuration_example=row["configuration_example"],
                application_entrypoint=SourceEvidence(**json.loads(row["application_entrypoint"])) if row["application_entrypoint"] else None,
                mongo_collections=tuple(json.loads(row["mongo_collections"])),
                mongo_methods=tuple(_mongo_method_from_json(method) for method in json.loads(row["mongo_methods"])),
                openapi_files=tuple(json.loads(row["openapi_files"])),
                kafka_methods=tuple(_kafka_method_from_json(method) for method in json.loads(row["kafka_methods"])),
                blocking_points=tuple(_blocking_point_from_json(point) for point in json.loads(row["blocking_points"])),
                rest_controllers=tuple(json.loads(row["rest_controllers"])),
                openapi_generated_clients=tuple(json.loads(row["openapi_generated_clients"])),
            )
            for row in rows
        ]

    def replace_module_dependencies(self, dependencies: list[ModuleDependency]) -> None:
        """Remplace le graphe des dépendances de build local au workspace."""
        self.conn.execute("DELETE FROM module_dependencies")
        self.conn.executemany(
            "INSERT INTO module_dependencies (source, target) VALUES (?, ?)",
            [(dependency.source, dependency.target) for dependency in dependencies],
        )

    def all_module_dependencies(self) -> list[ModuleDependency]:
        rows = self.conn.execute(
            "SELECT source, target FROM module_dependencies ORDER BY source, target"
        ).fetchall()
        return [ModuleDependency(source=row["source"], target=row["target"]) for row in rows]

    # -- normalized architecture relations --

    def replace_architecture_relations(self, relations: list[ArchitectureRelation]) -> None:
        """Replace the materialized relation graph after an index run."""
        self.conn.execute("DELETE FROM architecture_relations")
        self.conn.executemany(
            """
            INSERT INTO architecture_relations
                (id, source_kind, source_name, relation, target_kind, target_name,
                 origin, confidence, module, path, start_line, end_line, qualified_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    relation.id,
                    relation.source_kind,
                    relation.source_name,
                    relation.relation,
                    relation.target_kind,
                    relation.target_name,
                    relation.origin,
                    relation.confidence,
                    relation.module,
                    relation.path,
                    relation.start_line,
                    relation.end_line,
                    relation.qualified_name,
                )
                for relation in relations
            ],
        )

    def all_architecture_relations(self) -> list[ArchitectureRelation]:
        rows = self.conn.execute(
            "SELECT * FROM architecture_relations ORDER BY source_kind, source_name, relation, target_kind, target_name, path, start_line"
        ).fetchall()
        return [
            ArchitectureRelation(
                id=row["id"],
                source_kind=row["source_kind"],
                source_name=row["source_name"],
                relation=row["relation"],
                target_kind=row["target_kind"],
                target_name=row["target_name"],
                origin=row["origin"],
                confidence=row["confidence"],
                module=row["module"],
                path=row["path"],
                start_line=row["start_line"],
                end_line=row["end_line"],
                qualified_name=row["qualified_name"],
            )
            for row in rows
        ]

    def get_embedding_dim(self) -> int | None:
        raw = self.get_meta("embedding_dim")
        return int(raw) if raw else None

    # -- files --

    def set_file_hash(self, path: str, sha: str) -> None:
        self.conn.execute(
            "INSERT INTO files (path, sha256, indexed_at) VALUES (?, ?, datetime('now')) "
            "ON CONFLICT(path) DO UPDATE SET "
            "sha256 = excluded.sha256, indexed_at = excluded.indexed_at",
            (path, sha),
        )

    def get_file_hashes(self) -> dict[str, str]:
        cur = self.conn.execute("SELECT path, sha256 FROM files")
        return {row["path"]: row["sha256"] for row in cur.fetchall()}

    def remove_files(self, paths: list[str]) -> None:
        if not paths:
            return
        self._delete_rows_for_paths("files", paths)
        removed_ids = self._finding_ids_for_paths(paths)
        self._delete_rows_for_paths("findings", paths)
        self._delete_embeddings(removed_ids)
        self.replace_code_chunks_for_files(paths, [])
        self.replace_endpoints_for_files(paths, [])

    # -- findings --

    def _finding_ids_for_paths(self, paths: list[str]) -> list[str]:
        return self._ids_for_paths("findings", "id", paths)

    def count_findings_for_paths(self, paths: list[str]) -> int:
        return self._count_rows_for_paths("findings", paths)

    def replace_findings_for_files(self, paths: list[str], findings: list[Finding]) -> None:
        if paths:
            removed_ids = self._finding_ids_for_paths(paths)
            self._delete_rows_for_paths("findings", paths)
            self._delete_embeddings(removed_ids)
        for finding in findings:
            self.conn.execute(
                """
                INSERT INTO findings
                    (id, rule_id, severity, message, path, start_line, end_line,
                     snippet, fix, cwe, owasp, module, qualified_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    rule_id = excluded.rule_id,
                    severity = excluded.severity,
                    message = excluded.message,
                    path = excluded.path,
                    start_line = excluded.start_line,
                    end_line = excluded.end_line,
                    snippet = excluded.snippet,
                    fix = excluded.fix,
                    cwe = excluded.cwe,
                    owasp = excluded.owasp,
                    module = excluded.module,
                    qualified_name = excluded.qualified_name
                """,
                (
                    finding.id,
                    finding.rule_id,
                    finding.severity,
                    finding.message,
                    finding.path,
                    finding.start_line,
                    finding.end_line,
                    finding.snippet,
                    finding.fix,
                    json.dumps(finding.cwe),
                    json.dumps(finding.owasp),
                    finding.module,
                    finding.qualified_name,
                ),
            )

    # -- indexed code chunks (experimental CocoIndex-style target state) --

    def _code_chunk_ids_for_paths(self, paths: list[str]) -> list[str]:
        return self._ids_for_paths("code_chunks", "id", paths)

    def replace_code_chunks_for_files(
        self, paths: list[str], chunks: list[CodeChunk]
    ) -> None:
        if paths:
            removed_ids = self._code_chunk_ids_for_paths(paths)
            self._delete_rows_for_paths("code_chunks", paths)
            self._delete_code_chunk_embeddings(removed_ids)
        for chunk in chunks:
            self.conn.execute(
                """
                INSERT INTO code_chunks
                    (id, path, start_line, end_line, language, content)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    path = excluded.path,
                    start_line = excluded.start_line,
                    end_line = excluded.end_line,
                    language = excluded.language,
                    content = excluded.content
                """,
                (
                    chunk.id,
                    chunk.path,
                    chunk.start_line,
                    chunk.end_line,
                    chunk.language,
                    chunk.content,
                ),
            )

    def all_code_chunks(self) -> list[CodeChunk]:
        cur = self.conn.execute(
            "SELECT id, path, start_line, end_line, language, content FROM code_chunks"
        )
        return [_row_to_code_chunk(row) for row in cur.fetchall()]

    def all_findings(
        self,
        severity_at_least: str | None = None,
        rule_id: str | None = None,
        path_glob: str | None = None,
        module: str | None = None,
    ) -> list[Finding]:
        query = "SELECT * FROM findings"
        clauses: list[str] = []
        params: list[str] = []
        if severity_at_least:
            min_index = SEVERITY_ORDER.index(severity_at_least)
            severities = SEVERITY_ORDER[min_index:]
            placeholders = ",".join("?" for _ in severities)
            clauses.append(f"severity IN ({placeholders})")
            params.extend(severities)
        if rule_id:
            clauses.append("rule_id = ?")
            params.append(rule_id)
        if path_glob:
            clauses.append("path GLOB ?")
            params.append(_glob_to_sqlite(path_glob))
        if module:
            clauses.append("module = ?")
            params.append(module)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY path, start_line, end_line, id"
        cur = self.conn.execute(query, params)
        return [_row_to_finding(row) for row in cur.fetchall()]

    def all_findings_for_paths(self, paths: list[str]) -> list[Finding]:
        rows = self._rows_for_paths("findings", paths)
        return [_row_to_finding(row) for row in rows]

    # -- endpoints (message_endpoints, BACKLOG-10 K1) --

    def _endpoint_ids_for_paths(self, paths: list[str]) -> list[str]:
        return self._ids_for_paths("endpoints", "id", paths)

    def count_endpoints_for_paths(self, paths: list[str]) -> int:
        return self._count_rows_for_paths("endpoints", paths)

    def replace_endpoints_for_files(
        self, paths: list[str], endpoints: list[MessageEndpoint]
    ) -> None:
        if paths:
            removed_ids = self._endpoint_ids_for_paths(paths)
            self._delete_rows_for_paths("endpoints", paths)
            self._delete_endpoint_embeddings(removed_ids)
        for endpoint in endpoints:
            self.conn.execute(
                """
                INSERT INTO endpoints
                    (id, role, system, topic, topic_dynamic, source, framework,
                     path, start_line, end_line, snippet, module, qualified_name, message_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    role = excluded.role,
                    system = excluded.system,
                    topic = excluded.topic,
                    topic_dynamic = excluded.topic_dynamic,
                    source = excluded.source,
                    framework = excluded.framework,
                    path = excluded.path,
                    start_line = excluded.start_line,
                    end_line = excluded.end_line,
                    snippet = excluded.snippet,
                    module = excluded.module,
                    qualified_name = excluded.qualified_name,
                    message_type = excluded.message_type
                """,
                (
                    endpoint.id,
                    endpoint.role,
                    endpoint.system,
                    endpoint.topic,
                    int(endpoint.topic_dynamic),
                    endpoint.source,
                    endpoint.framework,
                    endpoint.path,
                    endpoint.start_line,
                    endpoint.end_line,
                    endpoint.snippet,
                    endpoint.module,
                    endpoint.qualified_name,
                    endpoint.message_type,
                ),
            )

    def all_endpoints(
        self,
        system: str | None = None,
        role: str | None = None,
        topic: str | None = None,
        path_glob: str | None = None,
        module: str | None = None,
    ) -> list[MessageEndpoint]:
        query = "SELECT * FROM endpoints"
        clauses: list[str] = []
        params: list[str] = []
        if system:
            clauses.append("system = ?")
            params.append(system)
        if role:
            clauses.append("role = ?")
            params.append(role)
        if topic:
            clauses.append("topic = ?")
            params.append(topic)
        if path_glob:
            clauses.append("path GLOB ?")
            params.append(_glob_to_sqlite(path_glob))
        if module:
            clauses.append("module = ?")
            params.append(module)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY path, start_line, end_line, id"
        cur = self.conn.execute(query, params)
        return [_row_to_endpoint(row) for row in cur.fetchall()]

    # -- embeddings --
    #
    # Vectors live in `vec_findings`, a sqlite-vec `vec0` virtual table (same
    # extension ccc/cocoindex-code uses for its own `.cocoindex_code/target_sqlite.db`).
    # `embedding_dim` (in `meta`) doubles as "does the table exist, and at what
    # dimension" — vec0 tables can't ALTER, so a dimension change drops and
    # recreates it; that only happens on a full re-embed (model change), which
    # already touches every finding.

    def _ensure_vec_table(self, dim: int) -> None:
        if self.get_embedding_dim() == dim:
            return
        self.conn.execute("DROP TABLE IF EXISTS vec_findings")
        self.conn.execute(
            f"CREATE VIRTUAL TABLE vec_findings USING vec0("
            f"embedding float[{dim}] distance_metric=cosine, +finding_id TEXT)"
        )
        self.set_meta("embedding_dim", str(dim))

    def _ensure_endpoint_vec_table(self, dim: int) -> None:
        raw_dim = self.get_meta("endpoint_embedding_dim")
        if raw_dim and int(raw_dim) == dim:
            return
        self.conn.execute("DROP TABLE IF EXISTS vec_endpoints")
        self.conn.execute(
            f"CREATE VIRTUAL TABLE vec_endpoints USING vec0("
            f"embedding float[{dim}] distance_metric=cosine, +endpoint_id TEXT)"
        )
        self.set_meta("endpoint_embedding_dim", str(dim))

    def _ensure_code_vec_table(self, dim: int) -> None:
        raw_dim = self.get_meta("code_embedding_dim")
        if raw_dim and int(raw_dim) == dim:
            return
        self.conn.execute("DROP TABLE IF EXISTS vec_code_chunks")
        self.conn.execute(
            f"CREATE VIRTUAL TABLE vec_code_chunks USING vec0("
            f"embedding float[{dim}] distance_metric=cosine, +chunk_id TEXT)"
        )
        self.set_meta("code_embedding_dim", str(dim))

    def _delete_embeddings(self, finding_ids: list[str]) -> None:
        if not finding_ids or self.get_embedding_dim() is None:
            return
        for chunk in _chunked(finding_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"DELETE FROM vec_findings WHERE finding_id IN ({placeholders})", chunk
            )

    def _delete_endpoint_embeddings(self, endpoint_ids: list[str]) -> None:
        if not endpoint_ids or self.get_meta("endpoint_embedding_dim") is None:
            return
        for chunk in _chunked(endpoint_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"DELETE FROM vec_endpoints WHERE endpoint_id IN ({placeholders})", chunk
            )

    def _delete_code_chunk_embeddings(self, chunk_ids: list[str]) -> None:
        if not chunk_ids or self.get_meta("code_embedding_dim") is None:
            return
        for chunk in _chunked(chunk_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"DELETE FROM vec_code_chunks WHERE chunk_id IN ({placeholders})", chunk
            )

    def set_embedding(self, finding_id: str, vector: np.ndarray) -> None:
        vector = vector.astype(np.float32)
        self._ensure_vec_table(vector.shape[0])
        self.conn.execute("DELETE FROM vec_findings WHERE finding_id = ?", (finding_id,))
        self.conn.execute(
            "INSERT INTO vec_findings (embedding, finding_id) VALUES (?, ?)",
            (sqlite_vec.serialize_float32(vector.tolist()), finding_id),
        )

    def set_endpoint_embedding(self, endpoint_id: str, vector: np.ndarray) -> None:
        vector = vector.astype(np.float32)
        self._ensure_endpoint_vec_table(vector.shape[0])
        self.conn.execute("DELETE FROM vec_endpoints WHERE endpoint_id = ?", (endpoint_id,))
        self.conn.execute(
            "INSERT INTO vec_endpoints (embedding, endpoint_id) VALUES (?, ?)",
            (sqlite_vec.serialize_float32(vector.tolist()), endpoint_id),
        )

    def set_code_chunk_embedding(self, chunk_id: str, vector: np.ndarray) -> None:
        vector = vector.astype(np.float32)
        self._ensure_code_vec_table(vector.shape[0])
        self.conn.execute("DELETE FROM vec_code_chunks WHERE chunk_id = ?", (chunk_id,))
        self.conn.execute(
            "INSERT INTO vec_code_chunks (embedding, chunk_id) VALUES (?, ?)",
            (sqlite_vec.serialize_float32(vector.tolist()), chunk_id),
        )

    def iter_embeddings(self) -> Iterable[tuple[str, np.ndarray]]:
        if self.get_embedding_dim() is None:
            return
        cur = self.conn.execute("SELECT finding_id, embedding FROM vec_findings")
        for row in cur.fetchall():
            yield row["finding_id"], np.frombuffer(row["embedding"], dtype=np.float32)

    def embedding_count(self) -> int:
        if self.get_embedding_dim() is None:
            return 0
        return self.conn.execute("SELECT COUNT(*) AS c FROM vec_findings").fetchone()["c"]

    def iter_endpoint_embeddings(self) -> Iterable[tuple[str, np.ndarray]]:
        if self.get_meta("endpoint_embedding_dim") is None:
            return
        cur = self.conn.execute("SELECT endpoint_id, embedding FROM vec_endpoints")
        for row in cur.fetchall():
            yield row["endpoint_id"], np.frombuffer(row["embedding"], dtype=np.float32)

    def endpoint_embedding_count(self) -> int:
        if self.get_meta("endpoint_embedding_dim") is None:
            return 0
        return self.conn.execute("SELECT COUNT(*) AS c FROM vec_endpoints").fetchone()["c"]

    def knn_search_endpoints(self, query_vec: np.ndarray, top_k: int) -> list[tuple[str, float]]:
        """Plus proches voisins parmi les endpoints indexés (BACKLOG-10 K3),
        même convention que `knn_search` : score = 1 - distance cosinus."""
        if top_k <= 0 or self.get_meta("endpoint_embedding_dim") is None:
            return []
        query_blob = sqlite_vec.serialize_float32(query_vec.astype(np.float32).tolist())
        cur = self.conn.execute(
            "SELECT endpoint_id, distance FROM vec_endpoints "
            "WHERE embedding MATCH ? AND k = ? ORDER BY distance",
            (query_blob, top_k),
        )
        return [(row["endpoint_id"], 1.0 - row["distance"]) for row in cur.fetchall()]

    def code_chunk_embedding_count(self) -> int:
        if self.get_meta("code_embedding_dim") is None:
            return 0
        return self.conn.execute("SELECT COUNT(*) AS c FROM vec_code_chunks").fetchone()["c"]

    def knn_search(self, query_vec: np.ndarray, top_k: int) -> list[tuple[str, float]]:
        """Nearest neighbors by cosine similarity, best first.

        Returns (finding_id, score) pairs, score = 1 - cosine_distance so higher
        is more similar (matches the old brute-force dot-product convention).
        """
        if top_k <= 0 or self.get_embedding_dim() is None:
            return []
        query_blob = sqlite_vec.serialize_float32(query_vec.astype(np.float32).tolist())
        cur = self.conn.execute(
            "SELECT finding_id, distance FROM vec_findings "
            "WHERE embedding MATCH ? AND k = ? ORDER BY distance",
            (query_blob, top_k),
        )
        return [(row["finding_id"], 1.0 - row["distance"]) for row in cur.fetchall()]

    def knn_search_code_chunks(
        self,
        query_vec: np.ndarray,
        top_k: int,
        offset: int = 0,
        language: str | None = None,
        path_glob: str | None = None,
    ) -> list[tuple[CodeChunk, float]]:
        """Nearest neighbors among `code_chunks`, best first.

        `language`/`path_glob` are applied after the KNN fetch (vec0 has no
        native metadata filter), so the fetch over-requests — same pattern as
        `ccc_bridge`'s severity overfetch — to survive filtering and `offset`
        before truncating to `top_k`.
        """
        if top_k <= 0 or self.get_meta("code_embedding_dim") is None:
            return []
        fetch_k = min(
            (offset + top_k) * _CODE_CHUNK_OVERFETCH_FACTOR, _CODE_CHUNK_OVERFETCH_CAP
        )
        query_blob = sqlite_vec.serialize_float32(query_vec.astype(np.float32).tolist())
        cur = self.conn.execute(
            "SELECT chunk_id, distance FROM vec_code_chunks "
            "WHERE embedding MATCH ? AND k = ? ORDER BY distance",
            (query_blob, fetch_k),
        )
        rows = cur.fetchall()
        if not rows:
            return []
        ids = [row["chunk_id"] for row in rows]
        placeholders = ",".join("?" for _ in ids)
        chunk_rows = self.conn.execute(
            f"SELECT * FROM code_chunks WHERE id IN ({placeholders})", ids
        ).fetchall()
        chunks_by_id = {row["id"]: _row_to_code_chunk(row) for row in chunk_rows}
        results: list[tuple[CodeChunk, float]] = []
        for row in rows:
            chunk = chunks_by_id.get(row["chunk_id"])
            if chunk is None:
                continue
            if language and chunk.language != language:
                continue
            if path_glob and not fnmatch.fnmatch(chunk.path, path_glob):
                continue
            results.append((chunk, 1.0 - row["distance"]))
        return results[offset : offset + top_k]

    def counts_by(self, dim: str) -> dict[str, int]:
        if dim not in _COUNTABLE_DIMENSIONS:
            raise ValueError(f"Dimension inconnue : {dim!r}")
        cur = self.conn.execute(
            f"SELECT {dim} AS d, COUNT(*) AS c FROM findings GROUP BY {dim}"
        )
        return {row["d"]: row["c"] for row in cur.fetchall()}

    def _ids_for_paths(self, table: str, column: str, paths: list[str]) -> list[str]:
        rows = self._rows_for_paths(table, paths, columns=column)
        return [row[column] for row in rows]

    def _rows_for_paths(
        self, table: str, paths: list[str], columns: str = "*"
    ) -> list[sqlite3.Row]:
        if not paths:
            return []
        rows: list[sqlite3.Row] = []
        unique_paths = list(dict.fromkeys(paths))
        for chunk in _chunked(unique_paths):
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"SELECT {columns} FROM {table} WHERE path IN ({placeholders}) "
                "ORDER BY path, start_line, end_line, id",
                chunk,
            )
            rows.extend(cur.fetchall())
        return rows

    def _count_rows_for_paths(self, table: str, paths: list[str]) -> int:
        if not paths:
            return 0
        total = 0
        for chunk in _chunked(list(dict.fromkeys(paths))):
            placeholders = ",".join("?" for _ in chunk)
            row = self.conn.execute(
                f"SELECT COUNT(*) AS c FROM {table} WHERE path IN ({placeholders})", chunk
            ).fetchone()
            total += int(row["c"])
        return total

    def _delete_rows_for_paths(self, table: str, paths: list[str]) -> None:
        if not paths:
            return
        for chunk in _chunked(list(dict.fromkeys(paths))):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(f"DELETE FROM {table} WHERE path IN ({placeholders})", chunk)


def _row_to_finding(row: sqlite3.Row) -> Finding:
    return Finding(
        id=row["id"],
        rule_id=row["rule_id"],
        severity=row["severity"],
        message=row["message"],
        path=row["path"],
        start_line=row["start_line"],
        end_line=row["end_line"],
        snippet=row["snippet"],
        fix=row["fix"],
        cwe=json.loads(row["cwe"]) if row["cwe"] else [],
        owasp=json.loads(row["owasp"]) if row["owasp"] else [],
        module=row["module"],
        qualified_name=row["qualified_name"],
    )


def _row_to_code_chunk(row: sqlite3.Row) -> CodeChunk:
    return CodeChunk(
        id=row["id"],
        path=row["path"],
        start_line=row["start_line"],
        end_line=row["end_line"],
        language=row["language"],
        content=row["content"],
    )


def _row_to_endpoint(row: sqlite3.Row) -> MessageEndpoint:
    return MessageEndpoint(
        id=row["id"],
        role=row["role"],
        system=row["system"],
        topic=row["topic"],
        topic_dynamic=bool(row["topic_dynamic"]),
        source=row["source"],
        framework=row["framework"],
        path=row["path"],
        start_line=row["start_line"],
        end_line=row["end_line"],
        snippet=row["snippet"],
        module=row["module"],
        qualified_name=row["qualified_name"],
        message_type=row["message_type"],
    )
