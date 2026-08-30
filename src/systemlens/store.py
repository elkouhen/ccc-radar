import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any

from systemlens.models import ArchitectureRelation, ExtractionDiagnostic, Finding, GraphFact, MessageEndpoint
from systemlens.modules import (
    BlockingPoint,
    DiscoveredModule,
    KafkaMethod,
    ModuleDependency,
    MongoMethod,
    MongoField,
    MongoPersistenceClass,
    SourceEvidence,
)
from systemlens.kubernetes import KubernetesWorkload
from systemlens.paths import db_path

SCHEMA_VERSION = "26"
SEVERITY_ORDER = ["INFO", "WARNING", "ERROR"]
_COUNTABLE_DIMENSIONS = ("rule_id", "severity")
_SQLITE_BIND_LIMIT = 900


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


def _evidence_from_json(data: dict[str, Any]) -> SourceEvidence | None:
    evidence = data.pop("evidence", None)
    return SourceEvidence(**evidence) if evidence else None


def _mongo_method_from_json(data: dict[str, Any]) -> MongoMethod:
    data = dict(data)
    evidence = _evidence_from_json(data)
    return MongoMethod(**data, evidence=evidence)


def _mongo_persistence_class_from_json(data: dict[str, Any]) -> MongoPersistenceClass:
    data = dict(data)
    data["fields"] = tuple(MongoField(
        **{**field, "references": tuple(field.get("references", []))}
    ) for field in data.get("fields", []))
    return MongoPersistenceClass(**data)


def _kafka_method_from_json(data: dict[str, Any]) -> KafkaMethod:
    data = dict(data)
    evidence = _evidence_from_json(data)
    return KafkaMethod(**data, evidence=evidence)


def _blocking_point_from_json(data: dict[str, Any]) -> BlockingPoint:
    data = dict(data)
    evidence = _evidence_from_json(data)
    return BlockingPoint(**data, evidence=evidence)


def _kubernetes_workload_from_json(data: dict[str, Any]) -> KubernetesWorkload:
    return KubernetesWorkload(**data)


class Store:
    def __init__(self, repo_root: Path, readonly: bool = False) -> None:
        self._repo_root = Path(repo_root).resolve()
        self._db_path = db_path(self._repo_root)
        self._conn: sqlite3.Connection | None = None
        self._readonly = readonly
        self._transaction_active = False

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
            self._check_schema_compatible()
            return self

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.row_factory = sqlite3.Row
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

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Atomically publish one architecture snapshot.

        Schema creation happens when the writable store is opened. All domain
        mutations for an index run must happen within this boundary, so an
        exception never exposes a mixture of old and new inventory facts.
        """
        if self._readonly:
            raise StoreError("Une transaction n'est pas disponible en lecture seule.")
        if self._transaction_active:
            raise StoreError("Les transactions Store imbriquées ne sont pas prises en charge.")
        self._transaction_active = True
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
        finally:
            self._transaction_active = False

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
                f"attendu {SCHEMA_VERSION!r} — relancez systemlens index sur ce projet."
            )

    @property
    def conn(self) -> sqlite3.Connection:
        assert self._conn is not None, "Store doit être utilisé comme context manager"
        return self._conn

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
                identity TEXT,
                build_system TEXT NOT NULL,
                version TEXT,
                kind TEXT NOT NULL,
                starts_application INTEGER NOT NULL DEFAULT 0,
                configuration_example TEXT NOT NULL,
                application_entrypoint TEXT,
                mongo_collections TEXT NOT NULL DEFAULT '[]',
                mongo_methods TEXT NOT NULL DEFAULT '[]',
                mongo_persistence_classes TEXT NOT NULL DEFAULT '[]',
                openapi_files TEXT NOT NULL DEFAULT '[]',
                kafka_methods TEXT NOT NULL DEFAULT '[]',
                blocking_points TEXT NOT NULL DEFAULT '[]',
                kubernetes_workloads TEXT NOT NULL DEFAULT '[]'
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
            CREATE TABLE IF NOT EXISTS extraction_diagnostics (
                path TEXT NOT NULL,
                extractor TEXT NOT NULL,
                category TEXT NOT NULL,
                severity TEXT NOT NULL,
                detail TEXT NOT NULL,
                PRIMARY KEY (path, extractor, category)
            );
            CREATE TABLE IF NOT EXISTS kafka_dto_definitions (
                id TEXT PRIMARY KEY,
                definition TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS openapi_contracts (
                module TEXT NOT NULL,
                path TEXT NOT NULL,
                spec TEXT NOT NULL,
                PRIMARY KEY (module, path)
            );
            CREATE TABLE IF NOT EXISTS graph_facts (
                id TEXT PRIMARY KEY,
                fact_type TEXT NOT NULL,
                kind TEXT NOT NULL,
                name TEXT,
                source_kind TEXT,
                source_name TEXT,
                target_kind TEXT,
                target_name TEXT,
                relation TEXT,
                origin TEXT NOT NULL,
                confidence TEXT NOT NULL,
                evidence_path TEXT,
                evidence_line INTEGER,
                note TEXT,
                technology TEXT,
                metadata TEXT NOT NULL DEFAULT '{}'
                ,namespace TEXT NOT NULL DEFAULT 'manual'
                ,status TEXT NOT NULL DEFAULT 'confirmed'
                ,pass_id TEXT
                ,source_revision TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_graph_facts_type ON graph_facts(fact_type);
            """
        )
        self._migrate_module_columns()
        self._migrate_endpoint_message_type()
        self._migrate_module_architecture_columns()
        self._migrate_module_identity()
        self._migrate_graph_fact_columns()
        if self.get_meta("schema_version") != SCHEMA_VERSION:
            self.set_meta("schema_version", SCHEMA_VERSION)
        self.conn.commit()

    def _migrate_module_columns(self) -> None:
        """Schema v4 -> v5 (BACKLOG-13 M1) : `module`/`qualified_name`
        ajoutés à `findings`/`endpoints`, purement additifs (`NULL` pour les
        lignes existantes jusqu'au prochain `systemlens index` qui les
        recalculera)."""
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
        for name in (
            "starts_application", "application_entrypoint", "mongo_collections",
            "mongo_methods", "mongo_persistence_classes", "openapi_files",
            "kafka_methods", "blocking_points", "rest_controllers",
            "openapi_generated_clients",
            "kubernetes_workloads",
        ):
            if name not in cols:
                if name == "application_entrypoint":
                    self.conn.execute("ALTER TABLE modules ADD COLUMN application_entrypoint TEXT")
                    continue
                default = "0" if name == "starts_application" else "'[]'"
                column_type = "INTEGER" if name == "starts_application" else "TEXT"
                self.conn.execute(f"ALTER TABLE modules ADD COLUMN {name} {column_type} NOT NULL DEFAULT {default}")

    def _migrate_module_identity(self) -> None:
        cols = {row["name"] for row in self.conn.execute("PRAGMA table_info(modules)")}
        if "identity" not in cols:
            self.conn.execute("ALTER TABLE modules ADD COLUMN identity TEXT")
        self.conn.execute("UPDATE modules SET identity = name WHERE identity IS NULL OR identity = ''")

    def _migrate_endpoint_message_type(self) -> None:
        """Schema v12 -> v13: payload Java type on Kafka integration sites."""
        cols = {row["name"] for row in self.conn.execute("PRAGMA table_info(endpoints)")}
        if "message_type" not in cols:
            self.conn.execute("ALTER TABLE endpoints ADD COLUMN message_type TEXT")

    def _migrate_graph_fact_columns(self) -> None:
        cols = {row["name"] for row in self.conn.execute("PRAGMA table_info(graph_facts)")}
        if "technology" not in cols:
            self.conn.execute("ALTER TABLE graph_facts ADD COLUMN technology TEXT")
        if "metadata" not in cols:
            self.conn.execute("ALTER TABLE graph_facts ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'")
        if "namespace" not in cols:
            self.conn.execute("ALTER TABLE graph_facts ADD COLUMN namespace TEXT NOT NULL DEFAULT 'manual'")
        if "status" not in cols:
            self.conn.execute("ALTER TABLE graph_facts ADD COLUMN status TEXT NOT NULL DEFAULT 'confirmed'")
        if "pass_id" not in cols:
            self.conn.execute("ALTER TABLE graph_facts ADD COLUMN pass_id TEXT")
        if "source_revision" not in cols:
            self.conn.execute("ALTER TABLE graph_facts ADD COLUMN source_revision TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_graph_facts_namespace ON graph_facts(namespace)")

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

    def delete_meta(self, key: str) -> None:
        """Remove obsolete metadata from the current index snapshot."""
        self.conn.execute("DELETE FROM meta WHERE key = ?", (key,))

    def replace_kafka_dto_definitions(self, definitions: list[dict[str, object]]) -> None:
        self.conn.execute("DELETE FROM kafka_dto_definitions")
        self.conn.executemany(
            "INSERT INTO kafka_dto_definitions (id, definition) VALUES (?, ?)",
            [
                (str(definition["id"]), json.dumps(definition))
                for definition in definitions
            ],
        )

    def all_kafka_dto_definitions(self) -> list[dict[str, object]]:
        return [
            json.loads(row["definition"])
            for row in self.conn.execute(
                "SELECT definition FROM kafka_dto_definitions ORDER BY id"
            )
        ]

    def replace_openapi_contracts(self, contracts: list[dict[str, object]]) -> None:
        self.conn.execute("DELETE FROM openapi_contracts")
        self.conn.executemany(
            "INSERT INTO openapi_contracts (module, path, spec) VALUES (?, ?, ?)",
            [
                (str(contract["module"]), str(contract["path"]), json.dumps(contract["spec"]))
                for contract in contracts
            ],
        )

    def all_openapi_contracts(self) -> list[dict[str, object]]:
        return [
            {"module": row["module"], "path": row["path"], "spec": json.loads(row["spec"])}
            for row in self.conn.execute(
                "SELECT module, path, spec FROM openapi_contracts ORDER BY module, path"
            )
        ]

    # -- modules --

    def replace_modules(self, modules: list[DiscoveredModule]) -> None:
        """Persist the build inventory produced during `systemlens index`."""
        self.conn.execute("DELETE FROM modules")
        self.conn.execute("DELETE FROM module_dependencies")
        for module in modules:
            relative_path = module.path.resolve().relative_to(self._repo_root).as_posix()
            self.conn.execute(
                """
                INSERT INTO modules (path, name, identity, build_system, version, kind, starts_application, configuration_example, application_entrypoint,
                                     mongo_collections, mongo_methods, mongo_persistence_classes, openapi_files, kafka_methods, blocking_points, rest_controllers, openapi_generated_clients, kubernetes_workloads)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    relative_path,
                    module.name,
                    module.identity or module.name,
                    module.build_system,
                    module.version,
                    module.kind,
                    int(module.starts_application),
                    module.configuration_example,
                    json.dumps(module.application_entrypoint.__dict__) if module.application_entrypoint else None,
                    json.dumps(module.mongo_collections),
                    json.dumps([_method_to_json(method) for method in module.mongo_methods]),
                    json.dumps([{
                        **item.__dict__,
                        "fields": [field.__dict__ for field in item.fields],
                    } for item in module.mongo_persistence_classes]),
                    json.dumps(module.openapi_files),
                    json.dumps([_method_to_json(method) for method in module.kafka_methods]),
                    json.dumps([_method_to_json(point) for point in module.blocking_points]),
                    json.dumps(module.rest_controllers),
                    json.dumps(module.openapi_generated_clients),
                    json.dumps([workload.__dict__ for workload in module.kubernetes_workloads]),
                ),
            )

    def all_modules(self) -> list[DiscoveredModule]:
        rows = self.conn.execute(
            "SELECT path, name, identity, build_system, version, kind, starts_application, configuration_example, application_entrypoint, mongo_collections, mongo_methods, mongo_persistence_classes, openapi_files, kafka_methods, blocking_points, rest_controllers, openapi_generated_clients, kubernetes_workloads "
            "FROM modules ORDER BY path"
        ).fetchall()
        return [
            DiscoveredModule(
                name=row["name"],
                identity=row["identity"] or row["name"],
                path=self._repo_root / row["path"],
                build_system=row["build_system"],
                version=row["version"],
                kind=row["kind"],
                starts_application=bool(row["starts_application"]),
                configuration_example=row["configuration_example"],
                application_entrypoint=SourceEvidence(**json.loads(row["application_entrypoint"])) if row["application_entrypoint"] else None,
                mongo_collections=tuple(json.loads(row["mongo_collections"])),
                mongo_methods=tuple(_mongo_method_from_json(method) for method in json.loads(row["mongo_methods"])),
                mongo_persistence_classes=tuple(_mongo_persistence_class_from_json(item) for item in json.loads(row["mongo_persistence_classes"])),
                openapi_files=tuple(json.loads(row["openapi_files"])),
                kafka_methods=tuple(_kafka_method_from_json(method) for method in json.loads(row["kafka_methods"])),
                blocking_points=tuple(_blocking_point_from_json(point) for point in json.loads(row["blocking_points"])),
                rest_controllers=tuple(json.loads(row["rest_controllers"])),
                openapi_generated_clients=tuple(json.loads(row["openapi_generated_clients"])),
                kubernetes_workloads=tuple(_kubernetes_workload_from_json(item) for item in json.loads(row["kubernetes_workloads"])),
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

    # -- AI/user graph facts --

    def upsert_graph_fact(self, fact: GraphFact) -> None:
        self.conn.execute(
            """INSERT INTO graph_facts
            (id, fact_type, kind, name, source_kind, source_name, target_kind,
            target_name, relation, origin, confidence, evidence_path,
             evidence_line, note, technology, metadata, namespace, status,
             pass_id, source_revision)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET fact_type=excluded.fact_type,
            kind=excluded.kind, name=excluded.name, source_kind=excluded.source_kind,
            source_name=excluded.source_name, target_kind=excluded.target_kind,
            target_name=excluded.target_name, relation=excluded.relation,
            origin=excluded.origin, confidence=excluded.confidence,
            evidence_path=excluded.evidence_path, evidence_line=excluded.evidence_line,
            note=excluded.note, technology=excluded.technology, metadata=excluded.metadata,
            namespace=excluded.namespace, status=excluded.status,
            pass_id=excluded.pass_id, source_revision=excluded.source_revision""",
            (fact.id, fact.fact_type, fact.kind, fact.name, fact.source_kind,
             fact.source_name, fact.target_kind, fact.target_name, fact.relation,
             fact.origin, fact.confidence, fact.evidence_path, fact.evidence_line,
             fact.note, fact.technology, json.dumps(fact.metadata or {}), fact.namespace,
             fact.status, fact.pass_id, fact.source_revision),
        )

    def insert_graph_fact(self, fact: GraphFact) -> bool:
        """Insert one enrichment fact without replacing an existing assertion."""
        before = self.conn.total_changes
        self.conn.execute(
            """INSERT OR IGNORE INTO graph_facts
            (id, fact_type, kind, name, source_kind, source_name, target_kind,
             target_name, relation, origin, confidence, evidence_path,
             evidence_line, note, technology, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (fact.id, fact.fact_type, fact.kind, fact.name, fact.source_kind,
             fact.source_name, fact.target_kind, fact.target_name, fact.relation,
             fact.origin, fact.confidence, fact.evidence_path, fact.evidence_line,
             fact.note, fact.technology, json.dumps(fact.metadata or {})),
        )
        return self.conn.total_changes > before

    def graph_fact_by_id(self, fact_id: str) -> GraphFact | None:
        row = self.conn.execute("SELECT * FROM graph_facts WHERE id = ?", (fact_id,)).fetchone()
        if row is None:
            return None
        return GraphFact(**{**dict(row), "metadata": json.loads(row["metadata"])})

    def delete_graph_fact(self, fact_id: str) -> bool:
        cur = self.conn.execute("DELETE FROM graph_facts WHERE id = ?", (fact_id,))
        return cur.rowcount > 0

    def all_graph_facts(self) -> list[GraphFact]:
        rows = self.conn.execute("SELECT * FROM graph_facts ORDER BY id").fetchall()
        return [GraphFact(**{**dict(row), "metadata": json.loads(row["metadata"])}) for row in rows]

    def graph_facts_by_namespace(self, namespace: str) -> list[GraphFact]:
        rows = self.conn.execute(
            "SELECT * FROM graph_facts WHERE namespace = ? ORDER BY id", (namespace,)
        ).fetchall()
        return [GraphFact(**{**dict(row), "metadata": json.loads(row["metadata"])}) for row in rows]

    def delete_graph_facts_not_in(self, namespace: str, fact_ids: set[str]) -> int:
        """Remove stale enrichment facts from one complete manifest namespace."""
        if fact_ids:
            placeholders = ", ".join("?" for _ in fact_ids)
            params: list[object] = [namespace, *sorted(fact_ids)]
            cur = self.conn.execute(
                f"DELETE FROM graph_facts WHERE namespace = ? AND id NOT IN ({placeholders})",
                params,
            )
        else:
            cur = self.conn.execute("DELETE FROM graph_facts WHERE namespace = ?", (namespace,))
        return cur.rowcount

    # -- extraction diagnostics --

    def replace_extraction_diagnostics_for_files(
        self, paths: list[str], diagnostics: list[ExtractionDiagnostic]
    ) -> None:
        for chunk in _chunked(paths):
            placeholders = ", ".join("?" for _ in chunk)
            self.conn.execute(f"DELETE FROM extraction_diagnostics WHERE path IN ({placeholders})", chunk)
        self.conn.executemany(
            "INSERT INTO extraction_diagnostics (path, extractor, category, severity, detail) VALUES (?, ?, ?, ?, ?)",
            [(item.path, item.extractor, item.category, item.severity, item.detail) for item in diagnostics],
        )

    def all_extraction_diagnostics(self) -> list[ExtractionDiagnostic]:
        rows = self.conn.execute(
            "SELECT path, extractor, category, severity, detail FROM extraction_diagnostics ORDER BY path, extractor, category"
        ).fetchall()
        return [ExtractionDiagnostic(**dict(row)) for row in rows]

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
        self._delete_rows_for_paths("findings", paths)
        self.replace_code_chunks_for_files(paths, [])
        self.replace_endpoints_for_files(paths, [])
        self.replace_extraction_diagnostics_for_files(paths, [])

    # -- findings --

    def count_findings_for_paths(self, paths: list[str]) -> int:
        return self._count_rows_for_paths("findings", paths)

    def clear_findings_once(self, migration_key: str) -> int:
        """Remove results from a retired analyzer exactly once per index.

        AST-only indexing must not present stale external-analyzer results,
        even when their source files are unchanged.
        """
        if self.get_meta(migration_key) == "1":
            return 0
        rows = self.conn.execute("SELECT id FROM findings").fetchall()
        self.conn.execute("DELETE FROM findings")
        self.set_meta(migration_key, "1")
        return len(rows)

    def replace_findings_for_files(self, paths: list[str], findings: list[Finding]) -> None:
        if paths:
            self._delete_rows_for_paths("findings", paths)
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

    # -- indexed code chunks --

    def replace_code_chunks_for_files(
        self, paths: list[str], chunks: list[CodeChunk]
    ) -> None:
        if paths:
            self._delete_rows_for_paths("code_chunks", paths)
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

    def count_endpoints_for_paths(self, paths: list[str]) -> int:
        return self._count_rows_for_paths("endpoints", paths)

    def replace_endpoints_for_files(
        self, paths: list[str], endpoints: list[MessageEndpoint]
    ) -> None:
        if paths:
            self._delete_rows_for_paths("endpoints", paths)
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
