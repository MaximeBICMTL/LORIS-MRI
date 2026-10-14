"""Generic, verified backups of selected logical resources."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import shutil
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, TextIO, cast
from uuid import uuid4

from sqlalchemy import Column, Table, select
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect, Engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from loris_data_manager.provider import ResourceModel, ResourceSchema
from loris_data_manager.resources import LocalPathRef, LocalPathType
from loris_data_manager.schema import (
    BatchLinkSource,
    LifecycleSemantics,
    LocalPathColumnSource,
    OrmEntitySource,
    PropertyCriterion,
)

BACKUP_FORMAT = "loris-resource-backup"
BACKUP_FORMAT_VERSION = 1
COPY_BUFFER_SIZE = 1024 * 1024
ROW_BATCH_SIZE = 500
MYSQL_RESTORE_DIALECT = mysql.dialect(paramstyle="named")


@dataclass(frozen=True, slots=True)
class BackupSelection:
    """One logical object kind selected for an operation."""

    kind: str
    criteria: tuple[PropertyCriterion, ...]
    where_expressions: tuple[str, ...] = ()
    select_all: bool = False

    def __post_init__(self) -> None:
        if not self.kind:
            raise ValueError("A backup selection requires an object kind")
        if not self.select_all and not self.criteria:
            raise ValueError("At least one selector is required unless select_all is enabled")


@dataclass(frozen=True, slots=True)
class BackupRequest:
    """Destination and environment for a logical-object backup."""

    output: Path | None
    selection: BackupSelection
    storage_roots: Mapping[str, Path]


@dataclass(frozen=True, slots=True)
class BackupResult:
    """Summary of a finalized and verified backup artifact."""

    output: Path
    operation_id: str
    kind: str
    selected_object_count: int
    database_row_count: int
    file_count: int
    blocker_count: int


@dataclass(frozen=True, slots=True)
class FileBackup:
    """File identity extracted while streaming an owned database table."""

    object_kind: str
    object_identity: Column[Any]
    storage_root: str
    path_attribute: Column[Any]


@dataclass(frozen=True, slots=True)
class DatabaseTableBackup:
    """Ordered rows restored by a backup, with optional owned files."""

    table: Table
    statement: Select[Any]
    action: str
    counts_selected_objects: bool = False
    files: tuple[FileBackup, ...] = ()


@dataclass(frozen=True, slots=True)
class DatabaseReferenceCheck:
    """Rows outside the backup closure that reference selected objects."""

    table: Table
    statement: Select[Any]
    reference_column: Column[Any]
    action: str


@dataclass(frozen=True, slots=True)
class BackupPlan:
    """Kind-specific physical capture plan consumed by the generic executor."""

    kind: str
    tables: tuple[DatabaseTableBackup, ...]
    references: tuple[DatabaseReferenceCheck, ...] = ()

    def __post_init__(self) -> None:
        selected_tables = sum(table.counts_selected_objects for table in self.tables)
        if selected_tables != 1:
            raise ValueError("A backup plan must have exactly one selected-object table")


@dataclass(frozen=True, slots=True)
class _PendingFile:
    object_kind: str
    object_id: str
    storage_root: str
    relative_path: PurePosixPath


def plan_backup(schema: ResourceSchema, selection: BackupSelection) -> BackupPlan:
    """Lower a semantic selection and its lifecycle bindings to physical queries."""

    provider = schema.provider(selection.kind)
    anchor_table = _model_table(provider.orm_model)
    anchor_identity = _single_primary_key(anchor_table, selection.kind)
    selected_statement = ResourceModel(schema).plan_selection(
        selection.kind,
        criteria=selection.criteria,
    )

    def selected_values(attribute: Any, name: str):
        attribute_model = getattr(attribute, "class_", provider.orm_model)
        if attribute_model is not provider.orm_model:
            raise ValueError(
                f"Set source for {selection.kind}.{name} does not belong to its provider"
            )
        return (
            selected_statement.with_only_columns(attribute)
            .order_by(None)
            .distinct()
            .subquery(f"selected_{selection.kind.replace('-', '_')}_{name.replace('-', '_')}")
        )

    selected_ids = selected_values(anchor_identity, "identities")
    tables: list[DatabaseTableBackup] = []
    references: list[DatabaseReferenceCheck] = []
    anchor_files: list[FileBackup] = []

    for link in schema.links(selection.kind):
        if link.lifecycle is LifecycleSemantics.OWNS:
            if isinstance(link.source, OrmEntitySource):
                if link.target_kind != "database-row":
                    raise ValueError(f"Owned entity link {selection.kind}.{link.name} is not a database row")
                continue
            if isinstance(link.source, LocalPathColumnSource):
                schema.storage_root(link.source.storage_root)
                if link.source.expected_type is not LocalPathType.FILE:
                    raise ValueError(
                        f"Owned path {selection.kind}.{link.name} is not a regular file"
                    )
                path_column = _mapped_column(link.source.attribute)
                if link.source.attribute.class_ is not provider.orm_model:
                    raise ValueError(
                        f"Path source for {selection.kind}.{link.name} does not belong to its provider"
                    )
                anchor_files.append(
                    FileBackup(
                        object_kind=selection.kind,
                        object_identity=anchor_identity,
                        storage_root=link.source.storage_root,
                        path_attribute=path_column,
                    )
                )
                continue
            if isinstance(link.source, BatchLinkSource):
                tables.append(
                    _plan_batch_table(
                        selected_values,
                        selection.kind,
                        link.name,
                        link.source,
                        action="delete",
                    )
                )
                continue
            raise ValueError(
                f"Owned link {selection.kind}.{link.name} has no set-oriented resource source"
            )

        if (
            link.lifecycle is LifecycleSemantics.REFERENCES
            and link.target_kind == "database-row"
        ):
            if not isinstance(link.source, BatchLinkSource):
                raise ValueError(
                    f"Database reference {selection.kind}.{link.name} has no set-oriented source"
                )
            table_plan = _plan_batch_table(
                selected_values,
                selection.kind,
                link.name,
                link.source,
                action="block",
            )
            target_attribute = link.source.set_target_attribute
            assert target_attribute is not None
            references.append(
                DatabaseReferenceCheck(
                    table=table_plan.table,
                    statement=table_plan.statement,
                    reference_column=_mapped_column(target_attribute),
                    action="block",
                )
            )

    anchor_statement = (
        select(anchor_table)
        .join(selected_ids, anchor_identity == next(iter(selected_ids.c)))
        .order_by(*anchor_table.primary_key.columns)
    )
    tables.insert(
        0,
        DatabaseTableBackup(
            table=anchor_table,
            statement=anchor_statement,
            action="delete",
            counts_selected_objects=True,
            files=tuple(anchor_files),
        ),
    )

    for supplement in schema.unmodeled_database_references:
        if supplement.source_kind != selection.kind:
            continue
        source_values = selected_values(supplement.source_attribute, "legacy-references")
        reference_column = _mapped_column(supplement.reference_attribute)
        table = _model_table(supplement.reference_attribute.class_)
        references.append(
            DatabaseReferenceCheck(
                table=table,
                statement=(
                    select(table)
                    .join(source_values, reference_column == next(iter(source_values.c)))
                    .order_by(*table.primary_key.columns)
                ),
                reference_column=reference_column,
                action="block",
            )
        )

    return BackupPlan(
        kind=selection.kind,
        tables=_order_database_tables(tuple(tables)),
        references=tuple(references),
    )


def required_storage_roots(schema: ResourceSchema, kind: str) -> tuple[str, ...]:
    """Return storage roots reached by owned local-path links of an object kind."""

    schema.provider(kind)
    return tuple(
        dict.fromkeys(
            link.source.storage_root
            for link in schema.links(kind)
            if link.lifecycle is LifecycleSemantics.OWNS
            and isinstance(link.source, LocalPathColumnSource)
        )
    )


def _plan_batch_table(
    selected_values: Any,
    kind: str,
    member_name: str,
    source: BatchLinkSource,
    *,
    action: str,
) -> DatabaseTableBackup:
    source_attribute = source.set_source_attribute
    target_attribute = source.set_target_attribute
    if source_attribute is None or target_attribute is None:
        raise ValueError(f"Link {kind}.{member_name} has no set-oriented mapping")
    values = selected_values(source_attribute, member_name)
    table = _model_table(target_attribute.class_)
    target_column = _mapped_column(target_attribute)
    return DatabaseTableBackup(
        table=table,
        statement=(
            select(table)
            .join(values, target_column == next(iter(values.c)))
            .order_by(*table.primary_key.columns)
        ),
        action=action,
    )


def _model_table(model: type[Any]) -> Table:
    table = getattr(model, "__table__", None)
    if not isinstance(table, Table):
        raise ValueError(f"ORM model {model.__name__!r} does not map to one physical table")
    return cast(Table, table)


def _mapped_column(attribute: Any) -> Column[Any]:
    columns = tuple(attribute.property.columns)
    if len(columns) != 1:
        raise ValueError(f"Mapped attribute {attribute} does not use exactly one column")
    return columns[0]


def _single_primary_key(table: Table, kind: str) -> Column[Any]:
    columns = tuple(table.primary_key.columns)
    if len(columns) != 1:
        raise ValueError(
            f"Logical object kind {kind!r} requires a single-column ORM identity for backup"
        )
    return columns[0]


def _order_database_tables(
    plans: tuple[DatabaseTableBackup, ...],
) -> tuple[DatabaseTableBackup, ...]:
    """Put parent tables before captured children using declared foreign keys."""

    by_table = {plan.table: plan for plan in plans}
    if len(by_table) != len(plans):
        raise ValueError("An ownership closure reaches the same database table more than once")
    ordered: list[DatabaseTableBackup] = []
    remaining = list(plans)
    while remaining:
        ready = [
            plan
            for plan in remaining
            if all(
                foreign_key.column.table not in by_table
                or foreign_key.column.table in {item.table for item in ordered}
                for foreign_key in plan.table.foreign_keys
            )
        ]
        if not ready:
            names = sorted(plan.table.fullname for plan in remaining)
            raise ValueError(f"Owned database tables contain a dependency cycle: {names}")
        for plan in ready:
            ordered.append(plan)
            remaining.remove(plan)
    return tuple(ordered)


def create_backup(
    engine: Engine,
    schema: ResourceSchema,
    request: BackupRequest,
) -> BackupResult:
    """Stream a selected resource backup into a verified, atomically finalized directory."""

    selection = request.selection
    schema.object_kind(selection.kind)
    if not schema.has_provider(selection.kind):
        raise ValueError(f"No provider is registered for object kind {selection.kind!r}")
    required_roots = required_storage_roots(schema, selection.kind)
    missing_roots = {
        name for name in required_roots if name not in request.storage_roots
    }
    if missing_roots:
        raise ValueError(f"Missing storage roots required by resource schema: {sorted(missing_roots)}")
    plan = plan_backup(schema, selection)
    if plan.kind != selection.kind:
        raise ValueError(
            f"Resource planner for {selection.kind!r} produced kind {plan.kind!r}"
        )

    operation_id = str(uuid4())
    output = (
        request.output
        if request.output is not None
        else Path(f"loris-backup-{operation_id}")
    ).resolve()
    if output.exists():
        raise ValueError(f"Backup destination already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))

    try:
        sql_path = staging / "restore.sql"
        pending_files: list[_PendingFile] = []
        table_entries: list[dict[str, object]] = []
        database_row_count = 0
        selected_object_count = 0

        connection = engine.connect()
        try:
            if engine.dialect.name == "mysql":
                connection = connection.execution_options(isolation_level="REPEATABLE READ")
            with Session(connection) as db:
                with sql_path.open("w", encoding="utf-8", newline="\n") as restore_sql:
                    restore_sql.write(
                        f"-- LORIS {selection.kind} restoration patch\n"
                        f"-- Backup operation: {operation_id}\n"
                        "-- Restore files before applying this transaction.\n\n"
                        "START TRANSACTION;\n\n"
                    )
                    for table_plan in plan.tables:
                        entry, files = _write_database_table(db, table_plan, restore_sql)
                        table_entries.append(entry)
                        row_count = cast(int, entry["row_count"])
                        database_row_count += row_count
                        if table_plan.counts_selected_objects:
                            selected_object_count = row_count
                        pending_files.extend(files)
                    restore_sql.write("COMMIT;\n")
                    restore_sql.flush()
                    os.fsync(restore_sql.fileno())

                reference_entries = tuple(
                    _describe_references(db, reference) for reference in plan.references
                )
        finally:
            connection.close()

        if selected_object_count == 0:
            raise ValueError(f"The backup selection matched no {selection.kind} objects")

        # The consistent database snapshot is deliberately closed before large file copies.
        file_entries = _copy_files(request.storage_roots, staging, pending_files)
        blocker_count = sum(cast(int, entry["row_count"]) for entry in reference_entries)
        manifest: dict[str, object] = {
            "format": BACKUP_FORMAT,
            "format_version": BACKUP_FORMAT_VERSION,
            "operation_id": operation_id,
            "created_at": datetime.now(UTC).isoformat(),
            "planner": {
                "name": "resource-ownership",
                "version": 1,
                "object_kind": plan.kind,
                "reference_inventory": "schema-with-legacy-supplements",
                "unknown_database_references_checked": False,
            },
            "selection": {
                "kind": selection.kind,
                "all": selection.select_all,
                "where": list(selection.where_expressions),
                "matched_count": selected_object_count,
            },
            "database": {
                "dialect": "mysql",
                "restore_patch": "restore.sql",
                "restore_patch_sha256": _hash_file(sql_path),
                "row_count": database_row_count,
                "tables": table_entries,
            },
            "files": file_entries,
            "references": list(reference_entries),
            "prospective_removal": {
                "eligible": blocker_count == 0,
                "blocker_count": blocker_count,
            },
        }
        manifest_path = staging / "manifest.json"
        with manifest_path.open("w", encoding="utf-8", newline="\n") as manifest_file:
            json.dump(manifest, manifest_file, indent=2, sort_keys=True)
            manifest_file.write("\n")
            manifest_file.flush()
            os.fsync(manifest_file.fileno())

        verify_backup(staging)
        os.replace(staging, output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    return BackupResult(
        output=output,
        operation_id=operation_id,
        kind=selection.kind,
        selected_object_count=selected_object_count,
        database_row_count=database_row_count,
        file_count=len(file_entries),
        blocker_count=blocker_count,
    )


def verify_backup(path: Path) -> Mapping[str, object]:
    """Verify the manifest and payload hashes of a generic backup artifact."""

    artifact = path.resolve()
    manifest_path = artifact / "manifest.json"
    try:
        loaded_manifest = cast(object, json.loads(manifest_path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Cannot read backup manifest at {manifest_path}") from error
    if not isinstance(loaded_manifest, dict):
        raise ValueError("Backup manifest must be a JSON object")
    manifest = cast(dict[str, object], loaded_manifest)
    if manifest.get("format") != BACKUP_FORMAT or manifest.get("format_version") != BACKUP_FORMAT_VERSION:
        raise ValueError("Unsupported resource backup format")

    loaded_database = manifest.get("database")
    if not isinstance(loaded_database, dict):
        raise ValueError("Backup manifest has no database payload")
    database = cast(dict[str, object], loaded_database)
    restore_patch = database.get("restore_patch")
    restore_hash = database.get("restore_patch_sha256")
    if not isinstance(restore_patch, str) or not isinstance(restore_hash, str):
        raise ValueError("Backup manifest has an invalid restoration patch")
    _verify_payload(artifact, PurePosixPath(restore_patch), restore_hash)

    loaded_files = manifest.get("files")
    if not isinstance(loaded_files, list):
        raise ValueError("Backup manifest has an invalid file list")
    files = cast(list[object], loaded_files)
    for loaded_entry in files:
        if not isinstance(loaded_entry, dict):
            raise ValueError("Backup manifest has an invalid file entry")
        entry = cast(dict[str, object], loaded_entry)
        payload = entry.get("payload")
        digest = entry.get("sha256")
        size = entry.get("size")
        if not isinstance(payload, str) or not isinstance(digest, str) or not isinstance(size, int):
            raise ValueError("Backup manifest has an invalid file entry")
        payload_path = _verify_payload(artifact, PurePosixPath(payload), digest)
        if payload_path.stat().st_size != size:
            raise ValueError(f"Backup payload has the wrong size: {payload}")
    return manifest


def _write_database_table(
    db: Session,
    plan: DatabaseTableBackup,
    restore_sql: TextIO,
) -> tuple[dict[str, object], list[_PendingFile]]:
    digest = hashlib.sha256()
    row_count = 0
    pending_files: list[_PendingFile] = []
    restore_sql.write(f"-- {plan.table.fullname}\n")
    result = db.execute(plan.statement.execution_options(yield_per=ROW_BATCH_SIZE)).mappings()
    for row in result:
        values = {column.key: row[column.key] for column in plan.table.columns}
        _update_row_digest(digest, plan.table, values)
        restore_sql.write(f"{_render_insert(plan.table, values)};\n")
        row_count += 1
        for file_plan in plan.files:
            raw_path = values[file_plan.path_attribute.key]
            if raw_path is not None:
                pending_files.append(
                    _PendingFile(
                        object_kind=file_plan.object_kind,
                        object_id=str(values[file_plan.object_identity.key]),
                        storage_root=file_plan.storage_root,
                        relative_path=_validated_relative_path(file_plan.storage_root, raw_path),
                    )
                )
    restore_sql.write("\n")
    return (
        {
            "table": plan.table.fullname,
            "action": plan.action,
            "row_count": row_count,
            "content_sha256": digest.hexdigest(),
            "order_by": [column.name for column in plan.table.primary_key.columns],
        },
        pending_files,
    )


def _describe_references(
    db: Session,
    plan: DatabaseReferenceCheck,
) -> dict[str, object]:
    digest = hashlib.sha256()
    row_count = 0
    result = db.execute(plan.statement.execution_options(yield_per=ROW_BATCH_SIZE)).mappings()
    for row in result:
        values = {column.key: row[column.key] for column in plan.table.columns}
        _update_row_digest(digest, plan.table, values)
        row_count += 1
    return {
        "table": plan.table.fullname,
        "column": plan.reference_column.name,
        "action": plan.action,
        "row_count": row_count,
        "content_sha256": digest.hexdigest(),
    }


def _copy_files(
    storage_roots: Mapping[str, Path],
    staging: Path,
    pending_files: list[_PendingFile],
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    seen: set[tuple[str, PurePosixPath]] = set()
    for pending in pending_files:
        identity = (pending.storage_root, pending.relative_path)
        if identity in seen:
            raise ValueError(
                f"Multiple selected objects own the same path: "
                f"{pending.storage_root}:{pending.relative_path}"
            )
        seen.add(identity)
        source = storage_roots[pending.storage_root].joinpath(*pending.relative_path.parts)
        payload = PurePosixPath("files", pending.storage_root, *pending.relative_path.parts)
        destination = staging.joinpath(*payload.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        size, digest, source_was_symlink = _copy_verified_regular_file(source, destination)
        entries.append(
            {
                "object": {"kind": pending.object_kind, "id": pending.object_id},
                "storage_root": pending.storage_root,
                "relative_path": str(pending.relative_path),
                "source_was_symlink": source_was_symlink,
                "payload": str(payload),
                "size": size,
                "sha256": digest,
            }
        )
    return entries


def _copy_verified_regular_file(source: Path, destination: Path) -> tuple[int, str, bool]:
    try:
        source_metadata = source.lstat()
        before = source.stat()
    except OSError as error:
        raise ValueError(f"Cannot read backup source file {source}") from error
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"Expected a regular backup source file or a symlink to one: {source}")

    digest = hashlib.sha256()
    try:
        with source.open("rb") as source_file, destination.open("xb") as destination_file:
            opened = os.fstat(source_file.fileno())
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise ValueError(f"Backup source file changed while opening it: {source}")
            while chunk := source_file.read(COPY_BUFFER_SIZE):
                destination_file.write(chunk)
                digest.update(chunk)
            destination_file.flush()
            os.fsync(destination_file.fileno())
            after = os.fstat(source_file.fileno())
        current = source.stat()
    except OSError as error:
        raise ValueError(f"Cannot copy backup source file {source}") from error
    if (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"Backup source file changed while it was copied: {source}")
    if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
        raise ValueError(f"Backup source symlink target changed while it was copied: {source}")
    return after.st_size, digest.hexdigest(), stat.S_ISLNK(source_metadata.st_mode)


def _validated_relative_path(storage_root: str, value: object) -> PurePosixPath:
    path = PurePosixPath(str(value))
    LocalPathRef(storage_root, path)
    if not path.parts or str(path) in {"", "."}:
        raise ValueError("A backup file path must not be empty")
    return path


def _update_row_digest(digest: Any, table: Table, values: Mapping[str, object]) -> None:
    encoded = {
        "table": table.fullname,
        "values": [[column.name, _canonical_value(values[column.key])] for column in table.columns],
    }
    digest.update(json.dumps(encoded, separators=(",", ":"), ensure_ascii=False).encode())
    digest.update(b"\n")


def _canonical_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return {"float": value.hex()}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, Path):
        return {"path": str(value)}
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"Cannot create a stable backup digest for {type(value).__name__}")


def _render_insert(table: Table, values: Mapping[str, object]) -> str:
    preparer = MYSQL_RESTORE_DIALECT.identifier_preparer
    table_name = preparer.quote_identifier(table.name)
    if table.schema is not None:
        table_name = f"{preparer.quote_identifier(table.schema)}.{table_name}"
    columns = ", ".join(preparer.quote_identifier(column.name) for column in table.columns)
    literals = ", ".join(
        _sql_literal(column, values[column.key], MYSQL_RESTORE_DIALECT)
        for column in table.columns
    )
    return f"INSERT INTO {table_name} ({columns}) VALUES ({literals})"


def _sql_literal(column: Column[Any], value: object, dialect: Dialect) -> str:
    processor = column.type.bind_processor(dialect)
    bound = processor(value) if processor is not None else value
    if bound is None:
        return "NULL"
    if isinstance(bound, bool):
        return "1" if bound else "0"
    if isinstance(bound, int):
        return str(bound)
    if isinstance(bound, float):
        if not math.isfinite(bound):
            raise ValueError(f"Cannot represent non-finite value in restoration SQL for {column}")
        return repr(bound)
    if isinstance(bound, Decimal):
        return str(bound)
    if isinstance(bound, datetime):
        return _mysql_string_literal(bound.isoformat(sep=" "))
    if isinstance(bound, date):
        return _mysql_string_literal(bound.isoformat())
    if isinstance(bound, str):
        return _mysql_string_literal(bound)
    if isinstance(bound, bytes):
        return f"X'{bound.hex()}'"
    raise TypeError(f"Cannot render {type(bound).__name__} in restoration SQL for {column}")


def _mysql_string_literal(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace("\0", "\\0")
        .replace("\b", "\\b")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\x1a", "\\Z")
        .replace("'", "''")
    )
    return f"'{escaped}'"


def _verify_payload(artifact: Path, relative_path: PurePosixPath, expected_hash: str) -> Path:
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"Backup payload escapes the artifact: {relative_path}")
    payload = artifact.joinpath(*relative_path.parts)
    current = artifact
    for part in relative_path.parts:
        current /= part
        try:
            metadata = current.lstat()
        except OSError as error:
            raise ValueError(f"Cannot read backup payload {relative_path}") from error
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"Backup payload must not contain symbolic links: {relative_path}")
    if not stat.S_ISREG(payload.lstat().st_mode):
        raise ValueError(f"Backup payload is not a regular file: {relative_path}")
    if _hash_file(payload) != expected_hash:
        raise ValueError(f"Backup payload checksum failed: {relative_path}")
    return payload


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(COPY_BUFFER_SIZE):
                digest.update(chunk)
    except OSError as error:
        raise ValueError(f"Cannot read backup payload {path}") from error
    return digest.hexdigest()
