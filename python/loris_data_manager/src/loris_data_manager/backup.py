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
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, TextIO, cast
from uuid import uuid4

from sqlalchemy import Column, MetaData, Table, and_, exists, inspect, select, union_all
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Connection, Dialect, Engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from loris_data_manager.projection import (
    ProjectionPlan,
    ResourceQuery,
    ResourceSelection,
    execute_projection,
    plan_resource_query,
)
from loris_data_manager.provider import ResourceSchema
from loris_data_manager.resources import LocalPathRef, LocalPathType, ObjectRef
from loris_data_manager.schema import (
    BatchLinkSource,
    DatabaseRowSetBinding,
    LifecycleSemantics,
    LocalPathColumnSource,
    LocalPathSetBinding,
    NullableReferenceSetBinding,
    OrmEntitySource,
    ReferenceUpdateSemantics,
    local_path_mapping,
)

BACKUP_FORMAT = "loris-resource-backup"
BACKUP_FORMAT_VERSION = 4
COPY_BUFFER_SIZE = 1024 * 1024
ROW_BATCH_SIZE = 500
MYSQL_RESTORE_DIALECT = mysql.dialect(paramstyle="named")


@dataclass(frozen=True, slots=True)
class BackupSelection(ResourceQuery):
    """Shared semantic projections plus their original CLI predicates."""

    where_expressions: tuple[str, ...] = ()
    updates: tuple[ResourceSelection, ...] = ()


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
    selected_object_count: int
    selected_object_counts: Mapping[str, int]
    database_row_count: int
    file_count: int
    blocker_count: int
    reference_update_count: int


@dataclass(frozen=True, slots=True)
class BackupPreview:
    """Read-only counts from a materialized backup/removal plan."""

    selected_object_count: int
    database_row_count: int
    file_count: int
    blocker_count: int
    reference_update_count: int


@dataclass(frozen=True, slots=True)
class FileBackup:
    """File identity extracted while streaming an owned database table."""

    object_kind: str
    object_identity: Column[Any]
    storage_root: str
    path_attribute: Column[Any]
    path_prefix: PurePosixPath = field(default_factory=PurePosixPath)


@dataclass(frozen=True, slots=True)
class DatabaseOwnership:
    """Expected FK that protects membership in an owned row collection."""

    parent_column: Column[Any]
    child_column: Column[Any]


@dataclass(frozen=True, slots=True)
class DatabaseTableBackup:
    """Ordered rows restored by a backup, with optional owned files."""

    table: Table
    statement: Select[Any]
    action: str
    counts_selected_objects: bool = False
    files: tuple[FileBackup, ...] = ()
    ownership: DatabaseOwnership | None = None


@dataclass(frozen=True, slots=True)
class DatabaseReferenceCheck:
    """Rows outside the backup closure that reference selected objects."""

    table: Table
    statement: Select[Any]
    reference_columns: tuple[Column[Any], ...]
    action: str
    relationship: str | None = None


@dataclass(frozen=True, slots=True)
class DatabaseReferenceUpdate:
    """Approved update of rows that reference a selected logical object."""

    relationship: str
    parent_column: Column[Any]
    child_column: Column[Any]


@dataclass(frozen=True, slots=True)
class FileCollectionBackup:
    """Owned file paths selected from rows related to the logical anchor."""

    table: Table
    statement: Select[Any]
    object_kind: str
    object_identity: Column[Any]
    storage_root: str
    path_attribute: Column[Any]
    path_prefix: PurePosixPath = field(default_factory=PurePosixPath)


@dataclass(frozen=True, slots=True)
class BackupPlan:
    """Combined physical capture plan for explicitly selected logical objects."""

    kinds: tuple[str, ...]
    tables: tuple[DatabaseTableBackup, ...]
    files: tuple[FileCollectionBackup, ...] = ()
    references: tuple[DatabaseReferenceCheck, ...] = ()
    reference_updates: tuple[DatabaseReferenceUpdate, ...] = ()

    def __post_init__(self) -> None:
        selected_tables = sum(table.counts_selected_objects for table in self.tables)
        if selected_tables < 1:
            raise ValueError("A backup plan must have at least one selected-object table")


@dataclass(frozen=True, slots=True)
class _PendingFile:
    object_kind: str
    object_id: str
    storage_root: str
    relative_path: PurePosixPath


@dataclass(frozen=True, slots=True)
class CapturedTableRows:
    """Locked primary-key identities captured while writing one backup table."""

    plan: DatabaseTableBackup
    identities: tuple[tuple[object, ...], ...]


@dataclass(frozen=True, slots=True)
class CapturedReferenceRows:
    """Locked reference-row values needed for guarded transactional updates."""

    plan: DatabaseReferenceCheck
    rows: tuple[Mapping[str, object], ...]


@dataclass(frozen=True, slots=True)
class VerifiedBackupCapture:
    """Materialized operation state available after its backup is verified."""

    result: BackupResult
    plan: BackupPlan
    tables: tuple[CapturedTableRows, ...]
    references: tuple[CapturedReferenceRows, ...]
    pending_files: tuple[_PendingFile, ...]
    file_entries: tuple[Mapping[str, object], ...]


class BackupTransactionAction(Protocol):
    """A destructive action performed after backup verification and before commit."""

    def validate(self, blocker_count: int) -> None:
        """Reject an ineligible locked plan before copying backup payloads."""

    def apply(self, db: Session, capture: VerifiedBackupCapture) -> None:
        """Apply database and reversible filesystem changes before commit."""

    def after_commit(self) -> None:
        """Finalize filesystem changes after the database commit succeeds."""

    def commit_failed(self) -> None:
        """Handle an indeterminate commit without assuming rollback."""


def plan_backup(
    schema: ResourceSchema,
    selection: BackupSelection,
) -> ProjectionPlan:
    """Plan shared semantic selection for a backup without executing it."""

    for selected in selection.selections:
        if selected.path.property is not None:
            raise ValueError(
                f"Backup selection {selected.expression!r} ends at a scalar property; "
                "select a logical object instead"
            )
        if not schema.has_provider(selected.target_kind):
            raise ValueError(
                f"Backup selection {selected.expression!r} does not end at a logical object "
                "with a registered provider"
            )
    _validated_reference_updates(schema, selection)
    return plan_resource_query(
        schema,
        selection,
        include_default_members=False,
    )


def _validated_reference_updates(
    schema: ResourceSchema,
    selection: BackupSelection,
) -> tuple[tuple[str, NullableReferenceSetBinding], ...]:
    """Resolve explicitly requested, schema-approved direct reference updates."""

    selected_roots = {
        selected.target_kind
        for selected in selection.selections
        if not selected.path.link_members and selected.path.property is None
    }
    resolved: list[tuple[str, NullableReferenceSetBinding]] = []
    seen: set[str] = set()
    for requested in selection.updates:
        if requested.expression in seen:
            raise ValueError(f"Reference update {requested.expression!r} was requested twice")
        seen.add(requested.expression)
        if requested.path.property is not None or len(requested.path.link_members) != 1:
            raise ValueError(
                f"Reference update {requested.expression!r} must name one direct relationship"
            )
        if requested.path.root_kind not in selected_roots:
            raise ValueError(
                f"Reference update {requested.expression!r} requires selecting its source "
                f"object {requested.path.root_kind!r}"
            )
        link = schema.path_links(requested.path)[0]
        if link.reference_update is not ReferenceUpdateSemantics.SET_NULL:
            raise ValueError(
                f"Relationship {requested.expression!r} does not permit reference updates"
            )
        source = link.source
        binding = source.set_binding if isinstance(source, BatchLinkSource) else None
        if not isinstance(binding, NullableReferenceSetBinding):
            raise ValueError(
                f"Relationship {requested.expression!r} has no nullable-reference binding"
            )
        resolved.append((requested.expression, binding))
    return tuple(resolved)


def _plan_backup_for_identities(
    connection: Connection,
    schema: ResourceSchema,
    kind: str,
    identities: tuple[object, ...],
    reference_updates: tuple[tuple[str, NullableReferenceSetBinding], ...] = (),
) -> BackupPlan:
    """Plan a closure from the immutable identities selected by a locking read."""

    provider = schema.provider(kind)
    anchor_table = _model_table(provider.orm_model)
    anchor_identity = _single_primary_key(anchor_table, kind)
    selected_statement = select(provider.orm_model).where(anchor_identity.in_(identities))
    return _plan_backup_from_selection(
        connection,
        schema,
        kind,
        selected_statement,
        reference_updates=reference_updates,
        inspect_dependencies=False,
    )


def _plan_backup_from_selection(
    connection: Connection,
    schema: ResourceSchema,
    kind: str,
    selected_statement: Select[Any],
    *,
    reference_updates: tuple[tuple[str, NullableReferenceSetBinding], ...] = (),
    inspect_dependencies: bool = True,
) -> BackupPlan:
    """Lower one anchor selection statement to its physical ownership closure."""

    provider = schema.provider(kind)
    anchor_table = _model_table(provider.orm_model)
    anchor_identity = _single_primary_key(anchor_table, kind)

    def selected_values(attribute: Any, name: str):
        attribute_model = getattr(attribute, "class_", provider.orm_model)
        if attribute_model is not provider.orm_model:
            raise ValueError(
                f"Set source for {kind}.{name} does not belong to its provider"
            )
        return (
            selected_statement.with_only_columns(attribute)
            .order_by(None)
            .distinct()
            .subquery(f"selected_{kind.replace('-', '_')}_{name.replace('-', '_')}")
        )

    selected_ids = selected_values(anchor_identity, "identities")
    tables: list[DatabaseTableBackup] = []
    anchor_files: list[FileBackup] = []
    file_collections: list[FileCollectionBackup] = []
    database_reference_updates: list[DatabaseReferenceUpdate] = []

    for relationship, binding in reference_updates:
        if binding.source_attribute.class_ is not provider.orm_model:
            raise ValueError(
                f"Reference update {relationship!r} does not belong to provider {kind!r}"
            )
        database_reference_updates.append(
            DatabaseReferenceUpdate(
                relationship=relationship,
                parent_column=_mapped_column(binding.source_attribute),
                child_column=_mapped_column(binding.target_attribute),
            )
        )

    for link in schema.links(kind):
        if link.lifecycle is LifecycleSemantics.OWNS:
            if isinstance(link.source, OrmEntitySource):
                if link.target_kind != "database-row":
                    raise ValueError(f"Owned entity link {kind}.{link.name} is not a database row")
                continue
            if isinstance(link.source, LocalPathColumnSource):
                path = link.source.path
                schema.storage_root(path.storage_root)
                if path.expected_type is not LocalPathType.FILE:
                    raise ValueError(
                        f"Owned path {kind}.{link.name} is not a regular file"
                    )
                path_column = _mapped_column(path.attribute)
                if path.attribute.class_ is not provider.orm_model:
                    raise ValueError(
                        f"Path source for {kind}.{link.name} does not belong to its provider"
                    )
                anchor_files.append(
                    FileBackup(
                        object_kind=kind,
                        object_identity=anchor_identity,
                        storage_root=path.storage_root,
                        path_attribute=path_column,
                        path_prefix=path.path_prefix,
                    )
                )
                continue
            if isinstance(link.source, BatchLinkSource):
                binding = link.source.set_binding
                if isinstance(binding, DatabaseRowSetBinding):
                    tables.append(
                        _plan_batch_table(
                            selected_values,
                            kind,
                            link.name,
                            binding,
                            action="delete",
                        )
                    )
                elif isinstance(binding, LocalPathSetBinding):
                    schema.storage_root(binding.path.storage_root)
                    if binding.path.expected_type is not LocalPathType.FILE:
                        raise ValueError(
                            f"Owned path {kind}.{link.name} is not a regular file"
                        )
                    file_collections.append(
                        _plan_batch_files(
                            selected_values,
                            kind,
                            link.name,
                            link.source,
                            binding,
                        )
                    )
                else:
                    raise ValueError(
                        f"Owned batch link {kind}.{link.name} has no set-oriented binding"
                    )
                continue
            raise ValueError(
                f"Owned link {kind}.{link.name} has no set-oriented resource source"
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

    if inspect_dependencies:
        ordered_tables, references = _inspect_database_dependencies(
            connection,
            tuple(tables),
            tuple(database_reference_updates),
        )
    else:
        ordered_tables = tuple(tables)
        references = []

    return BackupPlan(
        kinds=(kind,),
        tables=ordered_tables,
        files=tuple(file_collections),
        references=tuple(references),
        reference_updates=tuple(database_reference_updates),
    )


def required_storage_roots(schema: ResourceSchema, kind: str) -> tuple[str, ...]:
    """Return storage roots reached by owned local-path links of an object kind."""

    schema.provider(kind)
    paths = (
        path
        for link in schema.links(kind)
        if link.lifecycle is LifecycleSemantics.OWNS
        if (path := local_path_mapping(link.source)) is not None
    )
    return tuple(dict.fromkeys(path.storage_root for path in paths))


def _identity_values(
    schema: ResourceSchema,
    kind: str,
    refs: tuple[ObjectRef, ...],
) -> tuple[object, ...]:
    table = _model_table(schema.provider(kind).orm_model)
    identity = _single_primary_key(table, kind)
    python_type = identity.type.python_type
    try:
        return tuple(python_type(ref.key) for ref in refs)
    except (TypeError, ValueError) as error:
        raise ValueError(f"References to {kind!r} have invalid identities") from error


def _plan_batch_table(
    selected_values: Any,
    kind: str,
    member_name: str,
    binding: DatabaseRowSetBinding,
    *,
    action: str,
) -> DatabaseTableBackup:
    source_attribute = binding.source_attribute
    target_attribute = binding.target_attribute
    values = selected_values(source_attribute, member_name)
    table = _model_table(target_attribute.class_)
    target_column = _mapped_column(target_attribute)
    source_column = _mapped_column(source_attribute)
    return DatabaseTableBackup(
        table=table,
        statement=(
            select(table)
            .join(values, target_column == next(iter(values.c)))
            .order_by(*table.primary_key.columns)
        ),
        action=action,
        ownership=DatabaseOwnership(
            parent_column=source_column,
            child_column=target_column,
        ),
    )


def _plan_batch_files(
    selected_values: Any,
    kind: str,
    member_name: str,
    source: BatchLinkSource,
    binding: LocalPathSetBinding,
) -> FileCollectionBackup:
    values = selected_values(binding.source_attribute, member_name)
    table = _model_table(binding.owner_attribute.class_)
    owner_column = _mapped_column(binding.owner_attribute)
    path_column = _mapped_column(binding.path.attribute)
    statement = (
        source.statement_for_keys(select(next(iter(values.c))))
        .with_only_columns(
            owner_column.label("object_identity"),
            path_column.label("relative_path"),
        )
        .order_by(*table.primary_key.columns)
    )
    return FileCollectionBackup(
        table=table,
        statement=cast(Select[Any], statement),
        object_kind=kind,
        object_identity=owner_column,
        storage_root=binding.path.storage_root,
        path_attribute=path_column,
        path_prefix=binding.path.path_prefix,
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


def _inspect_database_dependencies(
    connection: Connection,
    plans: tuple[DatabaseTableBackup, ...],
    reference_updates: tuple[DatabaseReferenceUpdate, ...] = (),
) -> tuple[tuple[DatabaseTableBackup, ...], list[DatabaseReferenceCheck]]:
    """Reflect authoritative FKs and separate internal dependencies from blockers."""

    inspector = inspect(connection)
    default_schema = inspector.default_schema_name

    def key(schema: str | None, table: str) -> tuple[str | None, str]:
        return (schema or default_schema, table)

    by_table = {key(plan.table.schema, plan.table.name): plan for plan in plans}
    if len(by_table) != len(plans):
        raise ValueError("An ownership closure reaches the same database table more than once")
    dependencies: set[tuple[tuple[str | None, str], tuple[str | None, str]]] = set()
    reflected_foreign_keys: set[
        tuple[
            tuple[str | None, str],
            tuple[str, ...],
            tuple[str | None, str],
            tuple[str, ...],
        ]
    ] = set()
    references: list[DatabaseReferenceCheck] = []
    metadata = MetaData()
    updates_by_fk: dict[
        tuple[
            tuple[str | None, str],
            tuple[str, ...],
            tuple[str | None, str],
            tuple[str, ...],
        ],
        DatabaseReferenceUpdate,
    ] = {}
    for update_plan in reference_updates:
        update_key = (
            key(update_plan.child_column.table.schema, update_plan.child_column.table.name),
            (update_plan.child_column.name,),
            key(update_plan.parent_column.table.schema, update_plan.parent_column.table.name),
            (update_plan.parent_column.name,),
        )
        if update_key in updates_by_fk:
            raise ValueError(f"Reference update {update_plan.relationship!r} duplicates another update")
        updates_by_fk[update_key] = update_plan
    matched_updates: set[str] = set()

    # Deliberately inspect the connected database rather than ORM metadata. Custom
    # deployment tables are part of the safety inventory even when Python has no model.
    source_schema: str | None = None
    for source_name in inspector.get_table_names(schema=source_schema):
        source_key = key(source_schema, source_name)
        reflected_source: Table | None = None
        for foreign_key in inspector.get_foreign_keys(source_name, schema=source_schema):
            referred_table = foreign_key.get("referred_table")
            constrained_names = tuple(foreign_key.get("constrained_columns") or ())
            referred_names = tuple(foreign_key.get("referred_columns") or ())
            if not referred_table or not constrained_names:
                raise ValueError(
                    f"Cannot completely inspect foreign keys for {source_name!r}"
                )
            if len(constrained_names) != len(referred_names):
                raise ValueError(f"Foreign key on {source_name!r} has mismatched columns")
            target_key = key(foreign_key.get("referred_schema"), referred_table)
            foreign_key_key = (source_key, constrained_names, target_key, referred_names)
            reflected_foreign_keys.add(foreign_key_key)
            target_plan = by_table.get(target_key)
            if target_plan is None:
                continue
            if source_key in by_table:
                dependencies.add((source_key, target_key))

            if reflected_source is None:
                reflected_source = Table(
                    source_name,
                    metadata,
                    schema=source_schema,
                    autoload_with=connection,
                )
            target_rows = target_plan.statement.order_by(None).subquery(
                f"selected_{target_plan.table.name}"
            )
            source_columns = tuple(
                _column_named(reflected_source, name) for name in constrained_names
            )
            target_columns = tuple(
                _selectable_column_named(target_rows, target_plan.table, name)
                for name in referred_names
            )
            statement = select(reflected_source).join(
                target_rows,
                and_(
                    *(source == target for source, target in zip(source_columns, target_columns))
                ),
            )

            source_plan = by_table.get(source_key)
            if source_plan is not None:
                primary_key = tuple(reflected_source.primary_key.columns)
                if not primary_key:
                    raise ValueError(
                        f"Owned database table {reflected_source.fullname!r} has no primary key"
                    )
                selected_source = source_plan.statement.order_by(None).subquery(
                    f"selected_{source_plan.table.name}_sources"
                )
                statement = statement.where(
                    ~exists(
                        select(1).select_from(selected_source).where(
                            and_(
                                *(
                                    column
                                    == _selectable_column_named(
                                        selected_source,
                                        source_plan.table,
                                        column.name,
                                    )
                                    for column in primary_key
                                )
                            )
                        )
                    )
                )
            statement = statement.order_by(*reflected_source.primary_key.columns)
            approved_update = updates_by_fk.get(foreign_key_key)
            if approved_update is not None:
                if len(source_columns) != 1 or not source_columns[0].nullable:
                    raise ValueError(
                        f"Reference update {approved_update.relationship!r} does not use a "
                        "nullable single-column foreign key"
                    )
                matched_updates.add(approved_update.relationship)
            references.append(
                DatabaseReferenceCheck(
                    table=reflected_source,
                    statement=statement,
                    reference_columns=source_columns,
                    action="set-null" if approved_update is not None else "block",
                    relationship=(
                        approved_update.relationship if approved_update is not None else None
                    ),
                )
            )

    for plan in plans:
        ownership = plan.ownership
        if ownership is None:
            continue
        expected = (
            key(plan.table.schema, plan.table.name),
            (ownership.child_column.name,),
            key(ownership.parent_column.table.schema, ownership.parent_column.table.name),
            (ownership.parent_column.name,),
        )
        if expected not in reflected_foreign_keys:
            raise ValueError(
                f"Owned database rows in {plan.table.fullname!r} are not protected by "
                f"the expected foreign key {ownership.child_column.name!r} -> "
                f"{ownership.parent_column.table.fullname}."
                f"{ownership.parent_column.name}"
            )

    missing_updates = {
        update.relationship for update in reference_updates
    } - matched_updates
    if missing_updates:
        raise ValueError(
            "Reference updates are not protected by matching database foreign keys: "
            f"{sorted(missing_updates)}"
        )

    return _order_database_tables(plans, by_table, dependencies), references


def _column_named(table: Table, name: str) -> Column[Any]:
    for column in table.columns:
        if column.name == name:
            return column
    raise ValueError(f"Reflected table {table.fullname!r} has no column {name!r}")


def _selectable_column_named(selectable: Any, table: Table, name: str) -> Any:
    source_column = _column_named(table, name)
    try:
        return selectable.c[source_column.key]
    except KeyError as error:
        raise ValueError(
            f"Selection for {table.fullname!r} does not contain column {name!r}"
        ) from error


def _order_database_tables(
    plans: tuple[DatabaseTableBackup, ...],
    by_table: Mapping[tuple[str | None, str], DatabaseTableBackup],
    dependencies: set[tuple[tuple[str | None, str], tuple[str | None, str]]],
) -> tuple[DatabaseTableBackup, ...]:
    """Put referenced parent tables before captured children."""

    plan_keys = {id(plan): table_key for table_key, plan in by_table.items()}
    ordered: list[DatabaseTableBackup] = []
    remaining = list(plans)
    while remaining:
        ready = [
            plan
            for plan in remaining
            if all(
                parent in {plan_keys[id(item)] for item in ordered}
                for child, parent in dependencies
                if child == plan_keys[id(plan)] and child != parent
            )
        ]
        if not ready:
            names = sorted(plan.table.fullname for plan in remaining)
            raise ValueError(f"Owned database tables contain a dependency cycle: {names}")
        for plan in ready:
            ordered.append(plan)
            remaining.remove(plan)
    return tuple(ordered)


def _combine_backup_plans(
    connection: Connection,
    plans: tuple[BackupPlan, ...],
) -> BackupPlan:
    """Union per-kind ownership closures before dependency and blocker analysis."""

    grouped: dict[tuple[str | None, str], list[DatabaseTableBackup]] = {}
    for plan in plans:
        for table_plan in plan.tables:
            key = (table_plan.table.schema, table_plan.table.name)
            grouped.setdefault(key, []).append(table_plan)

    combined_tables = tuple(
        _combine_table_backups(table_plans)
        for table_plans in grouped.values()
    )
    ordered_tables, references = _inspect_database_dependencies(
        connection,
        combined_tables,
        tuple(update for plan in plans for update in plan.reference_updates),
    )
    return BackupPlan(
        kinds=tuple(dict.fromkeys(kind for plan in plans for kind in plan.kinds)),
        tables=ordered_tables,
        files=tuple(file for plan in plans for file in plan.files),
        references=tuple(references),
        reference_updates=tuple(
            update for plan in plans for update in plan.reference_updates
        ),
    )


def _combine_table_backups(
    plans: list[DatabaseTableBackup],
) -> DatabaseTableBackup:
    """Deduplicate one physical table reached through several logical selections."""

    first = plans[0]
    if len(plans) == 1:
        return first
    if any(plan.action != first.action for plan in plans[1:]):
        raise ValueError(
            f"Conflicting backup actions were planned for {first.table.fullname!r}"
        )
    ownership_keys = {_ownership_key(plan.ownership) for plan in plans}
    if len(ownership_keys) != 1:
        raise ValueError(
            f"The selected logical objects reach {first.table.fullname!r} through "
            "incompatible ownership paths"
        )

    selected_rows = union_all(
        *(plan.statement.order_by(None) for plan in plans)
    ).subquery(f"combined_{first.table.name}")
    primary_key = tuple(first.table.primary_key.columns)
    if not primary_key:
        raise ValueError(
            f"Cannot combine backup selections for {first.table.fullname!r} without a primary key"
        )
    statement = (
        select(first.table)
        .where(
            exists(
                select(1).select_from(selected_rows).where(
                    and_(
                        *(
                            column
                            == _selectable_column_named(
                                selected_rows,
                                first.table,
                                column.name,
                            )
                            for column in primary_key
                        )
                    )
                )
            )
        )
        .order_by(*primary_key)
    )
    files_by_key: dict[tuple[object, ...], FileBackup] = {}
    for plan in plans:
        for file in plan.files:
            key = (
                file.object_kind,
                file.object_identity.table.fullname,
                file.object_identity.name,
                file.storage_root,
                file.path_attribute.table.fullname,
                file.path_attribute.name,
                file.path_prefix,
            )
            files_by_key.setdefault(key, file)
    return DatabaseTableBackup(
        table=first.table,
        statement=statement,
        action=first.action,
        counts_selected_objects=any(plan.counts_selected_objects for plan in plans),
        files=tuple(files_by_key.values()),
        ownership=first.ownership,
    )


def _ownership_key(ownership: DatabaseOwnership | None) -> tuple[str, str, str, str] | None:
    if ownership is None:
        return None
    return (
        ownership.parent_column.table.fullname,
        ownership.parent_column.name,
        ownership.child_column.table.fullname,
        ownership.child_column.name,
    )


def _locking_statement(statement: Select[Any], table: Table) -> Select[Any]:
    """Request write locks while returning complete rows in deterministic order."""

    primary_key = tuple(table.primary_key.columns)
    if not primary_key:
        raise ValueError(f"Cannot lock database table {table.fullname!r} without a primary key")
    return (
        statement.order_by(None)
        .order_by(*primary_key)
        .with_for_update()
    )


def create_backup(
    engine: Engine,
    schema: ResourceSchema,
    request: BackupRequest,
) -> BackupResult:
    """Stream a selected resource backup into a verified, atomically finalized directory."""

    return create_backup_with_action(engine, schema, request)


def preview_backup(
    engine: Engine,
    schema: ResourceSchema,
    selection: BackupSelection,
) -> BackupPreview:
    """Materialize backup/removal counts without locking or writing an artifact."""

    semantic_plan = plan_backup(schema, selection)
    validated_updates = _validated_reference_updates(schema, selection)
    updates_by_kind: dict[str, list[tuple[str, NullableReferenceSetBinding]]] = {}
    for relationship, binding in validated_updates:
        source_kind = schema.projection_path(relationship).root_kind
        updates_by_kind.setdefault(source_kind, []).append((relationship, binding))

    with engine.connect() as connection, connection.begin(), Session(connection) as db:
        projection = execute_projection(db, semantic_plan)
        selected_refs = frozenset(
            ref
            for projected in projection.projected
            for ref in projected
            if isinstance(ref, ObjectRef)
        )
        if not selected_refs:
            raise ValueError("The backup selection projected no logical objects")
        refs_by_kind: dict[str, tuple[ObjectRef, ...]] = {
            kind: tuple(
                sorted(
                    (ref for ref in selected_refs if ref.kind == kind),
                    key=str,
                )
            )
            for kind in sorted({ref.kind for ref in selected_refs})
        }
        physical_plans = tuple(
            _plan_backup_for_identities(
                connection,
                schema,
                kind,
                tuple(_identity_values(schema, kind, refs)),
                tuple(updates_by_kind.get(kind, ())),
            )
            for kind, refs in refs_by_kind.items()
        )
        plan = _combine_backup_plans(connection, physical_plans)
        database_row_count = 0
        pending_file_count = 0
        sink = StringIO()
        for table_plan in plan.tables:
            entry, pending_files = _write_database_table(db, table_plan, sink)
            database_row_count += cast(int, entry["row_count"])
            pending_file_count += len(pending_files)
        for file_plan in plan.files:
            pending_file_count += len(
                _read_file_collection(db, file_plan, lock_rows=False)
            )
        reference_entries = tuple(
            _write_database_reference(
                db,
                reference,
                sink,
                lock_rows=False,
            )
            for reference in plan.references
        )

    return BackupPreview(
        selected_object_count=sum(len(refs) for refs in refs_by_kind.values()),
        database_row_count=database_row_count,
        file_count=pending_file_count,
        blocker_count=sum(
            cast(int, entry["row_count"])
            for entry in reference_entries
            if entry["action"] == "block"
        ),
        reference_update_count=sum(
            cast(int, entry["row_count"])
            for entry in reference_entries
            if entry["action"] == "set-null"
        ),
    )


def create_backup_with_action(
    engine: Engine,
    schema: ResourceSchema,
    request: BackupRequest,
    action: BackupTransactionAction | None = None,
) -> BackupResult:
    """Create a verified backup and optionally mutate resources in its transaction."""

    selection = request.selection
    semantic_plan = plan_backup(schema, selection)
    validated_updates = _validated_reference_updates(schema, selection)
    updates_by_kind: dict[str, list[tuple[str, NullableReferenceSetBinding]]] = {}
    for relationship, binding in validated_updates:
        source_kind = schema.projection_path(relationship).root_kind
        updates_by_kind.setdefault(source_kind, []).append((relationship, binding))
    selected_kinds = tuple(
        dict.fromkeys(selected.target_kind for selected in selection.selections)
    )
    required_roots = tuple(
        dict.fromkeys(
            root
            for kind in selected_kinds
            for root in required_storage_roots(schema, kind)
        )
    )
    missing_roots = {
        name for name in required_roots if name not in request.storage_roots
    }
    if missing_roots:
        raise ValueError(f"Missing storage roots required by resource schema: {sorted(missing_roots)}")
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
    finalized = False
    action_applied = False

    try:
        sql_path = staging / "restore.sql"
        pending_files: list[_PendingFile] = []
        table_entries: list[dict[str, object]] = []
        captured_tables: list[CapturedTableRows] = []
        database_row_count = 0
        selected_object_count = 0
        selected_object_counts: dict[str, int] = {}

        with engine.connect() as connection:
            if engine.dialect.name == "mysql":
                connection = connection.execution_options(isolation_level="REPEATABLE READ")
            with connection.begin():
                with Session(connection) as db:
                    projection = execute_projection(db, semantic_plan, lock_rows=True)
                    selected_refs = frozenset(
                        ref
                        for projected in projection.projected
                        for ref in projected
                        if isinstance(ref, ObjectRef)
                    )
                    if not selected_refs:
                        raise ValueError("The backup selection projected no logical objects")
                    refs_by_kind = {
                        kind: tuple(
                            sorted(
                                (ref for ref in selected_refs if ref.kind == kind),
                                key=str,
                            )
                        )
                        for kind in sorted({ref.kind for ref in selected_refs})
                    }
                    selected_object_counts = {
                        kind: len(refs) for kind, refs in refs_by_kind.items()
                    }
                    selected_object_count = sum(selected_object_counts.values())
                    physical_plans = tuple(
                        _plan_backup_for_identities(
                            connection,
                            schema,
                            kind,
                            tuple(_identity_values(schema, kind, refs)),
                            tuple(updates_by_kind.get(kind, ())),
                        )
                        for kind, refs in refs_by_kind.items()
                    )
                    plan = _combine_backup_plans(connection, physical_plans)

                    reference_entries: tuple[dict[str, object], ...]
                    with sql_path.open("w", encoding="utf-8", newline="\n") as restore_sql:
                        restore_sql.write(
                            "-- LORIS logical-resource restoration patch\n"
                            f"-- Backup operation: {operation_id}\n"
                            "-- Restore files before applying this transaction.\n\n"
                            "START TRANSACTION;\n\n"
                        )
                        for table_plan in plan.tables:
                            identities: list[tuple[object, ...]] = []
                            entry, files = _write_database_table(
                                db,
                                table_plan,
                                restore_sql,
                                lock_rows=True,
                                selected_identities=identities,
                            )
                            table_entries.append(entry)
                            captured_tables.append(
                                CapturedTableRows(table_plan, tuple(identities))
                            )
                            row_count = cast(int, entry["row_count"])
                            database_row_count += row_count
                            pending_files.extend(files)
                        for file_plan in plan.files:
                            pending_files.extend(_read_file_collection(db, file_plan))
                        reference_entries_list: list[dict[str, object]] = []
                        captured_references: list[CapturedReferenceRows] = []
                        for reference in plan.references:
                            reference_rows: list[Mapping[str, object]] = []
                            reference_entries_list.append(
                                _write_database_reference(
                                    db,
                                    reference,
                                    restore_sql,
                                    selected_rows=reference_rows,
                                )
                            )
                            captured_references.append(
                                CapturedReferenceRows(reference, tuple(reference_rows))
                            )
                        reference_entries = tuple(reference_entries_list)
                        restore_sql.write("COMMIT;\n")
                        restore_sql.flush()
                        os.fsync(restore_sql.fileno())

                    blocker_count = sum(
                        cast(int, entry["row_count"])
                        for entry in reference_entries
                        if entry["action"] == "block"
                    )
                    reference_update_count = sum(
                        cast(int, entry["row_count"])
                        for entry in reference_entries
                        if entry["action"] == "set-null"
                    )
                    if action is not None:
                        action.validate(blocker_count)

                    # Keep database locks until every backup payload is written and
                    # verified. A transaction action can run immediately afterward
                    # without a row-level time-of-check gap.
                    file_entries = _copy_files(request.storage_roots, staging, pending_files)
                    manifest: dict[str, object] = {
                        "format": BACKUP_FORMAT,
                        "format_version": BACKUP_FORMAT_VERSION,
                        "operation_id": operation_id,
                        "created_at": datetime.now(UTC).isoformat(),
                        "planner": {
                            "name": "resource-ownership",
                            "version": 2,
                            "object_kinds": list(plan.kinds),
                            "reference_inventory": "database-foreign-keys",
                            "database_foreign_keys_checked": True,
                            "database_row_locking": "select-for-update",
                            "selection_materialization": "locked-projected-identities",
                        },
                        "selection": {
                            "select": [
                                selected.expression for selected in selection.selections
                            ],
                            "all": selection.select_all,
                            "where": list(selection.where_expressions),
                            "anchor": {
                                "kind": semantic_plan.anchor_kind,
                                "matched_count": len(projection.matched),
                            },
                            "selected": selected_object_counts,
                            **(
                                {
                                    "update": [
                                        requested.expression
                                        for requested in selection.updates
                                    ]
                                }
                                if selection.updates
                                else {}
                            ),
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
                            **(
                                {"reference_update_count": reference_update_count}
                                if reference_update_count
                                else {}
                            ),
                        },
                    }
                    manifest_path = staging / "manifest.json"
                    with manifest_path.open(
                        "w", encoding="utf-8", newline="\n"
                    ) as manifest_file:
                        json.dump(manifest, manifest_file, indent=2, sort_keys=True)
                        manifest_file.write("\n")
                        manifest_file.flush()
                        os.fsync(manifest_file.fileno())

                    verify_backup(staging)
                    # Make the verified artifact visible before releasing database
                    # locks. If a later deletion or commit fails, retaining this
                    # valid backup is safe even though no removal took place.
                    os.replace(staging, output)
                    finalized = True
                    result = BackupResult(
                        output=output,
                        operation_id=operation_id,
                        selected_object_count=selected_object_count,
                        selected_object_counts=selected_object_counts,
                        database_row_count=database_row_count,
                        file_count=len(file_entries),
                        blocker_count=blocker_count,
                        reference_update_count=reference_update_count,
                    )
                    if action is not None:
                        action.apply(
                            db,
                            VerifiedBackupCapture(
                                result=result,
                                plan=plan,
                                tables=tuple(captured_tables),
                                references=tuple(captured_references),
                                pending_files=tuple(pending_files),
                                file_entries=tuple(file_entries),
                            ),
                        )
                        action_applied = True
    except BaseException:
        if not finalized:
            shutil.rmtree(staging, ignore_errors=True)
        if action_applied and action is not None:
            action.commit_failed()
        raise

    if action_applied and action is not None:
        action.after_commit()

    return result


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
        path_type = entry.get("path_type")
        payload = entry.get("payload")
        digest = entry.get("sha256")
        size = entry.get("size")
        if (
            path_type not in {"regular-file", "symlink"}
            or not isinstance(payload, str)
            or not isinstance(digest, str)
            or not isinstance(size, int)
        ):
            raise ValueError("Backup manifest has an invalid file entry")
        payload_path = PurePosixPath(payload)
        if path_type == "regular-file":
            verified_payload = _verify_payload(artifact, payload_path, digest)
            if verified_payload.stat().st_size != size:
                raise ValueError(f"Backup payload has the wrong size: {payload}")
        else:
            link_target = entry.get("link_target")
            if not isinstance(link_target, str):
                raise ValueError("Backup manifest has an invalid symbolic-link entry")
            _verify_symlink_payload(
                artifact,
                payload_path,
                expected_target=link_target,
                expected_size=size,
                expected_hash=digest,
            )
    return manifest


def _write_database_table(
    db: Session,
    plan: DatabaseTableBackup,
    restore_sql: TextIO,
    *,
    lock_rows: bool = False,
    selected_identities: list[tuple[object, ...]] | None = None,
) -> tuple[dict[str, object], list[_PendingFile]]:
    digest = hashlib.sha256()
    row_count = 0
    pending_files: list[_PendingFile] = []
    restore_sql.write(f"-- {plan.table.fullname}\n")
    statement = _locking_statement(plan.statement, plan.table) if lock_rows else plan.statement
    result = db.execute(statement.execution_options(yield_per=ROW_BATCH_SIZE)).mappings()
    primary_key = tuple(plan.table.primary_key.columns)
    for row in result:
        values = {column.key: row[column.key] for column in plan.table.columns}
        _update_row_digest(digest, plan.table, values)
        restore_sql.write(f"{_render_insert(plan.table, values)};\n")
        row_count += 1
        if selected_identities is not None:
            selected_identities.append(
                tuple(values[column.key] for column in primary_key)
            )
        for file_plan in plan.files:
            raw_path = values[file_plan.path_attribute.key]
            if raw_path is not None:
                relative_path = _validated_relative_path(
                    file_plan.storage_root, raw_path
                )
                if file_plan.path_prefix.parts:
                    relative_path = file_plan.path_prefix / relative_path
                pending_files.append(
                    _PendingFile(
                        object_kind=file_plan.object_kind,
                        object_id=str(values[file_plan.object_identity.key]),
                        storage_root=file_plan.storage_root,
                        relative_path=relative_path,
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


def _write_database_reference(
    db: Session,
    plan: DatabaseReferenceCheck,
    restore_sql: TextIO,
    *,
    lock_rows: bool = True,
    selected_rows: list[Mapping[str, object]] | None = None,
) -> dict[str, object]:
    digest = hashlib.sha256()
    row_count = 0
    statement = plan.statement.with_for_update() if lock_rows else plan.statement
    result = db.execute(
        statement.execution_options(yield_per=ROW_BATCH_SIZE)
    ).mappings()
    primary_key = tuple(plan.table.primary_key.columns)
    if plan.action == "set-null":
        if not primary_key:
            raise ValueError(
                f"Cannot restore updated references in {plan.table.fullname!r} "
                "without a primary key"
            )
        restore_sql.write(f"-- restore {plan.relationship}\n")
    for row in result:
        values = {column.key: row[column.key] for column in plan.table.columns}
        if selected_rows is not None:
            selected_rows.append(values)
        _update_row_digest(digest, plan.table, values)
        if plan.action == "set-null":
            restore_sql.write(f"{_render_reference_update(plan, values, primary_key)};\n")
        row_count += 1
    if plan.action == "set-null":
        restore_sql.write("\n")
    entry: dict[str, object] = {
        "table": plan.table.fullname,
        "columns": [column.name for column in plan.reference_columns],
        "action": plan.action,
        "row_count": row_count,
        "content_sha256": digest.hexdigest(),
    }
    if plan.relationship is not None:
        entry["relationship"] = plan.relationship
    return entry


def _read_file_collection(
    db: Session,
    plan: FileCollectionBackup,
    *,
    lock_rows: bool = True,
) -> list[_PendingFile]:
    pending_files: list[_PendingFile] = []
    statement = plan.statement.with_for_update() if lock_rows else plan.statement
    result = db.execute(
        statement.execution_options(yield_per=ROW_BATCH_SIZE)
    ).mappings()
    for row in result:
        raw_path = row["relative_path"]
        if raw_path is None:
            continue
        relative_path = _validated_relative_path(plan.storage_root, raw_path)
        if plan.path_prefix.parts:
            relative_path = plan.path_prefix / relative_path
        pending_files.append(
            _PendingFile(
                object_kind=plan.object_kind,
                object_id=str(row["object_identity"]),
                storage_root=plan.storage_root,
                relative_path=relative_path,
            )
        )
    return pending_files


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
        try:
            source_metadata = source.lstat()
        except OSError as error:
            raise ValueError(f"Cannot inspect backup source file {source}") from error
        if stat.S_ISLNK(source_metadata.st_mode):
            size, digest, link_target = _copy_verified_symlink(source, destination)
            path_metadata: dict[str, object] = {
                "path_type": "symlink",
                "link_target": link_target,
            }
        elif stat.S_ISREG(source_metadata.st_mode):
            size, digest = _copy_verified_regular_file(source, destination)
            path_metadata = {"path_type": "regular-file"}
        else:
            raise ValueError(f"Expected a regular file or symbolic link: {source}")
        entries.append(
            {
                "object": {"kind": pending.object_kind, "id": pending.object_id},
                "storage_root": pending.storage_root,
                "relative_path": str(pending.relative_path),
                **path_metadata,
                "payload": str(payload),
                "size": size,
                "sha256": digest,
            }
        )
    return entries


def _copy_verified_regular_file(source: Path, destination: Path) -> tuple[int, str]:
    try:
        source_metadata = source.lstat()
        before = source.stat()
    except OSError as error:
        raise ValueError(f"Cannot read backup source file {source}") from error
    if not stat.S_ISREG(source_metadata.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError(f"Expected a regular backup source file: {source}")
    if (source_metadata.st_dev, source_metadata.st_ino) != (before.st_dev, before.st_ino):
        raise ValueError(f"Backup source changed while it was inspected: {source}")

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
        current = source.lstat()
    except OSError as error:
        raise ValueError(f"Cannot copy backup source file {source}") from error
    if (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"Backup source file changed while it was copied: {source}")
    if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
        raise ValueError(f"Backup source changed while it was copied: {source}")
    return after.st_size, digest.hexdigest()


def _copy_verified_symlink(source: Path, destination: Path) -> tuple[int, str, str]:
    try:
        before = source.lstat()
        if not stat.S_ISLNK(before.st_mode):
            raise ValueError(f"Expected a symbolic-link backup source: {source}")
        link_target = os.readlink(source)
        after = source.lstat()
    except OSError as error:
        raise ValueError(f"Cannot read backup source symbolic link {source}") from error
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise ValueError(f"Backup source symbolic link changed while it was read: {source}")
    if link_target != os.readlink(source):
        raise ValueError(f"Backup source symbolic link target changed: {source}")

    target_bytes = os.fsencode(link_target)
    try:
        os.symlink(link_target, destination)
        copied = destination.lstat()
        copied_target = os.readlink(destination)
    except OSError as error:
        raise ValueError(f"Cannot copy backup source symbolic link {source}") from error
    if not stat.S_ISLNK(copied.st_mode) or copied_target != link_target:
        raise ValueError(f"Copied backup symbolic link does not match its source: {source}")
    return len(target_bytes), hashlib.sha256(target_bytes).hexdigest(), link_target


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
    table_name = _quoted_table_name(table)
    columns = ", ".join(preparer.quote_identifier(column.name) for column in table.columns)
    literals = ", ".join(
        _sql_literal(column, values[column.key], MYSQL_RESTORE_DIALECT)
        for column in table.columns
    )
    return f"INSERT INTO {table_name} ({columns}) VALUES ({literals})"


def _render_reference_update(
    plan: DatabaseReferenceCheck,
    values: Mapping[str, object],
    primary_key: tuple[Column[Any], ...],
) -> str:
    """Render a guarded restoration update for references cleared during removal."""

    preparer = MYSQL_RESTORE_DIALECT.identifier_preparer
    assignments = ", ".join(
        f"{preparer.quote_identifier(column.name)} = "
        f"{_sql_literal(column, values[column.key], MYSQL_RESTORE_DIALECT)}"
        for column in plan.reference_columns
    )
    identity = " AND ".join(
        f"{preparer.quote_identifier(column.name)} = "
        f"{_sql_literal(column, values[column.key], MYSQL_RESTORE_DIALECT)}"
        for column in primary_key
    )
    cleared = " AND ".join(
        f"{preparer.quote_identifier(column.name)} IS NULL"
        for column in plan.reference_columns
    )
    return (
        f"UPDATE {_quoted_table_name(plan.table)} SET {assignments} "
        f"WHERE {identity} AND {cleared}"
    )


def _quoted_table_name(table: Table) -> str:
    preparer = MYSQL_RESTORE_DIALECT.identifier_preparer
    table_name = preparer.quote_identifier(table.name)
    if table.schema is not None:
        table_name = f"{preparer.quote_identifier(table.schema)}.{table_name}"
    return table_name


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
    payload = _payload_with_real_parents(artifact, relative_path)
    try:
        metadata = payload.lstat()
    except OSError as error:
        raise ValueError(f"Cannot read backup payload {relative_path}") from error
    if stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"Backup payload must not be a symbolic link: {relative_path}")
    if not stat.S_ISREG(payload.lstat().st_mode):
        raise ValueError(f"Backup payload is not a regular file: {relative_path}")
    if _hash_file(payload) != expected_hash:
        raise ValueError(f"Backup payload checksum failed: {relative_path}")
    return payload


def _verify_symlink_payload(
    artifact: Path,
    relative_path: PurePosixPath,
    *,
    expected_target: str,
    expected_size: int,
    expected_hash: str,
) -> None:
    payload = _payload_with_real_parents(artifact, relative_path)
    try:
        metadata = payload.lstat()
        target = os.readlink(payload)
    except OSError as error:
        raise ValueError(f"Cannot read backup symbolic link {relative_path}") from error
    if not stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"Backup payload is not a symbolic link: {relative_path}")
    target_bytes = os.fsencode(target)
    if target != expected_target:
        raise ValueError(f"Backup symbolic-link target does not match: {relative_path}")
    if len(target_bytes) != expected_size:
        raise ValueError(f"Backup symbolic-link target has the wrong size: {relative_path}")
    if hashlib.sha256(target_bytes).hexdigest() != expected_hash:
        raise ValueError(f"Backup symbolic-link checksum failed: {relative_path}")


def _payload_with_real_parents(artifact: Path, relative_path: PurePosixPath) -> Path:
    if (
        relative_path.is_absolute()
        or ".." in relative_path.parts
        or not relative_path.parts
    ):
        raise ValueError(f"Backup payload escapes the artifact: {relative_path}")
    current = artifact
    for part in relative_path.parts[:-1]:
        current /= part
        try:
            metadata = current.lstat()
        except OSError as error:
            raise ValueError(f"Cannot read backup payload {relative_path}") from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(
                f"Backup payload parent is not a real directory: {relative_path}"
            )
    return artifact.joinpath(*relative_path.parts)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(COPY_BUFFER_SIZE):
                digest.update(chunk)
    except OSError as error:
        raise ValueError(f"Cannot read backup payload {path}") from error
    return digest.hexdigest()
