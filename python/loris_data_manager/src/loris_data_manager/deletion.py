"""Backup-first deletion of DICOM archives and their owned resources."""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import cast

from sqlalchemy import and_, delete, tuple_, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from loris_data_manager.backup import (
    COPY_BUFFER_SIZE,
    ROW_BATCH_SIZE,
    BackupRequest,
    BackupResult,
    BackupTransactionAction,
    CapturedReferenceRows,
    CapturedTableRows,
    VerifiedBackupCapture,
    create_backup_with_action,
    plan_backup,
    preview_backup,
    required_storage_roots,
)
from loris_data_manager.provider import ResourceSchema

QUARANTINE_DIRECTORY = ".loris-data-manager-quarantine"


@dataclass(frozen=True, slots=True)
class DeletionPreview:
    """Read-only summary of a materialized DICOM archive deletion plan."""

    selected_object_count: int
    database_row_count: int
    file_count: int
    blocker_count: int
    reference_update_count: int

    @property
    def eligible(self) -> bool:
        return self.blocker_count == 0


@dataclass(frozen=True, slots=True)
class DeletionResult:
    """Summary of a committed backup-first deletion."""

    backup: BackupResult
    deleted_database_row_count: int
    updated_reference_count: int
    quarantine_purged: bool
    cleanup_paths: tuple[Path, ...]
    warnings: tuple[str, ...]


def plan_deletion(
    engine: Engine,
    schema: ResourceSchema,
    request: BackupRequest,
) -> DeletionPreview:
    """Materialize a read-only DICOM archive deletion plan."""

    _validate_deletion_request(schema, request)
    preview = preview_backup(engine, schema, request.selection)

    return DeletionPreview(
        selected_object_count=preview.selected_object_count,
        database_row_count=preview.database_row_count,
        file_count=preview.file_count,
        blocker_count=preview.blocker_count,
        reference_update_count=preview.reference_update_count,
    )


def delete_resources(
    engine: Engine,
    schema: ResourceSchema,
    request: BackupRequest,
) -> DeletionResult:
    """Back up and delete selected DICOM archives in one database transaction."""

    _validate_deletion_request(schema, request)
    action = _DicomArchiveDeletion(request.storage_roots)
    backup = create_backup_with_action(engine, schema, request, action)
    return DeletionResult(
        backup=backup,
        deleted_database_row_count=action.deleted_database_row_count,
        updated_reference_count=action.updated_reference_count,
        quarantine_purged=not action.cleanup_paths,
        cleanup_paths=tuple(action.cleanup_paths),
        warnings=tuple(dict.fromkeys((*backup.warnings, *action.warnings))),
    )


def _validate_deletion_request(schema: ResourceSchema, request: BackupRequest):
    selection = request.selection
    if (
        len(selection.selections) != 1
        or selection.selections[0].path.root_kind != "dicom-archive"
        or selection.selections[0].target_kind != "dicom-archive"
        or selection.selections[0].path.link_members
        or selection.selections[0].path.property is not None
    ):
        raise ValueError(
            "Proof-of-concept deletion requires exactly one direct "
            "--select dicom-archive projection"
        )
    required_roots = required_storage_roots(schema, "dicom-archive")
    missing = {name for name in required_roots if name not in request.storage_roots}
    if missing:
        raise ValueError(
            f"Missing storage roots required by resource schema: {sorted(missing)}"
        )
    return plan_backup(schema, selection)


class _DicomArchiveDeletion(BackupTransactionAction):
    def __init__(self, storage_roots: Mapping[str, Path]) -> None:
        self.storage_roots = storage_roots
        self.deleted_database_row_count = 0
        self.updated_reference_count = 0
        self.cleanup_paths: list[Path] = []
        self.warnings: list[str] = []
        self._moved: list[tuple[Path, Path]] = []
        self._quarantine_roots: dict[str, Path] = {}

    def validate(self, blocker_count: int) -> None:
        if blocker_count:
            raise ValueError(
                "Deletion is blocked by "
                f"{blocker_count} unapproved external reference(s)"
            )

    def apply(self, db: Session, capture: VerifiedBackupCapture) -> None:
        try:
            self.updated_reference_count = _clear_references(
                db, capture.references
            )
            self.deleted_database_row_count = _delete_owned_rows(db, capture.tables)
            self._quarantine_files(capture)
        except BaseException as error:
            try:
                self._restore_quarantine()
            except BaseException as restore_error:
                raise RuntimeError(
                    "Deletion failed and quarantined files could not all be restored; "
                    f"manual reconciliation is required: {restore_error}"
                ) from error
            raise

    def after_commit(self) -> None:
        for quarantine in self._quarantine_roots.values():
            try:
                shutil.rmtree(quarantine)
            except OSError:
                self.cleanup_paths.append(quarantine)
        self._moved.clear()

    def commit_failed(self) -> None:
        paths = ", ".join(str(path) for path in self._quarantine_roots.values())
        raise RuntimeError(
            "The database commit outcome is indeterminate; quarantined files were "
            f"left for manual reconciliation at: {paths or '(no files)'}"
        )

    def _quarantine_files(self, capture: VerifiedBackupCapture) -> None:
        if len(capture.pending_files) != len(capture.file_entries):
            raise RuntimeError("Backup file capture is internally inconsistent")
        for pending, entry in zip(capture.pending_files, capture.file_entries):
            root = self.storage_roots[pending.storage_root].resolve(strict=True)
            if not root.is_dir():
                raise ValueError(f"Storage root is not a directory: {root}")
            relative = pending.relative_path
            if relative.parts[0] == QUARANTINE_DIRECTORY:
                raise ValueError(f"A managed file path enters the quarantine namespace: {relative}")
            if entry.get("storage_root") != pending.storage_root or entry.get(
                "relative_path"
            ) != str(relative):
                raise RuntimeError("Backup file order is internally inconsistent")
            path_type = entry.get("path_type")
            digest = entry.get("sha256")
            size = entry.get("size")
            if path_type not in {"missing", "regular-file", "symlink"}:
                raise RuntimeError("Verified backup has invalid file metadata")
            source = root.joinpath(*relative.parts)
            try:
                source.lstat()
            except FileNotFoundError:
                self.warnings.append(f"Expected managed path is missing: {source}")
                continue
            except OSError as error:
                raise ValueError(f"Cannot inspect deletion source {source}") from error
            if path_type == "missing":
                raise ValueError(
                    f"Managed path appeared after its backup recorded it missing: {source}"
                )
            if not isinstance(digest, str) or not isinstance(size, int):
                raise RuntimeError("Verified backup has invalid file metadata")
            link_target: str | None = None
            if path_type == "symlink":
                loaded_link_target = entry.get("link_target")
                if not isinstance(loaded_link_target, str):
                    raise RuntimeError(
                        "Verified backup has invalid symbolic-link metadata"
                    )
                link_target = loaded_link_target
                target_warning = _symlink_target_warning(source)
                if target_warning is not None:
                    self.warnings.append(target_warning)
            quarantine_root = self._quarantine_root(
                pending.storage_root,
                root,
                capture.result.operation_id,
            )
            destination = quarantine_root.joinpath(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            _require_real_path(root, relative.parent)
            _require_real_path(quarantine_root, relative.parent)
            if path_type == "regular-file":
                _quarantine_verified_file(
                    source,
                    destination,
                    expected_size=size,
                    expected_digest=digest,
                    moved=self._moved,
                )
            else:
                assert link_target is not None
                _quarantine_verified_symlink(
                    source,
                    destination,
                    expected_target=link_target,
                    expected_size=size,
                    expected_digest=digest,
                    moved=self._moved,
                )

    def _quarantine_root(self, name: str, root: Path, operation_id: str) -> Path:
        existing = self._quarantine_roots.get(name)
        if existing is not None:
            return existing
        parent = root / QUARANTINE_DIRECTORY
        try:
            parent.mkdir(mode=0o700, exist_ok=True)
        except OSError as error:
            raise ValueError(f"Cannot create quarantine directory {parent}") from error
        metadata = parent.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(f"Quarantine parent is not a real directory: {parent}")
        quarantine = parent / operation_id
        try:
            quarantine.mkdir(mode=0o700)
        except OSError as error:
            raise ValueError(f"Cannot create operation quarantine {quarantine}") from error
        self._quarantine_roots[name] = quarantine
        return quarantine

    def _restore_quarantine(self) -> None:
        failures: list[str] = []
        for source, quarantine in reversed(self._moved):
            try:
                if source.exists() or source.is_symlink():
                    raise OSError(f"live path was recreated: {source}")
                os.replace(quarantine, source)
            except OSError as error:
                failures.append(str(error))
        for quarantine in self._quarantine_roots.values():
            if not failures:
                shutil.rmtree(quarantine, ignore_errors=True)
        self._moved.clear()
        if failures:
            raise RuntimeError("; ".join(failures))


def _clear_references(
    db: Session,
    references: tuple[CapturedReferenceRows, ...],
) -> int:
    updated = 0
    for captured in references:
        plan = captured.plan
        if plan.action != "set-null":
            continue
        primary_key = tuple(plan.table.primary_key.columns)
        for row in captured.rows:
            guards = [column == row[column.key] for column in primary_key]
            guards.extend(
                column == row[column.key] for column in plan.reference_columns
            )
            result = db.execute(
                update(plan.table)
                .where(and_(*guards))
                .values({column.key: None for column in plan.reference_columns})
            )
            rowcount = cast(int, getattr(result, "rowcount"))
            if rowcount != 1:
                raise ValueError(
                    f"Expected to clear one reference in {plan.table.fullname!r}; "
                    f"updated {rowcount}"
                )
            updated += 1
    return updated


def _delete_owned_rows(
    db: Session,
    tables: tuple[CapturedTableRows, ...],
) -> int:
    deleted = 0
    for captured in reversed(tables):
        primary_key = tuple(captured.plan.table.primary_key.columns)
        for start in range(0, len(captured.identities), ROW_BATCH_SIZE):
            identities = captured.identities[start : start + ROW_BATCH_SIZE]
            if len(primary_key) == 1:
                predicate = primary_key[0].in_(identity[0] for identity in identities)
            else:
                predicate = tuple_(*primary_key).in_(identities)
            result = db.execute(delete(captured.plan.table).where(predicate))
            rowcount = cast(int, getattr(result, "rowcount"))
            if rowcount != len(identities):
                raise ValueError(
                    f"Expected to delete {len(identities)} row(s) from "
                    f"{captured.plan.table.fullname!r}; deleted {rowcount}"
                )
            deleted += rowcount
    return deleted


def _require_real_path(root: Path, relative: PurePosixPath) -> None:
    current = root
    for part in relative.parts:
        current /= part
        try:
            metadata = current.lstat()
        except OSError as error:
            raise ValueError(f"Cannot inspect managed path component {current}") from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(f"Managed path component is not a real directory: {current}")


def _symlink_target_warning(source: Path) -> str | None:
    try:
        link_target = os.readlink(source)
    except OSError:
        return None
    try:
        source.stat()
    except FileNotFoundError:
        return f"Symbolic-link target is missing: {source} -> {link_target}"
    except OSError as error:
        return f"Cannot inspect symbolic-link target for {source}: {error}"
    return None


def _quarantine_verified_file(
    source: Path,
    destination: Path,
    *,
    expected_size: int,
    expected_digest: str,
    moved: list[tuple[Path, Path]],
) -> None:
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise ValueError("Deletion requires O_NOFOLLOW support")
    try:
        path_metadata = source.lstat()
        if stat.S_ISLNK(path_metadata.st_mode) or not stat.S_ISREG(path_metadata.st_mode):
            raise ValueError(f"Deletion source is not a real regular file: {source}")
        descriptor = os.open(source, os.O_RDONLY | no_follow)
        with os.fdopen(descriptor, "rb") as source_file:
            before = os.fstat(source_file.fileno())
            digest = hashlib.sha256()
            while chunk := source_file.read(COPY_BUFFER_SIZE):
                digest.update(chunk)
            after = os.fstat(source_file.fileno())
        current = source.lstat()
    except OSError as error:
        raise ValueError(f"Cannot revalidate deletion source file {source}") from error
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"Deletion source is not a regular file: {source}")
    if (before.st_dev, before.st_ino) != (path_metadata.st_dev, path_metadata.st_ino):
        raise ValueError(f"Deletion source changed while opening it: {source}")
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"Deletion source changed while it was read: {source}")
    if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
        raise ValueError(f"Deletion source changed before quarantine: {source}")
    if after.st_size != expected_size or digest.hexdigest() != expected_digest:
        raise ValueError(f"Deletion source no longer matches its verified backup: {source}")
    if destination.parent.stat().st_dev != before.st_dev:
        raise ValueError(f"Quarantine is not on the source filesystem: {destination}")
    try:
        os.replace(source, destination)
        moved.append((source, destination))
        quarantined = destination.lstat()
    except OSError as error:
        raise ValueError(f"Cannot quarantine deletion source file {source}") from error
    if (quarantined.st_dev, quarantined.st_ino) != (before.st_dev, before.st_ino):
        raise ValueError(f"Quarantined file identity changed: {destination}")


def _quarantine_verified_symlink(
    source: Path,
    destination: Path,
    *,
    expected_target: str,
    expected_size: int,
    expected_digest: str,
    moved: list[tuple[Path, Path]],
) -> None:
    try:
        before = source.lstat()
        if not stat.S_ISLNK(before.st_mode):
            raise ValueError(f"Deletion source is not a symbolic link: {source}")
        target = os.readlink(source)
        current = source.lstat()
    except OSError as error:
        raise ValueError(f"Cannot revalidate deletion source symbolic link {source}") from error
    target_bytes = os.fsencode(target)
    if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
        raise ValueError(f"Deletion source symbolic link changed while it was read: {source}")
    if (
        target != expected_target
        or len(target_bytes) != expected_size
        or hashlib.sha256(target_bytes).hexdigest() != expected_digest
    ):
        raise ValueError(
            f"Deletion source symbolic link no longer matches its verified backup: {source}"
        )
    if destination.parent.stat().st_dev != before.st_dev:
        raise ValueError(f"Quarantine is not on the source filesystem: {destination}")
    try:
        os.replace(source, destination)
        moved.append((source, destination))
        quarantined = destination.lstat()
        quarantined_target = os.readlink(destination)
    except OSError as error:
        raise ValueError(
            f"Cannot quarantine deletion source symbolic link {source}"
        ) from error
    if (
        not stat.S_ISLNK(quarantined.st_mode)
        or (quarantined.st_dev, quarantined.st_ino) != (before.st_dev, before.st_ino)
        or quarantined_target != target
    ):
        raise ValueError(f"Quarantined symbolic link identity changed: {destination}")
