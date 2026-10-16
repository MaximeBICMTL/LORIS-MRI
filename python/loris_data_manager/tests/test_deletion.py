from pathlib import Path
from types import SimpleNamespace

import pytest
from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.dicom_archive_file import DbDicomArchiveFile
from lib.db.models.dicom_archive_series import DbDicomArchiveSeries
from lib.db.models.file import DbFile
from lib.db.models.mri_protocol_violated_scan import DbMriProtocolViolatedScan
from lib.db.models.mri_upload import DbMriUpload
from lib.db.models.mri_violation_log import DbMriViolationLog
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

import loris_data_manager.cli as cli_module
import loris_data_manager.deletion as deletion_module
from loris_data_manager.backup import BackupRequest, BackupSelection, verify_backup
from loris_data_manager.cli import app, make_resource_schema
from loris_data_manager.deletion import (
    QUARANTINE_DIRECTORY,
    delete_resources,
    plan_deletion,
)
from loris_data_manager.projection import parse_resource_selection


def _add_archive(db: Session, *, suffix: str = "archive") -> DbDicomArchive:
    archive = DbDicomArchive(
        study_uid=f"1.2.3.4.{suffix}",
        patient_id="candidate",
        patient_name="candidate",
        center_name="example",
        acquisition_count=1,
        dicom_file_count=1,
        non_dicom_file_count=0,
        creating_user="admin",
        sum_type_version=2,
        scanner_manufacturer="Example",
        scanner_model="Example",
        scanner_serial_number="123",
        scanner_software_version="1",
        source_path=Path(f"incoming/{suffix}"),
        path=Path(f"2026/{suffix}.tar"),
        acquisition_metadata="",
    )
    db.add(archive)
    db.flush()
    series = DbDicomArchiveSeries(
        archive_id=archive.id,
        series_number=1,
        number_of_files=1,
        modality="MR",
    )
    db.add(series)
    db.flush()
    db.add_all(
        (
            DbDicomArchiveFile(
                archive_id=archive.id,
                series_id=series.id,
                md5_sum="01234567890abcdef0123456789abcde",
                file_name="image.dcm",
            ),
            DbMriUpload(
                uploaded_by="admin",
                upload_path=Path(f"uploads/{suffix}.tar"),
                decompressed_path=Path(f"uploads/{suffix}"),
                patient_name="candidate",
                dicom_archive_id=archive.id,
            ),
        )
    )
    db.flush()
    return archive


def _request(
    archive_root: Path,
    output: Path,
    *,
    archive_id: int | None = None,
    update_upload: bool = False,
    update_all: bool = False,
    select_all: bool = False,
) -> BackupRequest:
    schema = make_resource_schema()
    return BackupRequest(
        output=output,
        selection=BackupSelection(
            selections=(parse_resource_selection(schema, "dicom-archive"),),
            criteria=(
                (schema.criterion("dicom-archive.id", archive_id),)
                if archive_id is not None
                else ()
            ),
            select_all=select_all,
            updates=(
                tuple(
                    parse_resource_selection(schema, expression)
                    for expression in (
                        "dicom-archive.mri-upload",
                        "dicom-archive.mri-violation",
                        "dicom-archive.protocol-violated-scan",
                        "dicom-archive.imaging-file",
                    )
                )
                if update_all
                else (
                    (parse_resource_selection(schema, "dicom-archive.mri-upload"),)
                    if update_upload
                    else ()
                )
            ),
        ),
        storage_roots={"dicom-archive": archive_root},
    )


def _write_archive(archive_root: Path, suffix: str = "archive") -> Path:
    source = archive_root / "2026" / f"{suffix}.tar"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(f"raw DICOM {suffix}".encode())
    return source


def test_deletion_preview_is_read_only_and_reports_blockers(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    source = _write_archive(archive_root)
    output = tmp_path / "backup"

    preview = plan_deletion(
        db_engine,
        make_resource_schema(),
        _request(archive_root, output, archive_id=archive_id),
    )

    assert preview.selected_object_count == 1
    assert preview.database_row_count == 3
    assert preview.file_count == 1
    assert preview.blocker_count == 1
    assert preview.reference_update_count == 0
    assert not preview.eligible
    assert source.is_file()
    assert not output.exists()
    with Session(db_engine) as db:
        assert db.get(DbDicomArchive, archive_id) is not None


def test_deletion_creates_backup_clears_references_and_removes_owned_resources(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        upload = db.scalar(
            select(DbMriUpload).where(DbMriUpload.dicom_archive_id == archive_id)
        )
        assert upload is not None
        upload_id = upload.id
        violation = DbMriViolationLog(
            dicom_archive_id=archive_id,
            file_path=Path("assembly/example/violation.mnc"),
        )
        protocol_scan = DbMriProtocolViolatedScan(
            dicom_archive_id=archive_id,
            file_path=Path("trashbin/example/unknown.mnc"),
        )
        imaging_file = DbFile(
            session_id=1,
            path=Path("assembly/example/native/example.mnc"),
            output_type="native",
            inserted_by_user_id="admin",
            dicom_archive_id=archive_id,
        )
        db.add_all((violation, protocol_scan, imaging_file))
        db.flush()
        violation_id = violation.id
        protocol_scan_id = protocol_scan.id
        imaging_file_id = imaging_file.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    source = _write_archive(archive_root)
    output = tmp_path / "backup"

    result = delete_resources(
        db_engine,
        make_resource_schema(),
        _request(
            archive_root,
            output,
            archive_id=archive_id,
            update_all=True,
        ),
    )

    assert result.deleted_database_row_count == 3
    assert result.updated_reference_count == 4
    assert result.backup.output == output.resolve()
    assert result.backup.blocker_count == 0
    assert result.quarantine_purged
    assert not source.exists()
    assert not tuple((archive_root / QUARANTINE_DIRECTORY).glob("*"))
    verify_backup(output)
    assert (
        output / "files" / "dicom-archive" / "2026" / "archive.tar"
    ).read_bytes() == b"raw DICOM archive"
    with Session(db_engine) as db:
        assert db.get(DbDicomArchive, archive_id) is None
        restored_upload = db.get(DbMriUpload, upload_id)
        assert restored_upload is not None
        assert restored_upload.dicom_archive_id is None
        restored_violation = db.get(DbMriViolationLog, violation_id)
        restored_protocol_scan = db.get(DbMriProtocolViolatedScan, protocol_scan_id)
        restored_imaging_file = db.get(DbFile, imaging_file_id)
        assert restored_violation is not None
        assert restored_violation.dicom_archive_id is None
        assert restored_protocol_scan is not None
        assert restored_protocol_scan.dicom_archive_id is None
        assert restored_imaging_file is not None
        assert restored_imaging_file.dicom_archive_id is None
        assert db.scalar(select(func.count()).select_from(DbDicomArchiveSeries)) == 0
        assert db.scalar(select(func.count()).select_from(DbDicomArchiveFile)) == 0


def test_blocked_deletion_fails_before_backup_and_retains_live_resources(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    source = _write_archive(archive_root)
    output = tmp_path / "backup"

    with pytest.raises(ValueError, match="blocked by 1"):
        delete_resources(
            db_engine,
            make_resource_schema(),
            _request(archive_root, output, archive_id=archive_id),
        )

    assert not output.exists()
    assert source.is_file()
    with Session(db_engine) as db:
        assert db.get(DbDicomArchive, archive_id) is not None


def test_quarantine_failure_restores_moved_files_and_rolls_back_database(
    db_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with Session(db_engine) as db:
        first = _add_archive(db, suffix="first")
        second = _add_archive(db, suffix="second")
        archive_ids = (first.id, second.id)
        db.commit()
    archive_root = tmp_path / "archive-root"
    first_source = _write_archive(archive_root, "first")
    second_source = _write_archive(archive_root, "second")
    output = tmp_path / "backup"
    original_quarantine = deletion_module._quarantine_verified_file  # pyright: ignore[reportPrivateUsage]
    call_count = 0

    def fail_second_quarantine(
        source: Path,
        destination: Path,
        *,
        expected_size: int,
        expected_digest: str,
        moved: list[tuple[Path, Path]],
    ) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise ValueError("simulated quarantine failure")
        original_quarantine(
            source,
            destination,
            expected_size=expected_size,
            expected_digest=expected_digest,
            moved=moved,
        )

    monkeypatch.setattr(
        deletion_module,
        "_quarantine_verified_file",
        fail_second_quarantine,
    )

    with pytest.raises(ValueError, match="simulated quarantine failure"):
        delete_resources(
            db_engine,
            make_resource_schema(),
            _request(
                archive_root,
                output,
                update_upload=True,
                select_all=True,
            ),
        )

    verify_backup(output)
    assert first_source.is_file()
    assert second_source.is_file()
    assert not tuple((archive_root / QUARANTINE_DIRECTORY).glob("*"))
    with Session(db_engine) as db:
        assert all(db.get(DbDicomArchive, archive_id) is not None for archive_id in archive_ids)
        assert db.scalar(select(func.count()).select_from(DbMriUpload)) == 2
        assert all(
            upload.dicom_archive_id is not None
            for upload in db.scalars(select(DbMriUpload))
        )


def test_deletion_rejects_symlink_resources_after_backing_them_up(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    target = tmp_path / "target.tar"
    target.write_bytes(b"external target")
    source = archive_root / "2026" / "archive.tar"
    source.parent.mkdir(parents=True)
    source.symlink_to(target)
    output = tmp_path / "backup"

    with pytest.raises(ValueError, match="symbolic-link resources"):
        delete_resources(
            db_engine,
            make_resource_schema(),
            _request(
                archive_root,
                output,
                archive_id=archive_id,
                update_upload=True,
            ),
        )

    verify_backup(output)
    assert source.is_symlink()
    assert target.read_bytes() == b"external target"
    with Session(db_engine) as db:
        assert db.get(DbDicomArchive, archive_id) is not None


def test_deletion_cli_defaults_to_dry_run_and_advertises_execute(
    db_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    source = _write_archive(archive_root)
    output = tmp_path / "backup"

    def load_config_without_io(profile: str | None) -> SimpleNamespace:
        del profile
        return SimpleNamespace(mysql=object())

    def use_test_engine(config: object) -> Engine:
        del config
        return db_engine

    def archive_root_config(db: Session, name: str) -> SimpleNamespace:
        del db, name
        return SimpleNamespace(value=str(archive_root))

    monkeypatch.setattr(cli_module, "load_config", load_config_without_io)
    monkeypatch.setattr(cli_module, "get_database_engine", use_test_engine)
    monkeypatch.setattr(
        cli_module,
        "try_get_config_with_setting_name",
        archive_root_config,
    )
    arguments = (
        "delete",
        "--select",
        "dicom-archive",
        "--update",
        "dicom-archive.mri-upload",
        "--where",
        f"dicom-archive.id={archive_id}",
        "--output",
        str(output),
    )

    preview = CliRunner().invoke(app, arguments)

    assert preview.exit_code == 0
    assert "Planned deletion" in preview.output
    assert "No changes made" in preview.output
    assert source.is_file()
    assert not output.exists()

    help_result = CliRunner().invoke(app, ("delete", "--help"))
    assert help_result.exit_code == 0
    assert "--execute" in help_result.output
    assert "backup-first DICOM archive deletion" in help_result.output
