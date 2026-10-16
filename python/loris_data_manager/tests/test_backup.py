from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.dicom_archive_file import DbDicomArchiveFile
from lib.db.models.dicom_archive_series import DbDicomArchiveSeries
from lib.db.models.file import DbFile
from lib.db.models.file_parameter import DbFileParameter
from lib.db.models.mri_protocol_violated_scan import DbMriProtocolViolatedScan
from lib.db.models.mri_upload import DbMriUpload
from lib.db.models.mri_violation_log import DbMriViolationLog
from lib.db.models.parameter_type import DbParameterType
from sqlalchemy import Column, Engine, ForeignKey, Integer, MetaData, String, Table, event, select
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session
from typer.testing import CliRunner

import loris_data_manager.cli as cli_module
from loris_data_manager.backup import (
    BackupRequest,
    BackupResult,
    BackupSelection,
    DatabaseOwnership,
    DatabaseTableBackup,
    _inspect_database_dependencies,  # pyright: ignore[reportPrivateUsage]
    _locking_statement,  # pyright: ignore[reportPrivateUsage]
    create_backup,
    plan_backup,
    required_storage_roots,
    verify_backup,
)
from loris_data_manager.cli import app, make_resource_schema
from loris_data_manager.projection import parse_resource_selection
from loris_data_manager.schema import PropertyCriterion


def _add_archive(db: Session) -> DbDicomArchive:
    archive = DbDicomArchive(
        study_uid="1.2.3.4",
        patient_id="candidate",
        patient_name="O'Brien 100% \\ archive",
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
        source_path=Path("incoming/archive"),
        path=Path("2026/archive.tar"),
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
                upload_path=Path("uploads/archive.tar"),
                decompressed_path=Path("uploads/archive"),
                patient_name="candidate",
                dicom_archive_id=archive.id,
            ),
        )
    )
    db.flush()
    return archive


def _create_archive_backup(
    db_engine: Engine,
    output: Path,
    archive_root: Path,
    criteria: tuple[PropertyCriterion, ...],
    *,
    where_expressions: tuple[str, ...] = (),
) -> BackupResult:
    schema = make_resource_schema()
    return create_backup(
        db_engine,
        schema,
        BackupRequest(
            output=output,
            selection=BackupSelection(
                selections=(
                    parse_resource_selection(schema, "dicom-archive"),
                ),
                criteria=criteria,
                where_expressions=where_expressions,
            ),
            storage_roots={"dicom-archive": archive_root},
        ),
    )


def test_dicom_backup_creates_verified_sql_manifest_and_file_artifact(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    source = archive_root / "2026" / "archive.tar"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"raw dicom archive")
    output = tmp_path / "backup"
    schema = make_resource_schema()
    expression = f"dicom-archive.id={archive_id}"
    result = _create_archive_backup(
        db_engine,
        output,
        archive_root,
        (schema.parse_criterion("dicom-archive.id", str(archive_id)),),
        where_expressions=(expression,),
    )

    assert result.selected_object_count == 1
    assert result.selected_object_counts == {"dicom-archive": 1}
    assert result.database_row_count == 3
    assert result.file_count == 1
    assert result.blocker_count == 1
    assert (output / "files" / "dicom-archive" / "2026" / "archive.tar").read_bytes() == (
        b"raw dicom archive"
    )

    restore_sql = (output / "restore.sql").read_text()
    assert "START TRANSACTION;" in restore_sql
    assert "INSERT INTO `tarchive` " in restore_sql
    assert "INSERT INTO `tarchive_series` " in restore_sql
    assert "INSERT INTO `tarchive_files` " in restore_sql
    assert "INSERT INTO `mri_upload` " not in restore_sql
    assert "O''Brien 100% \\\\ archive" in restore_sql
    assert "CONVERT(" not in restore_sql
    assert restore_sql.index("INSERT INTO `tarchive` ") < restore_sql.index(
        "INSERT INTO `tarchive_series` "
    )
    assert restore_sql.rstrip().endswith("COMMIT;")

    manifest = verify_backup(output)
    assert manifest["selection"] == {
        "all": False,
        "anchor": {"kind": "dicom-archive", "matched_count": 1},
        "select": ["dicom-archive"],
        "selected": {"dicom-archive": 1},
        "where": [f"dicom-archive.id={archive_id}"],
    }
    assert manifest["prospective_removal"] == {"blocker_count": 1, "eligible": False}
    database = cast(dict[str, object], manifest["database"])
    tables = cast(list[dict[str, object]], database["tables"])
    assert [entry["row_count"] for entry in tables] == [1, 1, 1]


def test_dicom_backup_records_approved_reference_updates_and_restoration_sql(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        imaging_file = DbFile(
            session_id=1,
            path=Path("assembly/example/native/example.mnc"),
            output_type="native",
            inserted_by_user_id="admin",
            dicom_archive_id=archive.id,
        )
        violation = DbMriViolationLog(
            dicom_archive_id=archive.id,
            file_path=Path("assembly/example/violation.mnc"),
        )
        protocol_violated_scan = DbMriProtocolViolatedScan(
            dicom_archive_id=archive.id,
            file_path=Path("trashbin/example/unknown.mnc"),
        )
        db.add_all((imaging_file, violation, protocol_violated_scan))
        db.flush()
        imaging_file_id = imaging_file.id
        violation_id = violation.id
        protocol_violated_scan_id = protocol_violated_scan.id
        upload = db.scalar(
            select(DbMriUpload).where(DbMriUpload.dicom_archive_id == archive.id)
        )
        assert upload is not None
        upload_id = upload.id
        db.commit()

    archive_root = tmp_path / "archive-root"
    archive_file = archive_root / "2026" / "archive.tar"
    archive_file.parent.mkdir(parents=True)
    archive_file.write_bytes(b"raw dicom archive")
    schema = make_resource_schema()
    update_expressions = (
        "dicom-archive.mri-upload",
        "dicom-archive.mri-violation",
        "dicom-archive.protocol-violated-scan",
        "dicom-archive.imaging-file",
    )
    output = tmp_path / "backup"

    result = create_backup(
        db_engine,
        schema,
        BackupRequest(
            output=output,
            selection=BackupSelection(
                selections=(parse_resource_selection(schema, "dicom-archive"),),
                criteria=(schema.criterion("dicom-archive.id", archive_id),),
                updates=tuple(
                    parse_resource_selection(schema, expression)
                    for expression in update_expressions
                ),
            ),
            storage_roots={"dicom-archive": archive_root},
        ),
    )

    assert result.blocker_count == 0
    assert result.reference_update_count == 4
    restore_sql = (output / "restore.sql").read_text()
    expected_updates = (
        f"UPDATE `mri_upload` SET `TarchiveID` = {archive_id} "
        f"WHERE `UploadID` = {upload_id} AND `TarchiveID` IS NULL;",
        f"UPDATE `mri_violations_log` SET `TarchiveID` = {archive_id} "
        f"WHERE `LogID` = {violation_id} AND `TarchiveID` IS NULL;",
        f"UPDATE `mri_protocol_violated_scans` SET `TarchiveID` = {archive_id} "
        f"WHERE `ID` = {protocol_violated_scan_id} AND `TarchiveID` IS NULL;",
        f"UPDATE `files` SET `TarchiveSource` = {archive_id} "
        f"WHERE `FileID` = {imaging_file_id} AND `TarchiveSource` IS NULL;",
    )
    for statement in expected_updates:
        assert statement in restore_sql
        assert restore_sql.index("INSERT INTO `tarchive` ") < restore_sql.index(statement)

    manifest = verify_backup(output)
    selection = cast(dict[str, object], manifest["selection"])
    assert selection["update"] == list(update_expressions)
    references = cast(list[dict[str, object]], manifest["references"])
    updated = {
        cast(str, entry["relationship"]): entry
        for entry in references
        if entry["action"] == "set-null"
    }
    assert set(updated) == set(update_expressions)
    assert all(entry["row_count"] == 1 for entry in updated.values())
    assert manifest["prospective_removal"] == {
        "blocker_count": 0,
        "eligible": True,
        "reference_update_count": 4,
    }

    with Session(db_engine) as db:
        assert db.get(DbMriUpload, upload_id).dicom_archive_id == archive_id  # type: ignore[union-attr]
        assert db.get(DbMriViolationLog, violation_id).dicom_archive_id == archive_id  # type: ignore[union-attr]
        assert (
            db.get(
                DbMriProtocolViolatedScan, protocol_violated_scan_id
            ).dicom_archive_id  # type: ignore[union-attr]
            == archive_id
        )
        assert db.get(DbFile, imaging_file_id).dicom_archive_id == archive_id  # type: ignore[union-attr]


def test_backup_rejects_an_update_without_schema_permission():
    schema = make_resource_schema()

    with pytest.raises(ValueError, match="does not permit reference updates"):
        plan_backup(
            schema,
            BackupSelection(
                selections=(parse_resource_selection(schema, "dicom-archive"),),
                select_all=True,
                updates=(parse_resource_selection(schema, "dicom-archive.session"),),
            ),
        )


def test_backup_verification_detects_a_modified_file(db_engine: Engine, tmp_path: Path):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    source = archive_root / "2026" / "archive.tar"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"raw dicom archive")
    output = tmp_path / "backup"
    schema = make_resource_schema()
    _create_archive_backup(
        db_engine,
        output,
        archive_root,
        (schema.criterion("dicom-archive.id", archive_id),),
    )
    (output / "files" / "dicom-archive" / "2026" / "archive.tar").write_bytes(b"changed")

    with pytest.raises(ValueError, match="checksum failed"):
        verify_backup(output)


def test_imaging_file_backup_captures_its_row_and_file_without_its_source_archive(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        imaging_file = DbFile(
            session_id=1,
            path=Path("assembly/example/native/example.mnc"),
            output_type="native",
            inserted_by_user_id="admin",
            dicom_archive_id=archive.id,
        )
        db.add(imaging_file)
        db.flush()
        parameter_types = {
            name: DbParameterType(id=index, name=name)
            for index, name in enumerate(
                (
                    "modality",
                    "bids_json_file",
                    "bids_bval",
                    "bids_bvec",
                    "check_pic_filename",
                ),
                start=1000,
            )
        }
        db.add_all(parameter_types.values())
        db.flush()
        db.add_all(
            (
                DbFileParameter(
                    file_id=imaging_file.id,
                    type_id=parameter_types["modality"].id,
                    value="MR",
                ),
                DbFileParameter(
                    file_id=imaging_file.id,
                    type_id=parameter_types["bids_json_file"].id,
                    value="bids/example.json",
                ),
                DbFileParameter(
                    file_id=imaging_file.id,
                    type_id=parameter_types["bids_bval"].id,
                    value="bids/example.bval",
                ),
                DbFileParameter(
                    file_id=imaging_file.id,
                    type_id=parameter_types["bids_bvec"].id,
                    value="bids/example.bvec",
                ),
                DbFileParameter(
                    file_id=imaging_file.id,
                    type_id=parameter_types["check_pic_filename"].id,
                    value="example/preview.png",
                ),
            )
        )
        imaging_file_id = imaging_file.id
        db.commit()
    data_root = tmp_path / "data-root"
    source = data_root / "assembly" / "example" / "native" / "example.mnc"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"derived imaging data")
    auxiliary_files = {
        "bids/example.json": b"{}",
        "bids/example.bval": b"0 1000",
        "bids/example.bvec": b"1 0 0",
        "pic/example/preview.png": b"preview",
    }
    for relative_path, contents in auxiliary_files.items():
        auxiliary = data_root / relative_path
        auxiliary.parent.mkdir(parents=True, exist_ok=True)
        auxiliary.write_bytes(contents)
    output = tmp_path / "backup"
    schema = make_resource_schema()

    result = create_backup(
        db_engine,
        schema,
        BackupRequest(
            output=output,
            selection=BackupSelection(
                selections=(
                    parse_resource_selection(schema, "imaging-file"),
                ),
                criteria=(schema.criterion("imaging-file.id", imaging_file_id),),
            ),
            storage_roots={"imaging-data": data_root},
        ),
    )

    assert result.selected_object_count == 1
    assert result.database_row_count == 6
    assert result.file_count == 5
    assert result.blocker_count == 0
    assert (
        output / "files" / "imaging-data" / "assembly" / "example" / "native" / "example.mnc"
    ).read_bytes() == b"derived imaging data"
    restore_sql = (output / "restore.sql").read_text()
    assert "INSERT INTO `files` " in restore_sql
    assert restore_sql.count("INSERT INTO `parameter_file` ") == 5
    assert "INSERT INTO `parameter_type` " not in restore_sql
    assert "INSERT INTO `tarchive` " not in restore_sql
    manifest = verify_backup(output)
    backed_up_paths = {
        entry["relative_path"] for entry in cast(list[dict[str, object]], manifest["files"])
    }
    assert backed_up_paths == {
        "assembly/example/native/example.mnc",
        "bids/example.json",
        "bids/example.bval",
        "bids/example.bvec",
        "pic/example/preview.png",
    }


def test_mri_upload_backup_captures_only_its_explicitly_selected_row(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        upload = db.scalar(
            select(DbMriUpload).where(DbMriUpload.dicom_archive_id == archive.id)
        )
        assert upload is not None
        upload_id = upload.id
        db.add(
            DbFile(
                session_id=1,
                path=Path("assembly/example/native/example.mnc"),
                output_type="native",
                inserted_by_user_id="admin",
                dicom_archive_id=archive.id,
            )
        )
        db.commit()
    output = tmp_path / "backup"
    schema = make_resource_schema()

    result = create_backup(
        db_engine,
        schema,
        BackupRequest(
            output=output,
            selection=BackupSelection(
                selections=(
                    parse_resource_selection(schema, "mri-upload"),
                ),
                criteria=(schema.criterion("mri-upload.id", upload_id),),
            ),
            storage_roots={},
        ),
    )

    assert result.selected_object_count == 1
    assert result.database_row_count == 1
    assert result.file_count == 0
    assert result.blocker_count == 0
    restore_sql = (output / "restore.sql").read_text()
    assert "INSERT INTO `mri_upload` " in restore_sql
    assert "INSERT INTO `tarchive` " not in restore_sql
    assert "INSERT INTO `files` " not in restore_sql
    verify_backup(output)


def test_protocol_violated_scan_backup_captures_its_row_and_file(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        scan = DbMriProtocolViolatedScan(
            dicom_archive_id=archive.id,
            file_path=Path("trashbin/example/unknown.mnc"),
        )
        db.add(scan)
        db.flush()
        scan_id = scan.id
        db.commit()

    data_root = tmp_path / "data-root"
    source = data_root / "trashbin" / "example" / "unknown.mnc"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"unclassified scan")
    schema = make_resource_schema()
    output = tmp_path / "backup"

    result = create_backup(
        db_engine,
        schema,
        BackupRequest(
            output=output,
            selection=BackupSelection(
                selections=(
                    parse_resource_selection(
                        schema, "mri-protocol-violated-scan"
                    ),
                ),
                criteria=(
                    schema.criterion("mri-protocol-violated-scan.id", scan_id),
                ),
            ),
            storage_roots={"imaging-data": data_root},
        ),
    )

    assert result.selected_object_count == 1
    assert result.database_row_count == 1
    assert result.file_count == 1
    assert result.blocker_count == 0
    assert (
        output
        / "files"
        / "imaging-data"
        / "trashbin"
        / "example"
        / "unknown.mnc"
    ).read_bytes() == b"unclassified scan"
    restore_sql = (output / "restore.sql").read_text()
    assert "INSERT INTO `mri_protocol_violated_scans` " in restore_sql
    assert "INSERT INTO `tarchive` " not in restore_sql
    verify_backup(output)


def test_backup_combines_explicit_archive_and_mri_upload_projections(
    db_engine: Engine,
    tmp_path: Path,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    archive_file = archive_root / "2026" / "archive.tar"
    archive_file.parent.mkdir(parents=True)
    archive_file.write_bytes(b"raw dicom archive")
    schema = make_resource_schema()
    output = tmp_path / "backup"

    result = create_backup(
        db_engine,
        schema,
        BackupRequest(
            output=output,
            selection=BackupSelection(
                selections=(
                    parse_resource_selection(schema, "dicom-archive"),
                    parse_resource_selection(schema, "dicom-archive.mri-upload"),
                ),
                criteria=(schema.criterion("dicom-archive.id", archive_id),),
                where_expressions=(f"dicom-archive.id={archive_id}",),
            ),
            storage_roots={"dicom-archive": archive_root},
        ),
    )

    assert result.selected_object_count == 2
    assert result.selected_object_counts == {
        "dicom-archive": 1,
        "mri-upload": 1,
    }
    assert result.database_row_count == 4
    assert result.blocker_count == 0
    restore_sql = (output / "restore.sql").read_text()
    assert "INSERT INTO `tarchive` " in restore_sql
    assert "INSERT INTO `mri_upload` " in restore_sql
    manifest = verify_backup(output)
    assert manifest["selection"] == {
        "all": False,
        "anchor": {"kind": "dicom-archive", "matched_count": 1},
        "select": ["dicom-archive", "dicom-archive.mri-upload"],
        "selected": {"dicom-archive": 1, "mri-upload": 1},
        "where": [f"dicom-archive.id={archive_id}"],
    }


def test_backup_copies_the_target_of_an_archive_symlink(db_engine: Engine, tmp_path: Path):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    target = tmp_path / "target-archive.tar"
    target.write_bytes(b"symlink target contents")
    source = archive_root / "2026" / "archive.tar"
    source.parent.mkdir(parents=True)
    source.symlink_to(target)
    output = tmp_path / "backup"
    schema = make_resource_schema()
    _create_archive_backup(
        db_engine,
        output,
        archive_root,
        (schema.criterion("dicom-archive.id", archive_id),),
    )

    payload = output / "files" / "dicom-archive" / "2026" / "archive.tar"
    assert payload.read_bytes() == b"symlink target contents"
    assert not payload.is_symlink()
    manifest = verify_backup(output)
    files = cast(list[dict[str, object]], manifest["files"])
    assert files[0]["source_was_symlink"] is True


def test_backup_rejects_an_empty_selection(db_engine: Engine, tmp_path: Path):
    schema = make_resource_schema()
    output = tmp_path / "backup"

    with pytest.raises(ValueError, match="projected no logical objects"):
        _create_archive_backup(
            db_engine,
            output,
            tmp_path,
            (schema.criterion("dicom-archive.id", 999),),
        )

    assert not output.exists()


def test_backup_cli_requires_a_selector_and_advertises_generic_selection(tmp_path: Path):
    help_result = CliRunner().invoke(app, ("backup", "--help"))
    result = CliRunner().invoke(
        app,
        (
            "backup",
            "--select",
            "dicom-archive",
            "--output",
            str(tmp_path / "backup"),
        ),
    )

    assert help_result.exit_code == 0
    assert "logical-resource backup" in help_result.output
    assert "--select" in help_result.output
    assert "--where" in help_result.output
    assert "--update" in help_result.output
    assert "--all" in help_result.output
    assert result.exit_code == 2
    assert "At least one selector is required" in result.output

    scalar_result = CliRunner().invoke(
        app,
        (
            "backup",
            "--select",
            "dicom-archive.id",
            "--where",
            "dicom-archive.id=53",
            "--output",
            str(tmp_path / "scalar-backup"),
        ),
    )
    assert scalar_result.exit_code == 2
    assert "ends at a scalar" in scalar_result.output
    assert "select a logical object instead" in scalar_result.output


def test_backup_uses_the_shared_semantic_projection_plan():
    schema = make_resource_schema()
    plan = plan_backup(
        schema,
        BackupSelection(
            selections=(parse_resource_selection(schema, "candidate"),),
            select_all=True,
        ),
    )

    assert plan.anchor_kind == "candidate"
    assert plan.paths == (schema.projection_path("candidate"),)
    assert required_storage_roots(schema, "candidate") == ()
    assert required_storage_roots(schema, "dicom-archive") == ("dicom-archive",)
    assert required_storage_roots(schema, "imaging-file") == ("imaging-data",)
    assert required_storage_roots(schema, "mri-upload") == ()
    assert required_storage_roots(schema, "mri-protocol-violated-scan") == (
        "imaging-data",
    )


def test_backup_discovers_reference_from_table_unknown_to_orm(
    db_engine: Engine,
    tmp_path: Path,
):
    custom_metadata = MetaData()
    Table("tarchive", custom_metadata, autoload_with=db_engine)
    custom_reference = Table(
        "custom_archive_reference",
        custom_metadata,
        Column("id", Integer, primary_key=True),
        Column("archive_id", Integer, ForeignKey("tarchive.TarchiveID"), nullable=False),
        Column("note", String, nullable=False),
    )
    custom_metadata.create_all(db_engine)
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.execute(custom_reference.insert().values(archive_id=archive_id, note="custom"))
        db.commit()

    archive_root = tmp_path / "archive-root"
    archive_file = archive_root / "2026" / "archive.tar"
    archive_file.parent.mkdir(parents=True)
    archive_file.write_bytes(b"raw dicom archive")
    output = tmp_path / "backup"
    schema = make_resource_schema()

    result = _create_archive_backup(
        db_engine,
        output,
        archive_root,
        (schema.criterion("dicom-archive.id", archive_id),),
    )

    assert result.blocker_count == 2
    manifest = verify_backup(output)
    planner = cast(dict[str, object], manifest["planner"])
    assert planner["reference_inventory"] == "database-foreign-keys"
    assert planner["database_foreign_keys_checked"] is True
    assert planner["database_row_locking"] == "select-for-update"
    assert planner["selection_materialization"] == "locked-projected-identities"
    references = cast(list[dict[str, object]], manifest["references"])
    custom = next(entry for entry in references if entry["table"] == "custom_archive_reference")
    assert custom["columns"] == ["archive_id"]
    assert custom["row_count"] == 1


def test_database_row_lock_uses_primary_key_order_and_for_update():
    table = cast(Table, DbDicomArchive.__table__)
    statement = _locking_statement(select(table), table)  # pyright: ignore[reportPrivateUsage]
    sql = str(statement.compile(dialect=mysql.dialect()))

    assert "ORDER BY tarchive.`TarchiveID`" in sql
    assert sql.endswith(" FOR UPDATE")


def test_owned_row_collection_requires_a_database_foreign_key(db_engine: Engine):
    metadata = MetaData()
    parent = Table("unprotected_parent", metadata, Column("id", Integer, primary_key=True))
    child = Table(
        "unprotected_child",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, nullable=False),
    )
    metadata.create_all(db_engine)
    plans = (
        DatabaseTableBackup(
            table=parent,
            statement=select(parent),
            action="delete",
            counts_selected_objects=True,
        ),
        DatabaseTableBackup(
            table=child,
            statement=select(child),
            action="delete",
            ownership=DatabaseOwnership(
                parent_column=parent.c.id,
                child_column=child.c.parent_id,
            ),
        ),
    )

    with db_engine.connect() as connection:
        with pytest.raises(ValueError, match="not protected by the expected foreign key"):
            _inspect_database_dependencies(  # pyright: ignore[reportPrivateUsage]
                connection,
                plans,
            )


def test_backup_uses_one_explicit_database_transaction(db_engine: Engine, tmp_path: Path):
    with Session(db_engine) as db:
        _add_archive(db)
        db.commit()
    archive_root = tmp_path / "archive-root"
    archive_file = archive_root / "2026" / "archive.tar"
    archive_file.parent.mkdir(parents=True)
    archive_file.write_bytes(b"raw dicom archive")
    events: list[str] = []
    executed_parameters: list[object] = []

    def record_begin(connection: Connection) -> None:
        del connection
        events.append("begin")

    def record_commit(connection: Connection) -> None:
        del connection
        events.append("commit")

    def record_rollback(connection: Connection) -> None:
        del connection
        events.append("rollback")

    def record_parameters(
        connection: Connection,
        cursor: Any,
        statement: str,
        parameters: object,
        context: Any,
        executemany: bool,
    ) -> None:
        del connection, cursor, statement, context, executemany
        executed_parameters.append(parameters)

    event.listen(db_engine, "begin", record_begin)
    event.listen(db_engine, "commit", record_commit)
    event.listen(db_engine, "rollback", record_rollback)
    event.listen(db_engine, "before_cursor_execute", record_parameters)
    schema = make_resource_schema()

    _create_archive_backup(
        db_engine,
        tmp_path / "backup",
        archive_root,
        (schema.criterion("dicom-archive.patient-name", "O'Brien 100% \\ archive"),),
    )

    assert events == ["begin", "commit"]
    assert sum("O'Brien 100%" in repr(parameters) for parameters in executed_parameters) == 1


def test_backup_cli_creates_the_artifact(
    db_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with Session(db_engine) as db:
        archive = _add_archive(db)
        archive_id = archive.id
        db.commit()
    archive_root = tmp_path / "archive-root"
    archive_file = archive_root / "2026" / "archive.tar"
    archive_file.parent.mkdir(parents=True)
    archive_file.write_bytes(b"raw dicom archive")
    monkeypatch.chdir(tmp_path)

    def load_config_without_io(profile: str | None) -> SimpleNamespace:
        return SimpleNamespace(mysql=object())

    def use_test_engine(config: object) -> Engine:
        return db_engine

    def archive_root_config(db: Session, name: str) -> SimpleNamespace:
        return SimpleNamespace(value=str(archive_root))

    monkeypatch.setattr(cli_module, "load_config", load_config_without_io)
    monkeypatch.setattr(cli_module, "get_database_engine", use_test_engine)
    monkeypatch.setattr(
        cli_module,
        "try_get_config_with_setting_name",
        archive_root_config,
    )

    result = CliRunner().invoke(
        app,
        (
            "backup",
            "--select",
            "dicom-archive",
            "--update",
            "dicom-archive.mri-upload",
            "--where",
            f"dicom-archive.id={archive_id}",
        ),
    )

    assert result.exit_code == 0
    assert "Created verified backup" in result.output
    assert "Prepared 1 nullable foreign-key update" in result.output
    assert "Recorded" not in result.output
    outputs = tuple(tmp_path.glob("loris-backup-*"))
    assert len(outputs) == 1
    output = outputs[0]
    assert (output / "manifest.json").is_file()
    manifest = verify_backup(output)
    selection = cast(dict[str, object], manifest["selection"])
    assert selection["select"] == ["dicom-archive"]
    assert selection["update"] == ["dicom-archive.mri-upload"]
    assert selection["selected"] == {"dicom-archive": 1}
    assert output.name == f"loris-backup-{manifest['operation_id']}"
