import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, ClassVar

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from lib.db.models.dicom_archive import DbDicomArchive
from lib.db.models.dicom_archive_file import DbDicomArchiveFile
from lib.db.models.dicom_archive_series import DbDicomArchiveSeries
from lib.db.models.mri_upload import DbMriUpload
from lib.db.models.project import DbProject
from lib.db.models.session import DbSession
from lib.db.models.site import DbSite
from lib.resource_model import (
    STRING_TYPE,
    DatabaseRowObject,
    LifecycleSemantics,
    LinkMember,
    LocalPathObject,
    LocalPathType,
    ObjectKind,
    ObjectRef,
    ObjectSelection,
    PropertyRef,
    RelationshipSemantics,
    ResourceGraph,
    ResourceModel,
    ResourceSchema,
    ValueMember,
)
from lib.resource_model.inspection import (
    FilesystemPropertyReadContext,
    InspectionQuery,
    format_inspection_json,
    format_inspection_text,
    inspect_resources,
    parse_inspection_selection,
)
from lib.resource_model.providers.core import (
    PROJECT,
    SESSION,
    SITE,
    SessionProvider,
    register_core_schema,
)
from lib.resource_model.providers.dicom import (
    DICOM_ARCHIVE,
    DICOM_PATIENT_NAME,
    DICOM_STUDY_UID,
    DicomArchiveObject,
    DicomArchiveProvider,
    register_dicom_schema,
)
from scripts.inspect_resources import main as inspect_main
from scripts.inspect_resources import make_parser, parse_where_expressions

READ_CONTEXT = FilesystemPropertyReadContext({})


def make_schema() -> ResourceSchema:
    schema = ResourceSchema()
    register_core_schema(schema)
    register_dicom_schema(schema)
    return schema


def inspection_selection(expression: str):
    return parse_inspection_selection(make_schema(), expression)


def add_dicom_archive(db: Session) -> DbDicomArchive:
    project = DbProject(id=3, name="Example Project", alias="example")
    site = DbSite(id=1, name="Example Site", alias="EX")
    session = DbSession(
        id=7,
        candidate_id=11,
        site_id=1,
        project_id=3,
        visit_label="V1",
        active=True,
    )
    archive = DbDicomArchive(
        study_uid="1.2.3.4",
        patient_id="DCC001_000001_V1",
        patient_name="DCC001_000001_V1",
        center_name="DCC001",
        acquisition_count=1,
        dicom_file_count=1,
        non_dicom_file_count=0,
        creating_user="admin",
        sum_type_version=2,
        tar_type_version=2,
        source_path=Path("incoming/study"),
        path=Path("2026/archive.tar"),
        scanner_manufacturer="Example",
        scanner_model="Example",
        scanner_serial_number="123",
        scanner_software_version="1",
        session_id=7,
        acquisition_metadata="",
    )
    db.add_all((project, site, session, archive))
    db.flush()
    db.add(
        DbMriUpload(
            uploaded_by="admin",
            upload_path=Path("uploads/archive.tar"),
            decompressed_path=Path("uploads/archive"),
            patient_name="DCC001_000001_V1",
            dicom_archive_id=archive.id,
            session_id=7,
        )
    )
    db.flush()

    series = DbDicomArchiveSeries(
        archive_id=archive.id,
        series_number=1,
        number_of_files=1,
        modality="MR",
    )
    db.add(series)
    db.flush()
    db.add(
        DbDicomArchiveFile(
            archive_id=archive.id,
            series_id=series.id,
            md5_sum="01234567890abcdef0123456789abcde",
            file_name="image.dcm",
        )
    )
    db.flush()
    db.expire(archive, ("series", "files"))
    return archive


def test_resolves_typed_logical_object_with_database_and_file_resources(db: Session):
    archive = add_dicom_archive(db)
    model = ResourceModel(make_schema())

    fragment = model.resolve(
        db,
        DicomArchiveProvider(),
        ObjectSelection(refs=frozenset({ObjectRef(DICOM_ARCHIVE.name, str(archive.id))})),
    )

    assert len(fragment.logical_objects) == 1
    logical_object = fragment.logical_objects[0]
    assert isinstance(logical_object, DicomArchiveObject)
    assert logical_object.orm is archive
    assert logical_object.ref == ObjectRef(DICOM_ARCHIVE.name, str(archive.id))
    values = {
        definition.name: definition.get_value(logical_object, READ_CONTEXT)
        for definition in make_schema().properties(DICOM_ARCHIVE.name)
    }
    assert values == {
        "id": archive.id,
        "study-uid": "1.2.3.4",
        "patient-name": "DCC001_000001_V1",
        "acquisition-count": 1,
    }
    assert len(fragment.physical_objects) == 5
    assert sum(isinstance(obj, DatabaseRowObject) for obj in fragment.physical_objects) == 4
    assert sum(isinstance(obj, LocalPathObject) for obj in fragment.physical_objects) == 1
    assert sum(
        make_schema().link(binding.member.object_kind, binding.member.property_name).lifecycle
        is LifecycleSemantics.OWNS
        for binding in fragment.links
    ) == 4
    assert sum(
        make_schema().link(binding.member.object_kind, binding.member.property_name).lifecycle
        is LifecycleSemantics.REFERENCES
        for binding in fragment.links
    ) == 2
    graph = ResourceGraph()
    graph.add(fragment)
    for physical_object in fragment.physical_objects:
        assert graph.get(physical_object.ref) is physical_object


def test_class_level_properties_read_and_query_orm_state(db: Session):
    archive = add_dicom_archive(db)
    logical_object = DicomArchiveProvider().find(
        db,
        ObjectSelection(refs=frozenset({ObjectRef(DICOM_ARCHIVE.name, str(archive.id))})),
    )[0]

    archive.patient_name = "corrected-name"

    values = {
        definition.name: definition.get_value(logical_object, READ_CONTEXT)
        for definition in make_schema().properties(DICOM_ARCHIVE.name)
    }
    assert values["patient-name"] == "corrected-name"

    matching = DicomArchiveProvider().find(
        db,
        ObjectSelection(predicates=(DICOM_STUDY_UID.predicate("1.2.3.4"),)),
    )
    assert matching == (logical_object,)


def test_selection_rejects_references_for_another_object_kind(db: Session):
    with pytest.raises(ValueError, match="references of kinds"):
        SessionProvider().find(
            db,
            ObjectSelection(refs=frozenset({ObjectRef(DICOM_ARCHIVE.name, "1")})),
        )


def test_non_queryable_property_cannot_create_a_predicate():
    with pytest.raises(ValueError, match="not queryable"):
        DICOM_PATIENT_NAME.predicate("example")


def test_composes_independently_resolved_provider_fragments(db: Session):
    archive = add_dicom_archive(db)
    model = ResourceModel(make_schema())
    graph = ResourceGraph()

    graph.add(
        model.resolve(
            db,
            DicomArchiveProvider(),
            ObjectSelection(refs=frozenset({ObjectRef(DICOM_ARCHIVE.name, str(archive.id))})),
        )
    )
    assert graph.unresolved_refs() == {ObjectRef(SESSION.name, "7")}

    graph.add(
        model.resolve(
            db,
            SessionProvider(),
            ObjectSelection(refs=frozenset({ObjectRef(SESSION.name, "7")})),
        )
    )
    assert graph.unresolved_refs() == {ObjectRef(PROJECT.name, "3"), ObjectRef(SITE.name, "1")}
    assert len(graph.logical_objects) == 2
    assert len(graph.physical_objects) == 6
    assert len(graph.objects) == 8
    assert len(graph.links) == 9


def test_schema_can_be_extended_without_modifying_core_types():
    @dataclass(frozen=True, slots=True)
    class GeneticDatasetObject:
        id: int
        assay: str
        properties: ClassVar[tuple[ValueMember[Any, Any, Any], ...]] = (
            ValueMember["GeneticDatasetObject", str, object](
                name="assay",
                value_type=STRING_TYPE,
                get_value=lambda obj, _: obj.assay,
            ),
        )

        @property
        def ref(self) -> ObjectRef:
            return ObjectRef("example.genetic-dataset", str(self.id))

    schema = make_schema()
    schema.register_object_kind(
        ObjectKind("example.genetic-dataset", GeneticDatasetObject, GeneticDatasetObject.properties)
    )

    assert {kind.name for kind in schema.object_kinds} == {
        "session",
        "project",
        "site",
        "dicom-archive",
        "database-row",
        "local-path",
        "example.genetic-dataset",
    }


def test_schema_resolves_and_introspects_qualified_properties():
    schema = make_schema()

    project_name = schema.property("project.name")

    assert project_name.name == "name"
    assert project_name.value_type is STRING_TYPE
    assert project_name in schema.properties(PROJECT.name)
    assert project_name in schema.queryable_properties(PROJECT.name)
    assert {property_definition.name for property_definition in schema.properties(PROJECT.name)} == {
        "id",
        "name",
        "alias",
    }


def test_relationships_are_named_schema_members_not_properties():
    schema = make_schema()

    assert {property_definition.name for property_definition in schema.properties(SESSION.name)} == {
        "id",
        "participant-id",
        "visit-label",
        "active",
    }
    assert schema.link(SESSION.name, "project").target_kind == PROJECT.name
    assert schema.link(SESSION.name, "site").target_kind == SITE.name
    assert schema.link(DICOM_ARCHIVE.name, "session").target_kind == SESSION.name


def test_schema_resolves_explicit_relationship_property_paths():
    schema = make_schema()

    path = schema.property_path("dicom-archive.session.project.name")

    assert path.root_kind == DICOM_ARCHIVE.name
    assert path.link_members == ("session", "project")
    assert path.property == PropertyRef(PROJECT.name, "name")
    assert tuple(link.name for link in schema.path_links(path)) == (
        "session",
        "project",
    )


def test_schema_rejects_unknown_relationship_property_paths():
    schema = make_schema()

    with pytest.raises(ValueError, match="Unknown property path"):
        schema.parse_criterion("session.unknown.name", "example")


def test_object_ids_are_queryable_semantic_properties(db: Session):
    archive = add_dicom_archive(db)

    cases = (
        ("session", "session.id=7", ObjectRef(SESSION.name, "7")),
        ("project", "project.id=3", ObjectRef(PROJECT.name, "3")),
        ("site", "site.id=1", ObjectRef(SITE.name, "1")),
        (
            "dicom-archive",
            f"dicom-archive.id={archive.id}",
            ObjectRef(DICOM_ARCHIVE.name, str(archive.id)),
        ),
    )
    for kind, expression, expected in cases:
        result = inspect_resources(
            db,
            InspectionQuery(
                selections=(inspection_selection(kind),),
                criteria=parse_where_expressions((expression,)),
            ),
        )
        assert result.selected == {expected}


def test_cli_uses_kebab_case_schema_names_and_has_no_identity_flags():
    parser = make_parser()

    args = parser.parse_args(("--select", "dicom-archive", "--where", "dicom-archive.id=1"))

    assert args.select == ["dicom-archive"]
    assert not hasattr(args, "session_id")
    assert not hasattr(args, "dicom_archive_id")


def test_schema_parses_text_criteria_with_the_property_query_operand_type():
    schema = make_schema()

    name = schema.parse_criterion("project.name", "Brainstorm")
    session_id = schema.parse_criterion("session.id", "7")

    assert name == schema.criterion("project.name", "Brainstorm")
    assert session_id == schema.criterion("session.id", 7)


def test_schema_reports_invalid_property_query_values():
    schema = make_schema()

    with pytest.raises(ValueError, match="Invalid integer value"):
        schema.parse_criterion("session.id", "not-an-id")

    with pytest.raises(ValueError, match="not queryable"):
        schema.parse_criterion("session.active", "true")


def test_property_ref_parses_namespaced_object_kinds():
    ref = PropertyRef.from_path("example.genetic-dataset.assay")

    assert ref.object_kind == "example.genetic-dataset"
    assert ref.property_name == "assay"
    assert str(ref) == "example.genetic-dataset.assay"


def test_schema_rejects_unknown_qualified_properties():
    schema = make_schema()

    with pytest.raises(ValueError, match=r"Unknown property project\.missing"):
        schema.property("project.missing")


def test_schema_rejects_relationships_with_unknown_endpoints():
    schema = ResourceSchema()
    register_core_schema(schema)

    with pytest.raises(ValueError, match="unknown target kind"):
        schema.register_member(
            SESSION.name,
            LinkMember(
                name="unknown",
                source_kind=SESSION.name,
                target_kind="unknown",
                targets_for_source=lambda obj: (),
            ),
        )


def test_local_paths_cannot_escape_their_storage_root():
    with pytest.raises(ValueError, match="within its storage root"):
        LocalPathObject("data", PurePosixPath("../outside"), LocalPathType.FILE)


def test_database_resource_uses_orm_table_identity_and_predicate(db: Session):
    add_dicom_archive(db)
    session = db.get_one(DbSession, 7)

    resource = DatabaseRowObject(session)
    ref = resource.ref

    assert resource.orm is session
    assert ref.table is DbSession.__table__
    assert ref.identity == (7,)
    assert ref.key == (("ID", 7),)
    selected_id = db.execute(select(ref.table.c.ID).where(ref.predicate())).scalar_one()
    assert selected_id == 7


def test_database_resource_rejects_a_transient_orm_instance():
    transient_session = DbSession(
        id=7,
        candidate_id=11,
        site_id=1,
        project_id=3,
        visit_label="V1",
        active=True,
    )

    with pytest.raises(ValueError, match="persistent"):
        DatabaseRowObject(transient_session).ref


def test_inspection_composes_project_and_visit_selectors_across_providers(db: Session):
    archive = add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive"),),
            criteria=parse_where_expressions(
                (
                    "dicom-archive.session.project.id=3",
                    "dicom-archive.session.visit-label=V1",
                )
            ),
            expand_related=True,
        ),
    )

    assert result.selected == {ObjectRef(DICOM_ARCHIVE.name, str(archive.id))}
    assert result.graph.get(ObjectRef(SESSION.name, "7")) is not None
    assert result.graph.get(ObjectRef(PROJECT.name, "3")) is not None
    assert result.graph.get(ObjectRef(SITE.name, "1")) is not None


def test_inspection_selects_projects_and_sites_by_dynamic_properties(db: Session):
    add_dicom_archive(db)

    project_result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("project"),),
            criteria=parse_where_expressions(("project.alias=example",)),
        ),
    )
    site_result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("site"),),
            criteria=parse_where_expressions(("site.alias=EX",)),
        ),
    )

    assert project_result.selected == {ObjectRef(PROJECT.name, "3")}
    assert site_result.selected == {ObjectRef(SITE.name, "1")}


def test_project_and_site_properties_scope_sessions_without_leaking_scope_objects(db: Session):
    add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("session"),),
            criteria=parse_where_expressions(
                ("session.project.name=Example Project", "session.site.alias=EX")
            ),
        ),
    )

    assert result.selected == {ObjectRef(SESSION.name, "7")}
    assert {obj.ref for obj in result.graph.logical_objects} == {ObjectRef(SESSION.name, "7")}
    assert result.graph.unresolved_refs() == {ObjectRef(PROJECT.name, "3"), ObjectRef(SITE.name, "1")}


def test_model_transitively_selects_dicom_by_belongs_to_properties(db: Session):
    archive = add_dicom_archive(db)
    schema = make_schema()
    model = ResourceModel(schema)

    fragment = model.select_objects(
        db,
        DICOM_ARCHIVE.name,
        criteria=(
            schema.criterion("project.name", "Example Project"),
            schema.criterion("site.alias", "EX"),
            schema.criterion("session.visit-label", "V1"),
        ),
    )

    assert {obj.ref for obj in fragment.logical_objects} == {
        ObjectRef(DICOM_ARCHIVE.name, str(archive.id))
    }


def test_dicom_session_link_uses_archive_session_id_as_authoritative(db: Session):
    archive = add_dicom_archive(db)
    archive.mri_uploads[0].session_id = 999
    schema = make_schema()
    model = ResourceModel(schema)

    matching = model.select_objects(
        db,
        DICOM_ARCHIVE.name,
        criteria=(schema.criterion("session.id", 7),),
    )
    mismatching = model.select_objects(
        db,
        DICOM_ARCHIVE.name,
        criteria=(schema.criterion("session.id", 999),),
    )

    assert {obj.ref for obj in matching.logical_objects} == {
        ObjectRef(DICOM_ARCHIVE.name, str(archive.id))
    }
    assert mismatching.logical_objects == ()


def test_inspection_accepts_dynamic_qualified_property_criteria(db: Session):
    archive = add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive"),),
            criteria=parse_where_expressions(
                (
                    "dicom-archive.session.project.name=Example Project",
                    "dicom-archive.session.site.alias=EX",
                    "dicom-archive.session.visit-label=V1",
                )
            ),
        ),
    )

    assert result.selected == {ObjectRef(DICOM_ARCHIVE.name, str(archive.id))}


def test_inspection_accepts_explicit_relationship_property_paths(db: Session):
    archive = add_dicom_archive(db)

    session_result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("session"),),
            criteria=parse_where_expressions(("session.project.name=Example Project",)),
        ),
    )
    dicom_result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive"),),
            criteria=parse_where_expressions(
                ("dicom-archive.session.project.name=Example Project",)
            ),
        ),
    )

    assert session_result.selected == {ObjectRef(SESSION.name, "7")}
    assert dicom_result.selected == {ObjectRef(DICOM_ARCHIVE.name, str(archive.id))}


def test_implicit_lookup_can_precede_an_explicit_relationship_path(db: Session):
    archive = add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive"),),
            criteria=parse_where_expressions(("session.project.id=3",)),
        ),
    )

    assert result.selected == {ObjectRef(DICOM_ARCHIVE.name, str(archive.id))}


def test_property_selection_uses_its_root_as_anchor_for_independent_filters(db: Session):
    archive = add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive.study-uid"),),
            criteria=parse_where_expressions(
                ("project.alias=example", "site.alias=EX")
            ),
        ),
    )

    assert result.selected == {ObjectRef(DICOM_ARCHIVE.name, str(archive.id))}
    document = json.loads(format_inspection_json(result))
    assert document["selections"] == [
        {
            "expression": "dicom-archive.study-uid",
            "property": "study-uid",
            "objects": [f"dicom-archive:{archive.id}"],
        }
    ]
    assert document["objects"] == [
        {
            "id": f"dicom-archive:{archive.id}",
            "kind": "dicom-archive",
            "key": str(archive.id),
            "role": "selected",
            "properties": {"study-uid": "1.2.3.4"},
        }
    ]


def test_inspection_projects_non_queryable_local_file_size(db: Session, tmp_path: Path):
    archive = add_dicom_archive(db)
    archive_path = tmp_path / "2026" / "archive.tar"
    archive_path.parent.mkdir()
    archive_path.write_bytes(b"dicom archive")

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive.file.size"),),
            criteria=parse_where_expressions((f"dicom-archive.id={archive.id}",)),
        ),
        storage_roots={"dicom-archive": tmp_path},
    )

    document = json.loads(format_inspection_json(result))
    assert document["selected"] == ["local-path:dicom-archive:2026/archive.tar"]
    assert document["objects"] == [
        {
            "id": "local-path:dicom-archive:2026/archive.tar",
            "kind": "local-path",
            "role": "selected",
            "properties": {"size": len(b"dicom archive")},
        }
    ]

    with pytest.raises(ValueError, match="not queryable"):
        parse_where_expressions(("dicom-archive.file.size=1",))


def test_inspection_follows_local_file_symlink_for_size(db: Session, tmp_path: Path):
    archive = add_dicom_archive(db)
    external_file = tmp_path.parent / "external-archive.tar"
    external_file.write_bytes(b"external dicom archive")
    archive_path = tmp_path / "2026" / "archive.tar"
    archive_path.parent.mkdir()
    archive_path.symlink_to(external_file)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive.file.size"),),
            criteria=parse_where_expressions((f"dicom-archive.id={archive.id}",)),
        ),
        storage_roots={"dicom-archive": tmp_path},
    )

    document = json.loads(format_inspection_json(result))
    assert document["objects"][0]["properties"] == {
        "size": len(b"external dicom archive")
    }


def test_where_parser_preserves_equals_signs_in_string_values():
    assert parse_where_expressions(("project.name=Study=A",)) == (
        make_schema().criterion("project.name", "Study=A"),
    )


def test_where_requires_an_explicit_selection():
    with pytest.raises(SystemExit):
        inspect_main(("--where", "project.name=Brainstorm"))


def test_property_selection_does_not_implicitly_reverse_belongs_to(db: Session):
    add_dicom_archive(db)
    model = ResourceModel(make_schema())

    with pytest.raises(ValueError, match="No belongs-to path"):
        model.select_objects(
            db,
            PROJECT.name,
            criteria=(make_schema().criterion("session.visit-label", "V1"),),
        )


def test_property_selection_rejects_ambiguous_belongs_to_paths(db: Session):
    add_dicom_archive(db)
    schema = make_schema()
    schema.register_member(
        DICOM_ARCHIVE.name,
        LinkMember(
            name="project",
            source_kind=DICOM_ARCHIVE.name,
            target_kind=PROJECT.name,
            targets_for_source=lambda obj: (),
            source_selection=lambda refs: ObjectSelection(),
            traversal_semantics=RelationshipSemantics.BELONGS_TO,
        ),
    )

    with pytest.raises(ValueError, match="Ambiguous belongs-to path"):
        ResourceModel(schema).select_objects(
            db,
            DICOM_ARCHIVE.name,
            criteria=(make_schema().criterion("project.name", "Example Project"),),
        )


def test_project_expansion_does_not_reverse_traverse_to_sessions(db: Session):
    add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("project"),),
            criteria=parse_where_expressions(("project.alias=example",)),
            expand_related=True,
        ),
    )

    assert result.selected == {ObjectRef(PROJECT.name, "3")}
    assert {obj.ref for obj in result.graph.logical_objects} == result.selected


def test_dicom_inspection_does_not_display_scoping_sessions_without_matches(db: Session):
    add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive"),),
            criteria=parse_where_expressions(
                (
                    "dicom-archive.session.project.id=3",
                    "dicom-archive.session.visit-label=V2",
                )
            ),
            expand_related=True,
        ),
    )

    assert result.selected == set()
    assert result.graph.objects == ()
    assert format_inspection_text(result) == "No matching objects."


def test_inspection_can_expand_related_session_and_render_text_and_json(db: Session):
    archive = add_dicom_archive(db)

    result = inspect_resources(
        db,
        InspectionQuery(
            selections=(inspection_selection("dicom-archive"),),
            criteria=parse_where_expressions((f"dicom-archive.id={archive.id}",)),
            expand_related=True,
        ),
    )

    text = format_inspection_text(result)
    assert f"dicom-archive:{archive.id} [selected]" in text
    assert "session:7 [related]" in text
    assert "row [owns] -> database-row:tarchive" in text
    assert "file [owns] -> local-path:dicom-archive:2026/archive.tar" in text

    document = json.loads(format_inspection_json(result))
    assert document["selected"] == [f"dicom-archive:{archive.id}"]
    assert document["unresolved"] == []
    assert {item["role"] for item in document["objects"]} == {"selected", "related"}
    archive_links = [
        link
        for link in document["links"]
        if link["source"] == f"dicom-archive:{archive.id}"
    ]
    assert {link["lifecycle"] for link in archive_links} == {
        "owns",
        "references",
    }


def test_inspection_requires_a_selector_unless_all_is_explicit():
    with pytest.raises(ValueError, match="selector"):
        InspectionQuery(selections=(inspection_selection("session"),))
