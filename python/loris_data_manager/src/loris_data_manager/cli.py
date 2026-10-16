#!/usr/bin/env python3

"""Command-line inspection of logical LORIS objects and their resources."""

from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from click import ClickException
from lib.config_file import load_config
from lib.db.connect import get_database_engine
from lib.db.queries.config import try_get_config_with_setting_name
from sqlalchemy.orm import Session

from loris_data_manager.backup import (
    BackupRequest,
    BackupSelection,
    create_backup,
    plan_backup,
    required_storage_roots,
)
from loris_data_manager.deletion import delete_resources, plan_deletion
from loris_data_manager.inspection import (
    InspectionQuery,
    execute_inspection_plan,
    format_inspection_json,
    format_inspection_sql,
    format_inspection_text,
    parse_inspection_selection,
    plan_inspection,
)
from loris_data_manager.projection import parse_resource_selection
from loris_data_manager.provider import ResourceSchema
from loris_data_manager.providers.core import register_core_schema
from loris_data_manager.providers.dicom import register_dicom_schema
from loris_data_manager.providers.imaging import register_imaging_schema
from loris_data_manager.providers.mri_protocol_violated_scan import (
    register_mri_protocol_violated_scan_schema,
)
from loris_data_manager.providers.mri_upload import register_mri_upload_schema
from loris_data_manager.providers.mri_violation import register_mri_violation_schema
from loris_data_manager.schema import (
    PropertyCriterion,
    local_path_mapping,
)

app = typer.Typer(
    add_completion=False,
    help="Inspect and protect logical LORIS objects and their physical resources.",
)


class OutputFormat(StrEnum):
    """Supported inspection output formats."""

    TEXT = "text"
    JSON = "json"


def make_resource_schema() -> ResourceSchema:
    """Compose the resource schema provided by this LORIS installation."""

    schema = ResourceSchema()
    register_core_schema(schema)
    register_dicom_schema(schema)
    register_imaging_schema(schema)
    register_mri_upload_schema(schema)
    register_mri_violation_schema(schema)
    register_mri_protocol_violated_scan_schema(schema)
    return schema


@app.callback()
def main() -> None:
    """Manage LORIS data and its physical resources."""


@app.command("inspect")
def inspect(
    select: Annotated[
        list[str],
        typer.Option(
            "--select",
            metavar="OBJECT-OR-PROPERTY",
            help="Select an object path or scalar member; repeat to add projections.",
        ),
    ],
    profile: Annotated[
        str | None,
        typer.Option("-p", "--profile", help="Python database configuration profile."),
    ] = None,
    where: Annotated[
        list[str] | None,
        typer.Option(
            "--where",
            metavar="PROPERTY-PATH=VALUE",
            help="Filter by a semantic property or relationship path; repeat to combine with AND.",
        ),
    ] = None,
    select_all: Annotated[
        bool,
        typer.Option("--all", help="Allow inspection without a narrowing selector."),
    ] = False,
    expand_related: Annotated[
        bool,
        typer.Option("--expand-related", help="Resolve directly related objects for context."),
    ] = False,
    output_format: Annotated[OutputFormat, typer.Option("--format")] = OutputFormat.TEXT,
    print_sql: Annotated[
        bool,
        typer.Option(
            "--print-sql",
            help="Print the planned SQL without connecting to or querying the database.",
        ),
    ] = False,
) -> None:
    """Inspect logical LORIS objects and their bound resources."""

    schema = make_resource_schema()
    try:
        query = make_inspection_query(
            schema,
            select,
            where or (),
            select_all=select_all,
            expand_related=expand_related,
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    config = load_config(profile)
    engine = get_database_engine(config.mysql)
    try:
        plan = plan_inspection(schema, query)
        if print_sql:
            typer.echo(format_inspection_sql(plan, engine.dialect))
            return
        with Session(engine) as db:
            storage_roots: dict[str, Path] = {}
            required_roots = {
                path.storage_root
                for selection in query.selections
                if selection.property_name == "size"
                for link in schema.path_links(selection.path)[-1:]
                if (path := local_path_mapping(link.source)) is not None
            }
            for name in required_roots:
                root = schema.storage_root(name)
                configured = try_get_config_with_setting_name(db, root.config_setting)
                if configured is None or configured.value is None:
                    raise typer.BadParameter(
                        f"Missing {root.config_setting} configuration for file inspection"
                    )
                storage_roots[root.name] = Path(configured.value)
            result = execute_inspection_plan(db, plan, storage_roots=storage_roots)
            formatter = (
                format_inspection_json
                if output_format is OutputFormat.JSON
                else format_inspection_text
            )
            output = formatter(result)
    finally:
        engine.dispose()

    typer.echo(output)


@app.command("backup")
def backup(
    select: Annotated[
        list[str],
        typer.Option(
            "--select",
            metavar="OBJECT-PATH",
            help="Select a logical object path to back up; repeat to add projections.",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            metavar="DIRECTORY",
            help=(
                "Create the standalone backup artifact in this new directory; "
                "defaults to loris-backup-<operation-uuid>."
            ),
        ),
    ] = None,
    profile: Annotated[
        str | None,
        typer.Option("-p", "--profile", help="Python database configuration profile."),
    ] = None,
    where: Annotated[
        list[str] | None,
        typer.Option(
            "--where",
            metavar="PROPERTY-PATH=VALUE",
            help="Filter selected objects by a semantic property; repeat to combine with AND.",
        ),
    ] = None,
    update: Annotated[
        list[str] | None,
        typer.Option(
            "--update",
            metavar="RELATIONSHIP-PATH",
            help=(
                "Plan an approved nullable foreign-key update and include its "
                "restoration patch; repeat to add relationships."
            ),
        ),
    ] = None,
    select_all: Annotated[
        bool,
        typer.Option("--all", help="Allow backup without a narrowing anchor selector."),
    ] = False,
) -> None:
    """Create a verified logical-resource backup with SQL and file payloads."""

    schema = make_resource_schema()
    expressions = tuple(where or ())
    try:
        criteria = parse_where_expressions(expressions, schema=schema)
        selections = tuple(
            parse_resource_selection(schema, expression) for expression in select
        )
        updates = tuple(
            parse_resource_selection(schema, expression) for expression in update or ()
        )
        selection = BackupSelection(
            selections=selections,
            criteria=criteria,
            where_expressions=expressions,
            select_all=select_all,
            updates=updates,
        )
        plan_backup(schema, selection)
        storage_root_names = tuple(
            dict.fromkeys(
                name
                for selected in selections
                for name in required_storage_roots(schema, selected.target_kind)
            )
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    config = load_config(profile)
    engine = get_database_engine(config.mysql)
    try:
        storage_roots: dict[str, Path] = {}
        with Session(engine) as db:
            for name in storage_root_names:
                root = schema.storage_root(name)
                configured = try_get_config_with_setting_name(db, root.config_setting)
                if configured is None or configured.value is None:
                    raise typer.BadParameter(
                        f"Missing {root.config_setting} configuration required by "
                        f"the selected resource schema"
                    )
                storage_roots[root.name] = Path(configured.value)
        request = BackupRequest(
            output=output,
            selection=selection,
            storage_roots=storage_roots,
        )
        result = create_backup(engine, schema, request)
    except (OSError, ValueError) as error:
        raise ClickException(str(error)) from error
    finally:
        engine.dispose()

    typer.echo(
        f"Created verified backup {result.output} for {result.selected_object_count} "
        "logical object(s): "
        f"{result.database_row_count} database row(s), {result.file_count} file(s)."
    )
    if result.blocker_count:
        typer.echo(
            f"Recorded {result.blocker_count} external reference(s) that currently block removal."
        )
    if result.reference_update_count:
        typer.echo(
            f"Prepared {result.reference_update_count} nullable foreign-key update(s) "
            "with restoration patches."
        )


@app.command("delete")
def delete_command(
    select: Annotated[
        list[str],
        typer.Option(
            "--select",
            metavar="OBJECT-PATH",
            help="Select DICOM archives to delete.",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            metavar="DIRECTORY",
            help=(
                "Create the mandatory backup in this new directory; defaults to "
                "loris-backup-<operation-uuid>."
            ),
        ),
    ] = None,
    profile: Annotated[
        str | None,
        typer.Option("-p", "--profile", help="Python database configuration profile."),
    ] = None,
    where: Annotated[
        list[str] | None,
        typer.Option(
            "--where",
            metavar="PROPERTY-PATH=VALUE",
            help="Filter selected archives; repeat to combine with AND.",
        ),
    ] = None,
    update: Annotated[
        list[str] | None,
        typer.Option(
            "--update",
            metavar="RELATIONSHIP-PATH",
            help=(
                "Authorize an approved nullable foreign-key update; repeat to add "
                "relationships."
            ),
        ),
    ] = None,
    select_all: Annotated[
        bool,
        typer.Option("--all", help="Allow deletion planning without a narrowing selector."),
    ] = False,
    execute: Annotated[
        bool,
        typer.Option(
            "--execute",
            help="Create the backup and execute the planned deletion.",
        ),
    ] = False,
) -> None:
    """Plan or execute backup-first DICOM archive deletion."""

    schema = make_resource_schema()
    expressions = tuple(where or ())
    try:
        criteria = parse_where_expressions(expressions, schema=schema)
        selections = tuple(
            parse_resource_selection(schema, expression) for expression in select
        )
        updates = tuple(
            parse_resource_selection(schema, expression) for expression in update or ()
        )
        selection = BackupSelection(
            selections=selections,
            criteria=criteria,
            where_expressions=expressions,
            select_all=select_all,
            updates=updates,
        )
        plan_backup(schema, selection)
        storage_root_names = tuple(
            dict.fromkeys(
                name
                for selected in selections
                for name in required_storage_roots(schema, selected.target_kind)
            )
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error

    config = load_config(profile)
    engine = get_database_engine(config.mysql)
    result = None
    preview = None
    try:
        storage_roots: dict[str, Path] = {}
        with Session(engine) as db:
            for name in storage_root_names:
                root = schema.storage_root(name)
                configured = try_get_config_with_setting_name(db, root.config_setting)
                if configured is None or configured.value is None:
                    raise typer.BadParameter(
                        f"Missing {root.config_setting} configuration required by "
                        "the selected resource schema"
                    )
                storage_roots[root.name] = Path(configured.value)
        request = BackupRequest(
            output=output,
            selection=selection,
            storage_roots=storage_roots,
        )
        if execute:
            result = delete_resources(engine, schema, request)
        else:
            preview = plan_deletion(engine, schema, request)
    except (OSError, RuntimeError, ValueError) as error:
        raise ClickException(str(error)) from error
    finally:
        engine.dispose()

    if not execute:
        assert preview is not None
        typer.echo(
            f"Planned deletion of {preview.selected_object_count} DICOM archive(s): "
            f"{preview.database_row_count} database row(s), {preview.file_count} file(s), "
            f"{preview.reference_update_count} nullable reference update(s), "
            f"{preview.blocker_count} blocking reference(s)."
        )
        if preview.eligible:
            typer.echo("No changes made. Re-run with --execute to back up and delete.")
        else:
            typer.echo("No changes made. The current plan is not eligible for deletion.")
        return

    assert result is not None
    typer.echo(
        f"Deleted {result.backup.selected_object_count} DICOM archive(s): "
        f"{result.deleted_database_row_count} database row(s), "
        f"{result.backup.file_count} file(s)."
    )
    typer.echo(f"Created verified backup {result.backup.output}.")
    if result.updated_reference_count:
        typer.echo(
            f"Cleared {result.updated_reference_count} approved nullable reference(s)."
        )
    if not result.quarantine_purged:
        typer.echo(
            "Deletion committed, but quarantine cleanup remains at: "
            + ", ".join(str(path) for path in result.cleanup_paths)
        )


def make_inspection_query(
    schema: ResourceSchema,
    selections: Sequence[str],
    where: Sequence[str],
    *,
    select_all: bool = False,
    expand_related: bool = False,
) -> InspectionQuery:
    """Build an inspection query from command-line expressions."""

    return InspectionQuery(
        selections=tuple(
            parse_inspection_selection(schema, expression) for expression in selections
        ),
        criteria=parse_where_expressions(where, schema=schema),
        select_all=select_all,
        expand_related=expand_related,
    )


def parse_where_expressions(
    expressions: Sequence[str],
    *,
    schema: ResourceSchema | None = None,
) -> tuple[PropertyCriterion, ...]:
    """Parse repeatable CLI property filters using the registered resource schema."""

    schema = schema or make_resource_schema()
    criteria: list[PropertyCriterion] = []
    for expression in expressions:
        path, separator, raw_value = expression.partition("=")
        path = path.strip()
        if not separator or not path:
            raise ValueError(
                f"Invalid --where expression {expression!r}; expected PROPERTY-PATH=VALUE"
            )
        try:
            criteria.append(schema.parse_criterion(path, raw_value))
        except ValueError as error:
            raise ValueError(f"Invalid --where expression {expression!r}: {error}") from error
    return tuple(criteria)


if __name__ == "__main__":
    app()
