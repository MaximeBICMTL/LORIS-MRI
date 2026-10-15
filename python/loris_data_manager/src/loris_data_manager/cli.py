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
    required_storage_roots,
)
from loris_data_manager.inspection import (
    InspectionQuery,
    execute_inspection_plan,
    format_inspection_json,
    format_inspection_sql,
    format_inspection_text,
    parse_inspection_selection,
    plan_inspection,
)
from loris_data_manager.provider import ResourceSchema
from loris_data_manager.providers.core import register_core_schema
from loris_data_manager.providers.dicom import register_dicom_schema
from loris_data_manager.providers.imaging import register_imaging_schema
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
        str,
        typer.Option(
            "--select",
            metavar="OBJECT-KIND",
            help="Select the logical object kind to back up.",
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
    select_all: Annotated[
        bool,
        typer.Option("--all", help="Back up every object of the selected kind."),
    ] = False,
) -> None:
    """Create a verified logical-resource backup with SQL and file payloads."""

    schema = make_resource_schema()
    expressions = tuple(where or ())
    try:
        criteria = parse_where_expressions(expressions, schema=schema)
        schema.object_kind(select)
        if not schema.has_provider(select):
            raise ValueError(f"No provider is registered for object kind {select!r}")
        selection = BackupSelection(
            kind=select,
            criteria=criteria,
            where_expressions=expressions,
            select_all=select_all,
        )
        storage_root_names = required_storage_roots(schema, select)
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
                        f"the resource schema for {select!r}"
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
        f"{result.kind} object(s): "
        f"{result.database_row_count} database row(s), {result.file_count} file(s)."
    )
    if result.blocker_count:
        typer.echo(
            f"Recorded {result.blocker_count} external reference(s) that currently block removal."
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
