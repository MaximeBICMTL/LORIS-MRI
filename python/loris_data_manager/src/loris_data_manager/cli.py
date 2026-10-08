#!/usr/bin/env python3

"""Command-line inspection of logical LORIS objects and their resources."""

from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from lib.config_file import load_config
from lib.db.connect import get_database_engine
from lib.db.queries.config import try_get_config_with_setting_name
from sqlalchemy.orm import Session

from loris_data_manager.inspection import (
    InspectionQuery,
    format_inspection_json,
    format_inspection_text,
    inspect_resources,
    parse_inspection_selection,
)
from loris_data_manager.provider import ResourceSchema
from loris_data_manager.providers.core import register_core_schema
from loris_data_manager.providers.dicom import register_dicom_schema
from loris_data_manager.schema import PropertyCriterion

app = typer.Typer(
    add_completion=False,
    help="Inspect logical LORIS objects, relationships, database rows, and local paths.",
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
            help="Select a whole logical object or one property; repeat to add projections.",
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
        with Session(engine) as db:
            storage_roots: dict[str, Path] = {}
            if any(selection.expression == "dicom-archive.file.size" for selection in query.selections):
                archive_root = try_get_config_with_setting_name(db, "tarchiveLibraryDir")
                if archive_root is None or archive_root.value is None:
                    raise typer.BadParameter(
                        "Missing tarchiveLibraryDir configuration for file inspection"
                    )
                storage_roots["dicom-archive"] = Path(archive_root.value)
            result = inspect_resources(db, schema, query, storage_roots=storage_roots)
            formatter = (
                format_inspection_json
                if output_format is OutputFormat.JSON
                else format_inspection_text
            )
            output = formatter(result)
    finally:
        engine.dispose()

    typer.echo(output)


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
