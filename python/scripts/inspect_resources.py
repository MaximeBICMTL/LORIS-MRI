#!/usr/bin/env python3

"""Inspect logical LORIS objects and the resources bound to them."""

import argparse
from collections.abc import Sequence

from sqlalchemy.orm import Session

from lib.config_file import load_config
from lib.db.connect import get_database_engine
from lib.resource_model.inspection import (
    InspectionQuery,
    format_inspection_json,
    format_inspection_text,
    inspect_resources,
    make_resource_schema,
    parse_inspection_selection,
)
from lib.resource_model.provider import ResourceSchema
from lib.resource_model.schema import PropertyCriterion


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect logical LORIS objects, relationships, database rows, and local paths.",
    )
    parser.add_argument("-p", "--profile", help="Python database configuration profile.")
    parser.add_argument(
        "--select",
        action="append",
        required=True,
        metavar="OBJECT-OR-PROPERTY",
        help="Select a whole logical object or one property; repeat to add projections.",
    )
    parser.add_argument(
        "--where",
        action="append",
        metavar="PROPERTY-PATH=VALUE",
        help="Filter by a semantic property or relationship path; repeat to combine with AND.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="select_all",
        help="Allow inspection without a narrowing selector.",
    )
    parser.add_argument(
        "--expand-related",
        action="store_true",
        help="Resolve directly related objects for context.",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)
    try:
        schema = make_resource_schema()
        criteria = parse_where_expressions(args.where or (), schema=schema)
        query = InspectionQuery(
            selections=tuple(
                parse_inspection_selection(schema, expression) for expression in args.select
            ),
            criteria=criteria,
            select_all=args.select_all,
            expand_related=args.expand_related,
        )
    except ValueError as error:
        parser.error(str(error))

    config = load_config(args.profile)
    engine = get_database_engine(config.mysql)
    try:
        with Session(engine) as db:
            result = inspect_resources(db, query)
            formatter = format_inspection_json if args.format == "json" else format_inspection_text
            output = formatter(result)
    finally:
        engine.dispose()

    print(output)
    return 0


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
    raise SystemExit(main())
