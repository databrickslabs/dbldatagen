"""SQL parsing via sqlglot — extract tables, columns, JOINs, and predicates."""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel


class SQLParseError(Exception):
    """Raised when SQL cannot be parsed or contains no tables."""


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ExtractedColumn:
    """A column reference found in a SQL query."""

    name: str
    table_alias: str | None = None
    table_name: str | None = None
    # Context clues
    in_select: bool = False
    in_where: bool = False
    in_join_on: bool = False
    in_group_by: bool = False
    in_order_by: bool = False
    cast_type: str | None = None
    compared_to_literal: object = None
    aggregated: bool = False


@dataclass
class ExtractedJoin:
    """A JOIN equality condition extracted from the query."""

    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str = "inner"


@dataclass
class ExtractedTable:
    """A table referenced in the query."""

    name: str
    alias: str | None = None
    columns: list[ExtractedColumn] = field(default_factory=list)
    schema_name: str | None = None
    catalog_name: str | None = None


@dataclass
class ExtractedSchema:
    """Everything extracted from parsing one or more SQL statements."""

    tables: list[ExtractedTable]
    joins: list[ExtractedJoin]
    original_sql: str
    dialect: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _alias_map_from_ast(ast: exp.Expression) -> dict[str, str]:
    """Build alias -> real table name mapping from all Table nodes."""
    mapping: dict[str, str] = {}
    for table_node in ast.find_all(exp.Table):
        name = table_node.name
        alias = table_node.alias
        if alias:
            mapping[alias] = name
        mapping[name] = name
    return mapping


def _cte_names(ast: exp.Expression) -> set[str]:
    """Return the set of CTE alias names defined in WITH clauses."""
    names: set[str] = set()
    for with_node in ast.find_all(exp.With):
        for cte in with_node.find_all(exp.CTE):
            if cte.alias:
                names.add(cte.alias)
    return names


def _get_join_type(join_node: exp.Join) -> str:
    """Return a normalised join type string."""
    side = join_node.side
    kind = join_node.kind
    if side:
        return side.lower()
    if kind:
        return kind.lower()
    return "inner"


def _resolve_table(col: exp.Column, alias_map: dict[str, str]) -> str | None:
    """Resolve a column's table reference through the alias map."""
    if col.table:
        return alias_map.get(col.table, col.table)
    return None


def _extract_literal_value(node: exp.Expression) -> object:
    """Extract a Python literal from a sqlglot Literal / Boolean node."""
    if isinstance(node, exp.Literal):
        if node.is_int:
            return int(node.this)
        if node.is_number:
            return float(node.this)
        return str(node.this)
    if isinstance(node, exp.Boolean):
        return node.this
    return None


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------


def _extract_tables(ast: exp.Expression, ctes: set[str]) -> dict[str, ExtractedTable]:
    """Extract all real (non-CTE) tables from the AST."""
    tables: dict[str, ExtractedTable] = {}
    for table_node in ast.find_all(exp.Table):
        name = table_node.name
        if not name or name in ctes:
            continue
        if name not in tables:
            tables[name] = ExtractedTable(
                name=name,
                alias=table_node.alias if table_node.alias != name else None,
                schema_name=table_node.db or None,
                catalog_name=table_node.catalog or None,
            )
    return tables


def _extract_columns(
    ast: exp.Expression,
    alias_map: dict[str, str],
    ctes: set[str],
    tables: dict[str, ExtractedTable],
) -> None:
    """Walk the AST and collect column references into their parent tables."""
    seen: dict[str, set[str]] = {t: set() for t in tables}
    # For unqualified columns, fall back to the sole table if there's exactly one
    real_table_names = [t for t in tables if t not in ctes]

    for col_node in ast.find_all(exp.Column):
        col_name = col_node.name
        if not col_name:
            continue
        table_name = _resolve_table(col_node, alias_map)
        # Skip CTE-only references
        if table_name and table_name in ctes:
            continue
        # Unqualified column: assign to single table if only one exists
        if table_name is None or table_name not in tables:
            if len(real_table_names) == 1:
                table_name = real_table_names[0]
            else:
                continue

        if col_name in seen[table_name]:
            continue
        seen[table_name].add(col_name)

        # Determine context
        ec = ExtractedColumn(
            name=col_name,
            table_alias=col_node.table or None,
            table_name=table_name,
        )

        # Walk up to find context (SELECT, WHERE, JOIN ON, GROUP BY, ORDER BY)
        parent = col_node.parent
        while parent:
            if isinstance(parent, exp.Select):
                ec.in_select = True
                break
            if isinstance(parent, exp.Where):
                ec.in_where = True
                break
            if isinstance(parent, exp.Join):
                ec.in_join_on = True
                break
            if isinstance(parent, exp.Group):
                ec.in_group_by = True
                break
            if isinstance(parent, exp.Order):
                ec.in_order_by = True
                break
            parent = parent.parent

        # Check for CAST
        cast_parent = col_node.find_ancestor(exp.Cast)
        if cast_parent:
            to_type = cast_parent.args.get("to")
            if to_type:
                ec.cast_type = to_type.sql()

        # Check for comparison with literal
        eq_parent = col_node.find_ancestor(exp.EQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NEQ)
        if eq_parent:
            for child in eq_parent.iter_expressions():
                if child is not col_node and not isinstance(child, exp.Column):
                    val = _extract_literal_value(child)  # type: ignore[arg-type]
                    if val is not None:
                        ec.compared_to_literal = val

        # Check for aggregate context
        agg_parent = col_node.find_ancestor(exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)
        if agg_parent:
            ec.aggregated = True

        tables[table_name].columns.append(ec)


def _extract_joins(
    ast: exp.Expression,
    alias_map: dict[str, str],
    ctes: set[str],
) -> list[ExtractedJoin]:
    """Extract JOIN equality conditions."""
    joins: list[ExtractedJoin] = []
    for join_node in ast.find_all(exp.Join):
        on_clause = join_node.args.get("on")
        if on_clause is None:
            # Check for USING clause
            using = join_node.args.get("using")
            if using:
                # USING(col) — both sides share the same column name
                joined_table_node = join_node.find(exp.Table)
                if joined_table_node:
                    joined_name = alias_map.get(
                        joined_table_node.alias or joined_table_node.name,
                        joined_table_node.name,
                    )
                    if joined_name in ctes:
                        continue
                    # Find the "other" table from the FROM clause
                    all_tables = [
                        alias_map.get(t.alias or t.name, t.name)
                        for t in ast.find_all(exp.Table)
                        if alias_map.get(t.alias or t.name, t.name) != joined_name
                        and alias_map.get(t.alias or t.name, t.name) not in ctes
                    ]
                    other_table = all_tables[0] if all_tables else ""
                    # using is a list of Identifier nodes
                    for id_node in using:
                        col_name = id_node.name if hasattr(id_node, "name") else str(id_node)
                        joins.append(
                            ExtractedJoin(
                                left_table=joined_name,
                                left_column=col_name,
                                right_table=other_table,
                                right_column=col_name,
                                join_type=_get_join_type(join_node),
                            )
                        )
            continue

        for eq in on_clause.find_all(exp.EQ):
            left = eq.left
            right = eq.right
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_table = _resolve_table(left, alias_map)
                right_table = _resolve_table(right, alias_map)
                if left_table and right_table:
                    if left_table in ctes or right_table in ctes:
                        continue
                    joins.append(
                        ExtractedJoin(
                            left_table=left_table,
                            left_column=left.name,
                            right_table=right_table,
                            right_column=right.name,
                            join_type=_get_join_type(join_node),
                        )
                    )
    return joins


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_sql(sql: str, *, dialect: str | None = None) -> ExtractedSchema:
    """Parse one or more SQL statements and extract schema information.

    Parameters
    ----------
    sql : str
        SQL text (SELECT, CREATE TABLE, or multi-statement).
    dialect : str | None
        sqlglot dialect name.  ``None`` = permissive generic parser.

    Returns
    -------
    ExtractedSchema

    Raises
    ------
    SQLParseError
        If the SQL cannot be parsed or contains no table references.
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect, error_level=ErrorLevel.WARN)
    except Exception as exc:
        raise SQLParseError(f"Failed to parse SQL: {exc}") from exc

    all_tables: dict[str, ExtractedTable] = {}
    all_joins: list[ExtractedJoin] = []

    for ast in statements:
        if ast is None:
            continue
        ctes = _cte_names(ast)  # type: ignore[arg-type]
        alias_map = _alias_map_from_ast(ast)  # type: ignore[arg-type]

        # Extract tables
        stmt_tables = _extract_tables(ast, ctes)  # type: ignore[arg-type]
        for name, table in stmt_tables.items():
            if name not in all_tables:
                all_tables[name] = table

        # Extract columns into tables
        _extract_columns(ast, alias_map, ctes, all_tables)  # type: ignore[arg-type]

        # Extract joins
        stmt_joins = _extract_joins(ast, alias_map, ctes)  # type: ignore[arg-type]
        all_joins.extend(stmt_joins)

    if not all_tables:
        raise SQLParseError("No tables found in SQL query.")

    return ExtractedSchema(
        tables=list(all_tables.values()),
        joins=all_joins,
        original_sql=sql,
        dialect=dialect,
    )
