"""Schema inference — types, PKs, and FK relationships from parsed SQL."""

from __future__ import annotations

import re
from dataclasses import dataclass

from dbldatagen.v1.connectors.sql.parser import (
    ExtractedColumn,
    ExtractedJoin,
    ExtractedSchema,
)
from dbldatagen.v1.schema import DataType


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class InferredColumn:
    """A column with inferred type and metadata."""

    name: str
    table_name: str
    inferred_type: DataType = DataType.STRING
    type_confidence: float = 0.3
    is_pk: bool = False
    fk_ref: str | None = None  # "parent_table.parent_column"
    nullable: bool = True


@dataclass
class InferredSchema:
    """Fully inferred schema ready for plan building."""

    tables: dict[str, list[InferredColumn]]  # table_name -> columns
    fk_edges: list[tuple[str, str, str, str]]  # (child, child_col, parent, parent_col)
    pk_columns: dict[str, str]  # table_name -> pk_column_name


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------

# Patterns ordered by specificity — first match wins.
_TYPE_NAME_RULES: list[tuple[re.Pattern, DataType, float]] = [
    # IDs
    (re.compile(r"^id$", re.I), DataType.LONG, 0.7),
    (re.compile(r"_id$", re.I), DataType.LONG, 0.7),
    # Booleans
    (re.compile(r"^is_|^has_|_flag$|^active$|^enabled$|^verified$|^deleted$", re.I), DataType.BOOLEAN, 0.8),
    # Timestamps / dates
    (re.compile(r"_at$|_date$|_time$|^created|^updated|^timestamp|^date$|_on$", re.I), DataType.TIMESTAMP, 0.8),
    # Money / decimal
    (re.compile(r"price|cost|amount|total|revenue|salary|fee|balance|tax|discount", re.I), DataType.DOUBLE, 0.7),
    # Counts / integers
    (re.compile(r"quantity|qty|count|num_|_count$|_num$|^age$|^year$", re.I), DataType.INT, 0.7),
    # Scores / decimals
    (
        re.compile(r"rating|score|percent|pct|ratio|fraction|weight|height|length|width|depth", re.I),
        DataType.DOUBLE,
        0.7,
    ),
    # Geo
    (re.compile(r"latitude|lat$", re.I), DataType.DOUBLE, 0.7),
    (re.compile(r"longitude|lng$|lon$", re.I), DataType.DOUBLE, 0.7),
]

# SQL type fragments → DataType mapping
_SQL_TYPE_MAP: dict[str, DataType] = {
    "int": DataType.INT,
    "integer": DataType.INT,
    "bigint": DataType.LONG,
    "long": DataType.LONG,
    "int64": DataType.LONG,
    "smallint": DataType.INT,
    "tinyint": DataType.INT,
    "float": DataType.FLOAT,
    "real": DataType.FLOAT,
    "float64": DataType.DOUBLE,
    "double": DataType.DOUBLE,
    "decimal": DataType.DOUBLE,
    "numeric": DataType.DOUBLE,
    "number": DataType.DOUBLE,
    "money": DataType.DOUBLE,
    "boolean": DataType.BOOLEAN,
    "bool": DataType.BOOLEAN,
    "bit": DataType.BOOLEAN,
    "date": DataType.DATE,
    "datetime": DataType.TIMESTAMP,
    "datetime2": DataType.TIMESTAMP,
    "timestamp": DataType.TIMESTAMP,
    "timestamptz": DataType.TIMESTAMP,
    "varchar": DataType.STRING,
    "nvarchar": DataType.STRING,
    "char": DataType.STRING,
    "text": DataType.STRING,
    "string": DataType.STRING,
    "clob": DataType.STRING,
}


def _map_sql_type(sql_type: str) -> DataType:
    """Map a SQL type string to a DataType."""
    key = sql_type.strip().lower().split("(")[0].strip()
    return _SQL_TYPE_MAP.get(key, DataType.STRING)


def infer_column_type(col: ExtractedColumn) -> tuple[DataType, float]:  # noqa: PLR0911
    """Infer the DataType and confidence for a parsed column."""
    # Signal 1: explicit CAST
    if col.cast_type:
        return _map_sql_type(col.cast_type), 0.95

    # Signal 2: comparison with literal
    if col.compared_to_literal is not None:
        lit = col.compared_to_literal
        if isinstance(lit, bool):
            return DataType.BOOLEAN, 0.8
        if isinstance(lit, int):
            return DataType.LONG, 0.8
        if isinstance(lit, float):
            return DataType.DOUBLE, 0.8
        if isinstance(lit, str):
            return DataType.STRING, 0.8

    # Signal 3: aggregate context (SUM/AVG suggests numeric)
    if col.aggregated:
        return DataType.DOUBLE, 0.6

    # Signal 4: name heuristics
    return infer_type_from_name(col.name)


def infer_type_from_name(name: str) -> tuple[DataType, float]:
    """Infer type from column name alone."""
    for pattern, dtype, confidence in _TYPE_NAME_RULES:
        if pattern.search(name):
            return dtype, confidence
    return DataType.STRING, 0.3


# ---------------------------------------------------------------------------
# PK detection
# ---------------------------------------------------------------------------


def detect_pk(table_name: str, column_names: list[str]) -> str:
    """Detect the primary key column for a table.

    Returns the detected PK column name, or ``"_synth_id"`` as a fallback.
    """
    names_lower = {c.lower(): c for c in column_names}

    # 1. Column named exactly "id"
    if "id" in names_lower:
        return names_lower["id"]

    # 2. Column named "{table_name}_id"  (e.g. customer_id in customers)
    singular = table_name.rstrip("s").lower()
    for candidate in [f"{table_name.lower()}_id", f"{singular}_id"]:
        if candidate in names_lower:
            return names_lower[candidate]

    # 3. First column ending in "_id"
    for c in column_names:
        if c.lower().endswith("_id"):
            return c

    # 4. Fallback: synthetic PK
    return "_synth_id"


# ---------------------------------------------------------------------------
# FK detection
# ---------------------------------------------------------------------------


def _orient_fk(
    left_table: str,
    left_col: str,
    right_table: str,
    right_col: str,
    pk_columns: dict[str, str],
) -> tuple[str, str, str, str]:
    """Determine (child_table, child_col, parent_table, parent_col).

    Heuristics:
      1. If one side is that table's PK, it's the parent.
      2. If one column ends in _id and the other doesn't, _id is the child.
      3. Alphabetical tiebreak (deterministic).
    """
    left_is_pk = pk_columns.get(left_table) == left_col
    right_is_pk = pk_columns.get(right_table) == right_col

    if left_is_pk and not right_is_pk:
        return right_table, right_col, left_table, left_col
    if right_is_pk and not left_is_pk:
        return left_table, left_col, right_table, right_col

    # Heuristic 2: _id suffix
    left_has_id = left_col.lower().endswith("_id")
    right_has_id = right_col.lower().endswith("_id")
    if left_has_id and not right_has_id:
        return left_table, left_col, right_table, right_col
    if right_has_id and not left_has_id:
        return right_table, right_col, left_table, left_col

    # Tiebreak: alphabetical
    if left_table <= right_table:
        return right_table, right_col, left_table, left_col
    return left_table, left_col, right_table, right_col


def detect_foreign_keys(
    joins: list[ExtractedJoin],
    pk_columns: dict[str, str],
) -> list[tuple[str, str, str, str]]:
    """Infer FK relationships from JOIN conditions.

    Returns list of ``(child_table, child_col, parent_table, parent_col)``.
    """
    fk_edges: list[tuple[str, str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for join in joins:
        if not join.left_table or not join.right_table:
            continue
        child_t, child_c, parent_t, parent_c = _orient_fk(
            join.left_table,
            join.left_column,
            join.right_table,
            join.right_column,
            pk_columns,
        )
        key = (child_t, child_c)
        if key not in seen:
            seen.add(key)
            fk_edges.append((child_t, child_c, parent_t, parent_c))
    return fk_edges


# ---------------------------------------------------------------------------
# Top-level inference
# ---------------------------------------------------------------------------


def infer_schema(extracted: ExtractedSchema) -> InferredSchema:
    """Run full inference on an extracted schema.

    1. Detect PK per table.
    2. Infer column types.
    3. Detect FK relationships from JOINs.
    4. Mark FK columns and nullable flags.
    """
    # Build column name lists per table
    table_col_names: dict[str, list[str]] = {}
    for tbl in extracted.tables:
        table_col_names[tbl.name] = [c.name for c in tbl.columns]

    # 1. Detect PKs
    pk_columns: dict[str, str] = {}
    for tbl in extracted.tables:
        col_names = table_col_names[tbl.name]
        pk_columns[tbl.name] = detect_pk(tbl.name, col_names)

    # 2. Detect FKs from JOINs
    fk_edges = detect_foreign_keys(extracted.joins, pk_columns)
    fk_map: dict[tuple[str, str], str] = {}  # (table, col) -> "parent.parent_col"
    for child_t, child_c, parent_t, parent_c in fk_edges:
        fk_map[(child_t, child_c)] = f"{parent_t}.{parent_c}"

    # 2b. If PK candidate is also an FK, demote to _synth_id
    for table_name, pk_col in list(pk_columns.items()):
        if (table_name, pk_col) in fk_map:
            pk_columns[table_name] = "_synth_id"

    # 3. Build inferred columns per table
    inferred_tables: dict[str, list[InferredColumn]] = {}
    for tbl in extracted.tables:
        pk_col_name = pk_columns[tbl.name]
        cols: list[InferredColumn] = []
        seen_names: set[str] = set()

        for ec in tbl.columns:
            if ec.name in seen_names:
                continue
            seen_names.add(ec.name)

            dtype, conf = infer_column_type(ec)
            is_pk = ec.name == pk_col_name
            fk_ref = fk_map.get((tbl.name, ec.name))

            cols.append(
                InferredColumn(
                    name=ec.name,
                    table_name=tbl.name,
                    inferred_type=dtype,
                    type_confidence=conf,
                    is_pk=is_pk,
                    fk_ref=fk_ref,
                    nullable=not is_pk,
                )
            )

        # Ensure PK column exists (may not be in SELECT)
        if pk_col_name not in seen_names:
            pk_type, pk_conf = infer_type_from_name(pk_col_name)
            cols.insert(
                0,
                InferredColumn(
                    name=pk_col_name,
                    table_name=tbl.name,
                    inferred_type=pk_type if pk_col_name != "_synth_id" else DataType.LONG,
                    type_confidence=pk_conf if pk_col_name != "_synth_id" else 1.0,
                    is_pk=True,
                    nullable=False,
                ),
            )

        # Ensure FK columns exist (from JOINs)
        for (child_t, child_c), ref in fk_map.items():
            if child_t == tbl.name and child_c not in seen_names:
                seen_names.add(child_c)
                dtype, conf = infer_type_from_name(child_c)
                cols.append(
                    InferredColumn(
                        name=child_c,
                        table_name=tbl.name,
                        inferred_type=dtype,
                        type_confidence=conf,
                        fk_ref=ref,
                        nullable=True,
                    )
                )

        inferred_tables[tbl.name] = cols

    return InferredSchema(
        tables=inferred_tables,
        fk_edges=fk_edges,
        pk_columns=pk_columns,
    )
