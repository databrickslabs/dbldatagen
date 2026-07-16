"""Core generation engine that turns a table spec into a Spark DataFrame.

Starts from `spark.range(rows)`, routes each column to its builder, and applies
the resulting expressions to produce the table.
"""

from __future__ import annotations

from typing import cast

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.faker_pool import build_faker_expr
from dbldatagen.core.engine.columns.numeric import build_range_column
from dbldatagen.core.engine.columns.pk import (
    build_sequential_pk,
)
from dbldatagen.core.engine.columns.string import (
    build_pattern_column,
    build_values_column,
)
from dbldatagen.core.engine.columns.temporal import build_date_column, build_timestamp_column
from dbldatagen.core.engine.columns.uuid import build_uuid_column
from dbldatagen.core.engine.fk import build_fk_column_expr
from dbldatagen.core.engine.planner import FKResolution, ResolvedPlan
from dbldatagen.core.engine.seed import GOLDEN_RATIO_HASH, cell_seed_expr, derive_column_seed, seed_xor
from dbldatagen.core.engine.utils import apply_column_phases, apply_null_fraction, create_range_df
from dbldatagen.core.spec.schema import (
    ArrayColumn,
    ColumnSpec,
    ConstantColumn,
    DataType,
    ExpressionColumn,
    FakerColumn,
    ForeignKeyColumn,
    PatternColumn,
    RangeColumn,
    SequenceColumn,
    StructColumn,
    TableSpec,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
)


def generate_table(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None = None,
) -> DataFrame:
    """Generates a single table as a deterministic Spark DataFrame.

    `table_spec.seed` must be set -- typically by going through `DataGenPlan`,
    which propagates the plan seed to each table. When `resolved_plan` is given,
    foreign-key columns resolve against its parent metadata; without it, FK
    columns raise.

    Args:
        spark: Active `SparkSession` used to build the DataFrame.
        table_spec: The table's schema, row count, and per-table seed.
        resolved_plan: Optional resolved plan carrying FK resolution info; it
            must contain a table named `table_spec.name` (default None).

    Returns:
        A DataFrame with `table_spec.rows` rows and one column per `ColumnSpec`.
        Output is deterministic for a given seed.

    Raises:
        ValueError: If `table_spec.seed` is None, or `resolved_plan` does not
            contain a table named `table_spec.name`.
    """
    # TableSpec validation normalises ``rows`` (declared int | str) to int, so
    # this is a pure type assertion -- not int(...), which would re-coerce.
    row_count = cast(int, table_spec.rows)
    if table_spec.seed is None:
        raise ValueError(
            f"TableSpec '{table_spec.name}'.seed is None.  Either set "
            f"it explicitly on the TableSpec or go through a "
            f"DataGenPlan (which propagates plan.seed to each table "
            f"during Pydantic validation)."
        )
    # Name-based (not identity) check: callers may construct fresh per-call
    # TableSpecs that share a name with a table in the resolved plan.
    if resolved_plan is not None:
        plan_table_names = {t.name for t in resolved_plan.plan.tables}
        if table_spec.name not in plan_table_names:
            raise ValueError(
                f"resolved_plan does not contain a table named "
                f"'{table_spec.name}' (available: {sorted(plan_table_names)}).  "
                f"Pass a ResolvedPlan built from a plan that owns this "
                f"TableSpec, or drop the ``resolved_plan`` argument."
            )
    global_seed = table_spec.seed

    df, id_column = create_range_df(spark, row_count)

    fk_res = resolved_plan.fk_resolutions if resolved_plan is not None else None
    col_exprs, udf_columns, seeded_columns = build_all_column_exprs(
        table_spec,
        id_column,
        fk_res,
        seed=global_seed,
        row_count=row_count,
    )

    df = apply_column_phases(df, id_column, col_exprs, udf_columns, seeded_columns)
    return df.select(*[col_spec.name for col_spec in table_spec.columns])


def build_all_column_exprs(
    table_spec: TableSpec,
    id_column: Column,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None = None,
    *,
    seed: int,
    row_count: int = 0,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Builds the column expressions for every column in a table.

    Returns the three groups consumed by `apply_column_phases`: plain Spark SQL
    columns, UDF-based columns (FK and Faker), and `seed_from`-derived columns.

    Args:
        table_spec: The table whose columns to build.
        id_column: Row-id column.
        fk_resolutions: Optional map of `(table, column)` to `FKResolution`
            (default None, meaning no FK columns).
        seed: Base seed for per-column seed derivation. Derive it from
            `table_spec.seed` so reruns stay reproducible.
        row_count: Optional number of rows, forwarded to builders (default 0).

    Returns:
        A tuple `(col_exprs, udf_columns, seeded_columns)` for
        `apply_column_phases`.
    """
    return _build_column_exprs_loop(
        table_spec,
        id_column,
        seed,
        row_count,
        fk_resolutions,
    )


def _build_column_exprs_loop(
    table_spec: TableSpec,
    id_column: Column,
    effective_global_seed: int,
    row_count: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Builds the column expression groups for a table.

    Iterates the columns, derives each per-column seed, and routes each column
    to its builder: `seed_from` columns to phase 3, FK and Faker columns to
    phase 2, and the rest to phase 1.

    Args:
        table_spec: The table whose columns to build.
        id_column: Row-id column.
        effective_global_seed: Base seed for per-column seed derivation.
        row_count: Number of rows, forwarded to per-column builders.
        fk_resolutions: Optional map of `(table, column)` to `FKResolution`.

    Returns:
        A tuple `(col_exprs, udf_columns, seeded_columns)` for
        `apply_column_phases`.
    """
    table_name = table_spec.name
    col_exprs: list[Column] = []
    udf_columns: list[tuple[str, Column]] = []
    seeded_columns: list[tuple[str, Column]] = []

    for col_spec in table_spec.columns:
        column_seed = derive_column_seed(effective_global_seed, table_name, col_spec.name)

        if col_spec.seed_from is not None:
            result = _build_seed_from_column(col_spec, column_seed, effective_global_seed, row_count)
            seeded_columns.append(result)
            continue

        # build_fk_column_expr raises if the resolution is missing, so we never
        # silently emit an all-NULL FK column.
        if col_spec.foreign_key is not None:
            udf_columns.append(
                build_fk_column_expr(
                    col_spec,
                    table_name,
                    id_column,
                    column_seed,
                    fk_resolutions,
                )
            )
            continue

        if isinstance(col_spec.gen, FakerColumn):
            udf_columns.append(build_faker_expr(col_spec, id_column, column_seed))
            continue

        column = _build_regular_column_expr(
            col_spec,
            id_column,
            column_seed,
            row_count,
            effective_global_seed,
        )
        if column is not None:
            col_exprs.append(column.alias(col_spec.name))

    return col_exprs, udf_columns, seeded_columns


def _build_seed_from_column(
    col_spec: ColumnSpec,
    column_seed: int,
    global_seed: int,
    row_count: int,
) -> tuple[str, Column]:
    """Builds the expression for a column whose seed comes from another column.

    The per-cell seed is derived from the `seed_from` source column, so this
    column correlates with that source.

    Args:
        col_spec: The column to build; its `seed_from` must be set.
        column_seed: Per-column seed.
        global_seed: Plan-level seed, forwarded to nested struct/array seeds.
        row_count: Number of rows.

    Returns:
        A tuple `(column_name, expr)`.

    Raises:
        ValueError: If `col_spec.seed_from` is not set.
    """
    if col_spec.seed_from is None:
        raise ValueError(
            f"_build_seed_from_column called for '{col_spec.name}' but seed_from is None -- "
            f"only columns with seed_from set should reach this helper"
        )
    effective_id = F.col(col_spec.seed_from)

    if isinstance(col_spec.gen, FakerColumn):
        return build_faker_expr(col_spec, effective_id, column_seed)

    column = build_column_expr(col_spec, effective_id, column_seed, row_count, global_seed)
    column = apply_null_fraction(column, column_seed, effective_id, col_spec.null_fraction)
    return (col_spec.name, column.alias(col_spec.name))


def _build_regular_column_expr(
    col_spec: ColumnSpec,
    id_column: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
) -> Column | None:
    """Builds a regular column expression (non-FK, non-`seed_from`).

    Wraps the strategy's builder with null injection when a null fraction is set.

    Args:
        col_spec: The column to build.
        id_column: Row-id column.
        column_seed: Per-column seed.
        row_count: Number of rows.
        global_seed: Plan-level seed, forwarded to nested struct/array seeds.

    Returns:
        A Spark `Column` for the column.
    """
    column = build_column_expr(
        col_spec,
        id_column,
        column_seed,
        row_count,
        global_seed,
    )

    return apply_null_fraction(column, column_seed, id_column, col_spec.null_fraction)


def build_column_expr(
    col_spec: ColumnSpec,
    id_column: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
) -> Column:
    """Maps a column spec to its strategy's builder and returns the column.

    Args:
        col_spec: The column to build.
        id_column: Row-id column.
        column_seed: Per-column seed.
        row_count: Number of rows, forwarded to builders that need it.
        global_seed: Plan-level seed, forwarded to nested struct/array seeds.

    Returns:
        A Spark `Column` of generated values.

    Raises:
        TypeError: If `col_spec.gen` is a `ForeignKeyColumn` (FK resolution
            must run earlier via `build_fk_column_expr`).
        ValueError: If `col_spec.gen` is an unsupported strategy (including
            `FakerColumn`, which is handled out-of-band).

    Note:
        `FakerColumn` and `ForeignKeyColumn` are not dispatched here. Faker
        goes through a column-level UDF, and foreign keys resolve against the
        FK metadata at the table level.
    """
    gen = col_spec.gen
    match gen:
        case RangeColumn():
            result = build_range_column(
                id_column,
                column_seed,
                gen.min,
                gen.max,
                distribution=gen.distribution,
                dtype=col_spec.dtype,
                precision=col_spec.precision,
                scale=col_spec.scale,
                step=gen.step,
            )
        case ValuesColumn():
            result = build_values_column(
                id_column,
                column_seed,
                gen.values,
                distribution=gen.distribution,
            )
        case PatternColumn():
            result = build_pattern_column(id_column, column_seed, gen.template)
        case SequenceColumn():
            result = build_sequential_pk(id_column, start=gen.start, step=gen.step)
        case UUIDColumn():
            result = build_uuid_column(id_column, column_seed)
        case ExpressionColumn():
            result = F.expr(gen.expr)
        case TimestampColumn() if col_spec.dtype == DataType.DATE:
            result = build_date_column(
                id_column,
                column_seed,
                gen.start,
                gen.end,
                gen.distribution,
            )
        case TimestampColumn():
            result = build_timestamp_column(
                id_column,
                column_seed,
                gen.start,
                gen.end,
                gen.distribution,
            )
        case ConstantColumn():
            result = F.lit(gen.value)
        case ForeignKeyColumn():
            # FK columns are resolved earlier via ColumnSpec.foreign_key in
            # build_fk_column_expr. Reaching dispatch means either the column
            # had no foreign_key set (should have been caught by ColumnSpec's
            # validator) or the FK loop short-circuit was bypassed.
            raise TypeError(
                f"ForeignKeyColumn '{col_spec.name}' reached build_column_expr — "
                f"FK resolution must run before column-strategy dispatch. "
                f"Check that ColumnSpec.foreign_key is set and the planner has "
                f"produced an FKResolution for this column."
            )
        case StructColumn():
            result = _build_struct_column(
                gen,
                id_column,
                column_seed,
                row_count,
                global_seed,
            )
        case ArrayColumn():
            result = _build_array_column(gen, id_column, column_seed, row_count, global_seed)
        case _:
            # Drift guard: ColumnSpec.gen is a Pydantic discriminated
            # union whose declared members are exactly the cases above
            # (minus FakerColumn, which is dispatched out-of-band via
            # build_faker_expr).  Reaching this arm means either a new
            # strategy type was added to the schema without a dispatch
            # case here, or FakerColumn slipped past the FK / Faker
            # short-circuits in _build_column_exprs_loop.
            raise ValueError(
                f"Unsupported column strategy '{gen.strategy}' for column '{col_spec.name}'. "
                f"Supported: range, values, faker, pattern, sequence, uuid, expression, "
                f"timestamp, constant, foreign_key, struct, array."
            )
    return result


def _build_struct_column(
    gen: StructColumn,
    id_column: Column,
    parent_seed: int,
    row_count: int,
    global_seed: int,
) -> Column:
    """Builds a Spark struct column from its child column specs.

    Supports arbitrary nesting (struct of struct). Each field gets an
    independent seed derived from the parent's, so fields are decorrelated.

    Args:
        gen: The struct strategy with its child column specs.
        id_column: Row-id column.
        parent_seed: Seed for the struct, used to derive each field's seed.
        row_count: Number of rows.
        global_seed: Plan-level seed, forwarded to nested struct/array seeds.

    Returns:
        A Spark struct `Column`.
    """
    field_cols: list[Column] = []
    for field_spec in gen.fields:
        child_seed = derive_column_seed(parent_seed, "", field_spec.name)
        child_expr = build_column_expr(
            field_spec,
            id_column,
            child_seed,
            row_count,
            global_seed,
        )
        child_expr = apply_null_fraction(child_expr, child_seed, id_column, field_spec.null_fraction)
        field_cols.append(child_expr.alias(field_spec.name))
    return F.struct(*field_cols)


def _build_array_column(
    gen: ArrayColumn,
    id_column: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
) -> Column:
    """Builds a variable-length Spark array column from an element strategy.

    Generates `max_length` elements, each with an independent seed, then slices
    each row to a random length in `[min_length, max_length]`.

    Args:
        gen: The array strategy (element strategy and length bounds).
        id_column: Row-id column.
        column_seed: Per-column seed.
        row_count: Number of rows.
        global_seed: Plan-level seed, forwarded to nested struct/array seeds.

    Returns:
        A Spark array `Column`.
    """
    element_cols: list[Column] = []
    # Internal spec that drives the per-element builder; never surfaced to the user.
    dummy_spec = ColumnSpec(name="elem", gen=gen.element)
    for element_index in range(gen.max_length):
        elem_seed = seed_xor(column_seed, (element_index + 1) * GOLDEN_RATIO_HASH)
        elem_expr = build_column_expr(
            dummy_spec,
            id_column,
            elem_seed,
            row_count,
            global_seed,
        )
        element_cols.append(elem_expr)

    full_array = F.array(*element_cols)

    if gen.min_length == gen.max_length:
        return full_array

    # Mix a constant into column_seed before the length hash so it decorrelates
    # from element[0]'s cell seed (which uses the unmixed column_seed); otherwise
    # array length and the first element's value share low bits and correlate.
    # The mix value is Knuth's Fibonacci-hashing constant floor(2^64 / phi).
    range_size = gen.max_length - gen.min_length + 1
    _LEN_SEED_MIX = 0x9E3779B97F4A7C15
    length_seed = seed_xor(column_seed, _LEN_SEED_MIX)
    seed_col = cell_seed_expr(length_seed, id_column)
    random_length = F.pmod(seed_col, F.lit(range_size)).cast("int") + F.lit(gen.min_length)
    return F.slice(full_array, 1, random_length)
