"""Core generation engine: spark.range(N) -> select([column_exprs]) -> DataFrame.

Routes each ColumnSpec to the appropriate column builder based on its strategy,
assembles all column expressions, and executes them in a single flat ``select``
to avoid the O(n^2) plan depth of chained ``withColumn`` calls.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.v1.engine.columns.faker_pool import build_faker_column
from dbldatagen.v1.engine.columns.numeric import build_range_column
from dbldatagen.v1.engine.columns.pk import (
    build_sequential_pk,
)
from dbldatagen.v1.engine.columns.string import (
    build_constant_column,
    build_expression_column,
    build_pattern_column,
    build_values_column,
)
from dbldatagen.v1.engine.columns.temporal import build_date_column, build_timestamp_column
from dbldatagen.v1.engine.columns.uuid import build_uuid_column
from dbldatagen.v1.engine.fk import build_fk_column
from dbldatagen.v1.engine.planner import FKResolution, ResolvedPlan
from dbldatagen.v1.engine.seed import cell_seed_expr, compute_batch_seed, derive_column_seed, null_mask_expr
from dbldatagen.v1.schema import (
    ArrayColumn,
    ColumnSpec,
    ConstantColumn,
    DataType,
    ExpressionColumn,
    FakerColumn,
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
    """Generate a single table as a Spark DataFrame.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_spec:
        Pydantic model describing the table schema and row count.
    resolved_plan:
        Optional ResolvedPlan from the planner carrying FK resolution info.

    Returns
    -------
    DataFrame with all columns generated.
    """
    row_count = int(table_spec.rows)
    global_seed = table_spec.seed if table_spec.seed is not None else 42

    # 1. Base DataFrame with deterministic row IDs
    #    Rename spark.range's "id" to "_synth_row_id" to avoid collisions
    #    with user columns named "id".
    df = spark.range(row_count).withColumnRenamed("id", "_synth_row_id")
    id_col = F.col("_synth_row_id")

    # 2. Build column expressions
    fk_res = resolved_plan.fk_resolutions if resolved_plan is not None else None
    col_exprs, udf_columns, seeded_columns = build_all_column_exprs(
        table_spec,
        id_col,
        fk_res,
        seed=global_seed,
        row_count=row_count,
    )

    # 3. Single flat select for non-UDF columns (avoids O(n^2) withColumn planning)
    df = df.select(id_col, *col_exprs)

    # 4. Apply UDF-based columns (FK, Faker) via withColumn
    for col_name, col_expr in udf_columns:
        df = df.withColumn(col_name, col_expr)

    # 5. Apply seed_from columns (depend on other columns being present)
    for col_name, col_expr in seeded_columns:
        df = df.withColumn(col_name, col_expr)

    # 6. Drop internal row-id column (never part of the output schema)
    df = df.drop("_synth_row_id")

    return df


def build_all_column_exprs(
    table_spec: TableSpec,
    id_col: Column,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None = None,
    *,
    seed: int = 42,
    row_count: int = 0,
    seed_fn: Callable[[ColumnSpec], int] | None = None,
    cell_seed_fn: Callable[[int, Column, ColumnSpec], Column | None] | None = None,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Build column expressions for all columns in a table.

    Returns ``(col_exprs, udf_columns, seeded_columns)`` —
    ``col_exprs`` are Spark SQL expressions for a flat ``select``,
    ``udf_columns`` are ``(name, expr)`` pairs for ``withColumn`` (FK, Faker),
    ``seeded_columns`` are ``(name, expr)`` pairs for columns with ``seed_from``
    (applied after phases 1 and 2 so the source column is available).

    Parameters
    ----------
    fk_resolutions :
        Dict mapping ``(table_name, col_name)`` to ``FKResolution``.
    seed :
        Base seed used for default column-seed derivation and passed
        through to ``build_column_expr`` as ``global_seed``.
    row_count :
        Row count passed through to ``build_column_expr``.
    seed_fn :
        ``(col_spec) -> int`` — override column-seed derivation.
        Default: ``derive_column_seed(seed, table_name, col_spec.name)``.
    cell_seed_fn :
        ``(column_seed, id_col, col_spec) -> Column | None`` — override
        the per-cell seed for ``build_column_expr``.
        Default: ``None`` (standard xxhash64).
    """
    table_name = table_spec.name
    col_exprs: list[Column] = []
    udf_columns: list[tuple[str, Column]] = []
    seeded_columns: list[tuple[str, Column]] = []

    for col_spec in table_spec.columns:
        column_seed = seed_fn(col_spec) if seed_fn is not None else derive_column_seed(seed, table_name, col_spec.name)

        # Defer seed_from columns to phase 3
        if col_spec.seed_from is not None:
            result = _build_seed_from_column(col_spec, column_seed, seed, row_count)
            seeded_columns.append(result)
            continue

        # FK columns
        if col_spec.foreign_key is not None:
            fk_result = _build_fk_column_expr(
                col_spec,
                table_name,
                id_col,
                column_seed,
                fk_resolutions,
            )
            if fk_result is not None:
                udf_columns.append(fk_result)
            else:
                col_exprs.append(F.lit(None).alias(col_spec.name))
            continue

        # Faker columns
        if isinstance(col_spec.gen, FakerColumn):
            udf_columns.append(_build_faker_expr(col_spec, id_col, column_seed))
            continue

        # Regular columns
        expr = _build_regular_column_expr(
            col_spec,
            id_col,
            column_seed,
            row_count,
            seed,
            cell_seed_fn=cell_seed_fn,
        )
        if expr is not None:
            col_exprs.append(expr.alias(col_spec.name))

    return col_exprs, udf_columns, seeded_columns


def _build_seed_from_column(
    col_spec: ColumnSpec,
    column_seed: int,
    global_seed: int,
    row_count: int,
) -> tuple[str, Column]:
    """Build expression for a column with seed_from."""
    assert col_spec.seed_from is not None
    effective_id = F.col(col_spec.seed_from)

    if isinstance(col_spec.gen, FakerColumn):
        return _build_faker_expr(col_spec, effective_id, column_seed)

    expr = build_column_expr(col_spec, effective_id, column_seed, row_count, global_seed)
    if col_spec.null_fraction > 0:
        is_null = null_mask_expr(column_seed, effective_id, col_spec.null_fraction)
        expr = F.when(is_null, F.lit(None)).otherwise(expr)
    return (col_spec.name, expr.alias(col_spec.name))


def _build_fk_column_expr(
    col_spec: ColumnSpec,
    table_name: str,
    id_col: Column,
    column_seed: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None,
) -> tuple[str, Column] | None:
    """Build FK column expression, returning (name, expr) or None."""
    fk_key = (table_name, col_spec.name)
    if fk_resolutions and fk_key in fk_resolutions:
        fk_expr = build_fk_column(id_col, column_seed, fk_resolutions[fk_key])
        return (col_spec.name, fk_expr)
    return None


def _build_faker_expr(
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int,
) -> tuple[str, Column]:
    """Build a Faker pool UDF expression."""
    gen = col_spec.gen
    assert isinstance(gen, FakerColumn)
    faker_expr = build_faker_column(
        id_col,
        column_seed,
        provider=gen.provider,
        kwargs=gen.kwargs or None,
        locale=gen.locale,
    )
    if col_spec.null_fraction > 0:
        is_null = null_mask_expr(column_seed, id_col, col_spec.null_fraction)
        faker_expr = F.when(is_null, F.lit(None)).otherwise(faker_expr)
    return (col_spec.name, faker_expr)


def _build_regular_column_expr(
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
    *,
    cell_seed_fn: Callable[[int, Column, ColumnSpec], Column | None] | None = None,
) -> Column | None:
    """Build a regular (non-FK, non-seed_from) column expression."""
    cell_override = cell_seed_fn(column_seed, id_col, col_spec) if cell_seed_fn is not None else None
    expr = build_column_expr(
        col_spec,
        id_col,
        column_seed,
        row_count,
        global_seed,
        cell_seed_override=cell_override,
    )

    if col_spec.null_fraction > 0:
        is_null = null_mask_expr(column_seed, id_col, col_spec.null_fraction)
        expr = F.when(is_null, F.lit(None)).otherwise(expr)

    return expr


def build_all_column_exprs_case_when(
    table_spec: TableSpec,
    id_col: Column,
    wb_col: Column,
    unique_wbs: list[int],
    global_seed: int,
    *,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None = None,
    row_count: int = 0,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Build column expressions with CASE WHEN per write-batch seed.

    PK sequence columns are computed directly from row_id (no CASE WHEN).
    FK and regular columns get a CASE WHEN that selects the expression
    based on the write-batch value.

    Returns ``(col_exprs, udf_columns, seeded_columns)`` like
    ``build_all_column_exprs``.
    """
    table_name = table_spec.name
    pk_cols = set()
    if table_spec.primary_key:
        pk_cols = set(table_spec.primary_key.columns)

    col_exprs: list[Column] = []
    udf_columns: list[tuple[str, Column]] = []
    seeded_columns: list[tuple[str, Column]] = []

    for col_spec in table_spec.columns:
        # PK sequence: deterministic from row_id, seed-independent
        if col_spec.name in pk_cols and isinstance(col_spec.gen, SequenceColumn):
            pk_expr = (id_col * F.lit(col_spec.gen.step) + F.lit(col_spec.gen.start)).cast("long")
            col_exprs.append(pk_expr.alias(col_spec.name))
            continue

        # Defer seed_from columns to phase 3
        if col_spec.seed_from is not None:
            effective_id = F.col(col_spec.seed_from)
            wb_exprs = []
            for wb in unique_wbs:
                s = compute_batch_seed(global_seed, wb)
                col_seed = derive_column_seed(s, table_name, col_spec.name)
                expr = build_column_expr(col_spec, effective_id, col_seed, row_count, s)
                if col_spec.null_fraction > 0:
                    is_null = null_mask_expr(col_seed, effective_id, col_spec.null_fraction)
                    expr = F.when(is_null, F.lit(None)).otherwise(expr)
                wb_exprs.append((wb, expr))
            combined = _build_write_batch_case_when(wb_col, wb_exprs)
            seeded_columns.append((col_spec.name, combined.alias(col_spec.name)))
            continue

        # FK columns
        if col_spec.foreign_key is not None:
            fk_key = (table_name, col_spec.name)
            if fk_resolutions and fk_key in fk_resolutions:
                wb_exprs = []
                for wb in unique_wbs:
                    s = compute_batch_seed(global_seed, wb)
                    col_seed = derive_column_seed(s, table_name, col_spec.name)
                    expr = build_fk_column(id_col, col_seed, fk_resolutions[fk_key])
                    wb_exprs.append((wb, expr))
                combined = _build_write_batch_case_when(wb_col, wb_exprs)
                udf_columns.append((col_spec.name, combined))
            else:
                col_exprs.append(F.lit(None).alias(col_spec.name))
            continue

        # Regular columns: CASE WHEN per write_batch for seed
        wb_exprs = []
        for wb in unique_wbs:
            s = compute_batch_seed(global_seed, wb)
            col_seed = derive_column_seed(s, table_name, col_spec.name)
            expr = build_column_expr(col_spec, id_col, col_seed, row_count, s)
            if col_spec.null_fraction > 0:
                is_null = null_mask_expr(col_seed, id_col, col_spec.null_fraction)
                expr = F.when(is_null, F.lit(None)).otherwise(expr)
            wb_exprs.append((wb, expr))

        combined = _build_write_batch_case_when(wb_col, wb_exprs)
        col_exprs.append(combined.alias(col_spec.name))

    return col_exprs, udf_columns, seeded_columns


def _build_write_batch_case_when(wb_col: Column, wb_exprs: list[tuple[int, Column]]) -> Column:
    """Build CASE WHEN on a write-batch column for a list of (batch, expr) pairs.

    When there's only one batch value, returns the expression directly
    (no CASE WHEN needed).
    """
    if len(wb_exprs) == 1:
        return wb_exprs[0][1]

    result = wb_exprs[-1][1]
    for i in range(len(wb_exprs) - 2, -1, -1):
        result = F.when(
            wb_col == F.lit(wb_exprs[i][0]).cast("long"),
            wb_exprs[i][1],
        ).otherwise(result)
    return result


def _build_timestamp_or_date(
    col_spec: ColumnSpec,
    gen: TimestampColumn,
    id_col: Column,
    column_seed: int,
    cell_seed_override: Column | None,
) -> Column:
    """Build a date or timestamp column depending on dtype."""
    builder = build_date_column if col_spec.dtype == DataType.DATE else build_timestamp_column
    return builder(
        id_col,
        column_seed,
        gen.start,
        gen.end,
        gen.distribution,
        cell_seed_override=cell_seed_override,
    )


def build_column_expr(
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
    *,
    cell_seed_override: Column | None = None,
) -> Column:
    """Dispatch to the appropriate column builder based on strategy type.

    Parameters
    ----------
    cell_seed_override :
        If provided, used as the per-cell seed instead of the default
        ``cell_seed_expr(column_seed, id_col)``.  Useful for snapshot
        generation where the seed incorporates per-row state (e.g. the
        last-write batch).
    """
    gen = col_spec.gen

    if isinstance(gen, RangeColumn):
        return build_range_column(
            id_col,
            column_seed,
            gen.min,
            gen.max,
            distribution=gen.distribution,
            dtype=col_spec.dtype,
            cell_seed_override=cell_seed_override,
        )

    if isinstance(gen, ValuesColumn):
        return build_values_column(
            id_col,
            column_seed,
            gen.values,
            distribution=gen.distribution,
            cell_seed_override=cell_seed_override,
        )

    if isinstance(gen, (PatternColumn, SequenceColumn, UUIDColumn, ExpressionColumn, ConstantColumn)):
        simple_dispatch = {
            PatternColumn: lambda g: build_pattern_column(id_col, column_seed, g.template),
            SequenceColumn: lambda g: build_sequential_pk(id_col, start=g.start, step=g.step),
            UUIDColumn: lambda g: build_uuid_column(id_col, column_seed),
            ExpressionColumn: lambda g: build_expression_column(g.expr),
            ConstantColumn: lambda g: build_constant_column(g.value),
        }
        return simple_dispatch[type(gen)](gen)

    if isinstance(gen, TimestampColumn):
        return _build_timestamp_or_date(col_spec, gen, id_col, column_seed, cell_seed_override)

    if isinstance(gen, (StructColumn, ArrayColumn)):
        builder = _build_struct_column if isinstance(gen, StructColumn) else _build_array_column
        return builder(gen, id_col, column_seed, row_count, global_seed)  # type: ignore[arg-type]

    # Unsupported strategy fallback
    return F.lit(f"<unsupported:{gen.strategy}>")


def _build_struct_column(
    gen: StructColumn,
    id_col: Column,
    parent_seed: int,
    row_count: int,
    global_seed: int,
) -> Column:
    """Build a Spark struct from child ColumnSpecs."""
    field_cols: list[Column] = []
    for field_spec in gen.fields:
        child_seed = derive_column_seed(parent_seed, "", field_spec.name)
        child_expr = build_column_expr(
            field_spec,
            id_col,
            child_seed,
            row_count,
            global_seed,
        )
        if field_spec.null_fraction > 0:
            is_null = null_mask_expr(child_seed, id_col, field_spec.null_fraction)
            child_expr = F.when(is_null, F.lit(None)).otherwise(child_expr)
        field_cols.append(child_expr.alias(field_spec.name))
    return F.struct(*field_cols)


def _build_array_column(
    gen: ArrayColumn,
    id_col: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
) -> Column:
    """Build a variable-length Spark array from an inner strategy."""
    # Generate max_length elements, each with a unique seed offset
    element_cols: list[Column] = []
    dummy_spec = ColumnSpec(name="_elem", gen=gen.element)
    for i in range(gen.max_length):
        elem_seed = column_seed ^ ((i + 1) * 0x9E3779B9)
        # Keep elem_seed in signed 64-bit range
        if elem_seed >= 0x8000000000000000:
            elem_seed -= 0x10000000000000000
        elem_expr = build_column_expr(
            dummy_spec,
            id_col,
            elem_seed,
            row_count,
            global_seed,
        )
        element_cols.append(elem_expr)

    full_array = F.array(*element_cols)

    if gen.min_length == gen.max_length:
        return full_array

    # Random length per row in [min_length, max_length]
    range_size = gen.max_length - gen.min_length + 1
    seed_col = cell_seed_expr(column_seed, id_col)
    rand_len = (F.abs(seed_col) % F.lit(range_size)).cast("int") + F.lit(gen.min_length)
    return F.slice(full_array, 1, rand_len)
