"""Core generation engine: spark.range(N) -> select([column_exprs]) -> DataFrame.

Routes each ColumnSpec to the appropriate column builder based on its strategy,
assembles all column expressions, and executes them in a single flat ``select``
to avoid the O(n^2) plan depth of chained ``withColumn`` calls.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

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
from dbldatagen.v1.engine.planner import FKResolution, ResolvedPlan
from dbldatagen.v1.engine.seed import (
    column_seed_lookup,
    column_seed_map,
    compute_batch_seed,
    derive_column_seed,
    null_mask_expr,
)
from dbldatagen.v1.engine.utils import (
    apply_column_phases,
    apply_null_fraction,
    create_range_df,
    get_pk_columns,
)
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
    df, id_col = create_range_df(spark, row_count)

    # 2. Build column expressions
    fk_res = resolved_plan.fk_resolutions if resolved_plan is not None else None
    col_exprs, udf_columns, seeded_columns = build_all_column_exprs(
        table_spec,
        id_col,
        fk_res,
        seed=global_seed,
        row_count=row_count,
    )

    # 3. Flat select + withColumn phases + drop _synth_row_id
    return apply_column_phases(df, id_col, col_exprs, udf_columns, seeded_columns)


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
            result = _build_fk_column_expr(
                col_spec,
                table_name,
                id_col,
                column_seed,
                fk_resolutions,
            )
            if result is not None:
                udf_columns.append(result)
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
            cell_seed_fn,
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
    effective_id = F.col(col_spec.seed_from)

    if isinstance(col_spec.gen, FakerColumn):
        return _build_faker_expr(col_spec, effective_id, column_seed)

    expr = build_column_expr(col_spec, effective_id, column_seed, row_count, global_seed)
    expr = apply_null_fraction(expr, column_seed, effective_id, col_spec.null_fraction)
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
        from dbldatagen.v1.engine.fk import build_fk_column

        fk_expr = build_fk_column(id_col, column_seed, fk_resolutions[fk_key])
        return (col_spec.name, fk_expr)
    return None


def _build_faker_expr(
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int,
) -> tuple[str, Column]:
    """Build a Faker pool UDF expression."""
    from dbldatagen.v1.engine.columns.faker_pool import build_faker_column

    faker_expr = build_faker_column(
        id_col,
        column_seed,
        provider=col_spec.gen.provider,
        kwargs=col_spec.gen.kwargs or None,
        locale=col_spec.gen.locale,
    )
    faker_expr = apply_null_fraction(faker_expr, column_seed, id_col, col_spec.null_fraction)
    return (col_spec.name, faker_expr)


def _build_regular_column_expr(
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
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

    return apply_null_fraction(expr, column_seed, id_col, col_spec.null_fraction)


def build_all_column_exprs_case_when(
    table_spec: TableSpec,
    id_col: Column,
    wb_col: Column,
    unique_wbs: list[int],
    global_seed: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None = None,
    *,
    row_count: int = 0,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Build column expressions for fused multi-batch CDC DataFrames.

    PERFORMANCE-CRITICAL FUNCTION — read before modifying:
        When there are many unique write-batch values (>1), seeds are resolved
        via a Spark **map literal** (``column_seed_map`` + ``element_at``),
        producing O(1) plan nodes per column regardless of batch count.

        The naive approach (CASE WHEN per write-batch per column) produced
        O(N_batches x N_columns) expression branches -- e.g. 365 batches x 10
        columns = 3,650+ nodes -- causing Catalyst to stall for minutes while
        the cluster sat at ~10% CPU utilization.  The map-based approach was
        verified at 500M-3B rows with full cluster utilization.

        Do not refactor ``_build_exprs_dynamic`` back to CASE WHEN.  See
        ``column_seed_map`` in seed.py for additional context.

    Returns ``(col_exprs, udf_columns, seeded_columns)``.
    """
    # Fast path: single write-batch (no CASE WHEN needed)
    if len(unique_wbs) == 1:
        return _build_exprs_scalar(
            table_spec,
            id_col,
            unique_wbs[0],
            global_seed,
            fk_resolutions,
            row_count=row_count,
        )

    # Map-based seed lookup: precompute seeds, O(1) lookup per row at runtime
    return _build_exprs_dynamic(
        table_spec,
        id_col,
        wb_col,
        unique_wbs,
        global_seed,
        fk_resolutions,
        row_count=row_count,
    )


def _build_exprs_scalar(
    table_spec: TableSpec,
    id_col: Column,
    wb: int,
    global_seed: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None,
    *,
    row_count: int = 0,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Build column expressions using a single scalar write-batch seed."""
    table_name = table_spec.name
    pk_cols = get_pk_columns(table_spec)
    s = compute_batch_seed(global_seed, wb)

    col_exprs: list[Column] = []
    udf_columns: list[tuple[str, Column]] = []
    seeded_columns: list[tuple[str, Column]] = []

    for col_spec in table_spec.columns:
        if col_spec.name in pk_cols and isinstance(col_spec.gen, SequenceColumn):
            pk_expr = (id_col * F.lit(col_spec.gen.step) + F.lit(col_spec.gen.start)).cast("long")
            col_exprs.append(pk_expr.alias(col_spec.name))
            continue

        col_seed = derive_column_seed(s, table_name, col_spec.name)

        if col_spec.seed_from is not None:
            effective_id = F.col(col_spec.seed_from)
            expr = build_column_expr(col_spec, effective_id, col_seed, row_count, s)
            if col_spec.null_fraction > 0:
                is_null = null_mask_expr(col_seed, effective_id, col_spec.null_fraction)
                expr = F.when(is_null, F.lit(None)).otherwise(expr)
            seeded_columns.append((col_spec.name, expr.alias(col_spec.name)))
            continue

        if col_spec.foreign_key is not None:
            fk_key = (table_name, col_spec.name)
            if fk_resolutions and fk_key in fk_resolutions:
                from dbldatagen.v1.engine.fk import build_fk_column

                expr = build_fk_column(id_col, col_seed, fk_resolutions[fk_key])
                udf_columns.append((col_spec.name, expr))
            else:
                col_exprs.append(F.lit(None).alias(col_spec.name))
            continue

        expr = build_column_expr(col_spec, id_col, col_seed, row_count, s)
        if col_spec.null_fraction > 0:
            is_null = null_mask_expr(col_seed, id_col, col_spec.null_fraction)
            expr = F.when(is_null, F.lit(None)).otherwise(expr)
        col_exprs.append(expr.alias(col_spec.name))

    return col_exprs, udf_columns, seeded_columns


def _build_exprs_dynamic(
    table_spec: TableSpec,
    id_col: Column,
    wb_col: Column,
    unique_wbs: list[int],
    global_seed: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None,
    *,
    row_count: int = 0,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Build column expressions using map-based seed lookup from ``_write_batch``.

    PERFORMANCE NOTE — do not convert back to CASE WHEN:
        Instead of a CASE WHEN with N branches per column (where N can be
        365+), the column seed is precomputed for each write-batch value and
        stored in a Spark **map literal**.  At execution time,
        ``element_at(map, _write_batch)`` gives O(1) seed lookup — one
        expression node per column instead of N CASE WHEN branches.  This
        eliminates the Catalyst plan compilation bottleneck that caused the
        cluster to idle at ~10% CPU at scale.  See ``column_seed_map`` in
        seed.py for the full rationale.
    """
    table_name = table_spec.name
    pk_cols = get_pk_columns(table_spec)

    col_exprs: list[Column] = []
    udf_columns: list[tuple[str, Column]] = []
    seeded_columns: list[tuple[str, Column]] = []

    for col_spec in table_spec.columns:
        if col_spec.name in pk_cols and isinstance(col_spec.gen, SequenceColumn):
            pk_expr = (id_col * F.lit(col_spec.gen.step) + F.lit(col_spec.gen.start)).cast("long")
            col_exprs.append(pk_expr.alias(col_spec.name))
            continue

        # Build a map literal: write_batch → column_seed
        seed_map = column_seed_map(
            global_seed,
            unique_wbs,
            table_name,
            col_spec.name,
        )
        col_seed_col = column_seed_lookup(seed_map, wb_col)

        if col_spec.seed_from is not None:
            effective_id = F.col(col_spec.seed_from)
            expr = build_column_expr(
                col_spec,
                effective_id,
                col_seed_col,
                row_count,
                global_seed,
            )
            if col_spec.null_fraction > 0:
                is_null = null_mask_expr(col_seed_col, effective_id, col_spec.null_fraction)
                expr = F.when(is_null, F.lit(None)).otherwise(expr)
            seeded_columns.append((col_spec.name, expr.alias(col_spec.name)))
            continue

        if col_spec.foreign_key is not None:
            fk_key = (table_name, col_spec.name)
            if fk_resolutions and fk_key in fk_resolutions:
                from dbldatagen.v1.engine.fk import build_fk_column

                expr = build_fk_column(id_col, col_seed_col, fk_resolutions[fk_key])
                udf_columns.append((col_spec.name, expr))
            else:
                col_exprs.append(F.lit(None).alias(col_spec.name))
            continue

        expr = build_column_expr(
            col_spec,
            id_col,
            col_seed_col,
            row_count,
            global_seed,
        )
        if col_spec.null_fraction > 0:
            is_null = null_mask_expr(col_seed_col, id_col, col_spec.null_fraction)
            expr = F.when(is_null, F.lit(None)).otherwise(expr)
        col_exprs.append(expr.alias(col_spec.name))

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


def build_column_expr(  # noqa: PLR0911
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int | Column,
    row_count: int,
    global_seed: int,
    *,
    cell_seed_override: Column | None = None,
) -> Column:
    """Dispatch to the appropriate column builder based on strategy type.

    Parameters
    ----------
    column_seed :
        Per-column seed.  May be a scalar ``int`` (planning-time constant)
        or a ``Column`` (dynamic seed derived from ``_write_batch`` via
        map-based lookup).  The ``int | Column`` type is required for the
        fused multi-batch CDC path — see ``_build_exprs_dynamic`` and
        ``column_seed_map`` in seed.py for the performance rationale.
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

    if isinstance(gen, PatternColumn):
        return build_pattern_column(id_col, column_seed, gen.template)

    if isinstance(gen, SequenceColumn):
        return build_sequential_pk(id_col, start=gen.start, step=gen.step)

    if isinstance(gen, UUIDColumn):
        return build_uuid_column(id_col, column_seed)

    if isinstance(gen, ExpressionColumn):
        return build_expression_column(gen.expr)

    if isinstance(gen, TimestampColumn):
        if col_spec.dtype == DataType.DATE:
            return build_date_column(
                id_col,
                column_seed,
                gen.start,
                gen.end,
                gen.distribution,
                cell_seed_override=cell_seed_override,
            )
        return build_timestamp_column(
            id_col,
            column_seed,
            gen.start,
            gen.end,
            gen.distribution,
            cell_seed_override=cell_seed_override,
        )

    if isinstance(gen, ConstantColumn):
        return build_constant_column(gen.value)

    if isinstance(gen, StructColumn):
        return _build_struct_column(gen, id_col, column_seed, row_count, global_seed)

    if isinstance(gen, ArrayColumn):
        return _build_array_column(gen, id_col, column_seed, row_count, global_seed)

    raise ValueError(
        f"Unsupported column strategy '{gen.strategy}' for column '{col_spec.name}'. "
        f"Supported: range, values, faker, pattern, sequence, uuid, expression, "
        f"timestamp, constant, struct, array."
    )


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
        child_expr = apply_null_fraction(child_expr, child_seed, id_col, field_spec.null_fraction)
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
    from dbldatagen.v1.engine.seed import cell_seed_expr

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
