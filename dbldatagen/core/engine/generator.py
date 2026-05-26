"""Core generation engine: spark.range(N) -> select([column_exprs]) -> DataFrame.

Routes each ColumnSpec to the appropriate column builder based on its strategy,
assembles all column expressions, and executes them in a single flat ``select``
to avoid the O(n^2) plan depth of chained ``withColumn`` calls.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.numeric import build_range_column
from dbldatagen.core.engine.columns.pk import (
    build_sequential_pk,
)
from dbldatagen.core.engine.columns.string import (
    build_constant_column,
    build_expression_column,
    build_pattern_column,
    build_values_column,
)
from dbldatagen.core.engine.columns.temporal import build_date_column, build_timestamp_column
from dbldatagen.core.engine.columns.uuid import build_uuid_column
from dbldatagen.core.engine.planner import FKResolution, ResolvedPlan
from dbldatagen.core.engine.seed import GOLDEN_RATIO_HASH, derive_column_seed
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
    """Generates a single table as a Spark ``DataFrame``.

    Builds a deterministic ``DataFrame`` from the schema described by
    ``table_spec``.  When ``resolved_plan`` is supplied, FK children
    resolve to the parent metadata it carries; otherwise FK columns
    raise at materialisation.  ``table_spec.seed`` must be set --
    typically by going through ``DataGenPlan``, which propagates
    ``plan.seed`` to each table during Pydantic validation.

    Args:
        spark: Active ``SparkSession`` used to construct the underlying
          ``DataFrame``.
        table_spec: Pydantic model describing the table schema, row
          count, and per-table seed.
        resolved_plan: Optional ``ResolvedPlan`` from the planner
          carrying FK resolution info.  Must have been produced from a
          plan that contains a table with ``table_spec.name``.  Unlike
          ``generate()`` (which checks ``resolved_plan.plan is plan``),
          this helper only checks the name -- the weaker check lets
          callers that legitimately build fresh per-call ``TableSpec``
          objects still reuse a ``ResolvedPlan``.  Use ``generate()``
          when the full cross-plan guarantee is needed; passing planA's
          ``TableSpec`` with ``resolve_plan(planB)`` where both plans
          have a same-named table will NOT be caught here.

    Returns:
        A ``DataFrame`` with one row per ``table_spec.rows`` and one
        column per ``ColumnSpec`` in ``table_spec.columns``.  Output is
        deterministic given ``table_spec.seed``.

        Column order is **not** strictly the declared order: the
        engine projects columns in three phases for performance
        (regular columns first, then FK / Faker columns, then
        ``seed_from``-derived columns; declaration order is preserved
        within each phase).  Schema-level operations (``df.schema``,
        ``df.columns``, ``df.select("name")``) are unaffected.  If
        callers need byte-exact declared order in the final
        DataFrame, append a projection: ``df.select(*[c.name for c in
        table_spec.columns])``.

    Raises:
        ValueError: ``table_spec.seed`` is ``None``, or ``resolved_plan``
          does not contain a table named ``table_spec.name``.
    """
    row_count = int(table_spec.rows)
    if table_spec.seed is None:
        raise ValueError(
            f"TableSpec '{table_spec.name}'.seed is None.  Either set "
            f"it explicitly on the TableSpec or go through a "
            f"DataGenPlan (which propagates plan.seed to each table "
            f"during Pydantic validation)."
        )
    # Name-based (not identity) check: callers may legitimately
    # construct fresh per-call TableSpecs that share a name with a
    # table in the resolved plan.
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
    seed: int,
    row_count: int = 0,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Builds column expressions for every column in a table.

    Drives the three-phase application pipeline that
    ``apply_column_phases`` later assembles: Phase 1 ``col_exprs``
    project via flat ``select``, Phase 2 ``udf_columns`` apply via
    ``withColumn`` (FK, Faker), Phase 3 ``seeded_columns`` apply
    after phases 1 and 2 so the ``seed_from`` source column is
    already materialised.

    Args:
        table_spec: Table whose columns to expand.
        id_col: Row-id ``Column`` (typically
          ``F.col("_synth_row_id")``).
        fk_resolutions: Mapping of ``(table_name, column_name)`` to
          ``FKResolution`` from ``ResolvedPlan``.  ``None`` means
          no FK columns on this table.
        seed: Base seed used for default column-seed derivation and
          passed through to ``build_column_expr`` as ``global_seed``.
          Required -- callers must derive this from
          ``table_spec.seed`` so reruns do not silently
          desynchronise.
        row_count: Row count passed through to ``build_column_expr``.

    Returns:
        A three-tuple ``(col_exprs, udf_columns, seeded_columns)``
        ready to feed into ``apply_column_phases``.
    """
    table_name = table_spec.name

    def _resolver(cs: ColumnSpec) -> int:
        return derive_column_seed(seed, table_name, cs.name)

    return _build_column_exprs_loop(
        table_spec,
        id_col,
        _resolver,
        seed,
        row_count,
        fk_resolutions,
    )


def _build_column_exprs_loop(
    table_spec: TableSpec,
    id_col: Column,
    seed_resolver: Callable[[ColumnSpec], int],
    effective_global_seed: int,
    row_count: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None,
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]:
    """Unified column-building loop.

    Iterates ``table_spec.columns``, classifies each column, and
    routes to the appropriate builder.  The *seed_resolver* callable
    controls how the per-column seed is derived.

    Args:
        table_spec: Table whose columns to expand.
        id_col: Row-id ``Column``.
        seed_resolver: ``(col_spec) -> int`` returning the per-column
          seed.
        effective_global_seed: Passed through to ``build_column_expr``
          as ``global_seed``.
        row_count: Row count threaded through to per-column builders.
        fk_resolutions: Optional FK resolution map from
          ``ResolvedPlan``.

    Returns:
        A three-tuple ``(col_exprs, udf_columns, seeded_columns)``
        feeding into ``apply_column_phases``.
    """
    table_name = table_spec.name
    col_exprs: list[Column] = []
    udf_columns: list[tuple[str, Column]] = []
    seeded_columns: list[tuple[str, Column]] = []

    for col_spec in table_spec.columns:
        column_seed = seed_resolver(col_spec)

        # Defer seed_from columns to phase 3
        if col_spec.seed_from is not None:
            result = _build_seed_from_column(col_spec, column_seed, effective_global_seed, row_count)
            seeded_columns.append(result)
            continue

        # FK columns — _build_fk_column_expr raises if the resolution
        # is missing, so we never silently emit an all-NULL column.
        if col_spec.foreign_key is not None:
            udf_columns.append(
                _build_fk_column_expr(
                    col_spec,
                    table_name,
                    id_col,
                    column_seed,
                    fk_resolutions,
                )
            )
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
            effective_global_seed,
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
    if col_spec.seed_from is None:
        raise RuntimeError(
            f"_build_seed_from_column called for '{col_spec.name}' but seed_from is None -- "
            f"only columns with seed_from set should reach this helper"
        )
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
) -> tuple[str, Column]:
    """Build FK column expression; raise if resolution is missing.

    The ForeignKeyColumn strategy was introduced specifically to close
    the silent-all-NULL class of bug (commit a78597b).  Returning None
    here — which the caller previously translated into
    ``F.lit(None).alias(...)`` — reintroduced it: a direct call to
    ``generate_table`` without a ``ResolvedPlan`` carrying the FK map
    silently produced an all-NULL column instead of surfacing the
    missing resolution.  Raise a clear error that names the column and
    the expected call sequence so the failure is impossible to miss.
    """
    fk_key = (table_name, col_spec.name)
    if fk_resolutions is None or fk_key not in fk_resolutions:
        raise RuntimeError(
            f"FK column '{table_name}.{col_spec.name}' has no FKResolution — "
            f"caller must resolve the plan (via ``resolve_plan`` / ``generate``) "
            f"before reaching ``build_column_expr``.  Calling ``generate_table`` "
            f"directly requires passing a ``ResolvedPlan`` that includes this "
            f"column's FK."
        )
    from dbldatagen.core.engine.fk import build_fk_column

    fk_expr = build_fk_column(id_col, column_seed, fk_resolutions[fk_key])
    return (col_spec.name, fk_expr)


def _build_faker_expr(
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int,
) -> tuple[str, Column]:
    """Build a Faker pool UDF expression."""
    from dbldatagen.core.engine.columns.faker_pool import build_faker_column

    if not isinstance(col_spec.gen, FakerColumn):
        raise RuntimeError(
            f"_build_faker_expr called for '{col_spec.name}' with non-FakerColumn gen "
            f"{type(col_spec.gen).__name__}; dispatcher invariant bypassed"
        )
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
) -> Column | None:
    """Build a regular (non-FK, non-seed_from) column expression."""
    expr = build_column_expr(
        col_spec,
        id_col,
        column_seed,
        row_count,
        global_seed,
    )

    return apply_null_fraction(expr, column_seed, id_col, col_spec.null_fraction)


def build_column_expr(  # noqa: PLR0911
    col_spec: ColumnSpec,
    id_col: Column,
    column_seed: int,
    row_count: int,
    global_seed: int,
) -> Column:
    """Dispatches to the appropriate column builder based on strategy type.

    The single point where ``ColumnSpec.gen`` is matched to the
    corresponding builder (``build_range_column``,
    ``build_values_column``, ``build_pattern_column``,
    ``build_sequential_pk``, ``build_uuid_column``,
    ``build_expression_column``, ``build_timestamp_column`` /
    ``build_date_column``, ``build_constant_column``,
    ``_build_struct_column``, ``_build_array_column``).  ``FakerColumn``
    and ``ForeignKeyColumn`` are intentionally not handled here -- the
    former goes through a column-level pandas_udf and the latter is
    resolved against ``fk_resolutions`` at the table level.

    Args:
        col_spec: The column specification to expand.
        id_col: Row-id ``Column``.
        column_seed: Per-column seed (planning-time constant).
        row_count: Row count threaded through to builders that need
          it (e.g. ``build_uuid_column`` for clamping).
        global_seed: Plan-level seed forwarded to nested struct /
          array seed derivation.

    Returns:
        A Spark ``Column`` carrying the generated values; runtime
        type depends on the strategy.

    Raises:
        RuntimeError: ``col_spec.gen`` is ``ForeignKeyColumn``.  FK
          resolution must run earlier via ``_build_fk_column_expr``;
          reaching this dispatch means the FK short-circuit in
          ``_build_column_exprs_loop`` was bypassed.
        ValueError: ``col_spec.gen`` is an unsupported strategy.
          Includes ``FakerColumn`` (handled out-of-band by
          ``_build_faker_expr`` as a column-level pandas_udf) and
          any future strategy that lacks an ``isinstance`` branch
          above.
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
            precision=col_spec.precision,
            scale=col_spec.scale,
        )

    if isinstance(gen, ValuesColumn):
        return build_values_column(
            id_col,
            column_seed,
            gen.values,
            distribution=gen.distribution,
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
            )
        return build_timestamp_column(
            id_col,
            column_seed,
            gen.start,
            gen.end,
            gen.distribution,
        )

    if isinstance(gen, ConstantColumn):
        return build_constant_column(gen.value)

    if isinstance(gen, ForeignKeyColumn):
        # FK columns are resolved earlier via ColumnSpec.foreign_key in
        # _build_fk_column_expr. Reaching dispatch means either the column
        # had no foreign_key set (should have been caught by ColumnSpec's
        # validator) or the FK loop short-circuit was bypassed.
        raise RuntimeError(
            f"ForeignKeyColumn '{col_spec.name}' reached build_column_expr — "
            f"FK resolution must run before column-strategy dispatch. "
            f"Check that ColumnSpec.foreign_key is set and the planner has "
            f"produced an FKResolution for this column."
        )

    if isinstance(gen, StructColumn):
        return _build_struct_column(
            gen,
            id_col,
            column_seed,
            row_count,
            global_seed,
            parent_col_name=col_spec.name,
        )

    if isinstance(gen, ArrayColumn):
        return _build_array_column(gen, id_col, column_seed, row_count, global_seed)

    raise ValueError(
        f"Unsupported column strategy '{gen.strategy}' for column '{col_spec.name}'. "
        f"Supported: range, values, faker, pattern, sequence, uuid, expression, "
        f"timestamp, constant, foreign_key, struct, array."
    )


def _build_struct_column(
    gen: StructColumn,
    id_col: Column,
    parent_seed: int,
    row_count: int,
    global_seed: int,
    *,
    parent_col_name: str,
) -> Column:
    """Build a Spark struct from child ColumnSpecs (nestable).

    Child field seeds are the polynomial hash chain
    ``derive_column_seed(parent_seed, "", field_name)``, evaluated on
    the driver since ``parent_seed`` is always an int at plan time.
    Supports arbitrary nesting (struct-of-struct) via recursion.
    """
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
    from dbldatagen.core.engine.seed import cell_seed_expr

    # Generate max_length elements, each with a unique seed offset
    element_cols: list[Column] = []
    # ``elem`` (not ``_elem``): ColumnSpec rejects leading-underscore names
    # (reserved for engine-internal metadata).  This dummy spec is never
    # surfaced to the user — it just drives the per-element builder.
    dummy_spec = ColumnSpec(name="elem", gen=gen.element)
    for i in range(gen.max_length):
        elem_seed = column_seed ^ ((i + 1) * GOLDEN_RATIO_HASH)
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

    # Random length per row in [min_length, max_length].  Mix a
    # constant into ``column_seed`` before computing the cell seed so
    # the length hash is decorrelated from element[0]'s cell seed
    # (which uses unmixed ``column_seed``).  Without the mix, two
    # separate values derived from the same ``cell_seed_expr(col,
    # id)`` -- one via direct use as element[0]'s seed, one via
    # pmod-for-length -- share the low bits and produce a subtle
    # correlation between array length and the first element's value.
    from dbldatagen.core.engine.seed import to_signed64

    range_size = gen.max_length - gen.min_length + 1
    # Fibonacci hashing constant from Knuth (TAOCP Vol 3, §6.4):
    # floor(2^64 / phi) = 0x9E3779B97F4A7C15.  Any high-entropy 64-bit
    # value would decorrelate the length hash from element[0]'s seed,
    # but using the named Knuth constant makes the choice
    # self-documenting -- a reader recognises it instead of having to
    # take a "random hex" comment on faith.
    _LEN_SEED_MIX = 0x9E3779B97F4A7C15
    length_seed = to_signed64(column_seed ^ _LEN_SEED_MIX)
    seed_col = cell_seed_expr(length_seed, id_col)
    rand_len = F.pmod(seed_col, F.lit(range_size)).cast("int") + F.lit(gen.min_length)
    return F.slice(full_array, 1, rand_len)
