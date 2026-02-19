"""dbldatagen.v1.cdc -- Public API for CDC data generation.

Simplest usage::

    from dbldatagen.v1.cdc import generate_cdc

    stream = generate_cdc(spark, base_plan, num_batches=5)
    initial = stream.initial["my_table"]       # DataFrame
    batch_1 = stream.batches[0]["my_table"]    # DataFrame

For large-scale generation on clusters, use ``generate_cdc_bulk``::

    from dbldatagen.v1.cdc import generate_cdc_bulk

    stream = generate_cdc_bulk(spark, plan)
    stream.initial["my_table"].write.format("delta").save(...)
    for chunk in stream.batches:
        chunk["my_table"].write.format("delta").mode("append").save(...)

For end-to-end generation + write in one call::

    from dbldatagen.v1.cdc import write_cdc_to_delta

    write_cdc_to_delta(spark, plan, catalog="my_catalog", schema="my_schema")

All tables in the base plan participate in CDC by default.  Per-table
customisation is available via ``CDCPlan``.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import overload

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.v1.cdc_schema import BatchPlan, CDCFormat, CDCPlan
from dbldatagen.v1.engine.cdc_formats import apply_format
from dbldatagen.v1.engine.cdc_generator import (
    apply_fk_delete_guard,
    batch_timestamp,
    compute_periods_from_config,
    generate_bulk_inserts,
    generate_cdc_batch_for_table,
    generate_for_indices,
    generate_fused_deletes,
    generate_fused_updates,
    generate_initial_snapshot,
)
from dbldatagen.v1.engine.cdc_state import resolve_batch_size
from dbldatagen.v1.engine.cdc_stateless import (
    birth_tick,
    delete_indices_at_batch_fast,
    insert_range,
    is_alive,
    max_k_at_batch,
    pre_image_batch,
    update_due,
    update_indices_at_batch,
)
from dbldatagen.v1.engine.generator import generate_table
from dbldatagen.v1.engine.planner import resolve_plan
from dbldatagen.v1.engine.seed import compute_batch_seed
from dbldatagen.v1.engine.utils import union_all
from dbldatagen.v1.schema import DataGenPlan


class _LazyBatchList:
    """List-like wrapper that generates CDC batches on-demand.

    Supports indexing (``batches[5]``), iteration, and ``len()``.
    Generated batches are cached so repeated access is free.
    """

    def __init__(
        self,
        spark: SparkSession,
        plan: CDCPlan,
        fmt_name: str,
    ) -> None:
        self._spark = spark
        self._plan = plan
        self._fmt_name = fmt_name
        self._cache: dict[int, dict[str, DataFrame]] = {}

    @overload
    def __getitem__(self, index: int) -> dict[str, DataFrame]: ...

    @overload
    def __getitem__(self, index: slice) -> list[dict[str, DataFrame]]: ...

    def __getitem__(self, index: int | slice) -> dict[str, DataFrame] | list[dict[str, DataFrame]]:
        if isinstance(index, slice):
            indices = range(*index.indices(len(self)))
            return [self[i] for i in indices]
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(f"batch index {index} out of range")
        if index not in self._cache:
            batch_id = index + 1  # batches[0] → batch_id 1
            self._cache[index] = _generate_batch(
                self._spark,
                self._plan,
                batch_id,
                self._fmt_name,
            )
        return self._cache[index]

    def __iter__(self) -> Iterator[dict[str, DataFrame]]:
        for i in range(len(self)):
            yield self[i]

    def __len__(self) -> int:
        return self._plan.num_batches


class _LazyChunkList:
    """List-like wrapper that groups CDC batches into larger chunks.

    Each element is a ``dict[str, DataFrame]`` containing multiple batches
    unioned together, so one ``.write()`` call triggers one large Spark job
    instead of many tiny ones.  This lets Spark distribute work across all
    cluster nodes.

    Supports indexing, iteration, and ``len()``.
    """

    def __init__(
        self,
        spark: SparkSession,
        plan: CDCPlan,
        fmt_name: str,
        chunk_size: int,
    ) -> None:
        self._spark = spark
        self._plan = plan
        self._fmt_name = fmt_name
        self._chunk_size = chunk_size
        self._cache: dict[int, dict[str, DataFrame]] = {}

    @overload
    def __getitem__(self, index: int) -> dict[str, DataFrame]: ...

    @overload
    def __getitem__(self, index: slice) -> list[dict[str, DataFrame]]: ...

    def __getitem__(self, index: int | slice) -> dict[str, DataFrame] | list[dict[str, DataFrame]]:
        if isinstance(index, slice):
            indices = range(*index.indices(len(self)))
            return [self[i] for i in indices]
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(f"chunk index {index} out of range")
        if index not in self._cache:
            self._cache[index] = self._build_chunk(index)
        return self._cache[index]

    def __iter__(self) -> Iterator[dict[str, DataFrame]]:
        for i in range(len(self)):
            yield self[i]

    def __len__(self) -> int:
        return math.ceil(self._plan.num_batches / self._chunk_size)

    def _build_chunk(self, chunk_index: int) -> dict[str, DataFrame]:
        """Build one chunk: bulk insert fusion + per-batch updates/deletes."""
        start_batch = chunk_index * self._chunk_size + 1  # batch_ids are 1-based
        end_batch = min(start_batch + self._chunk_size, self._plan.num_batches + 1)
        batch_ids = list(range(start_batch, end_batch))

        chunk_tables: dict[str, DataFrame] = {}
        for table_name in self._plan.cdc_tables:
            chunk_df = _generate_chunk_for_table(
                self._spark,
                self._plan,
                table_name,
                batch_ids,
                self._fmt_name,
            )
            if chunk_df is not None:
                chunk_tables[table_name] = chunk_df
            else:
                table_map = {t.name: t for t in self._plan.base_plan.tables}
                resolved = resolve_plan(self._plan.base_plan)
                chunk_tables[table_name] = generate_table(
                    self._spark,
                    table_map[table_name],
                    resolved,
                ).limit(0)

        return chunk_tables


@dataclass
class CDCStream:
    """Complete CDC stream: initial snapshot + batches of changes.

    Attributes
    ----------
    initial : dict[str, DataFrame]
        Full table snapshots at batch 0 (all ops = insert).
    batches :
        Lazy list of batch dicts ``{table_name: DataFrame}``.
        Supports indexing, iteration, and ``len()``.
        Batches are generated on first access and cached.
    plan : CDCPlan
        The plan used to generate this stream.
    """

    initial: dict[str, DataFrame] = field(default_factory=dict)
    batches: _LazyBatchList | _LazyChunkList | list[dict[str, DataFrame]] = field(default_factory=list)
    plan: CDCPlan | None = None


def generate_cdc(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None = None,
    fmt: str | CDCFormat | None = None,
) -> CDCStream:
    """Generate a complete CDC stream.

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        Either a ``CDCPlan`` (full config) or a ``DataGenPlan`` (uses
        defaults for all CDC settings).
    num_batches :
        Number of change batches.  Overrides ``plan.num_batches`` if
        the first argument is a ``CDCPlan``.  Required if passing a
        ``DataGenPlan``.
    fmt :
        Output format.  One of 'raw', 'delta_cdf', 'sql_server',
        'debezium'.  Defaults to 'raw'.

    Returns
    -------
    CDCStream with initial snapshot and lazy per-batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, fmt)

    fmt_name = plan.format.value

    initial_raw = generate_initial_snapshot(spark, plan)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _LazyBatchList(spark, plan, fmt_name)
    return CDCStream(initial=initial, batches=batches, plan=plan)


def generate_cdc_bulk(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None = None,
    fmt: str | CDCFormat | None = None,
    chunk_size: int | None = None,
) -> CDCStream:
    """Generate a CDC stream optimised for large-scale cluster execution.

    Like ``generate_cdc()``, but batches are grouped into chunks so each
    Spark job processes millions of rows instead of thousands.  This
    allows Spark to utilise all cluster nodes via its native parallelism.

    Usage is identical to ``generate_cdc()``::

        stream = generate_cdc_bulk(spark, plan)
        stream.initial["table"].write.format("delta").save(...)
        for chunk in stream.batches:
            chunk["table"].write.format("delta").mode("append").save(...)

    Each iteration of the loop yields a dict of DataFrames containing
    multiple logical batches unioned together.  The ``_batch_id`` /
    ``_commit_version`` column still distinguishes individual batches
    within each chunk.

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        Either a ``CDCPlan`` or a ``DataGenPlan``.
    num_batches :
        Override for ``plan.num_batches``.
    fmt :
        Output format override.
    chunk_size :
        Number of logical batches per chunk.  Auto-calculated if None
        (targets ~20M rows per chunk for good cluster utilisation).

    Returns
    -------
    CDCStream with initial snapshot and chunked batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, fmt)

    fmt_name = plan.format.value

    if chunk_size is None:
        chunk_size = _auto_chunk_size(plan)

    initial_raw = generate_initial_snapshot(spark, plan)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _LazyChunkList(spark, plan, fmt_name, chunk_size)
    return CDCStream(initial=initial, batches=batches, plan=plan)


def generate_cdc_batch(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    batch_id: int,
    fmt: str | CDCFormat | None = None,
) -> dict[str, DataFrame]:
    """Generate a single CDC batch independently.

    Useful for generating batch N without materialising batches 1..N-1.
    The state is replayed via metadata, not DataFrames.
    """
    if isinstance(plan_or_base, DataGenPlan):
        plan = CDCPlan(base_plan=plan_or_base)
    else:
        plan = plan_or_base

    if fmt is not None:
        resolved_fmt = CDCFormat(fmt) if isinstance(fmt, str) else fmt
        plan = plan.model_copy(update={"format": resolved_fmt})

    fmt_name = plan.format.value
    return _generate_batch(spark, plan, batch_id, fmt_name)


def generate_expected_state(
    spark: SparkSession,
    plan_or_stream: CDCPlan | CDCStream | DataGenPlan,
    table_name: str,
    batch_id: int,
) -> DataFrame:
    """Generate the expected table state at a given batch.

    Returns a DataFrame representing all live rows at ``batch_id``,
    with their current column values.  Useful for verifying that a
    downstream pipeline correctly applied all CDC changes.

    Uses the stateless modular-recurrence model: each row's lifecycle
    is computed from its index k and the batch number, with no
    driver-side state needed.
    """
    if isinstance(plan_or_stream, CDCStream):
        assert plan_or_stream.plan is not None, "CDCStream.plan must be set"
        plan = plan_or_stream.plan
    elif isinstance(plan_or_stream, DataGenPlan):
        plan = CDCPlan(base_plan=plan_or_stream)
    else:
        plan = plan_or_stream

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    config = plan.config_for(table_name)
    global_seed = plan.base_plan.seed
    initial_rows = int(table_spec.rows)

    # Apply FK parent delete guard
    config = apply_fk_delete_guard(plan, table_name, config)

    resolved = resolve_plan(plan.base_plan)

    batch_size = resolve_batch_size(config.batch_size, initial_rows)
    periods = compute_periods_from_config(initial_rows, batch_size, config)

    upper_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)
    live_indices = [
        k
        for k in range(upper_k)
        if is_alive(
            k,
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.death_period,
            min_life=config.min_life,
        )
    ]

    if len(live_indices) == 0:
        full = generate_table(spark, table_spec, resolved)
        return full.limit(0)

    batch_groups: dict[int, list[int]] = {}
    for k in live_indices:
        if update_due(
            k,
            batch_id,
            initial_rows,
            periods.inserts_per_batch,
            periods.death_period,
            update_period=periods.update_period,
            min_life=config.min_life,
            update_window=config.update_window,
        ):
            batch_groups.setdefault(batch_id, []).append(k)
        else:
            t_birth = birth_tick(k, initial_rows, periods.inserts_per_batch)
            if math.isinf(periods.update_period) or int(periods.update_period) <= 0:
                batch_groups.setdefault(t_birth, []).append(k)
            else:
                up = int(periods.update_period)
                remainder = (batch_id + k) % up
                last_update = batch_id - remainder
                if last_update <= t_birth:
                    batch_groups.setdefault(t_birth, []).append(k)
                else:
                    batch_groups.setdefault(last_update, []).append(k)

    dfs = []
    for write_batch, indices in batch_groups.items():
        seed = compute_batch_seed(global_seed, write_batch)
        df = generate_for_indices(spark, table_spec, resolved, indices, seed)
        dfs.append(df)

    return union_all(dfs)


def _generate_batch(
    spark: SparkSession,
    plan: CDCPlan,
    batch_id: int,
    fmt_name: str,
) -> dict[str, DataFrame]:
    """Generate one batch for all CDC tables, applying the output format."""
    resolved = resolve_plan(plan.base_plan)
    batch_tables: dict[str, DataFrame] = {}

    for table_name in plan.cdc_tables:
        result = generate_cdc_batch_for_table(spark, plan, table_name, batch_id)
        combined = result.to_dataframe()

        if combined is not None:
            batch_tables[table_name] = apply_format(combined, fmt_name)
        else:
            # Empty batch — can happen when min_life delays deletes
            table_map = {t.name: t for t in plan.base_plan.tables}
            empty = generate_table(spark, table_map[table_name], resolved).limit(0)
            empty = (
                empty.withColumn("_op", F.lit("I"))
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_ts", F.lit("1970-01-01 00:00:00").cast("timestamp"))
            )
            batch_tables[table_name] = apply_format(empty, fmt_name)

    return batch_tables


def _normalize_plan(
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None,
    fmt: str | CDCFormat | None,
) -> CDCPlan:
    """Normalise input to a CDCPlan, applying overrides."""
    if isinstance(plan_or_base, DataGenPlan):
        kwargs: dict = {"base_plan": plan_or_base}
        if num_batches is not None:
            kwargs["num_batches"] = num_batches
        if fmt is not None:
            kwargs["format"] = CDCFormat(fmt) if isinstance(fmt, str) else fmt
        return CDCPlan(**kwargs)
    else:
        plan = plan_or_base
        if num_batches is not None:
            plan = plan.model_copy(update={"num_batches": num_batches})
        if fmt is not None:
            resolved_fmt = CDCFormat(fmt) if isinstance(fmt, str) else fmt
            plan = plan.model_copy(update={"format": resolved_fmt})
        return plan


def _auto_chunk_size(plan: CDCPlan) -> int:
    """Pick chunk_size so each chunk has ~20M rows."""
    target_rows = 20_000_000
    max_rows_per_batch = 1
    for table_name in plan.cdc_tables:
        config = plan.config_for(table_name)
        table_spec = next(t for t in plan.base_plan.tables if t.name == table_name)
        batch_size = resolve_batch_size(config.batch_size, int(table_spec.rows))
        _ins_frac, upd_frac, _del_frac = config.operations.fractions
        # Each batch produces: batch_size + update_before_images
        rows = batch_size + int(batch_size * upd_frac)
        max_rows_per_batch = max(max_rows_per_batch, rows)
    chunk = max(1, target_rows // max(max_rows_per_batch, 1))
    return min(chunk, plan.num_batches)


def _generate_chunk_for_table(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_ids: list[int],
    fmt_name: str,
) -> DataFrame | None:
    """Generate all CDC rows for one table across multiple batches.

    Uses THREE fused DataFrames (one spark.range each) instead of
    per-batch generation, so Spark sees 3 scans instead of 48:

    1. Bulk insert fusion (ONE spark.range for all inserts)
    2. Fused deletes (ONE spark.range filtered by death_tick IN batch_ids)
    3. Fused updates (ONE spark.range filtered by update_due for any batch)

    Falls back to per-batch generation for tables with Faker columns.
    """

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    config = plan.config_for(table_name)
    global_seed = plan.base_plan.seed
    initial_rows = int(table_spec.rows)
    resolved = resolve_plan(plan.base_plan)

    # Apply FK parent delete guard
    effective_config = apply_fk_delete_guard(plan, table_name, config)

    batch_size = resolve_batch_size(effective_config.batch_size, initial_rows)
    periods = compute_periods_from_config(initial_rows, batch_size, effective_config)

    # --- Bulk insert fusion (driver-side is cheap: just start_k arithmetic) ---
    insert_infos = []
    for batch_id in batch_ids:
        start_k, end_k = insert_range(batch_id, initial_rows, periods.inserts_per_batch)
        insert_count = end_k - start_k
        if insert_count > 0:
            insert_infos.append((batch_id, start_k, insert_count))

    insert_df = None
    if insert_infos:
        insert_df = generate_bulk_inserts(
            spark,
            table_spec,
            resolved,
            insert_infos,
            global_seed,
            plan=plan,
        )

    if insert_df is None and insert_infos:
        # Faker fallback: per-batch approach for entire chunk
        return _generate_chunk_per_batch(
            spark,
            plan,
            table_name,
            batch_ids,
            fmt_name,
        )

    parts: list[DataFrame] = []
    if insert_df is not None:
        parts.append(apply_format(insert_df, fmt_name))

    # --- Fused deletes: ONE spark.range for all batch_ids ---
    del_df = generate_fused_deletes(
        spark,
        table_spec,
        resolved,
        periods,
        batch_ids,
        global_seed=global_seed,
        initial_rows=initial_rows,
        min_life=effective_config.min_life,
        plan=plan,
    )
    if del_df is not None:
        parts.append(apply_format(del_df, fmt_name))

    # --- Fused updates: ONE spark.range for before + after ---
    ub_df, ua_df = generate_fused_updates(
        spark,
        table_spec,
        resolved,
        periods,
        batch_ids,
        global_seed=global_seed,
        initial_rows=initial_rows,
        min_life=effective_config.min_life,
        plan=plan,
        update_window=effective_config.update_window,
    )
    if ub_df is not None:
        parts.append(apply_format(ub_df, fmt_name))
    if ua_df is not None:
        parts.append(apply_format(ua_df, fmt_name))

    if not parts:
        return None

    return union_all(parts)


def _generate_chunk_per_batch(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_ids: list[int],
    fmt_name: str,
) -> DataFrame | None:
    """Fallback: generate chunk using per-batch approach (for Faker tables)."""
    parts: list[DataFrame] = []
    for batch_id in batch_ids:
        batch_df = _generate_single_batch_for_table(
            spark,
            plan,
            table_name,
            batch_id,
            fmt_name,
        )
        if batch_df is not None:
            parts.append(batch_df)

    if not parts:
        return None

    return union_all(parts)


def _generate_single_batch_for_table(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_id: int,
    fmt_name: str,
) -> DataFrame | None:
    """Generate one batch for one table, formatted, as a single DataFrame."""
    result = generate_cdc_batch_for_table(spark, plan, table_name, batch_id)
    combined = result.to_dataframe()
    if combined is None:
        return None
    return apply_format(combined, fmt_name)


# ---------------------------------------------------------------------------
# Pre-computation API for large-scale CDC
# ---------------------------------------------------------------------------


def precompute_cdc_plans(
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None = None,
    fmt: str | CDCFormat | None = None,
) -> dict[str, list[BatchPlan]]:
    """Pre-compute all batch plans for a CDC stream (pure Python, no Spark).

    Uses stateless modular-recurrence functions to compute row indices
    for each batch.  O(1) per row, no driver-side state required.

    Parameters
    ----------
    plan_or_base :
        A ``CDCPlan`` or ``DataGenPlan``.
    num_batches :
        Override for ``plan.num_batches``.
    fmt :
        Output format override (only affects timestamp computation).

    Returns
    -------
    dict mapping table_name -> list of BatchPlan (one per batch).
    """
    plan = _normalize_plan(plan_or_base, num_batches, fmt)
    table_map = {t.name: t for t in plan.base_plan.tables}
    all_plans: dict[str, list[BatchPlan]] = {}

    for table_name in plan.cdc_tables:
        table_spec = table_map[table_name]
        config = plan.config_for(table_name)
        initial_rows = int(table_spec.rows)

        # Apply FK parent delete guard (must match generation path)
        config = apply_fk_delete_guard(plan, table_name, config)

        batch_size = resolve_batch_size(config.batch_size, initial_rows)
        periods = compute_periods_from_config(initial_rows, batch_size, config)

        batch_plans: list[BatchPlan] = []
        for batch_id in range(1, plan.num_batches + 1):
            ts = batch_timestamp(plan, batch_id)

            start_k, end_k = insert_range(batch_id, initial_rows, periods.inserts_per_batch)
            ins_count = end_k - start_k

            upd_indices = update_indices_at_batch(
                batch_id,
                initial_rows,
                periods.inserts_per_batch,
                periods.death_period,
                periods.update_period,
                min_life=config.min_life,
                update_window=config.update_window,
            )
            del_indices = delete_indices_at_batch_fast(
                batch_id,
                initial_rows,
                periods.inserts_per_batch,
                periods.death_period,
                config.min_life,
            )

            row_last_write: dict[int, int] = {}
            for k in upd_indices:
                row_last_write[k] = pre_image_batch(
                    k,
                    batch_id,
                    initial_rows,
                    periods.inserts_per_batch,
                    periods.update_period,
                )
            for k in del_indices:
                row_last_write[k] = pre_image_batch(
                    k,
                    batch_id,
                    initial_rows,
                    periods.inserts_per_batch,
                    periods.update_period,
                )

            batch_plans.append(
                BatchPlan(
                    batch_id=batch_id,
                    table_name=table_name,
                    update_indices=np.array(upd_indices, dtype=np.int64),
                    delete_indices=np.array(del_indices, dtype=np.int64),
                    insert_count=ins_count,
                    insert_start_index=start_k,
                    row_last_write=row_last_write,
                    batch_size=batch_size,
                    timestamp=ts,
                )
            )

        all_plans[table_name] = batch_plans

    return all_plans


# ---------------------------------------------------------------------------
# End-to-end write API
# ---------------------------------------------------------------------------


def write_cdc_to_delta(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    *,
    catalog: str,
    schema: str,
    num_batches: int | None = None,
    fmt: str | CDCFormat | None = None,
    chunk_size: int | None = None,
) -> dict[str, str]:
    """Generate a CDC stream and write it to Delta tables in one call.

    Each table in the plan is written to ``{catalog}.{schema}.{table_name}``.
    The initial snapshot is written as version 0, then each batch (or chunk)
    is appended as a separate Delta version.

    CDC metadata columns (e.g. ``__$operation``) are automatically renamed
    to clean, SQL-friendly names (e.g. ``cdc_operation``).

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        A ``CDCPlan`` or ``DataGenPlan``.
    catalog :
        Unity Catalog catalog name.
    schema :
        Unity Catalog schema name.
    num_batches :
        Override for ``plan.num_batches``.
    fmt :
        Output format override.
    chunk_size :
        Batches per Delta version.  Default ``None`` = auto-calculate
        targeting ~20M rows per chunk for good cluster utilisation.
        Use ``1`` for single-batch versions (best for CDC verification).

    Returns
    -------
    dict mapping table_name -> fully qualified UC table path.
    """
    plan = _normalize_plan(plan_or_base, num_batches, fmt)
    stream = generate_cdc_bulk(spark, plan, chunk_size=chunk_size)

    uc_tables: dict[str, str] = {}
    for table_name in plan.cdc_tables:
        uc_table = f"{catalog}.{schema}.{table_name}"
        uc_tables[table_name] = uc_table

        # Initial snapshot
        spark.sql(f"DROP TABLE IF EXISTS {uc_table}")
        initial_df = stream.initial[table_name]
        (initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(uc_table))

        print(f"{table_name}: v0 -> {uc_table}")

    # Batches (chunked)
    n = len(stream.batches)
    for chunk_idx, chunk in enumerate(stream.batches):
        for table_name in plan.cdc_tables:
            if table_name in chunk:
                (chunk[table_name].write.format("delta").mode("append").saveAsTable(uc_tables[table_name]))
        print(f"  chunk {chunk_idx + 1}/{n} written")

    print(f"Done! {n + 1} Delta versions per table.")
    return uc_tables
