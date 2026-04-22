"""Public CDC generation API: generate_cdc, generate_cdc_bulk, generate_cdc_batch, write_cdc_to_delta."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbldatagen.core.engine.cdc._common import CDCStream, _auto_chunk_size, _normalize_plan
from dbldatagen.core.engine.cdc.chunking import _generate_chunk_for_table
from dbldatagen.core.engine.cdc_formats import apply_format
from dbldatagen.core.engine.cdc_generator import (
    generate_cdc_batch_for_table,
    generate_initial_snapshot,
)
from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import resolve_plan
from dbldatagen.core.engine.utils import _LazyList
from dbldatagen.core.spec.cdc_schema import CDCFormat, CDCPlan
from dbldatagen.core.spec.schema import DataGenPlan


def _make_lazy_batch_list(
    spark: SparkSession,
    plan: CDCPlan,
    fmt_name: str,
) -> _LazyList[dict[str, DataFrame]]:
    """Create a lazy list that generates CDC batches on-demand."""

    def _gen(index: int) -> dict[str, DataFrame]:
        return _generate_batch(spark, plan, index + 1, fmt_name)

    return _LazyList(plan.num_batches, _gen)


def _make_lazy_chunk_list(
    spark: SparkSession,
    plan: CDCPlan,
    fmt_name: str,
    chunk_size: int,
) -> _LazyList[dict[str, DataFrame]]:
    """Create a lazy list that groups CDC batches into larger chunks."""
    num_chunks = -(-plan.num_batches // chunk_size)  # ceil division

    def _build_chunk(chunk_index: int) -> dict[str, DataFrame]:
        """Build one chunk: bulk insert fusion + per-batch updates/deletes."""
        start_batch = chunk_index * chunk_size + 1  # batch_ids are 1-based
        end_batch = min(start_batch + chunk_size, plan.num_batches + 1)
        batch_ids = list(range(start_batch, end_batch))

        chunk_tables: dict[str, DataFrame] = {}
        for table_name in plan.cdc_tables:
            chunk_df = _generate_chunk_for_table(
                spark,
                plan,
                table_name,
                batch_ids,
                fmt_name,
            )
            if chunk_df is not None:
                chunk_tables[table_name] = chunk_df
            else:
                table_map = {t.name: t for t in plan.base_plan.tables}
                resolved = resolve_plan(plan.base_plan)
                chunk_tables[table_name] = generate_table(
                    spark,
                    table_map[table_name],
                    resolved,
                ).limit(0)

        return chunk_tables

    return _LazyList(num_chunks, _build_chunk)


def generate_cdc(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None = None,
    format: str | CDCFormat | None = None,
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
    format :
        Output format.  One of 'raw', 'delta_cdf', 'sql_server',
        'debezium'.  Defaults to 'raw'.

    Returns
    -------
    CDCStream with initial snapshot and lazy per-batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, format)

    fmt_name = plan.format.value

    initial_raw = generate_initial_snapshot(spark, plan)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _make_lazy_batch_list(spark, plan, fmt_name)
    return CDCStream(initial=initial, batches=batches, plan=plan)


def generate_cdc_bulk(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    num_batches: int | None = None,
    format: str | CDCFormat | None = None,
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
    format :
        Output format override.
    chunk_size :
        Number of logical batches per chunk.  Auto-calculated if None
        (targets ~20M rows per chunk for good cluster utilisation).

    Returns
    -------
    CDCStream with initial snapshot and chunked batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, format)

    fmt_name = plan.format.value

    if chunk_size is None:
        chunk_size = _auto_chunk_size(plan)

    initial_raw = generate_initial_snapshot(spark, plan)
    initial = {name: apply_format(df, fmt_name) for name, df in initial_raw.items()}

    batches = _make_lazy_chunk_list(spark, plan, fmt_name, chunk_size)
    return CDCStream(initial=initial, batches=batches, plan=plan)


def generate_cdc_batch(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    batch_id: int,
    format: str | CDCFormat | None = None,
) -> dict[str, DataFrame]:
    """Generate a single CDC batch independently.

    Useful for generating batch N without materialising batches 1..N-1.
    The state is replayed via metadata, not DataFrames.
    """
    if isinstance(plan_or_base, DataGenPlan):
        plan = CDCPlan(base_plan=plan_or_base)
    else:
        plan = plan_or_base

    if format is not None:
        fmt = CDCFormat(format) if isinstance(format, str) else format
        plan = plan.model_copy(update={"format": fmt})

    fmt_name = plan.format.value
    return _generate_batch(spark, plan, batch_id, fmt_name)


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
        result = generate_cdc_batch_for_table(spark, plan, table_name, batch_id, resolved_plan=resolved)
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


def write_cdc_to_delta(
    spark: SparkSession,
    plan_or_base: CDCPlan | DataGenPlan,
    *,
    catalog: str,
    schema: str,
    num_batches: int | None = None,
    format: str | CDCFormat | None = None,
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
    format :
        Output format override.
    chunk_size :
        Batches per Delta version.  Default ``None`` = auto-calculate
        targeting ~20M rows per chunk for good cluster utilisation.
        Use ``1`` for single-batch versions (best for CDC verification).

    Returns
    -------
    dict mapping table_name -> fully qualified UC table path.
    """
    plan = _normalize_plan(plan_or_base, num_batches, format)
    stream = generate_cdc_bulk(spark, plan, chunk_size=chunk_size)

    uc_tables: dict[str, str] = {}
    for table_name in plan.cdc_tables:
        uc_table = f"`{catalog}`.`{schema}`.`{table_name}`"
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
