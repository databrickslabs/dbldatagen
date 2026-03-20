"""dbldatagen.v1.ingest -- Public API for full-row ingestion simulation.

Simplest usage::

    from dbldatagen.v1.ingest import generate_ingest

    stream = generate_ingest(spark, base_plan, num_batches=5)
    initial = stream.initial["my_table"]        # DataFrame
    batch_1 = stream.batches[0]["my_table"]     # DataFrame

For snapshot mode (all live rows per batch)::

    stream = generate_ingest(spark, plan, mode="snapshot")
    snapshot_5 = stream.batches[4]["my_table"]  # Full table at batch 5

For end-to-end generation + write::

    from dbldatagen.v1.ingest import write_ingest_to_delta

    write_ingest_to_delta(spark, plan, catalog="my_catalog", schema="my_schema")

Change detection between two snapshots::

    from dbldatagen.v1.ingest import detect_changes

    changes = detect_changes(spark, snapshot_t0, snapshot_t1, ["txn_id"])
    changes["inserts"].show()
    changes["updates"].show()
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from dbldatagen.v1.engine.ingest_generator import (
    detect_changes as _detect_changes,
)
from dbldatagen.v1.engine.ingest_generator import (
    generate_initial_snapshot,
    generate_stateless_incremental_batch,
    generate_stateless_snapshot_batch,
    generate_synthetic_incremental_batch,
    generate_synthetic_snapshot_batch,
)
from dbldatagen.v1.engine.utils import _LazyList, union_all
from dbldatagen.v1.ingest_schema import (
    IngestMode,
    IngestPlan,
    IngestStrategy,
)
from dbldatagen.v1.schema import DataGenPlan


# ---------------------------------------------------------------------------
# Lazy batch list
# ---------------------------------------------------------------------------


def _make_lazy_ingest_batch_list(
    spark: SparkSession,
    plan: IngestPlan,
) -> _LazyList[dict[str, DataFrame]]:
    """Create a lazy list that generates ingest batches on-demand."""

    def _gen(index: int) -> dict[str, DataFrame]:
        return _generate_ingest_batch(spark, plan, index + 1)

    return _LazyList(plan.num_batches, _gen)


# ---------------------------------------------------------------------------
# IngestStream result
# ---------------------------------------------------------------------------


@dataclass
class IngestStream:
    """Complete ingest stream: initial snapshot + batches.

    Attributes
    ----------
    initial : dict[str, DataFrame]
        Full table snapshots at batch 0.
    batches :
        Lazy list of batch dicts ``{table_name: DataFrame}``.
    plan : IngestPlan | None
        The plan used to generate this stream.
    """

    initial: dict[str, DataFrame] = field(default_factory=dict)
    batches: _LazyList[dict[str, DataFrame]] | list[dict[str, DataFrame]] = field(default_factory=list)
    plan: IngestPlan | None = None


# ---------------------------------------------------------------------------
# Internal batch generation dispatch
# ---------------------------------------------------------------------------


def _generate_ingest_batch(
    spark: SparkSession,
    plan: IngestPlan,
    batch_id: int,
) -> dict[str, DataFrame]:
    """Generate one ingest batch, dispatching on mode and strategy."""
    if plan.strategy == IngestStrategy.SYNTHETIC:
        if plan.mode == IngestMode.INCREMENTAL:
            return generate_synthetic_incremental_batch(spark, plan, batch_id)
        else:
            return generate_synthetic_snapshot_batch(spark, plan, batch_id)
    elif plan.strategy == IngestStrategy.STATELESS:
        result: dict[str, DataFrame] = {}
        for table_name in plan.ingest_tables:
            if plan.mode == IngestMode.INCREMENTAL:
                result.update(
                    generate_stateless_incremental_batch(
                        spark,
                        plan,
                        table_name,
                        batch_id,
                    )
                )
            else:
                result.update(
                    generate_stateless_snapshot_batch(
                        spark,
                        plan,
                        table_name,
                        batch_id,
                    )
                )
        return result
    else:
        raise ValueError(
            "Delta strategy requires explicit current_df; "
            "use generate_ingest_batch() with strategy='synthetic' "
            "or use the delta API directly via engine.ingest_generator"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_ingest(
    spark: SparkSession,
    plan_or_base: IngestPlan | DataGenPlan,
    *,
    num_batches: int | None = None,
    mode: str | IngestMode | None = None,
    strategy: str | IngestStrategy | None = None,
) -> IngestStream:
    """Generate a complete ingest stream.

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        Either an ``IngestPlan`` (full config) or a ``DataGenPlan``
        (uses defaults for all ingest settings).
    num_batches :
        Number of change batches. Overrides ``plan.num_batches``.
    mode :
        Output mode: 'incremental' or 'snapshot'.
    strategy :
        Generation strategy: 'synthetic' or 'delta'.

    Returns
    -------
    IngestStream with initial snapshot and lazy per-batch access.
    """
    plan = _normalize_plan(plan_or_base, num_batches, mode, strategy)

    initial = generate_initial_snapshot(spark, plan)
    batches = _make_lazy_ingest_batch_list(spark, plan)
    return IngestStream(initial=initial, batches=batches, plan=plan)


def generate_ingest_batch(
    spark: SparkSession,
    plan_or_base: IngestPlan | DataGenPlan,
    batch_id: int,
    *,
    mode: str | IngestMode | None = None,
) -> dict[str, DataFrame]:
    """Generate a single ingest batch independently.

    Useful for generating batch N without materialising batches 1..N-1
    (for synthetic strategy only).
    """
    plan = _normalize_plan(plan_or_base, None, mode, None)
    return _generate_ingest_batch(spark, plan, batch_id)


def write_ingest_to_delta(
    spark: SparkSession,
    plan_or_base: IngestPlan | DataGenPlan,
    *,
    catalog: str,
    schema: str,
    num_batches: int | None = None,
    mode: str | IngestMode | None = None,
    strategy: str | IngestStrategy | None = None,
    chunk_size: int | None = None,
) -> dict[str, str]:
    """Generate an ingest stream and write it to Delta tables.

    Each table is written to ``{catalog}.{schema}.{table_name}``.
    The initial snapshot is written as version 0, then each batch
    is appended as a separate Delta version.

    Parameters
    ----------
    spark :
        Active SparkSession.
    plan_or_base :
        An ``IngestPlan`` or ``DataGenPlan``.
    catalog :
        Unity Catalog catalog name.
    schema :
        Unity Catalog schema name.
    num_batches :
        Override for ``plan.num_batches``.
    mode :
        Output mode override.
    strategy :
        Generation strategy override.
    chunk_size :
        Batches per Delta version. Default = 1 (one version per batch).

    Returns
    -------
    dict mapping table_name -> fully qualified UC table path.
    """
    plan = _normalize_plan(plan_or_base, num_batches, mode, strategy)
    stream = generate_ingest(spark, plan)
    chunk_size = chunk_size or 1

    uc_tables: dict[str, str] = {}
    for table_name in plan.ingest_tables:
        uc_table = f"{catalog}.{schema}.{table_name}"
        uc_tables[table_name] = uc_table

        # Initial snapshot
        spark.sql(f"DROP TABLE IF EXISTS {uc_table}")
        initial_df = stream.initial[table_name]
        (initial_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(uc_table))
        print(f"{table_name}: v0 -> {uc_table}")

    # Batches
    n = len(stream.batches)
    total_chunks = -(-n // chunk_size)
    for i in range(0, n, chunk_size):
        chunk_idx = i // chunk_size + 1
        batch_dfs: dict[str, list[DataFrame]] = {t: [] for t in plan.ingest_tables}
        for j in range(i, min(i + chunk_size, n)):
            batch = stream.batches[j]
            for table_name in plan.ingest_tables:
                if table_name in batch:
                    batch_dfs[table_name].append(batch[table_name])

        for table_name in plan.ingest_tables:
            parts = batch_dfs[table_name]
            if parts:
                (union_all(parts).write.format("delta").mode("append").saveAsTable(uc_tables[table_name]))

        print(f"  chunk {chunk_idx}/{total_chunks} written")

    print(f"Done! {n + 1} Delta versions per table.")
    return uc_tables


def detect_changes(
    spark: SparkSession,
    before_df: DataFrame,
    after_df: DataFrame,
    key_columns: list[str],
    data_columns: list[str] | None = None,
) -> dict[str, DataFrame]:
    """Identify inserts, updates, deletes between two snapshots.

    Parameters
    ----------
    spark : SparkSession
    before_df : DataFrame
        Snapshot at time T.
    after_df : DataFrame
        Snapshot at time T+1.
    key_columns : list[str]
        Primary key column names.
    data_columns : list[str] | None
        Columns to compare for change detection.

    Returns
    -------
    dict with keys: 'inserts', 'updates', 'deletes', 'unchanged'.
    """
    return _detect_changes(spark, before_df, after_df, key_columns, data_columns)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_plan(
    plan_or_base: IngestPlan | DataGenPlan,
    num_batches: int | None,
    mode: str | IngestMode | None,
    strategy: str | IngestStrategy | None,
) -> IngestPlan:
    """Normalise input to an IngestPlan, applying overrides."""
    if isinstance(plan_or_base, DataGenPlan):
        kwargs: dict = {"base_plan": plan_or_base}
        if num_batches is not None:
            kwargs["num_batches"] = num_batches
        if mode is not None:
            kwargs["mode"] = IngestMode(mode) if isinstance(mode, str) else mode
        if strategy is not None:
            kwargs["strategy"] = IngestStrategy(strategy) if isinstance(strategy, str) else strategy
        return IngestPlan(**kwargs)
    else:
        plan = plan_or_base
        updates: dict[str, object] = {}
        if num_batches is not None:
            updates["num_batches"] = num_batches
        if mode is not None:
            updates["mode"] = IngestMode(mode) if isinstance(mode, str) else mode
        if strategy is not None:
            updates["strategy"] = IngestStrategy(strategy) if isinstance(strategy, str) else strategy
        if updates:
            plan = plan.model_copy(update=updates)
        return plan
