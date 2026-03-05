"""Performance benchmarks for CDC batch generation: native vs legacy path.

Measures wall-clock time for CDC batch generation across three dataset
sizes.  The native path pushes row filtering and pre-image computation
to Spark; the legacy path does it on the driver via Python loops.

Run with::

    pytest tests/test_cdc_perf.py -v -s              # all 3 sizes
    pytest tests/test_cdc_perf.py -v -s -k small      # just small
    pytest tests/test_cdc_perf.py -v -s -k medium      # just medium
    pytest tests/test_cdc_perf.py -v -s -k large       # just large

The ``-s`` flag is important so timing output is not captured by pytest.
"""

from __future__ import annotations

import time

import pytest
from pyspark.sql import SparkSession

from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights
from dbldatagen.v1.engine.cdc_generator import (
    RawBatchResult,
    _generate_delete_stream,
    _generate_delete_stream_native,
    _generate_insert_stream,
    _generate_update_after_stream,
    _generate_update_after_stream_native,
    _generate_update_before_stream,
    _generate_update_before_stream_native,
    batch_timestamp,
    compute_periods_from_config,
)
from dbldatagen.v1.engine.cdc_state import resolve_batch_size
from dbldatagen.v1.engine.cdc_stateless import (
    delete_indices_at_batch_fast,
    update_indices_at_batch,
)
from dbldatagen.v1.engine.planner import resolve_plan
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    TimestampColumn,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clearing_plan(initial_rows: int, batch_size: int, seed: int = 2024) -> CDCPlan:
    """Build a Mastercard-style clearing table plan at the given scale."""
    base = DataGenPlan(
        seed=seed,
        tables=[
            TableSpec(
                name="clearing",
                rows=initial_rows,
                primary_key=PrimaryKey(columns=["clear_id"]),
                columns=[
                    ColumnSpec(
                        name="clear_id",
                        dtype=DataType.LONG,
                        gen=SequenceColumn(start=1, step=1),
                    ),
                    ColumnSpec(
                        name="dw_sequence_nbr",
                        dtype=DataType.STRING,
                        gen=PatternColumn(template="CL{digit:12}"),
                    ),
                    ColumnSpec(
                        name="clearing_date",
                        dtype=DataType.TIMESTAMP,
                        gen=TimestampColumn(start="2025-01-01", end="2025-12-31"),
                    ),
                    ColumnSpec(
                        name="clearing_amount",
                        dtype=DataType.DOUBLE,
                        gen=RangeColumn(min=0.01, max=25000.0),
                    ),
                    ColumnSpec(
                        name="currency",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["USD", "EUR", "GBP", "JPY"]),
                    ),
                    ColumnSpec(
                        name="status",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["cleared", "pending", "rejected"]),
                    ),
                ],
            ),
        ],
    )
    return CDCPlan(
        base_plan=base,
        num_batches=10,
        table_configs={
            "clearing": CDCTableConfig(
                batch_size=batch_size,
                operations=OperationWeights(insert=6, update=3, delete=1),
                min_life=1,
            ),
        },
    )


def _force_materialize(result: RawBatchResult) -> int:
    """Force Spark to materialize all DataFrames; return total row count."""
    total = 0
    for df in (result.inserts, result.updates_before, result.updates_after, result.deletes):
        if df is not None:
            total += df.count()
    return total


def _run_legacy_batch(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_id: int,
) -> RawBatchResult:
    """Generate one batch using the legacy (Python-loop) path only."""
    from pyspark.sql import functions as F

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    config = plan.config_for(table_name)
    global_seed = plan.base_plan.seed
    initial_rows = int(table_spec.rows)
    batch_size = resolve_batch_size(config.batch_size, initial_rows)
    periods = compute_periods_from_config(initial_rows, batch_size, config)
    resolved = resolve_plan(plan.base_plan)
    batch_ts = batch_timestamp(plan, batch_id)

    # Inserts (same path for both)
    inserts = None
    if periods.inserts_per_batch > 0:
        inserts = _generate_insert_stream(
            spark,
            table_spec,
            resolved,
            periods,
            batch_id,
            global_seed,
        )
        inserts = (
            inserts.withColumn("_op", F.lit("I"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )

    # Deletes via legacy
    deletes = None
    del_indices = delete_indices_at_batch_fast(
        batch_id,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        config.min_life,
    )
    if del_indices:
        deletes = _generate_delete_stream(
            spark,
            table_spec,
            resolved,
            periods,
            del_indices,
            batch_id,
            global_seed,
            initial_rows,
        )
        deletes = (
            deletes.withColumn("_op", F.lit("D"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )

    # Updates via legacy
    updates_before = None
    updates_after = None
    upd_indices = update_indices_at_batch(
        batch_id,
        initial_rows,
        periods.inserts_per_batch,
        periods.death_period,
        periods.update_period,
        config.min_life,
    )
    if upd_indices:
        updates_before = _generate_update_before_stream(
            spark,
            table_spec,
            resolved,
            periods,
            upd_indices,
            batch_id,
            global_seed,
            initial_rows,
        )
        updates_before = (
            updates_before.withColumn("_op", F.lit("UB"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )
        updates_after = _generate_update_after_stream(
            spark,
            table_spec,
            resolved,
            periods,
            upd_indices,
            batch_id,
            global_seed,
            initial_rows,
        )
        updates_after = (
            updates_after.withColumn("_op", F.lit("U"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )

    return RawBatchResult(
        table_name=table_name,
        batch_id=batch_id,
        inserts=inserts,
        updates_before=updates_before,
        updates_after=updates_after,
        deletes=deletes,
    )


def _run_native_batch(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    batch_id: int,
) -> RawBatchResult:
    """Generate one batch using the native (Spark-side) path only."""
    from pyspark.sql import functions as F

    table_map = {t.name: t for t in plan.base_plan.tables}
    table_spec = table_map[table_name]
    config = plan.config_for(table_name)
    global_seed = plan.base_plan.seed
    initial_rows = int(table_spec.rows)
    batch_size = resolve_batch_size(config.batch_size, initial_rows)
    periods = compute_periods_from_config(initial_rows, batch_size, config)
    resolved = resolve_plan(plan.base_plan)
    batch_ts = batch_timestamp(plan, batch_id)

    # Inserts (same path for both)
    inserts = None
    if periods.inserts_per_batch > 0:
        inserts = _generate_insert_stream(
            spark,
            table_spec,
            resolved,
            periods,
            batch_id,
            global_seed,
        )
        inserts = (
            inserts.withColumn("_op", F.lit("I"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )

    # Deletes via native
    deletes = _generate_delete_stream_native(
        spark,
        table_spec,
        resolved,
        periods,
        batch_id,
        global_seed,
        initial_rows,
        config.min_life,
    )
    if deletes is not None:
        deletes = (
            deletes.withColumn("_op", F.lit("D"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )

    # Updates via native
    updates_before = _generate_update_before_stream_native(
        spark,
        table_spec,
        resolved,
        periods,
        batch_id,
        global_seed,
        initial_rows,
        config.min_life,
    )
    if updates_before is not None:
        updates_before = (
            updates_before.withColumn("_op", F.lit("UB"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )

    updates_after = _generate_update_after_stream_native(
        spark,
        table_spec,
        resolved,
        periods,
        batch_id,
        global_seed,
        initial_rows,
        config.min_life,
    )
    if updates_after is not None:
        updates_after = (
            updates_after.withColumn("_op", F.lit("U"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ts", F.lit(batch_ts).cast("timestamp"))
        )

    return RawBatchResult(
        table_name=table_name,
        batch_id=batch_id,
        inserts=inserts,
        updates_before=updates_before,
        updates_after=updates_after,
        deletes=deletes,
    )


def _benchmark_batches(
    spark: SparkSession,
    plan: CDCPlan,
    table_name: str,
    num_batches: int,
    runner,
    label: str,
) -> dict:
    """Run *num_batches* through *runner* and collect timing stats."""
    times = []
    total_rows = 0
    for batch_id in range(1, num_batches + 1):
        t0 = time.perf_counter()
        result = runner(spark, plan, table_name, batch_id)
        rows = _force_materialize(result)
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        total_rows += rows

    avg = sum(times) / len(times)
    total = sum(times)
    throughput = total_rows / total if total > 0 else 0

    print(f"\n  [{label}] {num_batches} batches:")
    print(f"    Total time:   {total:.2f}s")
    print(f"    Avg/batch:    {avg:.3f}s")
    print(f"    Min/batch:    {min(times):.3f}s")
    print(f"    Max/batch:    {max(times):.3f}s")
    print(f"    Total rows:   {total_rows:,}")
    print(f"    Throughput:   {throughput:,.0f} rows/s")

    return {
        "label": label,
        "total_time": total,
        "avg_time": avg,
        "min_time": min(times),
        "max_time": max(times),
        "total_rows": total_rows,
        "throughput": throughput,
    }


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

# Mark all tests in this module so they can be deselected easily:
#   pytest tests/ -q --ignore=tests/test_cdc_perf.py
pytestmark = pytest.mark.perf


SCENARIOS = {
    "small": {"initial_rows": 1_000, "batch_size": 100, "batches": 10},
    "medium": {"initial_rows": 100_000, "batch_size": 10_000, "batches": 5},
    "large": {"initial_rows": 1_000_000, "batch_size": 100_000, "batches": 3},
    "xlarge": {"initial_rows": 10_000_000, "batch_size": 500_000, "batches": 3},
    "xxl": {"initial_rows": 20_000_000, "batch_size": 950_000, "batches": 3},
}


class TestCDCPerfSmall:
    """1K initial rows, 100 events/batch, 10 batches."""

    def test_native_vs_legacy_small(self, spark):
        s = SCENARIOS["small"]
        plan = _clearing_plan(s["initial_rows"], s["batch_size"])
        n = s["batches"]

        print(f"\n{'='*60}")
        print(f"SCENARIO: small — {s['initial_rows']:,} rows, " f"{s['batch_size']:,} events/batch, {n} batches")
        print(f"{'='*60}")

        legacy = _benchmark_batches(spark, plan, "clearing", n, _run_legacy_batch, "legacy")
        native = _benchmark_batches(spark, plan, "clearing", n, _run_native_batch, "native")

        # Verify same row counts
        assert (
            legacy["total_rows"] == native["total_rows"]
        ), f"Row count mismatch: legacy={legacy['total_rows']}, native={native['total_rows']}"

        speedup = legacy["total_time"] / native["total_time"] if native["total_time"] > 0 else 0
        print(f"\n  Speedup: {speedup:.2f}x")
        print(f"  Legacy total: {legacy['total_time']:.2f}s")
        print(f"  Native total: {native['total_time']:.2f}s")


class TestCDCPerfMedium:
    """100K initial rows, 10K events/batch, 5 batches."""

    def test_native_vs_legacy_medium(self, spark):
        s = SCENARIOS["medium"]
        plan = _clearing_plan(s["initial_rows"], s["batch_size"])
        n = s["batches"]

        print(f"\n{'='*60}")
        print(f"SCENARIO: medium — {s['initial_rows']:,} rows, " f"{s['batch_size']:,} events/batch, {n} batches")
        print(f"{'='*60}")

        legacy = _benchmark_batches(spark, plan, "clearing", n, _run_legacy_batch, "legacy")
        native = _benchmark_batches(spark, plan, "clearing", n, _run_native_batch, "native")

        assert (
            legacy["total_rows"] == native["total_rows"]
        ), f"Row count mismatch: legacy={legacy['total_rows']}, native={native['total_rows']}"

        speedup = legacy["total_time"] / native["total_time"] if native["total_time"] > 0 else 0
        print(f"\n  Speedup: {speedup:.2f}x")
        print(f"  Legacy total: {legacy['total_time']:.2f}s")
        print(f"  Native total: {native['total_time']:.2f}s")


class TestCDCPerfLarge:
    """1M initial rows, 100K events/batch, 3 batches."""

    def test_native_vs_legacy_large(self, spark):
        s = SCENARIOS["large"]
        plan = _clearing_plan(s["initial_rows"], s["batch_size"])
        n = s["batches"]

        print(f"\n{'='*60}")
        print(f"SCENARIO: large — {s['initial_rows']:,} rows, " f"{s['batch_size']:,} events/batch, {n} batches")
        print(f"{'='*60}")

        legacy = _benchmark_batches(spark, plan, "clearing", n, _run_legacy_batch, "legacy")
        native = _benchmark_batches(spark, plan, "clearing", n, _run_native_batch, "native")

        assert (
            legacy["total_rows"] == native["total_rows"]
        ), f"Row count mismatch: legacy={legacy['total_rows']}, native={native['total_rows']}"

        speedup = legacy["total_time"] / native["total_time"] if native["total_time"] > 0 else 0
        print(f"\n  Speedup: {speedup:.2f}x")
        print(f"  Legacy total: {legacy['total_time']:.2f}s")
        print(f"  Native total: {native['total_time']:.2f}s")


class TestCDCPerfXLarge:
    """10M initial rows, 500K events/batch, 3 batches."""

    def test_native_vs_legacy_xlarge(self, spark):
        s = SCENARIOS["xlarge"]
        plan = _clearing_plan(s["initial_rows"], s["batch_size"])
        n = s["batches"]

        print(f"\n{'='*60}")
        print(f"SCENARIO: xlarge — {s['initial_rows']:,} rows, " f"{s['batch_size']:,} events/batch, {n} batches")
        print(f"{'='*60}")

        legacy = _benchmark_batches(spark, plan, "clearing", n, _run_legacy_batch, "legacy")
        native = _benchmark_batches(spark, plan, "clearing", n, _run_native_batch, "native")

        assert (
            legacy["total_rows"] == native["total_rows"]
        ), f"Row count mismatch: legacy={legacy['total_rows']}, native={native['total_rows']}"

        speedup = legacy["total_time"] / native["total_time"] if native["total_time"] > 0 else 0
        print(f"\n  Speedup: {speedup:.2f}x")
        print(f"  Legacy total: {legacy['total_time']:.2f}s")
        print(f"  Native total: {native['total_time']:.2f}s")


class TestCDCPerfXXL:
    """20M initial rows, 950K events/batch, 3 batches."""

    def test_native_vs_legacy_xxl(self, spark):
        s = SCENARIOS["xxl"]
        plan = _clearing_plan(s["initial_rows"], s["batch_size"])
        n = s["batches"]

        print(f"\n{'='*60}")
        print(f"SCENARIO: xxl — {s['initial_rows']:,} rows, " f"{s['batch_size']:,} events/batch, {n} batches")
        print(f"{'='*60}")

        legacy = _benchmark_batches(spark, plan, "clearing", n, _run_legacy_batch, "legacy")
        native = _benchmark_batches(spark, plan, "clearing", n, _run_native_batch, "native")

        assert (
            legacy["total_rows"] == native["total_rows"]
        ), f"Row count mismatch: legacy={legacy['total_rows']}, native={native['total_rows']}"

        speedup = legacy["total_time"] / native["total_time"] if native["total_time"] > 0 else 0
        print(f"\n  Speedup: {speedup:.2f}x")
        print(f"  Legacy total: {legacy['total_time']:.2f}s")
        print(f"  Native total: {native['total_time']:.2f}s")
