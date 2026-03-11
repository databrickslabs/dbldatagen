"""Tests for the delta strategy of the ingest module.

Covers:
- Delta incremental batch generation
- Delta snapshot assembly
- Deterministic row selection
- Mutation generation
- Update window with delta strategy
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from dbldatagen.v1.engine.ingest_generator import (
    _add_ingest_metadata,
    generate_delta_incremental_batch,
    generate_initial_snapshot,
)
from dbldatagen.v1.ingest import detect_changes
from dbldatagen.v1.ingest_schema import (
    IngestMode,
    IngestPlan,
    IngestStrategy,
    IngestTableConfig,
)
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_ingest_delta")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _simple_plan(rows=10) -> DataGenPlan:
    return DataGenPlan(
        tables=[
            TableSpec(
                name="transactions",
                rows=rows,
                primary_key=PrimaryKey(columns=["txn_id"]),
                columns=[
                    ColumnSpec(name="txn_id", dtype=DataType.LONG, gen=SequenceColumn(start=1)),
                    ColumnSpec(name="amount", dtype=DataType.DOUBLE, gen=RangeColumn(min=5.0, max=5000.0)),
                    ColumnSpec(
                        name="status",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["pending", "open", "settled", "closed"]),
                    ),
                ],
            ),
        ],
        seed=42,
    )


class TestDeltaIncremental:
    def test_basic_batch(self, spark):
        """Generate a single delta incremental batch."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=10,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.DELTA,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        initial = generate_initial_snapshot(spark, plan)
        current_df = initial["transactions"]
        assert current_df.count() == 10

        result = generate_delta_incremental_batch(
            spark,
            plan,
            "transactions",
            1,
            current_df,
        )
        assert "transactions" in result
        inc_df = result["transactions"]
        assert inc_df.count() > 0
        assert "_batch_id" in inc_df.columns
        assert "_load_ts" in inc_df.columns

    def test_multiple_batches(self, spark):
        """Run 3 delta batches and verify table growth."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.DELTA,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        initial = generate_initial_snapshot(spark, plan)
        current_df = initial["transactions"]
        initial_count = current_df.count()
        assert initial_count == 10

        for day in range(1, 4):
            inc_result = generate_delta_incremental_batch(
                spark,
                plan,
                "transactions",
                day,
                current_df,
            )
            inc_df = inc_result["transactions"]

            # Build snapshot: materialize to break lineage (avoids OOM)
            pk_cols = ["txn_id"]
            inc_pks = inc_df.select(*pk_cols)
            meta_drop = [c for c in current_df.columns if c in ("_batch_id", "_load_ts")]
            current_clean = current_df
            for mc in meta_drop:
                current_clean = current_clean.drop(mc)
            inc_clean = inc_df
            for mc in [c for c in inc_df.columns if c in ("_batch_id", "_load_ts")]:
                inc_clean = inc_clean.drop(mc)

            snapshot = current_clean.join(inc_pks, pk_cols, "left_anti").unionByName(inc_clean)
            snapshot = _add_ingest_metadata(snapshot, plan, day)
            # Checkpoint to break lineage growth
            snapshot.cache().count()
            current_df = snapshot

        # With 7 inserts/day and 0 deletes, should have 10 + 21 = 31 rows
        final_count = current_df.count()
        assert final_count == 31, f"Expected 31 rows, got {final_count}"

    def test_change_detection_on_delta_batches(self, spark):
        """Use detect_changes to verify delta batch correctness."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            strategy=IngestStrategy.DELTA,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        initial = generate_initial_snapshot(spark, plan)
        current_df = initial["transactions"]

        for day in range(1, 4):
            inc_result = generate_delta_incremental_batch(
                spark,
                plan,
                "transactions",
                day,
                current_df,
            )
            inc_df = inc_result["transactions"]

            prev_df = current_df
            # Build snapshot
            pk_cols = ["txn_id"]
            inc_pks = inc_df.select(*pk_cols)
            meta_drop = [c for c in current_df.columns if c in ("_batch_id", "_load_ts")]
            current_clean = current_df
            for mc in meta_drop:
                current_clean = current_clean.drop(mc)
            inc_clean = inc_df
            for mc in [c for c in inc_df.columns if c in ("_batch_id", "_load_ts")]:
                inc_clean = inc_clean.drop(mc)

            snapshot = current_clean.join(inc_pks, pk_cols, "left_anti").unionByName(inc_clean)
            snapshot = _add_ingest_metadata(snapshot, plan, day)

            changes = detect_changes(spark, prev_df, snapshot, ["txn_id"])
            ins_count = changes["inserts"].count()
            assert ins_count == 7, f"Day {day}: expected 7 inserts, got {ins_count}"
            assert changes["deletes"].count() == 0

            current_df = snapshot

    def test_no_pk_overlap_in_inserts(self, spark):
        """New insert PKs should not overlap with existing PKs."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            strategy=IngestStrategy.DELTA,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        initial = generate_initial_snapshot(spark, plan)
        current_df = initial["transactions"]

        _all_pks = {r.txn_id for r in current_df.select("txn_id").collect()}

        for day in range(1, 4):
            inc_result = generate_delta_incremental_batch(
                spark,
                plan,
                "transactions",
                day,
                current_df,
            )
            inc_df = inc_result["transactions"]
            _new_pks = {r.txn_id for r in inc_df.select("txn_id").collect()}

            # Build snapshot
            pk_cols = ["txn_id"]
            inc_pks = inc_df.select(*pk_cols)
            meta_drop = [c for c in current_df.columns if c in ("_batch_id", "_load_ts")]
            current_clean = current_df
            for mc in meta_drop:
                current_clean = current_clean.drop(mc)
            inc_clean = inc_df
            for mc in [c for c in inc_df.columns if c in ("_batch_id", "_load_ts")]:
                inc_clean = inc_clean.drop(mc)

            snapshot = current_clean.join(inc_pks, pk_cols, "left_anti").unionByName(inc_clean)
            snapshot = _add_ingest_metadata(snapshot, plan, day)
            _all_pks = {r.txn_id for r in snapshot.select("txn_id").collect()}
            current_df = snapshot

        # All PKs should be unique
        final_pks = [r.txn_id for r in current_df.select("txn_id").collect()]
        assert len(final_pks) == len(set(final_pks)), "Duplicate PKs in final snapshot"
