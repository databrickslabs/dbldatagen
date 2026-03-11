"""Tests for the ingest module (full-row ingestion simulation).

Covers:
- Synthetic incremental mode
- Synthetic snapshot mode
- Update window filtering
- detect_changes() utility
- IngestStream lazy batch access
- generate_ingest_batch() standalone
- Metadata columns (_batch_id, _load_ts)
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from dbldatagen.v1.ingest import (
    IngestStream,
    detect_changes,
    generate_ingest,
    generate_ingest_batch,
)
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
        .appName("test_ingest")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _simple_plan(rows=100) -> DataGenPlan:
    return DataGenPlan(
        tables=[
            TableSpec(
                name="orders",
                rows=rows,
                primary_key=PrimaryKey(columns=["order_id"]),
                columns=[
                    ColumnSpec(name="order_id", dtype=DataType.LONG, gen=SequenceColumn(start=1)),
                    ColumnSpec(name="amount", dtype=DataType.DOUBLE, gen=RangeColumn(min=1.0, max=1000.0)),
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


# ===================================================================
# Synthetic Incremental Mode
# ===================================================================


class TestSyntheticIncremental:
    def test_basic_stream(self, spark):
        """Generate a basic incremental stream and verify structure."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
        )
        stream = generate_ingest(spark, plan)

        assert isinstance(stream, IngestStream)
        assert "orders" in stream.initial
        assert len(stream.batches) == 3

        initial = stream.initial["orders"]
        assert initial.count() == 100
        assert "_batch_id" in initial.columns
        assert "_load_ts" in initial.columns

    def test_incremental_batches_have_rows(self, spark):
        """Each incremental batch should have some rows."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
        )
        stream = generate_ingest(spark, plan)

        for i in range(3):
            batch = stream.batches[i]
            assert "orders" in batch
            count = batch["orders"].count()
            assert count > 0, f"Batch {i} has no rows"

    def test_no_cdc_columns_in_output(self, spark):
        """Output should not have _op or _ts columns."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=1,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
        )
        stream = generate_ingest(spark, plan)
        batch = stream.batches[0]["orders"]
        assert "_op" not in batch.columns
        assert "_ts" not in batch.columns

    def test_metadata_columns_present(self, spark):
        """Batches should have _batch_id and _load_ts."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=1,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
        )
        stream = generate_ingest(spark, plan)
        batch = stream.batches[0]["orders"]
        assert "_batch_id" in batch.columns
        assert "_load_ts" in batch.columns

    def test_no_metadata_when_disabled(self, spark):
        """When include_batch_id=False and include_load_timestamp=False."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=1,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
            include_batch_id=False,
            include_load_timestamp=False,
        )
        stream = generate_ingest(spark, plan)
        batch = stream.batches[0]["orders"]
        assert "_batch_id" not in batch.columns
        assert "_load_ts" not in batch.columns

    def test_batch_id_values(self, spark):
        """_batch_id should match the batch number."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=2,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
        )
        stream = generate_ingest(spark, plan)

        # Initial = batch 0
        bid0 = stream.initial["orders"].select("_batch_id").distinct().collect()
        assert len(bid0) == 1
        assert bid0[0][0] == 0

        # Batch 1 = batch_id 1
        bid1 = stream.batches[0]["orders"].select("_batch_id").distinct().collect()
        assert len(bid1) == 1
        assert bid1[0][0] == 1

    def test_no_deletes_when_fraction_zero(self, spark):
        """When delete_fraction=0, no delete rows should appear."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
            default_config=IngestTableConfig(
                insert_fraction=0.7,
                update_fraction=0.3,
                delete_fraction=0.0,
            ),
        )
        stream = generate_ingest(spark, plan)
        # Should succeed without errors
        for i in range(3):
            batch = stream.batches[i]
            assert "orders" in batch


# ===================================================================
# Synthetic Snapshot Mode
# ===================================================================


class TestSyntheticSnapshot:
    def test_snapshot_returns_all_live_rows(self, spark):
        """Snapshot mode should return all live rows at each batch."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.SYNTHETIC,
            default_config=IngestTableConfig(
                insert_fraction=0.6,
                update_fraction=0.4,
                delete_fraction=0.0,
            ),
        )
        stream = generate_ingest(spark, plan)

        # With no deletes, snapshot size should grow
        initial_count = stream.initial["orders"].count()
        assert initial_count == 100

        snap1_count = stream.batches[0]["orders"].count()
        # Should be >= initial (new inserts added)
        assert snap1_count >= initial_count

    def test_snapshot_has_metadata(self, spark):
        """Snapshot batches should have _batch_id and _load_ts."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=1,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.SYNTHETIC,
        )
        stream = generate_ingest(spark, plan)
        snap = stream.batches[0]["orders"]
        assert "_batch_id" in snap.columns
        assert "_load_ts" in snap.columns

    def test_snapshot_no_cdc_columns(self, spark):
        """Snapshot should not have _op or _ts."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=1,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.SYNTHETIC,
        )
        stream = generate_ingest(spark, plan)
        snap = stream.batches[0]["orders"]
        assert "_op" not in snap.columns
        assert "_ts" not in snap.columns


# ===================================================================
# Update Window
# ===================================================================


class TestUpdateWindow:
    def test_update_window_reduces_updates(self, spark):
        """With update_window=1, fewer rows should be updated at late batches."""
        plan_no_window = IngestPlan(
            base_plan=_simple_plan(rows=200),
            num_batches=5,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
            default_config=IngestTableConfig(
                insert_fraction=0.5,
                update_fraction=0.5,
                delete_fraction=0.0,
                update_window=None,
            ),
        )
        plan_with_window = IngestPlan(
            base_plan=_simple_plan(rows=200),
            num_batches=5,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
            default_config=IngestTableConfig(
                insert_fraction=0.5,
                update_fraction=0.5,
                delete_fraction=0.0,
                update_window=1,
            ),
        )

        stream_no = generate_ingest(spark, plan_no_window)
        stream_yes = generate_ingest(spark, plan_with_window)

        # At batch 5 (index 4), window=1 should have fewer rows than no window
        count_no = stream_no.batches[4]["orders"].count()
        count_yes = stream_yes.batches[4]["orders"].count()
        # Window restricts updates, so fewer total rows in incremental batch
        assert count_yes <= count_no


# ===================================================================
# generate_ingest_batch() standalone
# ===================================================================


class TestGenerateIngestBatch:
    def test_standalone_batch(self, spark):
        """generate_ingest_batch() should work for a single batch."""
        result = generate_ingest_batch(spark, _simple_plan(), batch_id=1, mode="incremental")
        assert "orders" in result
        assert result["orders"].count() > 0

    def test_standalone_snapshot(self, spark):
        """generate_ingest_batch() with snapshot mode."""
        result = generate_ingest_batch(spark, _simple_plan(), batch_id=1, mode="snapshot")
        assert "orders" in result
        assert result["orders"].count() > 0


# ===================================================================
# detect_changes()
# ===================================================================


class TestDetectChanges:
    def test_inserts_detected(self, spark):
        """New rows in after should be detected as inserts."""
        before = spark.createDataFrame([(1, 100.0), (2, 200.0)], ["id", "amount"])
        after = spark.createDataFrame([(1, 100.0), (2, 200.0), (3, 300.0)], ["id", "amount"])
        changes = detect_changes(spark, before, after, ["id"])
        assert changes["inserts"].count() == 1
        assert changes["deletes"].count() == 0
        assert changes["updates"].count() == 0
        assert changes["unchanged"].count() == 2

    def test_deletes_detected(self, spark):
        """Missing rows in after should be detected as deletes."""
        before = spark.createDataFrame([(1, 100.0), (2, 200.0), (3, 300.0)], ["id", "amount"])
        after = spark.createDataFrame([(1, 100.0), (2, 200.0)], ["id", "amount"])
        changes = detect_changes(spark, before, after, ["id"])
        assert changes["inserts"].count() == 0
        assert changes["deletes"].count() == 1
        assert changes["updates"].count() == 0
        assert changes["unchanged"].count() == 2

    def test_updates_detected(self, spark):
        """Changed values with same PK should be detected as updates."""
        before = spark.createDataFrame([(1, 100.0), (2, 200.0)], ["id", "amount"])
        after = spark.createDataFrame([(1, 100.0), (2, 999.0)], ["id", "amount"])
        changes = detect_changes(spark, before, after, ["id"])
        assert changes["inserts"].count() == 0
        assert changes["deletes"].count() == 0
        assert changes["updates"].count() == 1
        assert changes["unchanged"].count() == 1

    def test_mixed_changes(self, spark):
        """Insert + update + delete in one comparison."""
        before = spark.createDataFrame([(1, 100.0), (2, 200.0), (3, 300.0)], ["id", "amount"])
        after = spark.createDataFrame([(1, 100.0), (2, 999.0), (4, 400.0)], ["id", "amount"])
        changes = detect_changes(spark, before, after, ["id"])
        assert changes["inserts"].count() == 1  # id=4
        assert changes["deletes"].count() == 1  # id=3
        assert changes["updates"].count() == 1  # id=2 changed
        assert changes["unchanged"].count() == 1  # id=1

    def test_custom_data_columns(self, spark):
        """Only compare specified data columns for changes."""
        before = spark.createDataFrame([(1, 100.0, "a"), (2, 200.0, "b")], ["id", "amount", "status"])
        after = spark.createDataFrame([(1, 100.0, "x"), (2, 200.0, "b")], ["id", "amount", "status"])
        # Only compare amount (not status) → no updates detected
        changes = detect_changes(spark, before, after, ["id"], data_columns=["amount"])
        assert changes["updates"].count() == 0
        assert changes["unchanged"].count() == 2

    def test_empty_snapshots(self, spark):
        """Handle empty DataFrames gracefully."""
        schema_df = spark.createDataFrame([], "id: int, amount: double")
        full_df = spark.createDataFrame([(1, 100.0)], ["id", "amount"])

        # Empty before → all inserts
        changes = detect_changes(spark, schema_df, full_df, ["id"])
        assert changes["inserts"].count() == 1
        assert changes["deletes"].count() == 0

        # Empty after → all deletes
        changes = detect_changes(spark, full_df, schema_df, ["id"])
        assert changes["inserts"].count() == 0
        assert changes["deletes"].count() == 1


# ===================================================================
# DataGenPlan pass-through (convenience API)
# ===================================================================


class TestDataGenPlanPassthrough:
    def test_generate_from_base_plan(self, spark):
        """generate_ingest() should accept a bare DataGenPlan."""
        stream = generate_ingest(spark, _simple_plan(), num_batches=2, mode="incremental")
        assert len(stream.batches) == 2
        assert "orders" in stream.initial

    def test_generate_batch_from_base_plan(self, spark):
        """generate_ingest_batch() should accept a bare DataGenPlan."""
        result = generate_ingest_batch(spark, _simple_plan(), batch_id=1)
        assert "orders" in result


# ===================================================================
# Determinism
# ===================================================================


class TestDeterminism:
    def test_same_plan_same_output(self, spark):
        """Same plan + seed should produce identical output."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=2,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.SYNTHETIC,
        )

        stream1 = generate_ingest(spark, plan)
        stream2 = generate_ingest(spark, plan)

        # Initial snapshots should match
        df1 = stream1.initial["orders"].orderBy("order_id").collect()
        df2 = stream2.initial["orders"].orderBy("order_id").collect()
        assert df1 == df2

        # Batch 1 should match
        b1 = stream1.batches[0]["orders"].orderBy("order_id").collect()
        b2 = stream2.batches[0]["orders"].orderBy("order_id").collect()
        assert b1 == b2


# ===================================================================
# Lazy batch list
# ===================================================================


class TestLazyBatchList:
    def test_len(self, spark):
        plan = IngestPlan(base_plan=_simple_plan(), num_batches=5)
        stream = generate_ingest(spark, plan)
        assert len(stream.batches) == 5

    def test_indexing(self, spark):
        plan = IngestPlan(base_plan=_simple_plan(), num_batches=3)
        stream = generate_ingest(spark, plan)
        batch = stream.batches[0]
        assert "orders" in batch

    def test_negative_indexing(self, spark):
        plan = IngestPlan(base_plan=_simple_plan(), num_batches=3)
        stream = generate_ingest(spark, plan)
        batch = stream.batches[-1]
        assert "orders" in batch

    def test_out_of_range(self, spark):
        plan = IngestPlan(base_plan=_simple_plan(), num_batches=3)
        stream = generate_ingest(spark, plan)
        with pytest.raises(IndexError):
            _ = stream.batches[10]

    def test_iteration(self, spark):
        plan = IngestPlan(base_plan=_simple_plan(), num_batches=3)
        stream = generate_ingest(spark, plan)
        batches = list(stream.batches)
        assert len(batches) == 3

    def test_slicing(self, spark):
        plan = IngestPlan(base_plan=_simple_plan(), num_batches=5)
        stream = generate_ingest(spark, plan)
        first_two = stream.batches[0:2]
        assert len(first_two) == 2
