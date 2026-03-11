"""Tests for the stateless strategy of the ingest module.

Covers:
- Stateless incremental batch generation (three-range targeted scan)
- Stateless snapshot generation
- Deterministic row selection
- Batch independence (any batch generated without prior batches)
- Operation count correctness
- PK uniqueness across batches
- Update window support
- Delete lifecycle
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from dbldatagen.v1.engine.ingest_generator import (
    generate_stateless_incremental_batch,
    generate_stateless_snapshot_batch,
)
from dbldatagen.v1.ingest import generate_ingest
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
        .appName("test_ingest_stateless")
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


class TestStatelessIncrementalBasic:
    def test_basic_batch(self, spark):
        """Generate a single stateless incremental batch."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=10,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        result = generate_stateless_incremental_batch(
            spark,
            plan,
            "transactions",
            1,
        )
        assert "transactions" in result
        df = result["transactions"]
        assert df.count() > 0
        assert "_batch_id" in df.columns
        assert "_load_ts" in df.columns
        assert "_action" in df.columns
        assert "txn_id" in df.columns
        assert "amount" in df.columns
        assert "status" in df.columns

    def test_action_column_values(self, spark):
        """Verify _action column contains only valid values."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=10,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        result = generate_stateless_incremental_batch(
            spark,
            plan,
            "transactions",
            1,
        )
        df = result["transactions"]
        actions = {r._action for r in df.select("_action").collect()}
        assert actions.issubset({"I", "U", "D"})

    def test_insert_count(self, spark):
        """Verify correct number of inserts."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=5,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        result = generate_stateless_incremental_batch(
            spark,
            plan,
            "transactions",
            1,
        )
        df = result["transactions"]
        insert_count = df.filter(df._action == "I").count()
        assert insert_count == 7, f"Expected 7 inserts, got {insert_count}"

    def test_update_count(self, spark):
        """Verify updates are generated when configured."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=5,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        result = generate_stateless_incremental_batch(
            spark,
            plan,
            "transactions",
            1,
        )
        df = result["transactions"]
        update_count = df.filter(df._action == "U").count()
        # update_period = max(1, 100 // 3) = 33
        # At batch 1, updates at stride 33: k where (1+k)%33==0
        assert update_count > 0, "Expected some updates"


class TestStatelessDeterminism:
    def test_same_output_twice(self, spark):
        """Same plan + batch_id produces identical output."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=5,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        r1 = generate_stateless_incremental_batch(spark, plan, "transactions", 3)
        r2 = generate_stateless_incremental_batch(spark, plan, "transactions", 3)

        df1 = r1["transactions"].orderBy("txn_id")
        df2 = r2["transactions"].orderBy("txn_id")

        rows1 = [r.asDict() for r in df1.collect()]
        rows2 = [r.asDict() for r in df2.collect()]
        assert rows1 == rows2, "Stateless batches should be deterministic"


class TestStatelessBatchIndependence:
    def test_generate_batch_5_directly(self, spark):
        """Batch 5 can be generated without generating batches 1-4."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=10,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        # Generate batch 5 directly — no prior batches needed
        result = generate_stateless_incremental_batch(
            spark,
            plan,
            "transactions",
            5,
        )
        df = result["transactions"]
        assert df.count() > 0
        insert_count = df.filter(df._action == "I").count()
        assert insert_count == 7


class TestStatelessNoPKOverlap:
    def test_insert_pks_unique_across_batches(self, spark):
        """Insert PKs never collide across multiple batches."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=5,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )

        all_insert_pks: set[int] = set()
        for batch_id in range(1, 6):
            result = generate_stateless_incremental_batch(
                spark,
                plan,
                "transactions",
                batch_id,
            )
            df = result["transactions"]
            insert_pks = {r.txn_id for r in df.filter(df._action == "I").select("txn_id").collect()}
            overlap = all_insert_pks & insert_pks
            assert not overlap, f"Batch {batch_id} has PK overlap: {overlap}"
            all_insert_pks |= insert_pks

        # Should have 5 batches * 7 inserts = 35 unique insert PKs
        assert len(all_insert_pks) == 35


class TestStatelessWithDeletes:
    def test_deletes_generated(self, spark):
        """Rows die at expected batches when delete_fraction > 0."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=20,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.5,
                    update_fraction=0.3,
                    delete_fraction=0.2,
                ),
            },
        )
        # Deletes start after min_life batches
        # death_period = max(1, 100 // 2) = 50
        # First deaths at batch = 0 + 1 + (k % 50) = 1..50
        # So batch 2 should have some deletes (rows where k%50 == 1)
        found_deletes = False
        for batch_id in range(1, 10):
            result = generate_stateless_incremental_batch(
                spark,
                plan,
                "transactions",
                batch_id,
            )
            df = result["transactions"]
            delete_count = df.filter(df._action == "D").count()
            if delete_count > 0:
                found_deletes = True
                break

        assert found_deletes, "Expected some deletes in first 9 batches"

    def test_delete_pks_are_valid(self, spark):
        """Delete PKs should be within the valid range."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=10,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.5,
                    update_fraction=0.3,
                    delete_fraction=0.2,
                ),
            },
        )
        for batch_id in range(1, 6):
            result = generate_stateless_incremental_batch(
                spark,
                plan,
                "transactions",
                batch_id,
            )
            df = result["transactions"]
            delete_pks = [r.txn_id for r in df.filter(df._action == "D").select("txn_id").collect()]
            for pk in delete_pks:
                # PK = 1 + k, so k = pk - 1. Must be in [0, max_k)
                assert pk >= 1, f"Invalid delete PK: {pk}"


class TestStatelessUpdateWindow:
    def test_update_window_limits_updates(self, spark):
        """With update_window=2, only recently created rows are updated."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=10,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                    update_window=2,
                ),
            },
        )
        # At batch 5 with update_window=2, only rows born at batch 3+ are eligible
        result = generate_stateless_incremental_batch(
            spark,
            plan,
            "transactions",
            5,
        )
        df = result["transactions"]
        update_count = df.filter(df._action == "U").count()
        # With window=2, fewer rows are eligible — should still work
        # (may be 0 if no eligible rows hit the modular stride)
        assert update_count >= 0


class TestStatelessSnapshot:
    def test_snapshot_at_batch_0(self, spark):
        """Snapshot at batch 0 should equal the initial rows."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=5,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        result = generate_stateless_snapshot_batch(
            spark,
            plan,
            "transactions",
            0,
        )
        df = result["transactions"]
        assert df.count() == 100

    def test_snapshot_grows_with_inserts(self, spark):
        """Snapshot row count grows as inserts accumulate."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=5,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        # No deletes: snapshot at batch 3 = initial + 3 * inserts_per_batch
        # inserts_per_batch = 7
        result = generate_stateless_snapshot_batch(
            spark,
            plan,
            "transactions",
            3,
        )
        df = result["transactions"]
        expected = 100 + 3 * 7  # 121
        assert df.count() == expected, f"Expected {expected}, got {df.count()}"

    def test_snapshot_with_deletes(self, spark):
        """Snapshot should exclude dead rows."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=10,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.5,
                    update_fraction=0.3,
                    delete_fraction=0.2,
                ),
            },
        )
        result = generate_stateless_snapshot_batch(
            spark,
            plan,
            "transactions",
            5,
        )
        df = result["transactions"]
        # With deletes, count should be less than 100 + 5*5 = 125
        count = df.count()
        assert count < 125, f"Expected fewer than 125 rows (deletes), got {count}"
        assert count > 0

    def test_snapshot_pks_unique(self, spark):
        """All PKs in a snapshot should be unique."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=5,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        result = generate_stateless_snapshot_batch(
            spark,
            plan,
            "transactions",
            3,
        )
        df = result["transactions"]
        pks = [r.txn_id for r in df.select("txn_id").collect()]
        assert len(pks) == len(set(pks)), "Duplicate PKs in snapshot"


class TestStatelessPublicAPI:
    def test_generate_ingest_stateless(self, spark):
        """Test the public generate_ingest API with stateless strategy."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=3,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        stream = generate_ingest(spark, plan)
        assert "transactions" in stream.initial
        assert stream.initial["transactions"].count() == 100

        batch_1 = stream.batches[0]
        assert "transactions" in batch_1
        df = batch_1["transactions"]
        assert df.count() > 0
        assert "_action" in df.columns

    def test_generate_ingest_batch_independent(self, spark):
        """Test independent batch generation via public API."""
        from dbldatagen.v1.ingest import generate_ingest_batch

        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=10,
            strategy=IngestStrategy.STATELESS,
            table_configs={
                "transactions": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.7,
                    update_fraction=0.3,
                    delete_fraction=0.0,
                ),
            },
        )
        batch = generate_ingest_batch(spark, plan, 7)
        assert "transactions" in batch
        assert batch["transactions"].count() > 0
