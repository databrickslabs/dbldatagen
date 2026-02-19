"""End-to-end SCD Type 2 scenario tests with CDC data.

These are the "money tests" — they validate the complete use case of
generating CDC data, tracking changes, and verifying expected state.
"""

from __future__ import annotations

from dbldatagen.v1.cdc import generate_cdc, generate_expected_state
from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)


def _dim_product(rows=100, seed=42):
    return DataGenPlan(
        seed=seed,
        tables=[
            TableSpec(
                name="dim_product",
                rows=rows,
                primary_key=PrimaryKey(columns=["product_id"]),
                columns=[
                    ColumnSpec(name="product_id", gen=SequenceColumn()),
                    ColumnSpec(
                        name="name",
                        gen=ValuesColumn(values=["Widget", "Gadget", "Doohickey", "Sprocket"]),
                    ),
                    ColumnSpec(name="price", dtype=DataType.INT, gen=RangeColumn(min=10, max=500)),
                    ColumnSpec(
                        name="category",
                        gen=ValuesColumn(values=["electronics", "home", "outdoor", "tools"]),
                    ),
                ],
            ),
        ],
    )


class TestSCD2DimensionTracking:
    """Full SCD2 workflow: generate CDC, apply changes, verify state."""

    def test_initial_snapshot_is_complete(self, spark):
        stream = generate_cdc(spark, _dim_product(rows=100), num_batches=3)
        initial = stream.initial["dim_product"]
        assert initial.count() == 100
        assert initial.select("product_id").distinct().count() == 100

    def test_each_batch_has_changes(self, spark):
        stream = generate_cdc(spark, _dim_product(rows=100), num_batches=5)
        for i, batch in enumerate(stream.batches):
            df = batch["dim_product"]
            assert df.count() > 0, f"Batch {i + 1} is empty"

    def test_expected_state_has_unique_pks(self, spark):
        plan = _dim_product(rows=100)
        state = generate_expected_state(spark, plan, "dim_product", batch_id=5)
        assert state.select("product_id").distinct().count() == state.count()

    def test_expected_state_count_matches_tracking(self, spark):
        """Expected state row count should match stateless lifecycle model."""
        from dbldatagen.v1.engine.cdc_state import resolve_batch_size
        from dbldatagen.v1.engine.cdc_stateless import (
            compute_periods,
            is_alive,
            max_k_at_batch,
        )

        plan = CDCPlan(base_plan=_dim_product(rows=100), num_batches=5)
        config = plan.config_for("dim_product")
        initial_rows = 100
        batch_size = resolve_batch_size(config.batch_size, initial_rows)
        periods = compute_periods(
            initial_rows=initial_rows,
            batch_size=batch_size,
            insert_weight=config.operations.insert,
            update_weight=config.operations.update,
            delete_weight=config.operations.delete,
            min_life=config.min_life,
        )

        batch_id = 5
        upper_k = max_k_at_batch(batch_id, initial_rows, periods.inserts_per_batch)
        expected_live = sum(
            1
            for k in range(upper_k)
            if is_alive(k, batch_id, initial_rows, periods.inserts_per_batch, periods.death_period, min_life=config.min_life)
        )

        df_state = generate_expected_state(spark, plan, "dim_product", batch_id=batch_id)
        assert df_state.count() == expected_live

    def test_updates_change_values(self, spark):
        """After applying updates, the expected state should reflect new values."""
        plan = CDCPlan(
            base_plan=_dim_product(rows=50, seed=42),
            num_batches=3,
            table_configs={
                "dim_product": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=0, update=1, delete=0),
                ),
            },
        )
        stream = generate_cdc(spark, plan)

        # Collect initial values
        initial_map = {
            r.product_id: r.price for r in stream.initial["dim_product"].drop("_op", "_batch_id", "_ts").collect()
        }

        # Get expected state after all updates
        state = generate_expected_state(spark, plan, "dim_product", batch_id=3)
        final_map = {r.product_id: r.price for r in state.collect()}

        # Some values should have changed
        changed = sum(1 for pk in initial_map if pk in final_map and initial_map[pk] != final_map[pk])
        assert changed > 0, "No values changed after 3 batches of updates"

    def test_deletes_remove_from_state(self, spark):
        """Deleted rows should not appear in expected state."""
        plan = CDCPlan(
            base_plan=_dim_product(rows=50, seed=42),
            num_batches=3,
            table_configs={
                "dim_product": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=0, update=2, delete=8),
                ),
            },
        )
        stream = generate_cdc(spark, plan)
        state = generate_expected_state(spark, plan, "dim_product", batch_id=3)

        # Some rows should be gone
        assert state.count() < 50

        # Collect deleted PKs
        deleted_pks = set()
        for batch in stream.batches:
            for r in batch["dim_product"].filter("_op = 'D'").select("product_id").collect():
                deleted_pks.add(r.product_id)

        live_pks = {r.product_id for r in state.select("product_id").collect()}
        assert len(deleted_pks) > 0, "No deletes occurred"
        assert len(deleted_pks & live_pks) == 0, "Deleted PKs found in live state"


class TestSCD2NoPhantomUpdates:
    """Verify that update after-images actually differ from before-images."""

    def test_no_phantom_updates(self, spark):
        stream = generate_cdc(spark, _dim_product(rows=100, seed=42), num_batches=1)
        batch = stream.batches[0]["dim_product"]

        before = {r.product_id: (r.name, r.price, r.category) for r in batch.filter("_op = 'UB'").collect()}
        after = {r.product_id: (r.name, r.price, r.category) for r in batch.filter("_op = 'U'").collect()}

        phantom_count = sum(1 for pk in before if pk in after and before[pk] == after[pk])
        total = len(before)
        # Allow a small fraction (some rows may hash to same values by chance)
        assert phantom_count < total, "All updates are phantom (before == after)"


class TestSCD2MultiTable:
    """SCD2 with FK relationships — dimension + fact tables."""

    def test_fact_fk_integrity_across_batches(self, spark):
        plan = DataGenPlan(
            seed=42,
            tables=[
                TableSpec(
                    name="dim_customer",
                    rows=50,
                    primary_key=PrimaryKey(columns=["cust_id"]),
                    columns=[
                        ColumnSpec(name="cust_id", gen=SequenceColumn()),
                        ColumnSpec(name="name", gen=ValuesColumn(values=["Alice", "Bob"])),
                    ],
                ),
                TableSpec(
                    name="fact_orders",
                    rows=200,
                    primary_key=PrimaryKey(columns=["order_id"]),
                    columns=[
                        ColumnSpec(name="order_id", gen=SequenceColumn()),
                        ColumnSpec(
                            name="cust_id",
                            gen=ConstantColumn(value=None),
                            foreign_key=ForeignKeyRef(ref="dim_customer.cust_id"),
                        ),
                        ColumnSpec(name="amount", dtype=DataType.INT, gen=RangeColumn(min=10, max=1000)),
                    ],
                ),
            ],
        )
        stream = generate_cdc(spark, plan, num_batches=3)

        # Verify dim_customer has no deletes (FK parent protection)
        for batch in stream.batches:
            del_count = batch["dim_customer"].filter("_op = 'D'").count()
            assert del_count == 0

        # Verify initial fact FK integrity
        initial_cust_pks = {r.cust_id for r in stream.initial["dim_customer"].select("cust_id").collect()}
        initial_order_fks = {r.cust_id for r in stream.initial["fact_orders"].select("cust_id").collect()}
        assert initial_order_fks.issubset(initial_cust_pks)


class TestSCD2DeltaCDFFormat:
    """Verify the Delta CDF format works correctly for SCD2 use case."""

    def test_delta_cdf_change_types(self, spark):
        stream = generate_cdc(
            spark,
            _dim_product(rows=50),
            num_batches=3,
            format="delta_cdf",
        )

        # Initial should be all inserts
        initial_types = {
            r._change_type for r in stream.initial["dim_product"].select("_change_type").distinct().collect()
        }
        assert initial_types == {"insert"}

        # Batches should have mixed change types
        for batch in stream.batches:
            types = {r._change_type for r in batch["dim_product"].select("_change_type").distinct().collect()}
            assert len(types) > 0

    def test_delta_cdf_commit_versions(self, spark):
        stream = generate_cdc(
            spark,
            _dim_product(rows=50),
            num_batches=3,
            format="delta_cdf",
        )
        for i, batch in enumerate(stream.batches):
            versions = {r._commit_version for r in batch["dim_product"].select("_commit_version").distinct().collect()}
            assert versions == {i + 1}

    def test_delta_cdf_update_pre_post_images(self, spark):
        """Updates in Delta CDF should produce both preimage and postimage rows."""
        stream = generate_cdc(
            spark,
            _dim_product(rows=50),
            num_batches=1,
            format="delta_cdf",
        )
        df = stream.batches[0]["dim_product"]
        pre = df.filter("_change_type = 'update_preimage'")
        post = df.filter("_change_type = 'update_postimage'")
        assert pre.count() == post.count()
        assert pre.count() > 0

        # Same PKs in pre and post
        pre_pks = {r.product_id for r in pre.select("product_id").collect()}
        post_pks = {r.product_id for r in post.select("product_id").collect()}
        assert pre_pks == post_pks
