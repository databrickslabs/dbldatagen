"""Tests covering remaining uncovered lines in generator.py and ingest_generator.py.

generator.py targets:
- Line 160: FK column with no resolution falls back to lit(None)
- Line 215: _build_fk_column_expr returns None
- Lines 341-342, 354, 359-360: _build_exprs_scalar seed_from+null, FK null, null_fraction
- Lines 411-423, 433, 444-445: _build_exprs_dynamic seed_from, FK, null_fraction
- Lines 457-466: _build_write_batch_case_when
- Lines 606-608: ArrayColumn with Column seed (dynamic path)

ingest_generator.py targets:
- Line 55: _convert_to_cdc_plan per-table config
- Line 261: _generate_inserts returns None (count <= 0)
- Lines 333-385: delta incremental batch (updates, deletes, update_window)
- Line 406: _select_rows_deterministic count <= 0
- Lines 457-466: _mutate_selected_rows dtype branches
- Lines 484-515: generate_delta_snapshot_batch
- Lines 569-677: generate_stateless_incremental_batch (updates, deletes, empty)
- Lines 752, 777-778: generate_stateless_snapshot_batch null_fraction
- Lines 837-839: _build_stateless_column_exprs FakerColumn branch
- Lines 944-945: detect_changes with no data_columns
"""

from __future__ import annotations

from pyspark.sql import functions as F

from dbldatagen.v1.cdc_schema import MutationSpec
from dbldatagen.v1.engine.generator import (
    _build_exprs_dynamic,
    _build_exprs_scalar,
    _build_write_batch_case_when,
    build_all_column_exprs,
    build_column_expr,
)
from dbldatagen.v1.engine.ingest_generator import (
    _mutate_selected_rows,
    _select_rows_deterministic,
    detect_changes,
    generate_delta_incremental_batch,
    generate_delta_snapshot_batch,
    generate_initial_snapshot,
    generate_stateless_incremental_batch,
    generate_stateless_snapshot_batch,
)
from dbldatagen.v1.engine.utils import create_range_df
from dbldatagen.v1.ingest_schema import (
    IngestMode,
    IngestPlan,
    IngestStrategy,
    IngestTableConfig,
)
from dbldatagen.v1.schema import (
    ArrayColumn,
    ColumnSpec,
    DataGenPlan,
    DataType,
    ForeignKeyRef,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_plan(rows: int = 50) -> DataGenPlan:
    return DataGenPlan(
        tables=[
            TableSpec(
                name="items",
                rows=rows,
                primary_key=PrimaryKey(columns=["item_id"]),
                columns=[
                    ColumnSpec(name="item_id", dtype=DataType.LONG, gen=SequenceColumn(start=1)),
                    ColumnSpec(name="price", dtype=DataType.DOUBLE, gen=RangeColumn(min=1.0, max=100.0)),
                    ColumnSpec(name="status", dtype=DataType.STRING, gen=ValuesColumn(values=["A", "B", "C"])),
                ],
            ),
        ],
        seed=99,
    )


def _simple_ingest_plan(rows: int = 50, num_batches: int = 3) -> IngestPlan:
    return IngestPlan(
        base_plan=_simple_plan(rows),
        num_batches=num_batches,
        mode=IngestMode.INCREMENTAL,
        strategy=IngestStrategy.SYNTHETIC,
    )


# ===================================================================
# generator.py: FK column with no resolution (line 160, 215)
# ===================================================================


class TestFKColumnNoResolution:
    def test_fk_no_resolution_falls_back_to_null(self, spark):
        """FK column without fk_resolutions => lit(None)."""
        spec = TableSpec(
            name="orders",
            rows=10,
            columns=[
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=RangeColumn(min=1, max=100),
                    foreign_key=ForeignKeyRef(ref="customers.id"),
                ),
            ],
        )
        _df, id_col = create_range_df(spark, 10)
        col_exprs, udf_columns, _ = build_all_column_exprs(
            spec,
            id_col,
            None,
            seed=42,
            row_count=10,
        )
        # FK without resolution goes to col_exprs as lit(None)
        assert len(col_exprs) == 1
        assert len(udf_columns) == 0


# ===================================================================
# generator.py: _build_exprs_scalar with null_fraction, FK, seed_from
# (lines 340-342, 354, 358-360)
# ===================================================================


class TestBuildExprsScalarBranches:
    def test_scalar_null_fraction_on_regular_column(self, spark):
        """Lines 358-360: regular column with null_fraction > 0."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="val",
                    gen=RangeColumn(min=1, max=100),
                    null_fraction=0.5,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        col_exprs, _, _ = _build_exprs_scalar(spec, id_col, 0, 42, None, row_count=20)
        # Should have pk + val
        assert len(col_exprs) == 2

    def test_scalar_seed_from_with_null_fraction(self, spark):
        """Lines 340-342: seed_from column with null_fraction > 0."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="group_id", gen=RangeColumn(min=1, max=5)),
                ColumnSpec(
                    name="derived",
                    gen=RangeColumn(min=10, max=99),
                    seed_from="group_id",
                    null_fraction=0.3,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        _, _, seeded_columns = _build_exprs_scalar(spec, id_col, 0, 42, None, row_count=20)
        assert len(seeded_columns) == 1
        assert seeded_columns[0][0] == "derived"

    def test_scalar_fk_no_resolution(self, spark):
        """Line 354: FK in scalar path without resolution."""
        spec = TableSpec(
            name="t",
            rows=10,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="ref_id",
                    gen=RangeColumn(min=1, max=10),
                    foreign_key=ForeignKeyRef(ref="other.id"),
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 10)
        col_exprs, udf_cols, _ = _build_exprs_scalar(spec, id_col, 0, 42, None, row_count=10)
        # FK without resolution goes to col_exprs as lit(None)
        assert len(udf_cols) == 0
        # pk + ref_id(null)
        assert len(col_exprs) == 2


# ===================================================================
# generator.py: _build_exprs_dynamic branches
# (lines 411-423, 433, 444-445)
# ===================================================================


class TestBuildExprsDynamic:
    def test_dynamic_seed_from_with_null(self, spark):
        """Lines 410-423: seed_from in dynamic path with null_fraction."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="group_id", gen=RangeColumn(min=1, max=5)),
                ColumnSpec(
                    name="derived",
                    gen=RangeColumn(min=10, max=99),
                    seed_from="group_id",
                    null_fraction=0.3,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        _, _, seeded = _build_exprs_dynamic(
            spec,
            id_col,
            F.col("_wb"),
            [0, 1],
            42,
            None,
            row_count=20,
        )
        assert len(seeded) == 1
        assert seeded[0][0] == "derived"

    def test_dynamic_fk_no_resolution(self, spark):
        """Line 433: FK in dynamic path without resolution."""
        spec = TableSpec(
            name="t",
            rows=10,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="ref_id",
                    gen=RangeColumn(min=1, max=10),
                    foreign_key=ForeignKeyRef(ref="other.id"),
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 10)
        _col_exprs, udf_cols, _ = _build_exprs_dynamic(
            spec,
            id_col,
            F.col("_wb"),
            [0, 1],
            42,
            None,
            row_count=10,
        )
        assert len(udf_cols) == 0

    def test_dynamic_regular_null_fraction(self, spark):
        """Lines 443-445: regular column with null_fraction in dynamic path."""
        spec = TableSpec(
            name="t",
            rows=20,
            columns=[
                ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="val",
                    gen=RangeColumn(min=1, max=100),
                    null_fraction=0.5,
                ),
            ],
            primary_key=PrimaryKey(columns=["pk"]),
        )
        _df, id_col = create_range_df(spark, 20)
        col_exprs, _, _ = _build_exprs_dynamic(
            spec,
            id_col,
            F.col("_wb"),
            [0, 1],
            42,
            None,
            row_count=20,
        )
        assert len(col_exprs) == 2  # pk + val


# ===================================================================
# generator.py: _build_write_batch_case_when (lines 457-466)
# ===================================================================


class TestBuildWriteBatchCaseWhen:
    def test_single_batch_returns_expr_directly(self, spark):
        """Line 457-458: single batch returns expression directly."""
        expr = F.lit(42)
        result = _build_write_batch_case_when(F.col("wb"), [(0, expr)])
        # Should return the expression directly (no CASE WHEN)
        df = spark.range(5).withColumn("wb", F.lit(0))
        out = df.select(result.alias("v")).collect()
        assert all(r.v == 42 for r in out)

    def test_multiple_batches_builds_case_when(self, spark):
        """Lines 460-466: multiple batches build CASE WHEN chain."""
        e0 = F.lit(10)
        e1 = F.lit(20)
        e2 = F.lit(30)
        result = _build_write_batch_case_when(
            F.col("wb"),
            [(0, e0), (1, e1), (2, e2)],
        )
        df = spark.createDataFrame([(0,), (1,), (2,)], ["wb"])
        out = df.select(result.alias("v")).collect()
        values = {r.v for r in out}
        assert values == {10, 20, 30}


# ===================================================================
# generator.py: ArrayColumn with Column seed (lines 606-608)
# ===================================================================


class TestArrayColumnWithColumnSeed:
    def test_array_column_with_column_seed(self, spark):
        """Lines 606-608: ArrayColumn where column_seed is a Column."""
        col_spec = ColumnSpec(
            name="arr",
            gen=ArrayColumn(
                element=RangeColumn(min=1, max=10),
                min_length=2,
                max_length=2,
            ),
        )
        df = spark.range(10).withColumn("_seed", F.lit(42).cast("long"))
        expr = build_column_expr(col_spec, F.col("id"), F.col("_seed"), 10, 42)
        result = df.select(expr.alias("arr")).collect()
        assert all(len(r.arr) == 2 for r in result)


# ===================================================================
# ingest_generator.py: _select_rows_deterministic count <= 0 (line 406)
# ===================================================================


class TestSelectRowsDeterministic:
    def test_zero_count_returns_none(self, spark):
        """Line 406: count <= 0 returns None."""
        df = spark.range(10)
        result = _select_rows_deterministic(df, ["id"], 0, 42, "test")
        assert result is None

    def test_negative_count_returns_none(self, spark):
        result = _select_rows_deterministic(spark.range(10), ["id"], -1, 42, "test")
        assert result is None


# ===================================================================
# ingest_generator.py: _mutate_selected_rows dtype branches (lines 457-466)
# ===================================================================


class TestMutateSelectedRows:
    def test_mutate_int_and_string_and_float(self, spark):
        """Lines 456-464: mutation generates correct types for int, float, string."""
        table_spec = TableSpec(
            name="items",
            rows=10,
            primary_key=PrimaryKey(columns=["item_id"]),
            columns=[
                ColumnSpec(name="item_id", dtype=DataType.LONG, gen=SequenceColumn(start=1)),
                ColumnSpec(name="price", dtype=DataType.DOUBLE, gen=RangeColumn(min=1.0, max=100.0)),
                ColumnSpec(name="status", dtype=DataType.STRING, gen=ValuesColumn(values=["A", "B"])),
                ColumnSpec(name="qty", dtype=DataType.INT, gen=RangeColumn(min=1, max=50)),
            ],
        )
        # Create a current_df
        current_df = spark.createDataFrame(
            [(1, 50.0, "A", 10), (2, 60.0, "B", 20), (3, 70.0, "A", 30)],
            ["item_id", "price", "status", "qty"],
        )
        update_pks = spark.createDataFrame([(1,), (2,)], ["item_id"])
        cfg = IngestTableConfig(
            insert_fraction=0.5,
            update_fraction=0.4,
            delete_fraction=0.1,
            mutations=MutationSpec(columns=["price", "status", "qty"]),
        )
        result = _mutate_selected_rows(
            current_df,
            update_pks,
            ["item_id"],
            table_spec,
            42,
            cfg,
        )
        assert result is not None
        assert result.count() == 2
        # Columns should be present
        assert set(result.columns) >= {"item_id", "price", "status", "qty"}


# ===================================================================
# ingest_generator.py: generate_delta_incremental_batch (lines 333-385)
# ===================================================================


class TestDeltaIncrementalBatch:
    def test_delta_incremental_with_updates_and_deletes(self, spark):
        """Lines 333-385: delta incremental batch with updates, deletes, update_window."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=30),
            num_batches=3,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.DELTA,
            default_config=IngestTableConfig(
                batch_size=10,
                insert_fraction=0.4,
                update_fraction=0.4,
                delete_fraction=0.2,
                update_window=5,
            ),
        )
        # Generate initial state
        initial = generate_initial_snapshot(spark, plan)
        current_df = initial["items"].drop("_action", "_batch_id", "_load_ts")

        result = generate_delta_incremental_batch(
            spark,
            plan,
            "items",
            batch_id=1,
            current_df=current_df,
        )
        assert "items" in result
        assert result["items"].count() > 0

    def test_delta_incremental_empty_batch(self, spark):
        """Lines 383-385: empty batch returns empty DataFrame."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=10),
            num_batches=1,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.DELTA,
            default_config=IngestTableConfig(
                batch_size=0,
                insert_fraction=0.5,
                update_fraction=0.3,
                delete_fraction=0.2,
            ),
        )
        current_df = spark.createDataFrame(
            [(1, 50.0, "A"), (2, 60.0, "B")],
            ["item_id", "price", "status"],
        )
        result = generate_delta_incremental_batch(
            spark,
            plan,
            "items",
            batch_id=1,
            current_df=current_df,
        )
        assert "items" in result


# ===================================================================
# ingest_generator.py: generate_delta_snapshot_batch (lines 484-515)
# ===================================================================


class TestDeltaSnapshotBatch:
    def test_delta_snapshot_with_pk(self, spark):
        """Lines 484-515: delta snapshot assembly with PKs."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=20),
            num_batches=2,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.DELTA,
            default_config=IngestTableConfig(
                batch_size=5,
                insert_fraction=0.6,
                update_fraction=0.3,
                delete_fraction=0.1,
            ),
        )
        initial = generate_initial_snapshot(spark, plan)
        current_df = initial["items"].drop("_action", "_batch_id", "_load_ts")

        result = generate_delta_snapshot_batch(
            spark,
            plan,
            "items",
            batch_id=1,
            current_df=current_df,
        )
        assert "items" in result
        assert result["items"].count() > 0

    def test_delta_snapshot_no_pk(self, spark):
        """Lines 499-503: snapshot without PK just unions."""
        no_pk_plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="logs",
                    rows=10,
                    columns=[
                        ColumnSpec(name="msg", dtype=DataType.STRING, gen=ValuesColumn(values=["a", "b"])),
                    ],
                ),
            ],
            seed=42,
        )
        plan = IngestPlan(
            base_plan=no_pk_plan,
            num_batches=2,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.DELTA,
            default_config=IngestTableConfig(
                batch_size=5,
                insert_fraction=1.0,
                update_fraction=0.0,
                delete_fraction=0.0,
            ),
        )
        initial = generate_initial_snapshot(spark, plan)
        current_df = initial["logs"].drop("_action", "_batch_id", "_load_ts")

        result = generate_delta_snapshot_batch(
            spark,
            plan,
            "logs",
            batch_id=1,
            current_df=current_df,
        )
        assert "logs" in result


# ===================================================================
# ingest_generator.py: generate_stateless_incremental_batch
# (lines 569-677: updates, deletes, empty batch)
# ===================================================================


class TestStatelessIncrementalBatch:
    def test_stateless_incremental_with_updates_and_deletes(self, spark):
        """Lines 569-668: stateless incremental with inserts, updates, deletes."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=10,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.STATELESS,
            default_config=IngestTableConfig(
                batch_size=20,
                insert_fraction=0.4,
                update_fraction=0.4,
                delete_fraction=0.2,
                min_life=2,
            ),
        )
        # Batch 5 should have inserts, updates, and possibly deletes
        result = generate_stateless_incremental_batch(spark, plan, "items", batch_id=5)
        assert "items" in result
        df = result["items"]
        assert df.count() > 0
        # Check _action column exists
        assert "_action" in df.columns

    def test_stateless_incremental_with_update_window(self, spark):
        """Line 621-623: update_window filtering."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=10,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.STATELESS,
            default_config=IngestTableConfig(
                batch_size=20,
                insert_fraction=0.4,
                update_fraction=0.4,
                delete_fraction=0.2,
                min_life=2,
                update_window=3,
            ),
        )
        result = generate_stateless_incremental_batch(spark, plan, "items", batch_id=5)
        assert "items" in result

    def test_stateless_incremental_batch_zero(self, spark):
        """Lines 671-677: batch_id=0 with no inserts/updates/deletes => empty batch."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=50),
            num_batches=5,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.STATELESS,
            default_config=IngestTableConfig(
                batch_size=10,
                insert_fraction=0.5,
                update_fraction=0.3,
                delete_fraction=0.2,
                min_life=1,
            ),
        )
        # batch_id=0 skips all operations (guard: batch_id >= 1)
        result = generate_stateless_incremental_batch(spark, plan, "items", batch_id=0)
        assert "items" in result


# ===================================================================
# ingest_generator.py: generate_stateless_snapshot_batch
# (lines 752, 777-778)
# ===================================================================


class TestStatelessSnapshotBatch:
    def test_stateless_snapshot_with_null_fraction(self, spark):
        """Lines 752, 776-778: snapshot with null_fraction on non-PK columns."""
        plan_with_nulls = DataGenPlan(
            tables=[
                TableSpec(
                    name="items",
                    rows=50,
                    primary_key=PrimaryKey(columns=["item_id"]),
                    columns=[
                        ColumnSpec(name="item_id", dtype=DataType.LONG, gen=SequenceColumn(start=1)),
                        ColumnSpec(
                            name="price",
                            dtype=DataType.DOUBLE,
                            gen=RangeColumn(min=1.0, max=100.0),
                            null_fraction=0.3,
                        ),
                    ],
                ),
            ],
            seed=99,
        )
        plan = IngestPlan(
            base_plan=plan_with_nulls,
            num_batches=3,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.STATELESS,
            default_config=IngestTableConfig(
                batch_size=10,
                insert_fraction=0.5,
                update_fraction=0.3,
                delete_fraction=0.2,
                min_life=1,
            ),
        )
        result = generate_stateless_snapshot_batch(spark, plan, "items", batch_id=2)
        assert "items" in result
        assert result["items"].count() > 0

    def test_stateless_snapshot_no_updates(self, spark):
        """Line 752: no update period => last_write = birth tick."""
        plan = IngestPlan(
            base_plan=_simple_plan(rows=50),
            num_batches=3,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.STATELESS,
            default_config=IngestTableConfig(
                batch_size=10,
                insert_fraction=1.0,
                update_fraction=0.0,
                delete_fraction=0.0,
            ),
        )
        result = generate_stateless_snapshot_batch(spark, plan, "items", batch_id=2)
        assert "items" in result
        assert result["items"].count() > 0


# ===================================================================
# ingest_generator.py: detect_changes with no data_columns (lines 944-945)
# ===================================================================


class TestDetectChanges:
    def test_detect_changes_no_data_columns(self, spark):
        """Lines 943-945: detect_changes when data_columns result in empty comparison."""
        before = spark.createDataFrame([(1,), (2,)], ["id"])
        after = spark.createDataFrame([(1,), (3,)], ["id"])
        result = detect_changes(spark, before, after, key_columns=["id"])
        assert "inserts" in result
        assert "updates" in result
        assert "deletes" in result
        assert "unchanged" in result
        assert result["inserts"].count() == 1  # id=3
        assert result["deletes"].count() == 1  # id=2

    def test_detect_changes_with_data_columns(self, spark):
        """Standard detect_changes path with data changes."""
        before = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "val"])
        after = spark.createDataFrame([(1, "A"), (2, "X"), (4, "D")], ["id", "val"])
        result = detect_changes(
            spark,
            before,
            after,
            key_columns=["id"],
            data_columns=["val"],
        )
        assert result["inserts"].count() == 1  # id=4
        assert result["deletes"].count() == 1  # id=3
        assert result["updates"].count() == 1  # id=2 changed B->X
        assert result["unchanged"].count() == 1  # id=1


# ===================================================================
# ingest_generator.py: _convert_to_cdc_plan per-table config (line 55)
# ===================================================================


class TestConvertToCdcPlanPerTable:
    def test_per_table_config_conversion(self):
        """Line 55: per-table configs are converted to CDCTableConfig."""
        from dbldatagen.v1.engine.ingest_generator import _convert_to_cdc_plan

        plan = IngestPlan(
            base_plan=_simple_plan(rows=50),
            num_batches=3,
            table_configs={
                "items": IngestTableConfig(
                    batch_size=10,
                    insert_fraction=0.5,
                    update_fraction=0.3,
                    delete_fraction=0.2,
                ),
            },
        )
        cdc_plan = _convert_to_cdc_plan(plan)
        assert "items" in cdc_plan.table_configs
        tc = cdc_plan.table_configs["items"]
        assert tc.operations.insert == 0.5
        assert tc.operations.update == 0.3
        assert tc.operations.delete == 0.2
        assert tc.batch_size == 10


# ===================================================================
# ingest_generator.py: generate_initial_snapshot
# ===================================================================


class TestGenerateInitialSnapshot:
    def test_initial_snapshot_produces_rows(self, spark):
        """generate_initial_snapshot returns correct row count."""
        plan = _simple_ingest_plan(rows=30)
        result = generate_initial_snapshot(spark, plan)
        assert "items" in result
        df = result["items"]
        assert df.count() == 30
        assert "_action" in df.columns
        assert "_batch_id" in df.columns
