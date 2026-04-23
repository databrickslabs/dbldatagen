"""Tests for CDC output format transformers.

Verifies column presence, operation codes, and update row handling
for raw, Delta CDF, and SQL Server formats.  Debezium is not yet
supported — removed from the ``CDCFormat`` enum so plans using it
fail at construction.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.engine.cdc import generate_cdc
from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)


def _plan(rows=100, seed=42):
    return DataGenPlan(
        seed=seed,
        tables=[
            TableSpec(
                name="items",
                rows=rows,
                primary_key=PrimaryKey(columns=["item_id"]),
                columns=[
                    ColumnSpec(name="item_id", gen=SequenceColumn()),
                    ColumnSpec(name="label", gen=ValuesColumn(values=["X", "Y", "Z"])),
                    ColumnSpec(name="qty", dtype=DataType.INT, gen=RangeColumn(min=1, max=100)),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Raw format
# ---------------------------------------------------------------------------


class TestRawFormat:
    def test_columns_present(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="raw")
        cols = stream.batches[0]["items"].columns
        assert "_op" in cols
        assert "_batch_id" in cols
        assert "_ts" in cols

    def test_op_values(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="raw")
        ops = {r._op for r in stream.batches[0]["items"].select("_op").distinct().collect()}
        assert ops.issubset({"I", "U", "UB", "D"})

    def test_initial_op_values(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="raw")
        ops = {r._op for r in stream.initial["items"].select("_op").distinct().collect()}
        assert ops == {"I"}

    def test_batch_id_correct(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=3, format="raw")
        for i, batch in enumerate(stream.batches):
            rows = batch["items"].select("_batch_id").distinct().collect()
            batch_ids = {r._batch_id for r in rows}
            assert batch_ids == {i + 1}


# ---------------------------------------------------------------------------
# Delta CDF format
# ---------------------------------------------------------------------------


class TestDeltaCDFFormat:
    def test_columns_present(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="delta_cdf")
        cols = stream.batches[0]["items"].columns
        assert "_change_type" in cols
        assert "_commit_version" in cols
        assert "_commit_timestamp" in cols
        assert "_op" not in cols

    def test_change_types(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="delta_cdf")
        types = {r._change_type for r in stream.batches[0]["items"].select("_change_type").distinct().collect()}
        assert types.issubset({"insert", "update_postimage", "update_preimage", "delete"})

    def test_updates_have_pre_and_post(self, spark):
        # Stateless engine needs update_period batches before updates appear.
        # With rows=100, default config, update_period=20 -- use enough batches.
        stream = generate_cdc(spark, _plan(rows=10), num_batches=3, format="delta_cdf")
        # Union all batches to find any update
        from functools import reduce

        all_batches = reduce(lambda a, b: a.unionByName(b), [stream.batches[i]["items"] for i in range(3)])
        pre_count = all_batches.filter("_change_type = 'update_preimage'").count()
        post_count = all_batches.filter("_change_type = 'update_postimage'").count()
        assert pre_count == post_count
        assert pre_count > 0

    def test_initial_all_inserts(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="delta_cdf")
        types = {r._change_type for r in stream.initial["items"].select("_change_type").distinct().collect()}
        assert types == {"insert"}


# ---------------------------------------------------------------------------
# SQL Server format
# ---------------------------------------------------------------------------


class TestSQLServerFormat:
    def test_columns_present(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="sql_server")
        cols = stream.batches[0]["items"].columns
        assert "__$operation" in cols
        assert "__$start_lsn" in cols
        assert "__$seqval" in cols
        assert "_op" not in cols

    def test_operation_codes(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="sql_server")
        op_codes = {r["__$operation"] for r in stream.batches[0]["items"].select("__$operation").distinct().collect()}
        # 1=delete, 2=insert, 3=update before, 4=update after
        assert op_codes.issubset({1, 2, 3, 4})

    def test_update_before_after_codes(self, spark):
        # Stateless engine needs update_period batches before updates appear.
        stream = generate_cdc(spark, _plan(rows=10), num_batches=3, format="sql_server")
        from functools import reduce

        all_batches = reduce(lambda a, b: a.unionByName(b), [stream.batches[i]["items"] for i in range(3)])
        before_count = all_batches.filter("`__$operation` = 3").count()
        after_count = all_batches.filter("`__$operation` = 4").count()
        assert before_count == after_count
        assert before_count > 0

    def test_lsn_not_null(self, spark):
        stream = generate_cdc(spark, _plan(), num_batches=1, format="sql_server")
        null_count = stream.batches[0]["items"].filter("`__$start_lsn` IS NULL").count()
        assert null_count == 0

    def test_rename_cdc_columns_helper(self, spark):
        from dbldatagen.core.spec.cdc_dsl import rename_cdc_columns

        stream = generate_cdc(spark, _plan(), num_batches=1, format="sql_server")
        df = rename_cdc_columns(stream.batches[0]["items"])
        assert "cdc_operation" in df.columns
        assert "cdc_lsn" in df.columns
        assert "cdc_seqval" in df.columns
        assert "__$operation" not in df.columns


# ---------------------------------------------------------------------------
# Debezium format
# ---------------------------------------------------------------------------


class TestDebeziumFormat:
    """Debezium was removed from ``CDCFormat`` entirely.

    The earlier flattened approximation silently dropped ``UB`` rows,
    producing structurally wrong output for real Debezium consumers
    (Kafka Connect, Flink).  The enum member was removed so any plan
    that references ``"debezium"`` fails at construction rather than
    at job execution.  Re-add once a faithful nested-struct
    implementation lands.
    """

    def test_enum_no_longer_accepts_debezium(self):
        from dbldatagen.core.spec.cdc_schema import CDCFormat

        with pytest.raises(ValueError):
            CDCFormat("debezium")

    def test_plan_rejects_debezium_at_construction(self):
        # format="debezium" flows through CDCFormat(str) validator in
        # generate_cdc.  With DEBEZIUM removed from the enum, the
        # cast raises ValueError before any Spark plan is built.
        from dbldatagen.core.spec.cdc_schema import CDCPlan

        with pytest.raises(ValueError):
            # Deliberately violating the annotation to exercise the runtime
            # CDCFormat(str) validator — the cast is what raises.
            CDCPlan(base_plan=_plan(), format="debezium")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Format consistency
# ---------------------------------------------------------------------------


class TestFormatConsistency:
    def test_all_implemented_formats_produce_data(self, spark):
        # Debezium is intentionally excluded — it raises NotImplementedError
        # until a faithful nested before/after implementation lands.
        for fmt in ["raw", "delta_cdf", "sql_server"]:
            stream = generate_cdc(spark, _plan(rows=50), num_batches=1, format=fmt)
            count = stream.batches[0]["items"].count()
            assert count > 0, f"Format {fmt} produced empty batch"

    def test_raw_and_delta_same_logical_changes(self, spark):
        """Raw and Delta CDF should have the same number of rows (both include pre+post)."""
        plan = _plan(rows=50, seed=42)
        raw = generate_cdc(spark, plan, num_batches=1, format="raw")
        delta = generate_cdc(spark, plan, num_batches=1, format="delta_cdf")
        assert raw.batches[0]["items"].count() == delta.batches[0]["items"].count()
