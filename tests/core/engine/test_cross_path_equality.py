"""Cross-path byte-equality harness.

The engine's determinism contract is: for a given
``(global_seed, table, column, row_index, batch_id)``, every generation
path produces **identical** output.  Historically this has been broken
in subtle ways that slipped past isolated tests:

- ``8b54e54`` — CDC bulk path used the wrong seed for table index > 0.
- ``15c7d49`` — struct field seeds collapsed on the fused multi-batch
  path; pre-image values diverged from single-batch.
- ``7fe0475`` — struct child seed derivation used XOR in the Column
  branch and polynomial hash in the int branch; bulk UB rows disagreed
  with per-batch UB rows for the same (pk, batch_id).

Each of those bugs survived past review because no single test
cross-compared the full column matrix between paths.  This file is
that test: one rich plan exercising every column strategy, run through
both scalar (``generate_cdc`` per-batch) and bulk
(``generate_cdc_bulk`` with chunk_size > 1), with every (pk, batch_id,
op) tuple required to carry identical column values on both paths.

Also pins the oracle↔initial-snapshot invariant: the driver-side
``generate_expected_state`` at batch_id=0 must match the CDC initial
snapshot byte-for-byte.
"""

from __future__ import annotations

import pytest
from pyspark.sql import Row

from dbldatagen.core.engine.cdc import generate_cdc, generate_cdc_bulk, generate_expected_state
from dbldatagen.core.spec.cdc_schema import CDCPlan
from dbldatagen.core.spec.dsl import (
    array,
    integer,
    pk_auto,
    struct,
    text,
)
from dbldatagen.core.spec.schema import (
    ArrayColumn,
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    DataType,
    ExpressionColumn,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    StructColumn,
    TableSpec,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
)


def _everything_plan(rows: int = 80, seed: int = 42) -> DataGenPlan:
    """One table exercising every non-Faker column strategy.

    Faker is deliberately excluded — the pandas-UDF path doesn't
    participate in the scalar/fused/bulk Spark-SQL rewrite the other
    strategies do, so the byte-equality invariant covers a different
    class of correctness (pool pickling, seed mix) tested separately.
    """
    return DataGenPlan(
        seed=seed,
        tables=[
            TableSpec(
                name="everything",
                rows=rows,
                primary_key=PrimaryKey(columns=["pk"]),
                columns=[
                    pk_auto("pk"),
                    ColumnSpec(
                        name="range_int",
                        dtype=DataType.INT,
                        gen=RangeColumn(min=1, max=1000),
                    ),
                    ColumnSpec(
                        name="range_decimal",
                        dtype=DataType.DECIMAL,
                        gen=RangeColumn(min=0.0, max=9999.99),
                        precision=10,
                        scale=2,
                    ),
                    ColumnSpec(
                        name="vals",
                        gen=ValuesColumn(values=["alpha", "beta", "gamma", "delta"]),
                    ),
                    ColumnSpec(
                        name="pat",
                        gen=PatternColumn(template="ORD-{digit:5}-{alpha:3}"),
                    ),
                    ColumnSpec(
                        name="seq",
                        dtype=DataType.LONG,
                        gen=SequenceColumn(start=1000, step=3),
                    ),
                    ColumnSpec(name="uuid_col", gen=UUIDColumn()),
                    ColumnSpec(
                        name="ts",
                        gen=TimestampColumn(start="2024-01-01", end="2024-12-31"),
                    ),
                    ColumnSpec(
                        name="const",
                        gen=ConstantColumn(value="fixed"),
                    ),
                    ColumnSpec(
                        name="expr_col",
                        gen=ExpressionColumn(expr="range_int * 2"),
                    ),
                    struct(
                        "addr",
                        [
                            text("city", ["Austin", "NYC", "LA", "Chicago"]),
                            integer("zip", min=10000, max=99999),
                        ],
                    ),
                    array(
                        "tags",
                        ValuesColumn(values=["red", "green", "blue", "yellow"]),
                        min_length=2,
                        max_length=4,
                    ),
                ],
            ),
        ],
    )


def _row_key(row: Row) -> tuple[int, int, str]:
    """Composite sort key for deterministic matching across paths."""
    return (row.pk, row._batch_id, row._op)


def _collect_and_index(stream_batches) -> dict[tuple[int, int, str], Row]:
    """Flatten a CDC stream's per-batch DataFrames into one dict keyed by
    (pk, batch_id, op).  Covers I / U / UB / D — every row type the
    engine emits.
    """
    indexed: dict[tuple[int, int, str], Row] = {}
    for b in stream_batches:
        for r in b["everything"].collect():
            indexed[_row_key(r)] = r
    return indexed


def _assert_rows_equal(label: str, a: Row, b: Row) -> None:
    """Compare two Spark Rows field-by-field with a clear diff on mismatch.

    Plain ``assert a == b`` on Row gives a terse "Row != Row" that
    hides which column diverged; we want the bug report from the
    harness to name the offending (pk, batch_id, op, column).
    """
    assert a.asDict(recursive=True) == b.asDict(recursive=True), (
        f"cross-path row mismatch at {label}\n"
        f"  scalar: {a.asDict(recursive=True)}\n"
        f"  bulk:   {b.asDict(recursive=True)}"
    )


@pytest.mark.parametrize("seed", [42, 1, 2**31 - 1])
class TestCrossPathByteEquality:
    """For every seed in the parametrize, scalar-per-batch and bulk-fused
    paths must produce byte-identical CDC streams on a plan exercising
    all column strategies.  Any divergence indicates a seed-derivation
    or expression-builder inconsistency between the two paths — the
    class of bug that has bitten this codebase repeatedly.
    """

    NUM_BATCHES = 3

    def test_initial_snapshot_matches(self, spark, seed):
        per = generate_cdc(spark, _everything_plan(seed=seed), num_batches=self.NUM_BATCHES)
        bulk = generate_cdc_bulk(
            spark, _everything_plan(seed=seed), num_batches=self.NUM_BATCHES, chunk_size=self.NUM_BATCHES
        )
        per_rows = per.initial["everything"].orderBy("pk").collect()
        bulk_rows = bulk.initial["everything"].orderBy("pk").collect()
        assert len(per_rows) == len(bulk_rows), (
            f"initial row count diverges: scalar={len(per_rows)}, bulk={len(bulk_rows)}"
        )
        for p, b in zip(per_rows, bulk_rows):
            _assert_rows_equal(f"initial pk={p.pk}", p, b)

    def test_batch_rows_match(self, spark, seed):
        """Per-batch and bulk paths must emit the same set of (pk, batch_id, op)
        tuples with identical column values."""
        per = generate_cdc(spark, _everything_plan(seed=seed), num_batches=self.NUM_BATCHES)
        bulk = generate_cdc_bulk(
            spark, _everything_plan(seed=seed), num_batches=self.NUM_BATCHES, chunk_size=self.NUM_BATCHES
        )

        per_idx = _collect_and_index(per.batches)
        bulk_idx = _collect_and_index(bulk.batches)

        assert per_idx, "scalar path emitted no batch rows — test plan too small"
        missing_in_bulk = set(per_idx) - set(bulk_idx)
        missing_in_scalar = set(bulk_idx) - set(per_idx)
        assert not missing_in_bulk and not missing_in_scalar, (
            f"(pk, batch_id, op) tuple disagreement\n"
            f"  only in scalar: {sorted(missing_in_bulk)[:5]}...\n"
            f"  only in bulk:   {sorted(missing_in_scalar)[:5]}..."
        )

        for key in sorted(per_idx):
            _assert_rows_equal(f"pk={key[0]} batch={key[1]} op={key[2]}", per_idx[key], bulk_idx[key])


class TestOracleMatchesInitialSnapshot:
    """generate_expected_state(batch_id=0) is the driver-side reference
    for the live state at the start of the stream.  It must match the
    CDC initial snapshot byte-for-byte, or any test that uses the
    oracle as ground truth is comparing apples to oranges.
    """

    def test_oracle_equals_initial(self, spark):
        plan = CDCPlan(base_plan=_everything_plan(rows=50))
        stream = generate_cdc(spark, plan, num_batches=2)
        oracle = generate_expected_state(spark, plan, "everything", batch_id=0)

        initial_rows = stream.initial["everything"].drop("_op", "_batch_id", "_ts").orderBy("pk").collect()
        oracle_rows = oracle.orderBy("pk").collect()

        assert len(initial_rows) == len(oracle_rows), (
            f"oracle / initial row count diverges: initial={len(initial_rows)}, oracle={len(oracle_rows)}"
        )
        for i, o in zip(initial_rows, oracle_rows):
            _assert_rows_equal(f"pk={i.pk}", i, o)
