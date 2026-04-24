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
from dbldatagen.core.spec.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights
from dbldatagen.core.spec.dsl import (
    array,
    integer,
    pk_auto,
    struct,
    text,
)
from dbldatagen.core.spec.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    DataType,
    ExpressionColumn,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
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
                            # Struct-of-struct: pins the nested-struct
                            # seed-derivation path, which previously raised
                            # at plan time on the fused multi-batch path
                            # because ``_build_struct_column`` didn't
                            # forward ``dyn_ctx`` to its recursive call.
                            struct(
                                "geo",
                                [
                                    integer("lat", min=-90, max=90),
                                    integer("lon", min=-180, max=180),
                                ],
                            ),
                        ],
                    ),
                    array(
                        "tags",
                        ValuesColumn(values=["red", "green", "blue", "yellow"]),
                        min_length=2,
                        max_length=4,
                    ),
                    # Pins min_length=0: some rows get empty arrays
                    # via ``F.slice(full_array, 1, 0)``.  All paths
                    # (scalar, fused, bulk, oracle) must agree on which
                    # rows produce empty arrays vs populated ones --
                    # the length hash was recently decorrelated from
                    # element[0]'s cell seed, so a length-hash change
                    # would have gone unnoticed without this coverage.
                    array(
                        "opt_codes",
                        RangeColumn(min=1, max=999),
                        min_length=0,
                        max_length=3,
                    ),
                ],
            ),
        ],
    )


def _row_key(row: Row) -> tuple[int, int, str]:
    """Composite sort key for deterministic matching across paths."""
    return (row.pk, row._batch_id, row._op)


def _collect_and_index(stream_batches, table_name: str = "everything") -> dict[tuple[int, int, str], Row]:
    """Flatten a CDC stream's per-batch DataFrames into one dict keyed by
    (pk, batch_id, op).  Covers I / U / UB / D — every row type the
    engine emits.
    """
    indexed: dict[tuple[int, int, str], Row] = {}
    for b in stream_batches:
        for r in b[table_name].collect():
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
        assert len(per_rows) == len(
            bulk_rows
        ), f"initial row count diverges: scalar={len(per_rows)}, bulk={len(bulk_rows)}"
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


class TestCrossPathTimestampsTzIndependent:
    """``_ts`` must agree between fused and single-batch paths under ANY
    ``spark.sql.session.timeZone`` — not just UTC.

    The fused path computes ``F.lit(base_epoch).cast("long").cast("timestamp")``
    (session-TZ-independent because long → timestamp reads as seconds
    UTC).  The single-batch path previously formatted the timestamp as a
    local-naive string and cast string → timestamp (session-TZ-dependent),
    so the two paths diverged whenever ``session.timeZone != UTC``.

    Run the batch-row equality check under a non-UTC session TZ to pin
    the fix.  Without the refactor that routed both paths through
    ``batch_timestamp_epoch`` + long-cast, this would fail with every
    ``_ts`` differing by the TZ offset.
    """

    def test_batch_ts_matches_under_non_utc_session(self, spark):
        key = "spark.sql.session.timeZone"
        prev = spark.conf.get(key, None)
        # Move the ``set`` inside the try so a raise here still restores
        # ``prev`` via the finally block — without this, a failure to
        # apply the non-UTC TZ would leak state to sibling tests.
        try:
            spark.conf.set(key, "America/Los_Angeles")
            num_b = 3
            per = generate_cdc(spark, _everything_plan(seed=42, rows=50), num_batches=num_b)
            bulk = generate_cdc_bulk(spark, _everything_plan(seed=42, rows=50), num_batches=num_b, chunk_size=num_b)

            per_idx = _collect_and_index(per.batches)
            bulk_idx = _collect_and_index(bulk.batches)

            assert per_idx and set(per_idx) == set(bulk_idx)

            for key_tuple in sorted(per_idx):
                p, b = per_idx[key_tuple], bulk_idx[key_tuple]
                assert p._ts == b._ts, (
                    f"_ts diverges under non-UTC session TZ at {key_tuple}: " f"scalar={p._ts} vs bulk={b._ts}"
                )
        finally:
            if prev is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, prev)


class TestCrossPathMultiUpdatePerChunk:
    """Regression test for the fused-updates under-emission bug.

    Scenario: ``update_period`` much smaller than chunk span, so each
    row should fire several update events inside one chunk.  The
    scalar per-batch path (``update_indices_at_batch``) emits all
    matching ``k`` values at every batch; the fused path
    (``generate_fused_updates``) computed a single ``candidate_b`` per
    row, emitting only the first in-chunk match and silently dropping
    every subsequent update for the same row.

    Construction:
        initial_rows=20, batch_size=10, weights (0, 10, 0) ->
        update_period = 20 // 10 = 2
        num_batches=6 with chunk_size=6 -> single fused chunk of 6
        batches; each row should fire ~3 updates.

    Without the multi-update fix, scalar emits ~6x as many U/UB rows
    as bulk for each row's batches, so the (pk, batch, op) tuple sets
    diverge visibly.
    """

    @staticmethod
    def _high_update_plan(rows: int = 20, seed: int = 42) -> DataGenPlan:
        return DataGenPlan(
            seed=seed,
            tables=[
                TableSpec(
                    name="items",
                    rows=rows,
                    primary_key=PrimaryKey(columns=["pk"]),
                    columns=[
                        pk_auto("pk"),
                        ColumnSpec(
                            name="v",
                            dtype=DataType.INT,
                            gen=RangeColumn(min=1, max=1000),
                        ),
                    ],
                ),
            ],
        )

    def test_multi_update_per_chunk_matches_scalar(self, spark):
        """Scalar and bulk must emit the same (pk, batch_id, op) set
        when one chunk contains multiple update_period boundaries."""
        plan = CDCPlan(
            base_plan=self._high_update_plan(),
            num_batches=6,
            table_configs={
                "items": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=0, update=10, delete=0),
                    min_life=1,
                ),
            },
        )
        per = generate_cdc(
            spark,
            CDCPlan(
                base_plan=self._high_update_plan(),
                num_batches=6,
                table_configs=plan.table_configs,
            ),
            num_batches=6,
        )
        bulk = generate_cdc_bulk(
            spark,
            plan,
            num_batches=6,
            chunk_size=6,  # one chunk -> fused path sees all 6 batches
        )
        per_idx = _collect_and_index(per.batches, table_name="items")
        bulk_idx = _collect_and_index(bulk.batches, table_name="items")
        assert per_idx, "scalar path emitted no batch rows"
        only_scalar = set(per_idx) - set(bulk_idx)
        only_bulk = set(bulk_idx) - set(per_idx)
        assert not only_scalar and not only_bulk, (
            f"multi-update-per-chunk divergence: "
            f"scalar has {len(only_scalar)} extra, bulk has {len(only_bulk)} extra.\n"
            f"  only in scalar: {sorted(only_scalar)[:10]}...\n"
            f"  only in bulk:   {sorted(only_bulk)[:10]}..."
        )


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

        assert len(initial_rows) == len(
            oracle_rows
        ), f"oracle / initial row count diverges: initial={len(initial_rows)}, oracle={len(oracle_rows)}"
        for i, o in zip(initial_rows, oracle_rows):
            _assert_rows_equal(f"pk={i.pk}", i, o)
