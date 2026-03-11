"""Tests for CDC chunking behavior and auto chunk_size calculation.

Verifies that auto-chunking produces large enough DataFrames for
cluster parallelism, and that chunk_size=1 vs auto produces
identical data but different work unit sizes.

Run with::

    pytest tests/v1/test_cdc_chunking.py -v -s

The ``-s`` flag shows chunk sizing output.
"""

from __future__ import annotations

from dbldatagen.v1.cdc import (
    _auto_chunk_size,
    generate_cdc_bulk,
    write_cdc_to_delta,
)
from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights
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


def _mastercard_plan(
    initial_rows: int = 50_000_000,
    batch_size: int = 950_000,
    num_batches: int = 365,
) -> CDCPlan:
    """Mastercard-scale clearing table plan."""
    base = DataGenPlan(
        seed=2024,
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
                        name="clearing_amount",
                        dtype=DataType.DOUBLE,
                        gen=RangeColumn(min=0.01, max=25000.0),
                    ),
                    ColumnSpec(
                        name="currency",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["USD", "EUR", "GBP"]),
                    ),
                    ColumnSpec(
                        name="status",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["cleared", "pending"]),
                    ),
                ],
            ),
        ],
    )
    return CDCPlan(
        base_plan=base,
        num_batches=num_batches,
        table_configs={
            "clearing": CDCTableConfig(
                batch_size=batch_size,
                operations=OperationWeights(insert=6, update=3, delete=1),
                min_life=1,
            ),
        },
    )


class TestAutoChunkSize:
    """Verify _auto_chunk_size produces sensible chunk sizes."""

    def test_mastercard_scale(self):
        """50M rows, 950K batch_size → chunk_size should be ~16."""
        plan = _mastercard_plan()
        chunk = _auto_chunk_size(plan)

        # 950K batch + 285K before-images = ~1.235M per batch
        # 20M target / 1.235M = ~16 batches per chunk
        print(f"\nMastercard scale: chunk_size={chunk}")
        print(f"  Batches per chunk: {chunk}")
        print(f"  Total chunks: {-(-plan.num_batches // chunk)}")
        print(f"  Rows per chunk: ~{chunk * 1_235_000:,}")

        assert chunk >= 10, f"chunk_size too small ({chunk}), won't utilize cluster"
        assert chunk <= 30, f"chunk_size too large ({chunk}), memory risk"

    def test_small_batch_size(self):
        """Small batch_size → larger chunk_size to compensate."""
        plan = _mastercard_plan(initial_rows=1_000_000, batch_size=10_000, num_batches=100)
        chunk = _auto_chunk_size(plan)

        print(f"\nSmall batch: chunk_size={chunk}")
        assert chunk >= 100, f"Should group many small batches (got {chunk})"

    def test_large_batch_size(self):
        """Large batch_size → smaller chunk_size."""
        plan = _mastercard_plan(initial_rows=100_000_000, batch_size=5_000_000, num_batches=50)
        chunk = _auto_chunk_size(plan)

        print(f"\nLarge batch: chunk_size={chunk}")
        assert chunk <= 5, f"Should use few batches per chunk (got {chunk})"

    def test_chunk_size_never_exceeds_num_batches(self):
        plan = _mastercard_plan(initial_rows=1_000, batch_size=10, num_batches=5)
        chunk = _auto_chunk_size(plan)
        assert chunk <= 5


class TestChunkVsBatch:
    """Compare chunk_size=1 (old default) vs auto chunking."""

    def test_chunk1_vs_auto_row_counts(self, spark):
        """Both modes produce the same total rows, but auto has fewer chunks."""
        plan = _mastercard_plan(initial_rows=10_000, batch_size=1_000, num_batches=20)

        stream_1 = generate_cdc_bulk(spark, plan, chunk_size=1)
        stream_auto = generate_cdc_bulk(spark, plan)

        n_chunks_1 = len(stream_1.batches)
        n_chunks_auto = len(stream_auto.batches)

        print(f"\nchunk_size=1:    {n_chunks_1} chunks")
        print(f"chunk_size=auto: {n_chunks_auto} chunks")

        assert n_chunks_1 == 20, "chunk_size=1 should produce 1 chunk per batch"
        assert n_chunks_auto < n_chunks_1, (
            f"Auto chunking should produce fewer chunks " f"({n_chunks_auto} >= {n_chunks_1})"
        )

        # Count total rows from each mode
        total_1 = 0
        for chunk in stream_1.batches:
            for df in chunk.values():
                total_1 += df.count()

        total_auto = 0
        for chunk in stream_auto.batches:
            for df in chunk.values():
                total_auto += df.count()

        print(f"Total rows chunk_size=1:    {total_1:,}")
        print(f"Total rows chunk_size=auto: {total_auto:,}")
        assert total_1 == total_auto, f"Row count mismatch: chunk_size=1={total_1}, auto={total_auto}"

    def test_auto_chunks_are_larger(self, spark):
        """Auto-chunked DataFrames should have more rows per write."""
        plan = _mastercard_plan(initial_rows=10_000, batch_size=1_000, num_batches=20)

        stream_1 = generate_cdc_bulk(spark, plan, chunk_size=1)
        stream_auto = generate_cdc_bulk(spark, plan)

        # Measure rows per chunk for chunk_size=1
        rows_per_chunk_1 = []
        for chunk in stream_1.batches:
            chunk_rows = sum(df.count() for df in chunk.values())
            rows_per_chunk_1.append(chunk_rows)

        # Measure rows per chunk for auto
        rows_per_chunk_auto = []
        for chunk in stream_auto.batches:
            chunk_rows = sum(df.count() for df in chunk.values())
            rows_per_chunk_auto.append(chunk_rows)

        avg_1 = sum(rows_per_chunk_1) / len(rows_per_chunk_1)
        avg_auto = sum(rows_per_chunk_auto) / len(rows_per_chunk_auto)

        print(f"\nchunk_size=1:    avg {avg_1:,.0f} rows/chunk ({len(rows_per_chunk_1)} chunks)")
        print(f"chunk_size=auto: avg {avg_auto:,.0f} rows/chunk ({len(rows_per_chunk_auto)} chunks)")

        assert avg_auto > avg_1, f"Auto chunks should be larger: {avg_auto:.0f} <= {avg_1:.0f}"


class TestWriteCDCDefaultChunkSize:
    """Verify write_cdc_to_delta uses auto chunk_size by default."""

    def test_default_is_none(self):
        """write_cdc_to_delta should default to chunk_size=None (auto)."""
        import inspect

        sig = inspect.signature(write_cdc_to_delta)
        default = sig.parameters["chunk_size"].default
        assert default is None, f"write_cdc_to_delta chunk_size default should be None, got {default}"
