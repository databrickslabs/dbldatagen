"""Tests covering uncovered lines in dbldatagen.v1.ingest.

Targets:
- _normalize_plan() with string mode/strategy and IngestPlan input
- _generate_ingest_batch() stateless snapshot branch (line 159)
- _generate_ingest_batch() unknown strategy error (line 169)
- write_ingest_to_delta() end-to-end (lines 272-307)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dbldatagen.v1.ingest import (
    _generate_ingest_batch,
    _normalize_plan,
    generate_ingest,
    write_ingest_to_delta,
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
                ],
            ),
        ],
        seed=99,
    )


# ===================================================================
# _normalize_plan
# ===================================================================


class TestNormalizePlan:
    def test_string_mode_and_strategy_from_datagenplan(self):
        """String mode/strategy are converted to enums when input is DataGenPlan."""
        plan = _normalize_plan(_simple_plan(), num_batches=2, mode="snapshot", strategy="stateless")
        assert plan.mode == IngestMode.SNAPSHOT
        assert plan.strategy == IngestStrategy.STATELESS
        assert plan.num_batches == 2

    def test_string_mode_and_strategy_from_ingest_plan(self):
        """String mode/strategy are converted to enums when input is IngestPlan."""
        base = IngestPlan(base_plan=_simple_plan(), num_batches=3)
        plan = _normalize_plan(base, num_batches=7, mode="snapshot", strategy="stateless")
        assert plan.mode == IngestMode.SNAPSHOT
        assert plan.strategy == IngestStrategy.STATELESS
        assert plan.num_batches == 7

    def test_ingest_plan_passthrough_no_overrides(self):
        """IngestPlan passes through unchanged when no overrides given."""
        base = IngestPlan(base_plan=_simple_plan(), num_batches=4, mode=IngestMode.SNAPSHOT)
        plan = _normalize_plan(base, num_batches=None, mode=None, strategy=None)
        assert plan.num_batches == 4
        assert plan.mode == IngestMode.SNAPSHOT

    def test_unknown_strategy_raises(self):
        """Invalid strategy string raises ValueError."""
        with pytest.raises(ValueError):
            _normalize_plan(_simple_plan(), num_batches=1, mode=None, strategy="bogus")


# ===================================================================
# _generate_ingest_batch -- stateless snapshot branch
# ===================================================================


class TestStatelessSnapshotBranch:
    def test_stateless_snapshot_batch(self, spark):
        """Stateless strategy + snapshot mode hits the uncovered branch."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=2,
            mode=IngestMode.SNAPSHOT,
            strategy=IngestStrategy.STATELESS,
        )
        batch = _generate_ingest_batch(spark, plan, batch_id=1)
        assert "items" in batch
        assert batch["items"].count() > 0


# ===================================================================
# _generate_ingest_batch -- unknown strategy error
# ===================================================================


class TestUnknownStrategyError:
    def test_delta_strategy_raises(self, spark):
        """DELTA strategy without current_df raises ValueError."""
        plan = IngestPlan(
            base_plan=_simple_plan(),
            num_batches=1,
            mode=IngestMode.INCREMENTAL,
            strategy=IngestStrategy.DELTA,
        )
        with pytest.raises(ValueError, match="Delta strategy"):
            _generate_ingest_batch(spark, plan, batch_id=1)


# ===================================================================
# write_ingest_to_delta
# ===================================================================


class TestWriteIngestToDelta:
    def test_write_creates_tables(self, spark):
        """write_ingest_to_delta exercises all orchestration logic (lines 272-307).

        Delta is not available in local Spark, so we mock generate_ingest
        to return mock DataFrames with mock write paths.
        """
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        mock_df = MagicMock()
        mock_df.write = mock_writer

        mock_stream = MagicMock()
        mock_stream.initial = {"items": mock_df}
        mock_stream.batches = [{"items": mock_df}, {"items": mock_df}]

        mock_spark = MagicMock()

        with patch("dbldatagen.v1.ingest.generate_ingest", return_value=mock_stream), \
             patch("dbldatagen.v1.ingest.union_all", return_value=mock_df):
            result = write_ingest_to_delta(
                mock_spark,
                _simple_plan(rows=20),
                catalog="my_catalog",
                schema="my_schema",
                num_batches=2,
                mode="incremental",
                strategy="synthetic",
            )

        assert "items" in result
        assert result["items"] == "my_catalog.my_schema.items"
        # initial overwrite + 2 batch appends
        assert mock_writer.saveAsTable.call_count == 3
        mock_spark.sql.assert_called()

    def test_write_with_chunk_size(self, spark):
        """write_ingest_to_delta with chunk_size > 1 groups batches into fewer writes."""
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        mock_df = MagicMock()
        mock_df.write = mock_writer

        mock_stream = MagicMock()
        mock_stream.initial = {"items": mock_df}
        mock_stream.batches = [{"items": mock_df} for _ in range(4)]

        mock_spark = MagicMock()

        with patch("dbldatagen.v1.ingest.generate_ingest", return_value=mock_stream), \
             patch("dbldatagen.v1.ingest.union_all", return_value=mock_df):
            result = write_ingest_to_delta(
                mock_spark,
                _simple_plan(rows=20),
                catalog="my_catalog",
                schema="my_schema",
                num_batches=4,
                chunk_size=2,
            )

        assert "items" in result
        assert result["items"] == "my_catalog.my_schema.items"
        # initial overwrite + 2 chunk appends (4 batches / chunk_size 2)
        assert mock_writer.saveAsTable.call_count == 3
