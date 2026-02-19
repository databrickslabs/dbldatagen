"""Tests for ingest_schema.py -- Pydantic models for ingestion simulation."""

from __future__ import annotations

import pytest

from dbldatagen.v1.ingest_schema import (
    IngestMode,
    IngestPlan,
    IngestStrategy,
    IngestTableConfig,
)
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
)


def _simple_plan():
    return DataGenPlan(
        tables=[
            TableSpec(
                name="orders",
                rows=1000,
                primary_key=PrimaryKey(columns=["order_id"]),
                columns=[
                    ColumnSpec(name="order_id", dtype="long", gen=SequenceColumn(start=1)),
                    ColumnSpec(name="amount", dtype="double", gen=RangeColumn(min=1.0, max=100.0)),
                ],
            ),
        ],
        seed=42,
    )


class TestIngestTableConfig:
    def test_defaults(self):
        cfg = IngestTableConfig()
        assert cfg.insert_fraction == 0.6
        assert cfg.update_fraction == 0.3
        assert cfg.delete_fraction == 0.1
        assert cfg.update_window is None
        assert cfg.min_life == 1

    def test_custom_fractions(self):
        cfg = IngestTableConfig(insert_fraction=0.5, update_fraction=0.5, delete_fraction=0.0)
        assert cfg.insert_fraction == 0.5
        assert cfg.update_fraction == 0.5
        assert cfg.delete_fraction == 0.0

    def test_invalid_fractions_sum(self):
        with pytest.raises(ValueError, match="sum to ~1.0"):
            IngestTableConfig(insert_fraction=0.5, update_fraction=0.1, delete_fraction=0.1)

    def test_all_zero_fractions(self):
        with pytest.raises(ValueError, match="positive"):
            IngestTableConfig(insert_fraction=0.0, update_fraction=0.0, delete_fraction=0.0)

    def test_batch_size_string(self):
        cfg = IngestTableConfig(batch_size="100K")
        assert cfg.batch_size == 100_000

    def test_batch_size_string_million(self):
        cfg = IngestTableConfig(batch_size="1M")
        assert cfg.batch_size == 1_000_000

    def test_batch_size_float(self):
        cfg = IngestTableConfig(batch_size=0.1)
        assert cfg.batch_size == 0.1

    def test_update_window(self):
        cfg = IngestTableConfig(update_window=5)
        assert cfg.update_window == 5


class TestIngestPlan:
    def test_defaults(self):
        plan = IngestPlan(base_plan=_simple_plan())
        assert plan.num_batches == 5
        assert plan.mode == IngestMode.INCREMENTAL
        assert plan.strategy == IngestStrategy.SYNTHETIC
        assert plan.ingest_tables == ["orders"]
        assert plan.include_batch_id is True
        assert plan.include_load_timestamp is True

    def test_snapshot_mode(self):
        plan = IngestPlan(base_plan=_simple_plan(), mode=IngestMode.SNAPSHOT)
        assert plan.mode == IngestMode.SNAPSHOT

    def test_delta_strategy(self):
        plan = IngestPlan(base_plan=_simple_plan(), strategy=IngestStrategy.DELTA)
        assert plan.strategy == IngestStrategy.DELTA

    def test_invalid_table_ref(self):
        with pytest.raises(ValueError, match="unknown table"):
            IngestPlan(
                base_plan=_simple_plan(),
                table_configs={"nonexistent": IngestTableConfig()},
            )

    def test_invalid_ingest_tables_ref(self):
        with pytest.raises(ValueError, match="unknown table"):
            IngestPlan(
                base_plan=_simple_plan(),
                ingest_tables=["nonexistent"],
            )

    def test_custom_table_config(self):
        plan = IngestPlan(
            base_plan=_simple_plan(),
            table_configs={
                "orders": IngestTableConfig(
                    batch_size=500,
                    insert_fraction=0.7,
                    update_fraction=0.2,
                    delete_fraction=0.1,
                    update_window=3,
                ),
            },
        )
        cfg = plan.config_for("orders")
        assert cfg.batch_size == 500
        assert cfg.update_window == 3

    def test_config_for_default_fallback(self):
        plan = IngestPlan(base_plan=_simple_plan())
        cfg = plan.config_for("orders")
        assert cfg.insert_fraction == 0.6  # default

    def test_populate_ingest_tables(self):
        base = DataGenPlan(
            tables=[
                TableSpec(
                    name="t1",
                    rows=100,
                    columns=[ColumnSpec(name="a", dtype="int", gen=RangeColumn())],
                ),
                TableSpec(
                    name="t2",
                    rows=200,
                    columns=[ColumnSpec(name="b", dtype="int", gen=RangeColumn())],
                ),
            ],
        )
        plan = IngestPlan(base_plan=base)
        assert plan.ingest_tables == ["t1", "t2"]

    def test_explicit_ingest_tables(self):
        base = DataGenPlan(
            tables=[
                TableSpec(
                    name="t1",
                    rows=100,
                    columns=[ColumnSpec(name="a", dtype="int", gen=RangeColumn())],
                ),
                TableSpec(
                    name="t2",
                    rows=200,
                    columns=[ColumnSpec(name="b", dtype="int", gen=RangeColumn())],
                ),
            ],
        )
        plan = IngestPlan(base_plan=base, ingest_tables=["t1"])
        assert plan.ingest_tables == ["t1"]

    def test_batch_interval(self):
        plan = IngestPlan(base_plan=_simple_plan(), batch_interval_seconds=3600)
        assert plan.batch_interval_seconds == 3600


class TestIngestMode:
    def test_enum_values(self):
        assert IngestMode.INCREMENTAL == "incremental"
        assert IngestMode.SNAPSHOT == "snapshot"


class TestIngestStrategy:
    def test_enum_values(self):
        assert IngestStrategy.SYNTHETIC == "synthetic"
        assert IngestStrategy.DELTA == "delta"
