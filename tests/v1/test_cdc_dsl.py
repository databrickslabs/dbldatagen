"""Tests for CDC DSL convenience constructors and presets."""

from __future__ import annotations

from dbldatagen.v1.cdc_dsl import (
    append_only_config,
    cdc_config,
    cdc_plan,
    high_churn_config,
    mutations,
    ops,
    scd2_config,
)
from dbldatagen.v1.cdc_schema import CDCFormat, CDCPlan, OperationWeights
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    PrimaryKey,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)


def _base():
    return DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="users",
                rows=100,
                primary_key=PrimaryKey(columns=["id"]),
                columns=[
                    ColumnSpec(name="id", gen=SequenceColumn()),
                    ColumnSpec(name="name", gen=ValuesColumn(values=["A", "B"])),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# ops()
# ---------------------------------------------------------------------------


class TestOps:
    def test_default(self):
        w = ops()
        assert w.insert == 3.0
        assert w.update == 5.0
        assert w.delete == 2.0

    def test_custom(self):
        w = ops(1, 8, 1)
        ins, upd, dlt = w.fractions
        assert abs(ins - 0.1) < 0.01
        assert abs(upd - 0.8) < 0.01
        assert abs(dlt - 0.1) < 0.01


# ---------------------------------------------------------------------------
# mutations()
# ---------------------------------------------------------------------------


class TestMutations:
    def test_default(self):
        m = mutations()
        assert m.columns is None
        assert m.fraction == 0.5

    def test_custom(self):
        m = mutations(columns=["name"], fraction=1.0)
        assert m.columns == ["name"]


# ---------------------------------------------------------------------------
# cdc_config()
# ---------------------------------------------------------------------------


class TestCDCConfig:
    def test_defaults(self):
        c = cdc_config()
        assert c.batch_size == 0.1
        assert isinstance(c.operations, OperationWeights)

    def test_custom(self):
        c = cdc_config(batch_size=50, operations=ops(1, 1, 1))
        assert c.batch_size == 50
        assert c.operations.insert == 1


# ---------------------------------------------------------------------------
# cdc_plan()
# ---------------------------------------------------------------------------


class TestCDCPlanDSL:
    def test_minimal(self):
        p = cdc_plan(_base())
        assert isinstance(p, CDCPlan)
        assert p.num_batches == 5
        assert p.format == CDCFormat.RAW
        assert "users" in p.cdc_tables

    def test_with_format(self):
        p = cdc_plan(_base(), fmt="delta_cdf")
        assert p.format == CDCFormat.DELTA_CDF

    def test_with_table_config(self):
        p = cdc_plan(
            _base(),
            num_batches=10,
            users=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
        )
        assert p.num_batches == 10
        cfg = p.config_for("users")
        assert cfg.batch_size == 0.05
        assert cfg.operations.insert == 2

    def test_with_multiple_tables(self):
        base = DataGenPlan(
            seed=42,
            tables=[
                TableSpec(
                    name="a",
                    rows=50,
                    primary_key=PrimaryKey(columns=["id"]),
                    columns=[ColumnSpec(name="id", gen=SequenceColumn())],
                ),
                TableSpec(
                    name="b",
                    rows=50,
                    primary_key=PrimaryKey(columns=["id"]),
                    columns=[ColumnSpec(name="id", gen=SequenceColumn())],
                ),
            ],
        )
        p = cdc_plan(base, a=cdc_config(batch_size=10), b=cdc_config(batch_size=20))
        assert p.config_for("a").batch_size == 10
        assert p.config_for("b").batch_size == 20


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------


class TestPresets:
    def test_append_only(self):
        c = append_only_config()
        assert c.operations.insert == 1
        assert c.operations.update == 0
        assert c.operations.delete == 0

    def test_append_only_custom_size(self):
        c = append_only_config(batch_size=500)
        assert c.batch_size == 500

    def test_high_churn(self):
        c = high_churn_config()
        assert c.operations.update == 6
        assert c.operations.delete == 3
        assert c.batch_size == 0.2

    def test_scd2(self):
        c = scd2_config()
        ins, upd, dlt = c.operations.fractions
        assert upd > ins
        assert upd > dlt

    def test_presets_in_plan(self):
        p = cdc_plan(_base(), users=scd2_config())
        cfg = p.config_for("users")
        assert cfg.operations.update == 8
