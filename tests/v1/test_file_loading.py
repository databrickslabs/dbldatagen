"""Tests for loading DataGenPlan from JSON and YAML files."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from dbldatagen.v1 import generate
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
)
from dbldatagen.v1.validation import validate_referential_integrity

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_json_plan() -> DataGenPlan:
    raw = json.loads((FIXTURES / "plan.json").read_text())
    return DataGenPlan.model_validate(raw)


def _load_yaml_plan() -> DataGenPlan:
    raw = yaml.safe_load((FIXTURES / "plan.yml").read_text())
    return DataGenPlan.model_validate(raw)


def _assert_plan_generates_correctly(spark, plan: DataGenPlan):
    """Shared assertions for any loaded plan."""
    dfs = generate(spark, plan)

    # correct tables present
    assert set(dfs.keys()) == {"customers", "orders"}

    # row counts match spec
    assert dfs["customers"].count() == 200
    assert dfs["orders"].count() == 1000

    # PK uniqueness
    for tbl, pk in [("customers", "customer_id"), ("orders", "order_id")]:
        df = dfs[tbl]
        assert df.count() == df.select(pk).distinct().count(), f"Duplicate PKs in {tbl}"

    # FK referential integrity
    errors = validate_referential_integrity(dfs, plan)
    assert errors == [], f"FK integrity errors: {errors}"

    # expected columns present
    assert set(dfs["customers"].columns) == {"customer_id", "name", "signup_date"}
    assert set(dfs["orders"].columns) == {"order_id", "customer_id", "amount", "status"}

    return dfs


# ---------------------------------------------------------------------------
# JSON tests
# ---------------------------------------------------------------------------


class TestJsonPlan:
    def test_json_loads_valid_plan(self):
        plan = _load_json_plan()
        assert isinstance(plan, DataGenPlan)
        assert len(plan.tables) == 2
        assert plan.seed == 42

    def test_json_generates_correctly(self, spark):
        plan = _load_json_plan()
        _assert_plan_generates_correctly(spark, plan)

    def test_json_determinism(self, spark):
        plan = _load_json_plan()
        dfs1 = generate(spark, plan)
        dfs2 = generate(spark, plan)

        for name in ["customers", "orders"]:
            rows1 = [tuple(r) for r in dfs1[name].orderBy(name[:-1] + "_id").collect()]
            rows2 = [tuple(r) for r in dfs2[name].orderBy(name[:-1] + "_id").collect()]
            assert rows1 == rows2, f"{name} not deterministic from JSON"


# ---------------------------------------------------------------------------
# YAML tests
# ---------------------------------------------------------------------------


class TestYamlPlan:
    def test_yaml_loads_valid_plan(self):
        plan = _load_yaml_plan()
        assert isinstance(plan, DataGenPlan)
        assert len(plan.tables) == 2
        assert plan.seed == 42

    def test_yaml_generates_correctly(self, spark):
        plan = _load_yaml_plan()
        _assert_plan_generates_correctly(spark, plan)

    def test_yaml_determinism(self, spark):
        plan = _load_yaml_plan()
        dfs1 = generate(spark, plan)
        dfs2 = generate(spark, plan)

        for name in ["customers", "orders"]:
            rows1 = [tuple(r) for r in dfs1[name].orderBy(name[:-1] + "_id").collect()]
            rows2 = [tuple(r) for r in dfs2[name].orderBy(name[:-1] + "_id").collect()]
            assert rows1 == rows2, f"{name} not deterministic from YAML"

    def test_yaml_and_json_produce_same_plan(self):
        json_plan = _load_json_plan()
        yaml_plan = _load_yaml_plan()
        assert json_plan == yaml_plan


# ---------------------------------------------------------------------------
# Round-trip tests: programmatic -> file -> generate
# ---------------------------------------------------------------------------


class TestRoundTrip:
    @staticmethod
    def _make_plan() -> DataGenPlan:
        customers = TableSpec(
            name="customers",
            rows=100,
            primary_key=PrimaryKey(columns=["customer_id"]),
            columns=[
                ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="name", gen=ConstantColumn(value="test")),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=500,
            primary_key=PrimaryKey(columns=["order_id"]),
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="customer_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.customer_id"),
                ),
                ColumnSpec(
                    name="amount",
                    dtype=DataType.DOUBLE,
                    gen=RangeColumn(min=10, max=500),
                ),
            ],
        )
        return DataGenPlan(tables=[customers, orders], seed=99)

    def test_json_round_trip_generates(self, spark, tmp_path):
        """Programmatic plan -> JSON file -> reload -> generate -> verify."""
        plan = self._make_plan()

        # dump to JSON file
        json_file = tmp_path / "plan.json"
        json_file.write_text(plan.model_dump_json(indent=2))

        # reload from file
        reloaded = DataGenPlan.model_validate_json(json_file.read_text())
        assert reloaded == plan

        # generate and verify
        dfs = generate(spark, reloaded)
        assert dfs["customers"].count() == 100
        assert dfs["orders"].count() == 500
        errors = validate_referential_integrity(dfs, reloaded)
        assert errors == []

    def test_yaml_round_trip_generates(self, spark, tmp_path):
        """Programmatic plan -> YAML file -> reload -> generate -> verify."""
        plan = self._make_plan()

        # dump to YAML file
        yaml_file = tmp_path / "plan.yml"
        yaml_file.write_text(yaml.dump(plan.model_dump(mode="json"), default_flow_style=False))

        # reload from file
        raw = yaml.safe_load(yaml_file.read_text())
        reloaded = DataGenPlan.model_validate(raw)
        assert reloaded == plan

        # generate and verify
        dfs = generate(spark, reloaded)
        assert dfs["customers"].count() == 100
        assert dfs["orders"].count() == 500
        errors = validate_referential_integrity(dfs, reloaded)
        assert errors == []
