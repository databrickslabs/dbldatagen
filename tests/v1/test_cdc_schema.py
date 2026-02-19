"""Tests for CDC schema models: validation, serialization, YAML loading."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dbldatagen.v1.cdc_schema import (
    CDCFormat,
    CDCPlan,
    CDCTableConfig,
    MutationSpec,
    OperationWeights,
)
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    PrimaryKey,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _simple_base_plan(**kwargs) -> DataGenPlan:
    return DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="users",
                rows=100,
                primary_key=PrimaryKey(columns=["user_id"]),
                columns=[
                    ColumnSpec(name="user_id", gen=SequenceColumn()),
                    ColumnSpec(name="name", gen=ValuesColumn(values=["A", "B", "C"])),
                ],
            ),
        ],
        **kwargs,
    )


# ---------------------------------------------------------------------------
# OperationWeights
# ---------------------------------------------------------------------------


class TestOperationWeights:
    def test_default_fractions(self):
        w = OperationWeights()
        ins, upd, delw = w.fractions
        assert abs(ins - 0.3) < 0.01
        assert abs(upd - 0.5) < 0.01
        assert abs(delw - 0.2) < 0.01

    def test_custom_fractions(self):
        w = OperationWeights(insert=1, update=1, delete=1)
        assert all(abs(f - 1 / 3) < 0.01 for f in w.fractions)

    def test_zero_weight_allowed(self):
        w = OperationWeights(insert=0, update=5, delete=0)
        ins, upd, delw = w.fractions
        assert ins == 0.0
        assert upd == 1.0
        assert delw == 0.0

    def test_all_zero_rejected(self):
        with pytest.raises(ValueError, match="positive"):
            OperationWeights(insert=0, update=0, delete=0)

    def test_serialization_round_trip(self):
        w = OperationWeights(insert=2, update=7, delete=1)
        data = w.model_dump()
        w2 = OperationWeights.model_validate(data)
        assert w2.insert == 2
        assert w2.update == 7
        assert w2.delete == 1


# ---------------------------------------------------------------------------
# CDCTableConfig
# ---------------------------------------------------------------------------


class TestCDCTableConfig:
    def test_defaults(self):
        c = CDCTableConfig()
        assert c.batch_size == 0.1
        assert c.operations.insert == 3.0

    def test_string_batch_size_k(self):
        c = CDCTableConfig(batch_size="10K")
        assert c.batch_size == 10_000

    def test_string_batch_size_m(self):
        c = CDCTableConfig(batch_size="1M")
        assert c.batch_size == 1_000_000

    def test_int_batch_size(self):
        c = CDCTableConfig(batch_size=500)
        assert c.batch_size == 500

    def test_float_fraction(self):
        c = CDCTableConfig(batch_size=0.05)
        assert c.batch_size == 0.05

    def test_min_life_default(self):
        c = CDCTableConfig()
        assert c.min_life == 3

    def test_min_life_custom(self):
        c = CDCTableConfig(min_life=10)
        assert c.min_life == 10

    def test_min_life_zero(self):
        c = CDCTableConfig(min_life=0)
        assert c.min_life == 0

    def test_min_life_serialization(self):
        c = CDCTableConfig(min_life=7)
        data = c.model_dump()
        c2 = CDCTableConfig.model_validate(data)
        assert c2.min_life == 7


# ---------------------------------------------------------------------------
# MutationSpec
# ---------------------------------------------------------------------------


class TestMutationSpec:
    def test_defaults(self):
        m = MutationSpec()
        assert m.columns is None
        assert m.fraction == 0.5

    def test_custom(self):
        m = MutationSpec(columns=["name", "score"], fraction=0.8)
        assert m.columns == ["name", "score"]
        assert m.fraction == 0.8


# ---------------------------------------------------------------------------
# CDCPlan validation
# ---------------------------------------------------------------------------


class TestCDCPlanValidation:
    def test_valid_plan(self):
        plan = CDCPlan(base_plan=_simple_base_plan(), num_batches=5)
        assert plan.num_batches == 5
        assert plan.cdc_tables == ["users"]

    def test_default_format(self):
        plan = CDCPlan(base_plan=_simple_base_plan())
        assert plan.format == CDCFormat.RAW

    def test_custom_format(self):
        plan = CDCPlan(base_plan=_simple_base_plan(), format="delta_cdf")
        assert plan.format == CDCFormat.DELTA_CDF

    def test_invalid_table_config_ref(self):
        with pytest.raises(ValueError, match="unknown table"):
            CDCPlan(
                base_plan=_simple_base_plan(),
                table_configs={"nonexistent": CDCTableConfig()},
            )

    def test_invalid_cdc_tables_ref(self):
        with pytest.raises(ValueError, match="unknown table"):
            CDCPlan(
                base_plan=_simple_base_plan(),
                cdc_tables=["nonexistent"],
            )

    def test_cdc_tables_populated_by_default(self):
        plan = CDCPlan(base_plan=_simple_base_plan())
        assert plan.cdc_tables == ["users"]

    def test_config_for_default(self):
        plan = CDCPlan(base_plan=_simple_base_plan())
        cfg = plan.config_for("users")
        assert cfg.batch_size == 0.1

    def test_config_for_override(self):
        plan = CDCPlan(
            base_plan=_simple_base_plan(),
            table_configs={"users": CDCTableConfig(batch_size=50)},
        )
        cfg = plan.config_for("users")
        assert cfg.batch_size == 50


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


class TestCDCPlanSerialization:
    def test_round_trip_json(self):
        plan = CDCPlan(
            base_plan=_simple_base_plan(),
            num_batches=3,
            format="delta_cdf",
            table_configs={"users": CDCTableConfig(batch_size=20)},
        )
        data = json.loads(plan.model_dump_json())
        plan2 = CDCPlan.model_validate(data)
        assert plan2.num_batches == 3
        assert plan2.format == CDCFormat.DELTA_CDF
        assert plan2.config_for("users").batch_size == 20

    def test_round_trip_dict(self):
        plan = CDCPlan(base_plan=_simple_base_plan(), num_batches=7)
        data = plan.model_dump(mode="json")
        plan2 = CDCPlan.model_validate(data)
        assert plan2.num_batches == 7


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------


class TestCDCPlanFromYAML:
    def test_load_fixture(self):
        raw = yaml.safe_load((FIXTURES / "cdc_plan.yml").read_text())
        plan = CDCPlan.model_validate(raw)
        assert plan.num_batches == 3
        assert plan.format == CDCFormat.RAW
        assert "customers" in plan.cdc_tables
        cfg = plan.config_for("customers")
        assert cfg.batch_size == 20
        ins, upd, delw = cfg.operations.fractions
        assert abs(ins - 0.3) < 0.01
        assert abs(upd - 0.5) < 0.01
        assert abs(delw - 0.2) < 0.01

    def test_yaml_round_trip(self):
        plan = CDCPlan(
            base_plan=_simple_base_plan(),
            num_batches=5,
            format="raw",
        )
        yaml_str = yaml.dump(plan.model_dump(mode="json"), default_flow_style=False)
        plan2 = CDCPlan.model_validate(yaml.safe_load(yaml_str))
        assert plan2.num_batches == 5
        assert plan2.cdc_tables == ["users"]
