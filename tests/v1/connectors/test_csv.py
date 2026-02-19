"""Tests for the CSV schema inference connector."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from dbldatagen.v1.connectors.csv import (
    CSVConnector,
    _detect_primary_key,
    _infer_columns,
    extract_from_csv,
)
from dbldatagen.v1.schema import (
    DataGenPlan,
    DataType,
    FakerColumn,
    SequenceColumn,
    ValuesColumn,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
USERS_CSV = FIXTURES / "users.csv"
ORDERS_CSV = FIXTURES / "orders.csv"


# ---------------------------------------------------------------------------
# Column inference tests
# ---------------------------------------------------------------------------


class TestColumnInference:
    """Verify pandas type inference produces correct InferredColumn values."""

    @pytest.fixture()
    def users_columns(self):
        import pandas as pd

        df = pd.read_csv(USERS_CSV)
        return {c.name: c for c in _infer_columns(df)}

    @pytest.fixture()
    def orders_columns(self):
        import pandas as pd

        df = pd.read_csv(ORDERS_CSV)
        return {c.name: c for c in _infer_columns(df)}

    def test_integer_column(self, users_columns):
        col = users_columns["user_id"]
        assert col.synth_dtype == DataType.LONG

    def test_integer_column_unique(self, users_columns):
        col = users_columns["user_id"]
        assert col.unique is True

    def test_string_column(self, users_columns):
        col = users_columns["name"]
        assert col.synth_dtype == DataType.STRING

    def test_email_column(self, users_columns):
        col = users_columns["email"]
        assert col.synth_dtype == DataType.STRING

    def test_age_column(self, users_columns):
        col = users_columns["age"]
        assert col.synth_dtype == DataType.LONG

    def test_boolean_string_column(self, users_columns):
        """Pandas reads true/false as strings; we detect and map to BOOLEAN."""
        col = users_columns["is_active"]
        assert col.synth_dtype == DataType.BOOLEAN

    def test_city_column_low_cardinality(self, users_columns):
        col = users_columns["city"]
        assert col.synth_dtype == DataType.STRING
        assert col.distinct_count is not None
        assert col.distinct_count <= 10

    def test_float_column(self, orders_columns):
        col = orders_columns["amount"]
        assert col.synth_dtype == DataType.DOUBLE

    def test_non_nullable_columns(self, users_columns):
        for col in users_columns.values():
            assert col.nullable is False

    def test_sample_values_populated(self, users_columns):
        col = users_columns["name"]
        assert len(col.sample_values) > 0
        assert "Alice Johnson" in col.sample_values


# ---------------------------------------------------------------------------
# PK detection tests
# ---------------------------------------------------------------------------


class TestPKDetection:
    """Verify primary key heuristic: first unique, non-null column with 'id' in name."""

    def test_detects_user_id(self):
        import pandas as pd

        df = pd.read_csv(USERS_CSV)
        cols = _infer_columns(df)
        pk = _detect_primary_key(cols)
        assert pk == "user_id"

    def test_detects_order_id(self):
        import pandas as pd

        df = pd.read_csv(ORDERS_CSV)
        cols = _infer_columns(df)
        pk = _detect_primary_key(cols)
        assert pk == "order_id"

    def test_no_pk_when_no_id_column(self):
        import pandas as pd

        df = pd.DataFrame({"name": ["a", "b"], "value": [1, 2]})
        cols = _infer_columns(df)
        pk = _detect_primary_key(cols)
        assert pk is None

    def test_no_pk_when_id_not_unique(self):
        import pandas as pd

        df = pd.DataFrame({"id": [1, 1, 2], "name": ["a", "b", "c"]})
        cols = _infer_columns(df)
        pk = _detect_primary_key(cols)
        assert pk is None


# ---------------------------------------------------------------------------
# Strategy assignment tests
# ---------------------------------------------------------------------------


class TestStrategyAssignment:
    """Verify that select_strategy picks reasonable strategies for CSV columns."""

    @pytest.fixture()
    def users_plan(self):
        return extract_from_csv(USERS_CSV)

    @pytest.fixture()
    def users_cols(self, users_plan):
        table = users_plan.tables[0]
        return {c.name: c for c in table.columns}

    def test_pk_gets_sequence(self, users_cols):
        col = users_cols["user_id"]
        assert isinstance(col.gen, SequenceColumn)

    def test_email_gets_faker(self, users_cols):
        col = users_cols["email"]
        assert isinstance(col.gen, FakerColumn)
        assert col.gen.provider == "email"

    def test_age_gets_values(self, users_cols):
        """Age has only 10 distinct values -- low cardinality picks ValuesColumn."""
        col = users_cols["age"]
        assert isinstance(col.gen, ValuesColumn)

    def test_city_gets_faker(self, users_cols):
        """'city' matches the Faker name heuristic (higher priority than low cardinality)."""
        col = users_cols["city"]
        assert isinstance(col.gen, FakerColumn)
        assert col.gen.provider == "city"

    def test_boolean_column_gets_values(self, users_cols):
        col = users_cols["is_active"]
        assert isinstance(col.gen, ValuesColumn)


# ---------------------------------------------------------------------------
# CSVConnector extract tests
# ---------------------------------------------------------------------------


class TestCSVConnector:
    """Test the CSVConnector class."""

    def test_single_file(self):
        plan = CSVConnector(USERS_CSV).extract()
        assert isinstance(plan, DataGenPlan)
        assert len(plan.tables) == 1
        assert plan.tables[0].name == "users"

    def test_multiple_files(self):
        plan = CSVConnector([USERS_CSV, ORDERS_CSV]).extract()
        assert len(plan.tables) == 2
        names = {t.name for t in plan.tables}
        assert names == {"users", "orders"}

    def test_table_name_from_path(self):
        plan = CSVConnector(USERS_CSV).extract()
        assert plan.tables[0].name == "users"

    def test_default_rows(self):
        plan = CSVConnector(USERS_CSV, default_rows=5000).extract()
        assert plan.tables[0].rows == 5000

    def test_primary_key_set(self):
        plan = CSVConnector(USERS_CSV).extract()
        table = plan.tables[0]
        assert table.primary_key is not None
        assert table.primary_key.columns == ["user_id"]

    def test_column_count_users(self):
        plan = CSVConnector(USERS_CSV).extract()
        assert len(plan.tables[0].columns) == 7

    def test_column_count_orders(self):
        plan = CSVConnector(ORDERS_CSV).extract()
        assert len(plan.tables[0].columns) == 5

    def test_string_path_accepted(self):
        plan = CSVConnector(str(USERS_CSV)).extract()
        assert len(plan.tables) == 1

    def test_extract_from_csv_convenience(self):
        plan = extract_from_csv(USERS_CSV)
        assert isinstance(plan, DataGenPlan)
        assert len(plan.tables) == 1


# ---------------------------------------------------------------------------
# YAML export tests
# ---------------------------------------------------------------------------


class TestYAMLExport:
    """Test YAML serialisation."""

    def test_to_yaml_creates_file(self, tmp_path):
        output = str(tmp_path / "plan.yaml")
        CSVConnector(USERS_CSV).to_yaml(output)
        assert os.path.exists(output)

    def test_to_yaml_roundtrip(self, tmp_path):
        """YAML should be loadable and contain expected keys."""
        import yaml

        output = str(tmp_path / "plan.yaml")
        CSVConnector(USERS_CSV).to_yaml(output)
        with open(output) as f:
            data = yaml.safe_load(f)
        assert "tables" in data
        assert data["tables"][0]["name"] == "users"
        assert len(data["tables"][0]["columns"]) == 7

    def test_to_yaml_enum_serialization(self, tmp_path):
        """Enums should be serialised as strings, not objects."""

        output = str(tmp_path / "plan.yaml")
        CSVConnector(USERS_CSV).to_yaml(output)
        with open(output) as f:
            content = f.read()
        # DataType enums should appear as plain strings
        assert "!!python" not in content


# ---------------------------------------------------------------------------
# Integration: generate synthetic data from inferred plan
# ---------------------------------------------------------------------------


class TestIntegration:
    """End-to-end: infer schema from CSV, then generate synthetic data with Spark."""

    def test_generate_from_inferred_plan(self, spark):
        from dbldatagen.v1 import generate

        plan = extract_from_csv(USERS_CSV, default_rows=100)
        result = generate(spark, plan)
        assert "users" in result
        df = result["users"]
        assert df.count() == 100
        assert len(df.columns) == 7
