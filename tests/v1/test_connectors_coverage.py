"""Tests targeting uncovered lines in connectors/__init__.py and connectors/sql/__init__.py."""

from __future__ import annotations

import importlib
import sys
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# connectors/__init__.py — ImportError branches & guard-raising helpers
# ---------------------------------------------------------------------------


class TestConnectorsInitImportErrorBranches:
    """Cover the except-ImportError branches for JDBC, CSV, and SQL extras."""

    def test_jdbc_import_error_sets_flag_false(self):
        """Lines 24-25: when dbldatagen.v1.connectors.jdbc is missing, _HAS_JDBC = False."""
        mod_name = "dbldatagen.v1.connectors"
        # Temporarily remove the module so it can be re-imported
        saved = sys.modules.pop(mod_name, None)

        with mock.patch.dict(sys.modules, {"dbldatagen.v1.connectors.jdbc": None}):
            # Force ImportError on `from dbldatagen.v1.connectors.jdbc import ...`
            mod = importlib.import_module(mod_name)
            importlib.reload(mod)
            assert mod._HAS_JDBC is False
            # __all__ should NOT contain JDBC symbols
            assert "JDBCConnector" not in mod.__all__
            assert "extract_from_jdbc" not in mod.__all__

        # Restore
        if saved is not None:
            sys.modules[mod_name] = saved
        else:
            sys.modules.pop(mod_name, None)

    def test_csv_import_error_sets_flag_false(self):
        """Lines 31-32: when dbldatagen.v1.connectors.csv is missing, _HAS_CSV = False."""
        mod_name = "dbldatagen.v1.connectors"
        saved = sys.modules.pop(mod_name, None)

        with mock.patch.dict(sys.modules, {"dbldatagen.v1.connectors.csv": None}):
            mod = importlib.import_module(mod_name)
            importlib.reload(mod)
            assert mod._HAS_CSV is False
            assert "CSVConnector" not in mod.__all__
            assert "extract_from_csv" not in mod.__all__

        if saved is not None:
            sys.modules[mod_name] = saved
        else:
            sys.modules.pop(mod_name, None)

    def test_sql_import_error_sets_flag_false(self):
        """Lines 38-39: when dbldatagen.v1.connectors.sql is missing, _HAS_SQL = False."""
        mod_name = "dbldatagen.v1.connectors"
        saved = sys.modules.pop(mod_name, None)

        with mock.patch.dict(sys.modules, {"dbldatagen.v1.connectors.sql": None}):
            mod = importlib.import_module(mod_name)
            importlib.reload(mod)
            assert mod._HAS_SQL is False
            assert "SQLParseError" not in mod.__all__
            assert "extract_from_sql" not in mod.__all__
            assert "sql_to_yaml" not in mod.__all__

        if saved is not None:
            sys.modules[mod_name] = saved
        else:
            sys.modules.pop(mod_name, None)


class TestConnectorsInitGuardRaises:
    """Cover the guard clauses that raise ImportError when extras are missing."""

    def test_extract_from_database_raises_without_jdbc(self):
        """Lines 57-59: extract_from_database raises ImportError when _HAS_JDBC is False."""
        import dbldatagen.v1.connectors as conn_mod

        original = conn_mod._HAS_JDBC
        try:
            conn_mod._HAS_JDBC = False
            with pytest.raises(ImportError, match="JDBC connector requires SQLAlchemy"):
                conn_mod.extract_from_database("sqlite:///test.db")
        finally:
            conn_mod._HAS_JDBC = original

    def test_extract_from_sql_query_raises_without_sql(self):
        """Lines 80-82: extract_from_sql_query raises ImportError when _HAS_SQL is False."""
        import dbldatagen.v1.connectors as conn_mod

        original = conn_mod._HAS_SQL
        try:
            conn_mod._HAS_SQL = False
            with pytest.raises(ImportError, match="SQL connector requires sqlglot"):
                conn_mod.extract_from_sql_query("SELECT 1")
        finally:
            conn_mod._HAS_SQL = original


class TestConnectorsInitAllExports:
    """Cover the __all__ augmentation branches (lines 95-100)."""

    def test_all_includes_jdbc_when_available(self):
        """Lines 95->97: when _HAS_JDBC is True, __all__ includes JDBC symbols."""
        import dbldatagen.v1.connectors as conn_mod

        if conn_mod._HAS_JDBC:
            assert "JDBCConnector" in conn_mod.__all__
            assert "extract_from_jdbc" in conn_mod.__all__

    def test_all_includes_csv_when_available(self):
        """Lines 97->99: when _HAS_CSV is True, __all__ includes CSV symbols."""
        import dbldatagen.v1.connectors as conn_mod

        if conn_mod._HAS_CSV:
            assert "CSVConnector" in conn_mod.__all__
            assert "extract_from_csv" in conn_mod.__all__

    def test_all_includes_sql_when_available(self):
        """Lines 99->exit: when _HAS_SQL is True, __all__ includes SQL symbols."""
        import dbldatagen.v1.connectors as conn_mod

        if conn_mod._HAS_SQL:
            assert "SQLParseError" in conn_mod.__all__
            assert "extract_from_sql" in conn_mod.__all__
            assert "sql_to_yaml" in conn_mod.__all__


# ---------------------------------------------------------------------------
# connectors/sql/__init__.py — sql_generate (lines 71-78)
# ---------------------------------------------------------------------------


class TestSqlGenerate:
    """Cover sql_generate including the register_temp_views=False path."""

    def test_sql_generate_returns_dataframes(self, spark):
        """Lines 71-78: sql_generate produces DataFrames and registers temp views."""
        from dbldatagen.v1.connectors.sql import sql_generate

        dfs = sql_generate(
            spark,
            "SELECT id, name FROM customers",
            row_counts={"customers": 10},
        )
        assert "customers" in dfs
        assert dfs["customers"].count() == 10

        # Temp view should be registered by default
        result = spark.sql("SELECT * FROM customers")
        assert result.count() == 10

    def test_sql_generate_no_temp_views(self, spark):
        """Lines 75-77 false branch: register_temp_views=False skips view creation."""
        from dbldatagen.v1.connectors.sql import sql_generate

        # Use a unique table name to avoid collisions with other tests
        dfs = sql_generate(
            spark,
            "SELECT id, name FROM no_view_table",
            row_counts={"no_view_table": 5},
            register_temp_views=False,
        )
        assert "no_view_table" in dfs
        assert dfs["no_view_table"].count() == 5

        # Temp view should NOT be registered
        with pytest.raises(Exception):  # noqa: B017
            spark.sql("SELECT * FROM no_view_table")

    def test_sql_generate_with_dialect_and_seed(self, spark):
        """Ensure dialect and seed are forwarded through sql_generate."""
        from dbldatagen.v1.connectors.sql import sql_generate

        dfs = sql_generate(
            spark,
            "SELECT id, email FROM users",
            dialect="spark",
            row_counts={"users": 8},
            seed=123,
        )
        assert "users" in dfs
        assert dfs["users"].count() == 8

    def test_sql_generate_multi_table(self, spark):
        """Lines 76-77: iterate over multiple tables to register temp views."""
        from dbldatagen.v1.connectors.sql import sql_generate

        dfs = sql_generate(
            spark,
            """SELECT c.name, o.amount
               FROM customers_mt c
               JOIN orders_mt o ON o.customer_id = c.id""",
            row_counts={"customers_mt": 20, "orders_mt": 50},
            register_temp_views=True,
        )
        assert set(dfs.keys()) == {"customers_mt", "orders_mt"}

        # Both temp views should exist
        r1 = spark.sql("SELECT * FROM customers_mt")
        r2 = spark.sql("SELECT * FROM orders_mt")
        assert r1.count() == 20
        assert r2.count() == 50
