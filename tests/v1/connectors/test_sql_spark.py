"""End-to-end Spark tests: SQL -> plan -> generate DataFrames -> save parquet.

Verifies that the full sql_generate pipeline produces valid DataFrames
with correct schemas, FK integrity, and that data can be written to parquet.
"""

from __future__ import annotations

import os
import shutil

import pytest

from dbldatagen.v1.connectors.sql import extract_from_sql, sql_generate
from dbldatagen.v1.validation import validate_referential_integrity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PARQUET_DIR = os.path.join(os.path.dirname(__file__), "_parquet_output")


@pytest.fixture(autouse=True)
def _cleanup_parquet():
    """Remove parquet output dir before and after each test."""
    if os.path.exists(PARQUET_DIR):
        shutil.rmtree(PARQUET_DIR)
    yield
    if os.path.exists(PARQUET_DIR):
        shutil.rmtree(PARQUET_DIR)


def _save_parquet(dfs: dict, subdir: str) -> dict[str, str]:
    """Save all DataFrames to parquet and return paths."""
    paths = {}
    for name, df in dfs.items():
        path = os.path.join(PARQUET_DIR, subdir, name)
        df.write.mode("overwrite").parquet(path)
        paths[name] = path
    return paths


# ---------------------------------------------------------------------------
# Simple single table
# ---------------------------------------------------------------------------


class TestSingleTable:
    def test_simple_select(self, spark):
        """Single table with id PK — the column that triggered AMBIGUOUS_REFERENCE."""
        dfs = sql_generate(
            spark,
            "SELECT id, name, email FROM customers",
            row_counts={"customers": 100},
        )
        assert "customers" in dfs
        df = dfs["customers"]
        assert df.count() == 100
        assert set(df.columns) == {"id", "name", "email"}

        # PK uniqueness
        assert df.select("id").distinct().count() == 100

        # Save to parquet
        paths = _save_parquet(dfs, "single_table")
        reloaded = spark.read.parquet(paths["customers"])
        assert reloaded.count() == 100

    def test_many_columns(self, spark):
        """Table with various column name patterns."""
        dfs = sql_generate(
            spark,
            """SELECT id, first_name, last_name, email, phone,
                      created_at, status, amount, quantity
               FROM users""",
            row_counts={"users": 50},
        )
        df = dfs["users"]
        assert df.count() == 50
        assert "id" in df.columns
        assert len(df.columns) == 9

        paths = _save_parquet(dfs, "many_columns")
        reloaded = spark.read.parquet(paths["users"])
        assert reloaded.count() == 50


# ---------------------------------------------------------------------------
# Two-table join (FK relationship)
# ---------------------------------------------------------------------------


class TestTwoTableJoin:
    def test_customers_orders(self, spark):
        """customers -> orders FK join, both tables have 'id' PKs."""
        dfs = sql_generate(
            spark,
            """SELECT c.name, o.amount
               FROM customers c
               JOIN orders o ON o.customer_id = c.id""",
            row_counts={"customers": 100, "orders": 500},
        )
        assert set(dfs.keys()) == {"customers", "orders"}
        assert dfs["customers"].count() == 100
        assert dfs["orders"].count() == 500

        # FK integrity: all orders reference valid customers
        plan = extract_from_sql(
            """SELECT c.name, o.amount
               FROM customers c
               JOIN orders o ON o.customer_id = c.id""",
            row_counts={"customers": 100, "orders": 500},
        )
        errors = validate_referential_integrity(dfs, plan)
        assert errors == [], f"FK errors: {errors}"

        # Save to parquet
        paths = _save_parquet(dfs, "two_table")
        for name in ["customers", "orders"]:
            reloaded = spark.read.parquet(paths[name])
            assert reloaded.count() == dfs[name].count()


# ---------------------------------------------------------------------------
# Star schema (multiple FK chains)
# ---------------------------------------------------------------------------


class TestStarSchema:
    def test_star_schema_sql(self, spark):
        """4-table star schema: customers + products -> orders -> line_items."""
        sql = """
            SELECT c.name, o.order_date, li.quantity, p.product_name
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            JOIN line_items li ON li.order_id = o.id
            JOIN products p ON p.id = li.product_id
        """
        dfs = sql_generate(
            spark,
            sql,
            row_counts={
                "customers": 50,
                "products": 30,
                "orders": 200,
                "line_items": 800,
            },
        )
        assert set(dfs.keys()) == {"customers", "products", "orders", "line_items"}
        assert dfs["customers"].count() == 50
        assert dfs["products"].count() == 30
        assert dfs["orders"].count() == 200
        assert dfs["line_items"].count() == 800

        # FK integrity
        plan = extract_from_sql(
            sql,
            row_counts={
                "customers": 50,
                "products": 30,
                "orders": 200,
                "line_items": 800,
            },
        )
        errors = validate_referential_integrity(dfs, plan)
        assert errors == [], f"FK errors: {errors}"

        # Save to parquet
        paths = _save_parquet(dfs, "star_schema")
        for name, df in dfs.items():
            reloaded = spark.read.parquet(paths[name])
            assert reloaded.count() == df.count()


# ---------------------------------------------------------------------------
# Complex analytics query (the original failing case)
# ---------------------------------------------------------------------------


class TestComplexAnalytics:
    def test_complex_analytics_query(self, spark):
        """5-table query with WHERE, GROUP BY, HAVING — the original bug trigger."""
        sql = """
            SELECT
                r.region_name,
                p.category,
                SUM(li.quantity * p.unit_price) AS revenue,
                COUNT(DISTINCT o.id) AS order_count
            FROM line_items li
            JOIN orders o ON li.order_id = o.id
            JOIN customers c ON o.customer_id = c.id
            JOIN regions r ON c.region_id = r.id
            JOIN products p ON p.id = li.product_id
            WHERE o.order_date >= '2024-01-01'
            GROUP BY r.region_name, p.category
            HAVING SUM(li.quantity * p.unit_price) > 10000
            ORDER BY revenue DESC
        """
        dfs = sql_generate(
            spark,
            sql,
            row_counts={
                "regions": 10,
                "customers": 100,
                "products": 50,
                "orders": 500,
                "line_items": 2000,
            },
        )
        expected_tables = {"regions", "customers", "products", "orders", "line_items"}
        assert set(dfs.keys()) == expected_tables

        # Verify row counts
        assert dfs["regions"].count() == 10
        assert dfs["customers"].count() == 100
        assert dfs["products"].count() == 50
        assert dfs["orders"].count() == 500
        assert dfs["line_items"].count() == 2000

        # Tables referenced with t.id in SQL have 'id' column
        for name in ["regions", "customers", "products", "orders"]:
            assert "id" in dfs[name].columns, f"Table {name} missing 'id' column"
        # line_items has no natural id — inference synthesizes _synth_id PK
        assert "_synth_id" in dfs["line_items"].columns

        # FK integrity
        plan = extract_from_sql(
            sql,
            row_counts={
                "regions": 10,
                "customers": 100,
                "products": 50,
                "orders": 500,
                "line_items": 2000,
            },
        )
        errors = validate_referential_integrity(dfs, plan)
        assert errors == [], f"FK errors: {errors}"

        # Save all to parquet
        paths = _save_parquet(dfs, "complex_analytics")
        for name, df in dfs.items():
            reloaded = spark.read.parquet(paths[name])
            assert reloaded.count() == df.count()
            assert set(reloaded.columns) == set(df.columns)

    def test_temp_views_queryable(self, spark):
        """Verify generated temp views allow the original SQL to execute."""
        sql = """
            SELECT
                r.region_name,
                p.category,
                SUM(li.quantity * p.unit_price) AS revenue,
                COUNT(DISTINCT o.id) AS order_count
            FROM line_items li
            JOIN orders o ON li.order_id = o.id
            JOIN customers c ON o.customer_id = c.id
            JOIN regions r ON c.region_id = r.id
            JOIN products p ON p.id = li.product_id
            WHERE o.order_date >= '2024-01-01'
            GROUP BY r.region_name, p.category
            HAVING SUM(li.quantity * p.unit_price) > 10000
            ORDER BY revenue DESC
        """
        sql_generate(
            spark,
            sql,
            row_counts={
                "regions": 10,
                "customers": 100,
                "products": 50,
                "orders": 500,
                "line_items": 2000,
            },
            register_temp_views=True,
        )

        # The original query should now be runnable against the temp views
        result = spark.sql(sql)
        # Just verify it doesn't throw — actual rows depend on generated data
        assert result.columns == ["region_name", "category", "revenue", "order_count"]


# ---------------------------------------------------------------------------
# CTE and subquery
# ---------------------------------------------------------------------------


class TestCTEAndSubquery:
    def test_cte_query(self, spark):
        """CTE should resolve to the real underlying table."""
        dfs = sql_generate(
            spark,
            """
            WITH active AS (
                SELECT id, name FROM customers WHERE status = 'active'
            )
            SELECT a.name, o.amount
            FROM active a
            JOIN orders o ON o.customer_id = a.id
            """,
            row_counts={"customers": 80, "orders": 400},
        )
        assert "customers" in dfs
        assert "orders" in dfs
        assert "active" not in dfs

        paths = _save_parquet(dfs, "cte")
        for name in ["customers", "orders"]:
            reloaded = spark.read.parquet(paths[name])
            assert reloaded.count() == dfs[name].count()

    def test_subquery(self, spark):
        """Subquery should not create extra tables."""
        dfs = sql_generate(
            spark,
            """
            SELECT * FROM orders
            WHERE customer_id IN (
                SELECT id FROM customers WHERE tier = 'pro'
            )
            """,
            row_counts={"customers": 60, "orders": 300},
        )
        assert set(dfs.keys()) == {"orders", "customers"}

        paths = _save_parquet(dfs, "subquery")
        for name in ["customers", "orders"]:
            reloaded = spark.read.parquet(paths[name])
            assert reloaded.count() == dfs[name].count()


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_sql_generate_deterministic(self, spark):
        """Same SQL + seed produces identical DataFrames."""
        sql = """SELECT c.name, o.amount
                 FROM customers c
                 JOIN orders o ON o.customer_id = c.id"""
        kwargs = {"row_counts": {"customers": 50, "orders": 200}, "seed": 99}

        dfs1 = sql_generate(spark, sql, **kwargs)
        dfs2 = sql_generate(spark, sql, **kwargs)

        for name in ["customers", "orders"]:
            rows1 = sorted(dfs1[name].collect(), key=lambda r: r[0])
            rows2 = sorted(dfs2[name].collect(), key=lambda r: r[0])
            assert [tuple(r) for r in rows1] == [tuple(r) for r in rows2], f"Table {name} not deterministic"
