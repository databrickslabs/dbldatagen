"""End-to-end integration tests: SQL -> plan -> YAML round-trip."""

import pytest
import yaml

from dbldatagen.v1.connectors.sql import extract_from_sql, sql_to_yaml
from dbldatagen.v1.connectors.sql.parser import SQLParseError
from dbldatagen.v1.schema import DataGenPlan

# ---------------------------------------------------------------------------
# extract_from_sql  end-to-end
# ---------------------------------------------------------------------------


class TestExtractFromSql:
    def test_simple_select(self):
        plan = extract_from_sql("SELECT id, name, email FROM customers")
        assert isinstance(plan, DataGenPlan)
        assert len(plan.tables) == 1
        assert plan.tables[0].name == "customers"
        col_names = {c.name for c in plan.tables[0].columns}
        assert "id" in col_names
        assert "name" in col_names
        assert "email" in col_names

    def test_two_table_join(self):
        plan = extract_from_sql(
            """
            SELECT c.name, o.amount
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
        """
        )
        assert len(plan.tables) == 2
        table_names = {t.name for t in plan.tables}
        assert table_names == {"customers", "orders"}

        # Orders should have FK to customers
        orders = next(t for t in plan.tables if t.name == "orders")
        fk_col = next((c for c in orders.columns if c.name == "customer_id"), None)
        assert fk_col is not None
        assert fk_col.foreign_key is not None

    def test_star_schema(self):
        plan = extract_from_sql(
            """
            SELECT c.name, o.order_date, li.quantity, p.product_name
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            JOIN line_items li ON li.order_id = o.id
            JOIN products p ON p.id = li.product_id
        """
        )
        table_names = {t.name for t in plan.tables}
        assert table_names == {"customers", "orders", "line_items", "products"}

        # Topological order: customers/products before orders before line_items
        names = [t.name for t in plan.tables]
        assert names.index("customers") < names.index("orders")
        assert names.index("orders") < names.index("line_items")

    def test_cte_query(self):
        plan = extract_from_sql(
            """
            WITH active AS (
                SELECT id, name FROM customers WHERE status = 'active'
            )
            SELECT a.name, o.amount
            FROM active a
            JOIN orders o ON o.customer_id = a.id
        """
        )
        table_names = {t.name for t in plan.tables}
        assert "customers" in table_names
        assert "orders" in table_names
        assert "active" not in table_names

    def test_custom_row_counts(self):
        plan = extract_from_sql(
            "SELECT id, name FROM customers",
            row_counts={"customers": "5K"},
        )
        assert plan.tables[0].rows == 5000

    def test_custom_seed(self):
        plan = extract_from_sql("SELECT id FROM customers", seed=123)
        assert plan.seed == 123

    def test_dialect_spark(self):
        plan = extract_from_sql(
            "SELECT id, name FROM db.customers LIMIT 10",
            dialect="spark",
        )
        assert plan.tables[0].name == "customers"

    def test_error_no_tables(self):
        with pytest.raises(SQLParseError):
            extract_from_sql("SELECT 1 + 1")

    def test_complex_analytics(self):
        plan = extract_from_sql(
            """
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
        )
        table_names = {t.name for t in plan.tables}
        assert table_names == {"line_items", "orders", "customers", "regions", "products"}

    def test_subquery(self):
        plan = extract_from_sql(
            """
            SELECT * FROM orders
            WHERE customer_id IN (
                SELECT id FROM customers WHERE tier = 'pro'
            )
        """
        )
        table_names = {t.name for t in plan.tables}
        assert table_names == {"orders", "customers"}

    def test_union(self):
        plan = extract_from_sql(
            """
            SELECT id, name FROM customers
            UNION ALL
            SELECT id, name FROM suppliers
        """
        )
        table_names = {t.name for t in plan.tables}
        assert table_names == {"customers", "suppliers"}


# ---------------------------------------------------------------------------
# sql_to_yaml  round-trip
# ---------------------------------------------------------------------------


class TestSqlToYaml:
    def test_yaml_output_valid(self):
        yaml_str = sql_to_yaml("SELECT id, name, email FROM customers")
        data = yaml.safe_load(yaml_str)
        assert "tables" in data
        assert len(data["tables"]) == 1
        assert data["tables"][0]["name"] == "customers"

    def test_yaml_preserves_structure(self):
        yaml_str = sql_to_yaml(
            """
            SELECT c.name, o.amount
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
        """
        )
        data = yaml.safe_load(yaml_str)
        table_names = {t["name"] for t in data["tables"]}
        assert table_names == {"customers", "orders"}

    def test_yaml_has_seed(self):
        yaml_str = sql_to_yaml("SELECT id FROM customers", seed=99)
        data = yaml.safe_load(yaml_str)
        assert data["seed"] == 99

    def test_yaml_has_row_counts(self):
        yaml_str = sql_to_yaml(
            "SELECT id FROM customers",
            row_counts={"customers": 500},
        )
        data = yaml.safe_load(yaml_str)
        assert data["tables"][0]["rows"] == 500

    def test_yaml_roundtrip_plan(self):
        """YAML output should be loadable back into a DataGenPlan."""
        sql = """
            SELECT c.id, c.name, c.email, o.id, o.amount, o.customer_id
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
        """
        yaml_str = sql_to_yaml(sql)
        data = yaml.safe_load(yaml_str)
        plan = DataGenPlan(**data)
        assert len(plan.tables) == 2


# ---------------------------------------------------------------------------
# Self-join
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_self_join(self):
        plan = extract_from_sql(
            """
            SELECT e1.name, e2.name AS manager
            FROM employees e1
            JOIN employees e2 ON e1.manager_id = e2.id
        """
        )
        assert len(plan.tables) == 1
        assert plan.tables[0].name == "employees"

    def test_multiple_joins_same_parent(self):
        plan = extract_from_sql(
            """
            SELECT * FROM orders o
            JOIN customers c1 ON o.billing_customer_id = c1.id
            JOIN customers c2 ON o.shipping_customer_id = c2.id
        """
        )
        table_names = {t.name for t in plan.tables}
        assert table_names == {"orders", "customers"}

    def test_window_function(self):
        plan = extract_from_sql(
            """
            SELECT id, name, tier,
                ROW_NUMBER() OVER (PARTITION BY tier ORDER BY created_at DESC) AS rn
            FROM customers
        """
        )
        col_names = {c.name for c in plan.tables[0].columns}
        assert "tier" in col_names

    def test_case_expression(self):
        plan = extract_from_sql(
            """
            SELECT id,
                CASE WHEN amount > 1000 THEN 'high'
                     WHEN amount > 100 THEN 'medium'
                     ELSE 'low'
                END AS tier
            FROM orders
        """
        )
        assert plan.tables[0].name == "orders"

    def test_join_using(self):
        plan = extract_from_sql(
            """
            SELECT c.name, o.amount
            FROM customers c
            JOIN orders o USING (customer_id)
        """
        )
        table_names = {t.name for t in plan.tables}
        assert table_names == {"customers", "orders"}

    def test_recursive_cte(self):
        plan = extract_from_sql(
            """
            WITH RECURSIVE org_chart AS (
                SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL
                UNION ALL
                SELECT e.id, e.name, e.manager_id
                FROM employees e JOIN org_chart oc ON e.manager_id = oc.id
            )
            SELECT * FROM org_chart
        """
        )
        table_names = {t.name for t in plan.tables}
        assert "employees" in table_names
        assert "org_chart" not in table_names
