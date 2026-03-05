"""Exhaustive tests for dbldatagen.v1.connectors.sql.parser."""

import pytest

from dbldatagen.v1.connectors.sql.parser import SQLParseError, parse_sql


# ---------------------------------------------------------------------------
# Simple table extraction
# ---------------------------------------------------------------------------


class TestSimpleTableExtraction:
    def test_single_table(self):
        s = parse_sql("SELECT id, name FROM customers")
        assert len(s.tables) == 1
        assert s.tables[0].name == "customers"

    def test_two_tables_join(self):
        s = parse_sql("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id")
        names = {t.name for t in s.tables}
        assert names == {"orders", "customers"}

    def test_three_tables(self):
        s = parse_sql(
            """
            SELECT c.name, o.order_date, p.product_name
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            JOIN products p ON p.id = o.product_id
        """
        )
        assert {t.name for t in s.tables} == {"customers", "orders", "products"}

    def test_insert_statement(self):
        s = parse_sql("INSERT INTO orders (id, customer_id) VALUES (1, 2)")
        assert s.tables[0].name == "orders"

    def test_update_statement(self):
        s = parse_sql("UPDATE customers SET name = 'Alice' WHERE id = 1")
        assert s.tables[0].name == "customers"

    def test_delete_statement(self):
        s = parse_sql("DELETE FROM orders WHERE id = 1")
        assert s.tables[0].name == "orders"


# ---------------------------------------------------------------------------
# CTE handling
# ---------------------------------------------------------------------------


class TestCTEs:
    def test_cte_not_treated_as_table(self):
        s = parse_sql(
            """
            WITH active AS (SELECT * FROM customers WHERE status = 'active')
            SELECT * FROM active a JOIN orders o ON o.customer_id = a.id
        """
        )
        names = {t.name for t in s.tables}
        assert "active" not in names
        assert "customers" in names
        assert "orders" in names

    def test_multiple_ctes(self):
        s = parse_sql(
            """
            WITH
                active_cust AS (SELECT id, name FROM customers WHERE status = 'active'),
                recent_orders AS (SELECT * FROM orders WHERE order_date > '2024-01-01')
            SELECT ac.name, ro.order_date
            FROM active_cust ac
            JOIN recent_orders ro ON ro.customer_id = ac.id
        """
        )
        names = {t.name for t in s.tables}
        assert names == {"customers", "orders"}
        assert "active_cust" not in names
        assert "recent_orders" not in names

    def test_recursive_cte(self):
        s = parse_sql(
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
        names = {t.name for t in s.tables}
        assert "employees" in names
        assert "org_chart" not in names


# ---------------------------------------------------------------------------
# Subqueries
# ---------------------------------------------------------------------------


class TestSubqueries:
    def test_in_subquery(self):
        s = parse_sql(
            """
            SELECT * FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'pro')
        """
        )
        names = {t.name for t in s.tables}
        assert names == {"orders", "customers"}

    def test_exists_subquery(self):
        s = parse_sql(
            """
            SELECT * FROM customers c
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
        """
        )
        names = {t.name for t in s.tables}
        assert names == {"customers", "orders"}

    def test_derived_table(self):
        s = parse_sql(
            """
            SELECT x.id, x.name
            FROM (SELECT id, name FROM customers WHERE active = true) x
        """
        )
        names = {t.name for t in s.tables}
        assert "customers" in names
        assert len(s.tables) == 1  # derived table alias 'x' is NOT a real table

    def test_scalar_subquery(self):
        s = parse_sql(
            """
            SELECT name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count
            FROM customers c
        """
        )
        names = {t.name for t in s.tables}
        assert names == {"customers", "orders"}


# ---------------------------------------------------------------------------
# UNION / INTERSECT / EXCEPT
# ---------------------------------------------------------------------------


class TestSetOperations:
    def test_union(self):
        s = parse_sql(
            """
            SELECT id, name FROM customers
            UNION ALL
            SELECT id, name FROM suppliers
        """
        )
        names = {t.name for t in s.tables}
        assert names == {"customers", "suppliers"}

    def test_union_same_table_dedup(self):
        s = parse_sql(
            """
            SELECT id, name FROM customers WHERE tier = 'free'
            UNION ALL
            SELECT id, name FROM customers WHERE tier = 'pro'
        """
        )
        assert len(s.tables) == 1
        assert s.tables[0].name == "customers"

    def test_intersect(self):
        s = parse_sql(
            """
            SELECT id FROM customers
            INTERSECT
            SELECT customer_id FROM orders
        """
        )
        names = {t.name for t in s.tables}
        assert names == {"customers", "orders"}


# ---------------------------------------------------------------------------
# JOIN extraction
# ---------------------------------------------------------------------------


class TestJoinExtraction:
    def test_inner_join(self):
        s = parse_sql(
            """
            SELECT * FROM orders o
            JOIN customers c ON o.customer_id = c.id
        """
        )
        assert len(s.joins) == 1
        j = s.joins[0]
        cols = {j.left_column, j.right_column}
        assert "customer_id" in cols
        assert "id" in cols

    def test_left_join(self):
        s = parse_sql(
            """
            SELECT * FROM orders o
            LEFT JOIN returns r ON r.order_id = o.id
        """
        )
        assert len(s.joins) == 1
        assert s.joins[0].join_type == "left"

    def test_right_join(self):
        s = parse_sql(
            """
            SELECT * FROM orders o
            RIGHT JOIN customers c ON o.customer_id = c.id
        """
        )
        assert s.joins[0].join_type == "right"

    def test_full_outer_join(self):
        s = parse_sql(
            """
            SELECT * FROM a
            FULL OUTER JOIN b ON a.id = b.a_id
        """
        )
        assert len(s.joins) == 1

    def test_multi_condition_join(self):
        s = parse_sql(
            """
            SELECT * FROM a
            JOIN b ON a.x = b.x AND a.y = b.y
        """
        )
        assert len(s.joins) == 2

    def test_multiple_joins(self):
        s = parse_sql(
            """
            SELECT c.name, o.order_date, li.quantity, p.product_name
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            JOIN line_items li ON li.order_id = o.id
            JOIN products p ON p.id = li.product_id
        """
        )
        assert len(s.joins) == 3

    def test_cross_join_no_condition(self):
        s = parse_sql("SELECT * FROM a CROSS JOIN b")
        assert len(s.joins) == 0  # no ON clause

    def test_self_join(self):
        s = parse_sql(
            """
            SELECT e1.name, e2.name as manager
            FROM employees e1
            JOIN employees e2 ON e1.manager_id = e2.id
        """
        )
        assert len(s.tables) == 1  # deduplicated
        assert s.tables[0].name == "employees"
        assert len(s.joins) == 1


# ---------------------------------------------------------------------------
# Column extraction
# ---------------------------------------------------------------------------


class TestColumnExtraction:
    def test_explicit_columns(self):
        s = parse_sql("SELECT id, name, email FROM customers")
        cols = {c.name for c in s.tables[0].columns}
        assert cols == {"id", "name", "email"}

    def test_qualified_columns_resolved(self):
        s = parse_sql(
            """
            SELECT c.id, c.name, o.order_date
            FROM customers c JOIN orders o ON o.customer_id = c.id
        """
        )
        cust = next(t for t in s.tables if t.name == "customers")
        cust_cols = {c.name for c in cust.columns}
        assert "id" in cust_cols
        assert "name" in cust_cols

    def test_where_columns(self):
        s = parse_sql("SELECT id FROM customers WHERE status = 'active' AND age > 18")
        cols = {c.name for c in s.tables[0].columns}
        assert "status" in cols
        assert "age" in cols

    def test_group_by_columns(self):
        s = parse_sql("SELECT tier, COUNT(*) FROM customers GROUP BY tier")
        cols = {c.name for c in s.tables[0].columns}
        assert "tier" in cols

    def test_order_by_columns(self):
        s = parse_sql("SELECT id, name FROM customers ORDER BY created_at DESC")
        cols = {c.name for c in s.tables[0].columns}
        assert "created_at" in cols

    def test_join_columns_included(self):
        s = parse_sql(
            """
            SELECT c.name FROM customers c
            JOIN orders o ON o.customer_id = c.id
        """
        )
        order_cols = {c.name for c in next(t for t in s.tables if t.name == "orders").columns}
        assert "customer_id" in order_cols

    def test_select_star_no_columns(self):
        """SELECT * without DDL means no explicit column references."""
        s = parse_sql("SELECT * FROM customers")
        # May have zero columns or only those found elsewhere
        # The key is it doesn't crash
        assert s.tables[0].name == "customers"


# ---------------------------------------------------------------------------
# Predicate hints
# ---------------------------------------------------------------------------


class TestPredicateHints:
    def test_literal_comparison_int(self):
        s = parse_sql("SELECT id FROM customers WHERE age > 18")
        age_col = next(c for c in s.tables[0].columns if c.name == "age")
        assert age_col.compared_to_literal == 18

    def test_literal_comparison_string(self):
        s = parse_sql("SELECT id FROM customers WHERE status = 'active'")
        status_col = next(c for c in s.tables[0].columns if c.name == "status")
        assert status_col.compared_to_literal == "active"

    def test_literal_comparison_float(self):
        s = parse_sql("SELECT id FROM products WHERE price > 99.99")
        price_col = next(c for c in s.tables[0].columns if c.name == "price")
        assert isinstance(price_col.compared_to_literal, (int, float))

    def test_cast_type_detected(self):
        s = parse_sql("SELECT CAST(amount AS DECIMAL) FROM orders")
        amount_col = next(c for c in s.tables[0].columns if c.name == "amount")
        assert amount_col.cast_type is not None

    def test_aggregate_context(self):
        s = parse_sql("SELECT SUM(amount) FROM orders")
        amount_col = next(c for c in s.tables[0].columns if c.name == "amount")
        assert amount_col.aggregated is True


# ---------------------------------------------------------------------------
# Schema-qualified and multi-part names
# ---------------------------------------------------------------------------


class TestMultiPartNames:
    def test_schema_qualified(self):
        s = parse_sql("SELECT id FROM dbo.customers")
        assert s.tables[0].name == "customers"
        assert s.tables[0].schema_name == "dbo"

    def test_catalog_schema_table(self):
        s = parse_sql("SELECT id FROM my_catalog.my_schema.customers")
        assert s.tables[0].name == "customers"


# ---------------------------------------------------------------------------
# Multi-dialect parsing
# ---------------------------------------------------------------------------


class TestDialects:
    def test_spark(self):
        s = parse_sql("SELECT id, name FROM db.customers LIMIT 10", dialect="spark")
        assert s.tables[0].name == "customers"

    def test_bigquery_backticks(self):
        s = parse_sql(
            "SELECT id, name FROM `project.dataset.customers` LIMIT 10",
            dialect="bigquery",
        )
        assert s.tables[0].name == "customers"

    def test_tsql_top(self):
        s = parse_sql("SELECT TOP 10 id, name FROM customers", dialect="tsql")
        assert s.tables[0].name == "customers"

    def test_postgres(self):
        s = parse_sql("SELECT id, name FROM customers LIMIT 10", dialect="postgres")
        assert s.tables[0].name == "customers"

    def test_mysql(self):
        s = parse_sql("SELECT id, name FROM customers LIMIT 10", dialect="mysql")
        assert s.tables[0].name == "customers"

    def test_snowflake(self):
        s = parse_sql("SELECT id, name FROM customers LIMIT 10", dialect="snowflake")
        assert s.tables[0].name == "customers"


# ---------------------------------------------------------------------------
# Complex real-world queries
# ---------------------------------------------------------------------------


class TestComplexQueries:
    def test_star_schema_analytics(self):
        s = parse_sql(
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
        names = {t.name for t in s.tables}
        assert names == {"line_items", "orders", "customers", "regions", "products"}
        assert len(s.joins) == 4

    def test_window_function(self):
        s = parse_sql(
            """
            SELECT
                id, name, tier,
                ROW_NUMBER() OVER (PARTITION BY tier ORDER BY created_at DESC) as rn
            FROM customers
        """
        )
        cols = {c.name for c in s.tables[0].columns}
        assert "tier" in cols
        assert "created_at" in cols

    def test_case_expression(self):
        s = parse_sql(
            """
            SELECT id,
                CASE
                    WHEN amount > 1000 THEN 'high'
                    WHEN amount > 100 THEN 'medium'
                    ELSE 'low'
                END AS tier
            FROM orders
        """
        )
        cols = {c.name for c in s.tables[0].columns}
        assert "amount" in cols

    def test_nested_cte_with_join(self):
        s = parse_sql(
            """
            WITH
                top_customers AS (
                    SELECT c.id, c.name, SUM(o.amount) AS total
                    FROM customers c
                    JOIN orders o ON o.customer_id = c.id
                    GROUP BY c.id, c.name
                    HAVING SUM(o.amount) > 10000
                )
            SELECT tc.name, p.product_name, COUNT(*) AS purchases
            FROM top_customers tc
            JOIN orders o2 ON o2.customer_id = tc.id
            JOIN line_items li ON li.order_id = o2.id
            JOIN products p ON p.id = li.product_id
            GROUP BY tc.name, p.product_name
        """
        )
        names = {t.name for t in s.tables}
        assert "customers" in names
        assert "orders" in names
        assert "line_items" in names
        assert "products" in names
        assert "top_customers" not in names

    def test_correlated_subquery(self):
        s = parse_sql(
            """
            SELECT c.name,
                (SELECT MAX(o.amount) FROM orders o WHERE o.customer_id = c.id) AS max_order
            FROM customers c
            WHERE c.tier = 'enterprise'
        """
        )
        names = {t.name for t in s.tables}
        assert names == {"customers", "orders"}

    def test_multiple_joins_same_parent(self):
        s = parse_sql(
            """
            SELECT * FROM orders o
            JOIN customers c1 ON o.billing_customer_id = c1.id
            JOIN customers c2 ON o.shipping_customer_id = c2.id
        """
        )
        # customers should be deduplicated
        names = {t.name for t in s.tables}
        assert names == {"orders", "customers"}


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrors:
    def test_no_tables(self):
        with pytest.raises(SQLParseError, match="No tables"):
            parse_sql("SELECT 1 + 1")

    def test_empty_string(self):
        with pytest.raises(SQLParseError):
            parse_sql("")

    def test_whitespace_only(self):
        with pytest.raises(SQLParseError):
            parse_sql("   ")

    def test_preserves_original_sql(self):
        sql = "SELECT id FROM customers"
        s = parse_sql(sql)
        assert s.original_sql == sql

    def test_preserves_dialect(self):
        s = parse_sql("SELECT id FROM customers", dialect="spark")
        assert s.dialect == "spark"
