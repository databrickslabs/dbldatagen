"""Tests for the JDBC/SQLAlchemy schema extraction connector."""

from __future__ import annotations

import sqlite3

import pytest
import yaml

from dbldatagen.v1.connectors.jdbc import JDBCConnector, extract_from_jdbc
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    DataType,
    SequenceColumn,
    TableSpec,
    UUIDColumn,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_db(tmp_path):
    """Create a SQLite database with customers and orders tables.

    Uses ``customer_name`` (not ``first_name`` / ``last_name``) to avoid
    triggering the Faker-based name heuristic in ``select_strategy``.
    """
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            notes TEXT,
            UNIQUE (email)
        )
    """
    )

    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    """
    )

    # Insert sample data
    customers = [
        (1, "Alice Smith", "alice@example.com", 1, None),
        (2, "Bob Jones", "bob@example.com", 1, "VIP"),
        (3, "Carol White", "carol@example.com", 0, None),
    ]
    cur.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?)", customers)

    orders = [
        (1, 1, 99.99, "shipped", "2024-01-15 10:30:00"),
        (2, 1, 49.50, "delivered", "2024-02-20 14:00:00"),
        (3, 2, 199.00, "pending", "2024-03-01 09:15:00"),
        (4, 3, 25.00, "shipped", "2024-03-10 16:45:00"),
    ]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders)

    conn.commit()
    conn.close()
    return f"sqlite:///{db_path}"


@pytest.fixture()
def empty_db(tmp_path):
    """Create an empty SQLite database with schema only (no rows)."""
    db_path = tmp_path / "empty.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            item_label TEXT NOT NULL,
            price REAL
        )
    """
    )
    conn.commit()
    conn.close()
    return f"sqlite:///{db_path}"


# ---------------------------------------------------------------------------
# Extraction tests
# ---------------------------------------------------------------------------


class TestExtractAll:
    def test_extract_returns_data_gen_plan(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        assert isinstance(plan, DataGenPlan)

    def test_extracts_all_tables(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        table_names = {t.name for t in plan.tables}
        assert table_names == {"customers", "orders"}

    def test_filter_specific_tables(self, sample_db):
        plan = extract_from_jdbc(sample_db, tables=["customers"])
        assert len(plan.tables) == 1
        assert plan.tables[0].name == "customers"

    def test_column_count(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        orders = _find_table(plan, "orders")
        assert len(customers.columns) == 5
        assert len(orders.columns) == 5

    def test_default_rows(self, sample_db):
        plan = extract_from_jdbc(sample_db, default_rows=5000)
        for t in plan.tables:
            assert t.rows == 5000

    def test_empty_table_extraction(self, empty_db):
        plan = extract_from_jdbc(empty_db)
        items = _find_table(plan, "items")
        assert len(items.columns) == 3
        assert items.primary_key is not None


# ---------------------------------------------------------------------------
# PK detection
# ---------------------------------------------------------------------------


class TestPKDetection:
    def test_primary_key_detected(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        assert customers.primary_key is not None
        assert "id" in customers.primary_key.columns

    def test_pk_column_uses_sequence(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        id_col = _find_column(customers, "id")
        assert isinstance(id_col.gen, SequenceColumn)

    def test_no_pk_table(self, tmp_path):
        """Table without a primary key."""
        db_path = tmp_path / "nopk.db"
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("CREATE TABLE logs (msg TEXT, level TEXT)")
        conn.commit()
        conn.close()

        plan = extract_from_jdbc(f"sqlite:///{db_path}")
        logs = _find_table(plan, "logs")
        assert logs.primary_key is None


# ---------------------------------------------------------------------------
# FK detection
# ---------------------------------------------------------------------------


class TestFKDetection:
    def test_foreign_key_detected(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        orders = _find_table(plan, "orders")
        cust_id_col = _find_column(orders, "customer_id")
        assert cust_id_col.foreign_key is not None
        assert cust_id_col.foreign_key.ref == "customers.id"

    def test_fk_column_uses_constant_placeholder(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        orders = _find_table(plan, "orders")
        cust_id_col = _find_column(orders, "customer_id")
        assert isinstance(cust_id_col.gen, ConstantColumn)

    def test_composite_fk(self, tmp_path):
        """Table with a composite foreign key."""
        db_path = tmp_path / "composite.db"
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE parent (
                a INTEGER,
                b INTEGER,
                val TEXT,
                PRIMARY KEY (a, b)
            )
        """
        )
        cur.execute(
            """
            CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                pa INTEGER,
                pb INTEGER,
                FOREIGN KEY (pa, pb) REFERENCES parent(a, b)
            )
        """
        )
        conn.commit()
        conn.close()

        plan = extract_from_jdbc(f"sqlite:///{db_path}")
        child = _find_table(plan, "child")
        pa_col = _find_column(child, "pa")
        pb_col = _find_column(child, "pb")
        assert pa_col.foreign_key is not None
        assert pa_col.foreign_key.ref == "parent.a"
        assert pb_col.foreign_key is not None
        assert pb_col.foreign_key.ref == "parent.b"

    def test_non_fk_column_has_no_ref(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        orders = _find_table(plan, "orders")
        amount_col = _find_column(orders, "amount")
        assert amount_col.foreign_key is None


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


class TestTypeMapping:
    def test_integer_pk_maps_to_long(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        id_col = _find_column(customers, "id")
        assert id_col.dtype == DataType.LONG

    def test_text_maps_to_string(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        name_col = _find_column(customers, "customer_name")
        assert name_col.dtype == DataType.STRING

    def test_real_maps_to_double(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        orders = _find_table(plan, "orders")
        amount_col = _find_column(orders, "amount")
        assert amount_col.dtype == DataType.DOUBLE

    def test_boolean_maps_to_boolean(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        active_col = _find_column(customers, "is_active")
        # SQLite BOOLEAN is reported as BOOLEAN by SQLAlchemy
        assert active_col.dtype == DataType.BOOLEAN


# ---------------------------------------------------------------------------
# Nullable detection
# ---------------------------------------------------------------------------


class TestNullable:
    def test_not_null_column(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        name_col = _find_column(customers, "customer_name")
        assert name_col.nullable is False

    def test_nullable_column(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        notes_col = _find_column(customers, "notes")
        assert notes_col.nullable is True

    def test_pk_not_nullable(self, sample_db):
        plan = extract_from_jdbc(sample_db)
        customers = _find_table(plan, "customers")
        id_col = _find_column(customers, "id")
        assert id_col.nullable is False


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


class TestSampling:
    def test_sampling_enabled_by_default(self, sample_db):
        connector = JDBCConnector(sample_db)
        plan = connector.extract()
        # Low-cardinality 'status' column in orders should use ValuesColumn
        orders = _find_table(plan, "orders")
        status_col = _find_column(orders, "status")
        assert isinstance(status_col.gen, ValuesColumn)

    def test_sampling_disabled(self, sample_db):
        connector = JDBCConnector(sample_db, sample=False)
        plan = connector.extract()
        orders = _find_table(plan, "orders")
        status_col = _find_column(orders, "status")
        # Without sampling, strategy falls back to type-based defaults
        assert not isinstance(status_col.gen, ValuesColumn)

    def test_empty_table_no_samples(self, empty_db):
        connector = JDBCConnector(empty_db)
        plan = connector.extract()
        items = _find_table(plan, "items")
        # Should still extract columns even with no sample data
        assert len(items.columns) == 3


# ---------------------------------------------------------------------------
# YAML export
# ---------------------------------------------------------------------------


class TestYAMLExport:
    def test_to_yaml_creates_file(self, sample_db, tmp_path):
        output = str(tmp_path / "plan.yaml")
        connector = JDBCConnector(sample_db)
        connector.to_yaml(output)

        with open(output) as f:
            data = yaml.safe_load(f)

        assert "tables" in data
        assert len(data["tables"]) == 2

    def test_yaml_roundtrip(self, sample_db, tmp_path):
        output = str(tmp_path / "plan.yaml")
        connector = JDBCConnector(sample_db)
        connector.to_yaml(output)

        with open(output) as f:
            data = yaml.safe_load(f)

        # Should be deserializable back into a DataGenPlan
        plan = DataGenPlan(**data)
        assert len(plan.tables) == 2

    def test_yaml_enum_serialization(self, sample_db, tmp_path):
        """Ensure enums serialize as plain strings, not objects."""
        output = str(tmp_path / "plan.yaml")
        connector = JDBCConnector(sample_db)
        connector.to_yaml(output)

        raw = (tmp_path / "plan.yaml").read_text()
        # DataType enum values should appear as bare strings
        assert "dtype: long" in raw or "dtype: string" in raw
        # Should NOT contain Python enum repr
        assert "DataType." not in raw


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


class TestExtractFromJDBC:
    def test_convenience_matches_class(self, sample_db):
        plan_fn = extract_from_jdbc(sample_db)
        plan_cls = JDBCConnector(sample_db).extract()
        assert len(plan_fn.tables) == len(plan_cls.tables)
        for t1, t2 in zip(plan_fn.tables, plan_cls.tables):
            assert t1.name == t2.name
            assert len(t1.columns) == len(t2.columns)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_single_table_filter(self, sample_db):
        plan = extract_from_jdbc(sample_db, tables=["orders"])
        assert len(plan.tables) == 1
        assert plan.tables[0].name == "orders"

    def test_unique_constraint_marks_column(self, tmp_path):
        """A UNIQUE constraint on a non-Faker column should produce UUIDColumn."""
        db_path = tmp_path / "unique.db"
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE tokens (
                id INTEGER PRIMARY KEY,
                token TEXT NOT NULL,
                UNIQUE (token)
            )
        """
        )
        conn.commit()
        conn.close()

        connector = JDBCConnector(f"sqlite:///{db_path}", sample=False)
        plan = connector.extract()
        tokens = _find_table(plan, "tokens")
        token_col = _find_column(tokens, "token")
        # UUIDColumn is used for unique string columns by select_strategy
        assert isinstance(token_col.gen, UUIDColumn)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_table(plan: DataGenPlan, name: str) -> TableSpec:
    for t in plan.tables:
        if t.name == name:
            return t
    raise ValueError(f"Table {name!r} not found in plan")


def _find_column(table: TableSpec, name: str) -> ColumnSpec:
    for c in table.columns:
        if c.name == name:
            return c
    raise ValueError(f"Column {name!r} not found in table {table.name!r}")
