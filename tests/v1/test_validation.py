"""Tests for post-generation referential integrity validation."""

from __future__ import annotations

from pyspark.sql import functions as F

from dbldatagen.v1.engine.generator import generate_table
from dbldatagen.v1.engine.planner import resolve_plan
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    ForeignKeyRef,
    PrimaryKey,
    SequenceColumn,
    TableSpec,
)
from dbldatagen.v1.validation import validate_referential_integrity


class TestValidDataPasses:
    def test_valid_data_passes(self, spark):
        """Generate valid data; validation returns empty error list."""
        customers = TableSpec(
            name="customers",
            rows=100,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=500,
            primary_key=PrimaryKey(columns=["oid"]),
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)

        cust_df = generate_table(spark, customers, resolved)
        order_df = generate_table(spark, orders, resolved)

        errors = validate_referential_integrity({"customers": cust_df, "orders": order_df}, plan)
        assert errors == []


class TestOrphanFKDetected:
    def test_orphan_fk_detected(self, spark):
        """Manually corrupt FK column; validation catches orphan values."""
        customers = TableSpec(
            name="customers",
            rows=100,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=500,
            primary_key=PrimaryKey(columns=["oid"]),
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)

        cust_df = generate_table(spark, customers, resolved)
        order_df = generate_table(spark, orders, resolved)

        # Corrupt the FK column: replace cid with a value that doesn't exist in customers
        corrupted = order_df.withColumn("cid", F.lit(999999))

        errors = validate_referential_integrity({"customers": cust_df, "orders": corrupted}, plan)
        assert len(errors) == 1
        assert "orphan" in errors[0].lower()


class TestMissingTable:
    def test_missing_table_error(self, spark):
        """When a table from the plan is missing from dataframes dict, report error."""
        customers = TableSpec(
            name="customers",
            rows=10,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=50,
            primary_key=PrimaryKey(columns=["oid"]),
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)

        # Pass only customers -- orders is missing from the dict
        resolved = resolve_plan(plan)
        cust_df = generate_table(spark, customers, resolved)

        errors = validate_referential_integrity({"customers": cust_df}, plan)
        assert len(errors) == 1
        assert "orders" in errors[0]
        assert "missing" in errors[0].lower()


class TestMissingParentTable:
    def test_missing_parent_table_error(self, spark):
        """When a FK references a parent table not in dataframes dict, report error."""
        customers = TableSpec(
            name="customers",
            rows=10,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=50,
            primary_key=PrimaryKey(columns=["oid"]),
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)

        # Pass only orders -- customers (the parent) is missing
        resolved = resolve_plan(plan)
        order_df = generate_table(spark, orders, resolved)

        errors = validate_referential_integrity({"orders": order_df}, plan)
        # Should have error about missing customers table (the table itself)
        # AND error about missing parent table for orders.cid FK
        assert any("customers" in e and "missing" in e.lower() for e in errors)
        assert any("parent" in e.lower() or "customers" in e for e in errors)
