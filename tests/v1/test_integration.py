"""End-to-end integration tests for the dbldatagen.v1 library.

Tests the public generate() API with multi-table schemas, FK joins,
determinism, and row count verification.
"""

from __future__ import annotations

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
    Zipf,
)
from dbldatagen.v1.validation import validate_referential_integrity


def _customers_orders_plan():
    """Simple customers -> orders plan."""
    customers = TableSpec(
        name="customers",
        rows=200,
        primary_key=PrimaryKey(columns=["customer_id"]),
        columns=[
            ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
            ColumnSpec(name="name", gen=ConstantColumn(value="test")),
        ],
    )
    orders = TableSpec(
        name="orders",
        rows=1000,
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
                dtype=DataType.LONG,
                gen=RangeColumn(min=10, max=500),
            ),
        ],
    )
    return DataGenPlan(tables=[customers, orders], seed=42)


class TestCustomersOrdersJoin:
    def test_customers_orders_join(self, spark):
        """Customers -> orders: inner join, all orders match a customer."""
        plan = _customers_orders_plan()
        dfs = generate(spark, plan)

        assert "customers" in dfs
        assert "orders" in dfs

        cust_df = dfs["customers"]
        order_df = dfs["orders"]

        # Inner join should match ALL orders
        joined = order_df.join(
            cust_df,
            order_df["customer_id"] == cust_df["customer_id"],
            "inner",
        )
        assert joined.count() == order_df.count()


class TestStarSchema:
    def test_star_schema(self, spark):
        """products + customers -> orders -> order_items: all FK joins work."""
        products = TableSpec(
            name="products",
            rows=50,
            primary_key=PrimaryKey(columns=["product_id"]),
            columns=[
                ColumnSpec(name="product_id", gen=SequenceColumn(start=1, step=1)),
            ],
        )
        customers = TableSpec(
            name="customers",
            rows=100,
            primary_key=PrimaryKey(columns=["customer_id"]),
            columns=[
                ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
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
            ],
        )
        order_items = TableSpec(
            name="order_items",
            rows=2000,
            primary_key=PrimaryKey(columns=["item_id"]),
            columns=[
                ColumnSpec(name="item_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="order_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="orders.order_id"),
                ),
                ColumnSpec(
                    name="product_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="products.product_id", distribution=Zipf(exponent=1.3)),
                ),
            ],
        )

        plan = DataGenPlan(tables=[products, customers, orders, order_items], seed=42)
        dfs = generate(spark, plan)

        # Verify all FK joins work (no orphans)
        errors = validate_referential_integrity(dfs, plan)
        assert errors == [], f"Validation errors: {errors}"


class TestDeterminismFull:
    def test_determinism_full(self, spark):
        """Generate entire plan twice; DataFrames are identical."""
        plan = _customers_orders_plan()

        dfs1 = generate(spark, plan)
        dfs2 = generate(spark, plan)

        for table_name in ["customers", "orders"]:
            df1 = dfs1[table_name].orderBy(dfs1[table_name].columns[0]).collect()
            df2 = dfs2[table_name].orderBy(dfs2[table_name].columns[0]).collect()

            rows1 = [tuple(r) for r in df1]
            rows2 = [tuple(r) for r in df2]
            assert rows1 == rows2, f"Table {table_name} not deterministic"


class TestGenerateAPI:
    def test_generate_api(self, spark):
        """Test the public generate(spark, plan) function end-to-end."""
        plan = _customers_orders_plan()
        dfs = generate(spark, plan)

        assert isinstance(dfs, dict)
        assert len(dfs) == 2
        assert "customers" in dfs
        assert "orders" in dfs

        # Each value is a DataFrame
        for name, df in dfs.items():
            assert df.count() > 0


class TestRowCounts:
    def test_row_counts(self, spark):
        """Verify exact row counts match spec."""
        plan = _customers_orders_plan()
        dfs = generate(spark, plan)

        assert dfs["customers"].count() == 200
        assert dfs["orders"].count() == 1000


class TestPKUniqueness:
    def test_pk_uniqueness(self, spark):
        """Verify PK columns have no duplicates."""
        plan = _customers_orders_plan()
        dfs = generate(spark, plan)

        for table_name, pk_col in [("customers", "customer_id"), ("orders", "order_id")]:
            df = dfs[table_name]
            total = df.count()
            distinct = df.select(pk_col).distinct().count()
            assert total == distinct, f"Duplicate PKs in {table_name}.{pk_col}"
