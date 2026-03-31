"""Tests for the plan resolution engine: FK validation, topological sort, metadata extraction."""

from __future__ import annotations

import pytest

from dbldatagen.v1.engine.planner import (
    resolve_plan,
)
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    ExpressionColumn,
    ForeignKeyRef,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    UUIDColumn,
)


def _make_simple_plan():
    """Two tables: customers -> orders with FK."""
    customers = TableSpec(
        name="customers",
        rows=1000,
        primary_key=PrimaryKey(columns=["customer_id"]),
        columns=[
            ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
            ColumnSpec(name="name", gen=ConstantColumn(value="test")),
        ],
    )
    orders = TableSpec(
        name="orders",
        rows=5000,
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
    return DataGenPlan(tables=[customers, orders], seed=42)


class TestResolveSimplePlan:
    def test_generation_order(self):
        """Parents come before children in generation order."""
        plan = _make_simple_plan()
        resolved = resolve_plan(plan)

        assert resolved.generation_order.index("customers") < resolved.generation_order.index("orders")

    def test_fk_resolution_exists(self):
        """FK resolution is created for the FK column."""
        plan = _make_simple_plan()
        resolved = resolve_plan(plan)

        assert ("orders", "customer_id") in resolved.fk_resolutions
        fk_res = resolved.fk_resolutions[("orders", "customer_id")]
        assert fk_res.parent_meta.table_name == "customers"
        assert fk_res.parent_meta.pk_column == "customer_id"
        assert fk_res.parent_meta.row_count == 1000


class TestResolveStarSchema:
    def test_four_table_star_schema(self):
        """4-table star schema: products, customers -> orders -> order_items."""
        products = TableSpec(
            name="products",
            rows=100,
            primary_key=PrimaryKey(columns=["product_id"]),
            columns=[
                ColumnSpec(name="product_id", gen=SequenceColumn()),
            ],
        )
        customers = TableSpec(
            name="customers",
            rows=500,
            primary_key=PrimaryKey(columns=["customer_id"]),
            columns=[
                ColumnSpec(name="customer_id", gen=SequenceColumn()),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=2000,
            primary_key=PrimaryKey(columns=["order_id"]),
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn()),
                ColumnSpec(
                    name="customer_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.customer_id"),
                ),
            ],
        )
        order_items = TableSpec(
            name="order_items",
            rows=8000,
            primary_key=PrimaryKey(columns=["item_id"]),
            columns=[
                ColumnSpec(name="item_id", gen=SequenceColumn()),
                ColumnSpec(
                    name="order_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="orders.order_id"),
                ),
                ColumnSpec(
                    name="product_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="products.product_id"),
                ),
            ],
        )

        plan = DataGenPlan(tables=[products, customers, orders, order_items], seed=42)
        resolved = resolve_plan(plan)

        order = resolved.generation_order
        # Parents must come before children
        assert order.index("customers") < order.index("orders")
        assert order.index("orders") < order.index("order_items")
        assert order.index("products") < order.index("order_items")


class TestCycleDetection:
    def test_circular_fk_raises(self):
        """Circular FK references should raise ValueError."""
        table_a = TableSpec(
            name="a",
            rows=100,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[
                ColumnSpec(name="id", gen=SequenceColumn()),
                ColumnSpec(
                    name="b_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="b.id"),
                ),
            ],
        )
        table_b = TableSpec(
            name="b",
            rows=100,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[
                ColumnSpec(name="id", gen=SequenceColumn()),
                ColumnSpec(
                    name="a_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="a.id"),
                ),
            ],
        )

        plan = DataGenPlan(tables=[table_a, table_b], seed=42)
        with pytest.raises(ValueError, match="Circular"):
            resolve_plan(plan)


class TestMissingRef:
    def test_missing_table_ref(self):
        """FK to non-existent table raises ValueError."""
        orders = TableSpec(
            name="orders",
            rows=100,
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn()),
                ColumnSpec(
                    name="customer_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="nonexistent.id"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[orders], seed=42)
        with pytest.raises(ValueError, match="does not exist"):
            resolve_plan(plan)

    def test_missing_column_ref(self):
        """FK to non-existent column raises ValueError."""
        customers = TableSpec(
            name="customers",
            rows=100,
            primary_key=PrimaryKey(columns=["customer_id"]),
            columns=[
                ColumnSpec(name="customer_id", gen=SequenceColumn()),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=100,
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn()),
                ColumnSpec(
                    name="customer_id",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.nonexistent"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        with pytest.raises(ValueError, match="does not exist"):
            resolve_plan(plan)


class TestPKMetadataExtraction:
    def test_sequence_pk_metadata(self):
        """Verify correct metadata for sequence PK."""
        plan = _make_simple_plan()
        resolved = resolve_plan(plan)
        meta = resolved.fk_resolutions[("orders", "customer_id")].parent_meta

        assert meta.pk_type == "sequence"
        assert meta.pk_start == 1
        assert meta.pk_step == 1
        assert meta.row_count == 1000

    def test_pattern_pk_metadata(self):
        """Verify correct metadata for pattern PK."""
        customers = TableSpec(
            name="customers",
            rows=500,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[
                ColumnSpec(name="cid", gen=PatternColumn(template="CUST-{digit:6}")),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=2000,
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn()),
                ColumnSpec(
                    name="cid",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)
        meta = resolved.fk_resolutions[("orders", "cid")].parent_meta

        assert meta.pk_type == "pattern"
        assert meta.pk_template == "CUST-{digit:6}"

    def test_uuid_pk_metadata(self):
        """Verify correct metadata for UUID PK."""
        customers = TableSpec(
            name="customers",
            rows=300,
            primary_key=PrimaryKey(columns=["uid"]),
            columns=[
                ColumnSpec(name="uid", gen=UUIDColumn()),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=1000,
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn()),
                ColumnSpec(
                    name="uid",
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.uid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)
        meta = resolved.fk_resolutions[("orders", "uid")].parent_meta

        assert meta.pk_type == "uuid"
        assert meta.row_count == 300


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestExpressionColumnValidation:
    def test_undefined_reference_warns(self):
        import warnings

        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    columns=[
                        ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
                        ColumnSpec(name="b", gen=ExpressionColumn(expr="unknown_col + 1")),
                    ],
                )
            ]
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            resolve_plan(plan)
            assert any("unknown_col" in str(warning.message) for warning in w)

    def test_valid_reference_ok(self):
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    columns=[
                        ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
                        ColumnSpec(name="b", gen=ExpressionColumn(expr="a + 1")),
                    ],
                )
            ]
        )
        resolved = resolve_plan(plan)
        assert "t" in resolved.generation_order


class TestSeedFromValidation:
    def test_nonexistent_seed_from_raises(self):
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    columns=[
                        ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
                        ColumnSpec(name="b", gen=RangeColumn(min=1, max=10), seed_from="nonexistent"),
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="seed_from='nonexistent'"):
            resolve_plan(plan)

    def test_valid_seed_from_ok(self):
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    columns=[
                        ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
                        ColumnSpec(name="b", gen=RangeColumn(min=1, max=10), seed_from="a"),
                    ],
                )
            ]
        )
        resolved = resolve_plan(plan)
        assert "t" in resolved.generation_order


class TestPrimaryKeyValidation:
    def test_pk_column_not_in_table_raises(self):
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    primary_key=PrimaryKey(columns=["id", "missing"]),
                    columns=[
                        ColumnSpec(name="id", gen=SequenceColumn()),
                        ColumnSpec(name="val", gen=RangeColumn(min=1, max=10)),
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="Primary key column 'missing'"):
            resolve_plan(plan)
