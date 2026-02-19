"""Tests for FK generation from JSON-defined schemas.

Loads a star-schema plan from JSON (regions -> customers -> orders -> order_items,
products -> order_items, products -> reviews, customers -> reviews) and verifies
FK integrity, join correctness, nullable FKs, Zipf skew, and determinism.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dbldatagen.v1 import generate
from dbldatagen.v1.schema import DataGenPlan
from dbldatagen.v1.validation import validate_referential_integrity

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def star_plan():
    raw = json.loads((FIXTURES / "star_schema.json").read_text())
    return DataGenPlan.model_validate(raw)


@pytest.fixture(scope="module")
def star_dfs(spark, star_plan):
    """Generate the full star schema once for all tests."""
    return generate(spark, star_plan)


# ---------------------------------------------------------------------------
# Schema-level validation
# ---------------------------------------------------------------------------


class TestStarSchemaLoading:
    def test_loads_all_tables(self, star_plan):
        names = [t.name for t in star_plan.tables]
        assert names == ["regions", "customers", "products", "orders", "order_items", "reviews"]

    def test_fk_refs_parsed(self, star_plan):
        table_map = {t.name: t for t in star_plan.tables}
        fk_cols = [c for c in table_map["order_items"].columns if c.foreign_key is not None]
        refs = {c.name: c.foreign_key.ref for c in fk_cols}
        assert refs == {"order_id": "orders.order_id", "product_id": "products.product_id"}


# ---------------------------------------------------------------------------
# Row counts
# ---------------------------------------------------------------------------


class TestRowCounts:
    def test_all_tables_generated(self, star_dfs):
        assert set(star_dfs.keys()) == {
            "regions",
            "customers",
            "products",
            "orders",
            "order_items",
            "reviews",
        }

    def test_exact_row_counts(self, star_dfs):
        expected = {
            "regions": 10,
            "customers": 500,
            "products": 100,
            "orders": 5000,
            "order_items": 15000,
            "reviews": 3000,
        }
        for name, count in expected.items():
            assert star_dfs[name].count() == count, f"{name} row count mismatch"


# ---------------------------------------------------------------------------
# FK referential integrity -- every FK value exists in the parent PK
# ---------------------------------------------------------------------------


class TestFKIntegrity:
    def test_validate_all_fks(self, star_dfs, star_plan):
        errors = validate_referential_integrity(star_dfs, star_plan)
        assert errors == [], f"FK integrity errors: {errors}"

    def test_orders_customer_join(self, star_dfs):
        """Every order's customer_id exists in customers."""
        orders = star_dfs["orders"]
        customers = star_dfs["customers"]
        orphans = orders.join(
            customers,
            orders["customer_id"] == customers["customer_id"],
            "left_anti",
        )
        assert orphans.count() == 0

    def test_customers_region_join(self, star_dfs):
        """Every customer's region_id exists in regions."""
        customers = star_dfs["customers"]
        regions = star_dfs["regions"]
        orphans = customers.join(
            regions,
            customers["region_id"] == regions["region_id"],
            "left_anti",
        )
        assert orphans.count() == 0

    def test_order_items_order_join(self, star_dfs):
        """Every order_item's order_id exists in orders."""
        items = star_dfs["order_items"]
        orders = star_dfs["orders"]
        orphans = items.join(
            orders,
            items["order_id"] == orders["order_id"],
            "left_anti",
        )
        assert orphans.count() == 0

    def test_order_items_product_join(self, star_dfs):
        """Every order_item's product_id exists in products (UUID PK)."""
        items = star_dfs["order_items"]
        products = star_dfs["products"]
        orphans = items.join(
            products,
            items["product_id"] == products["product_id"],
            "left_anti",
        )
        assert orphans.count() == 0

    def test_reviews_product_join(self, star_dfs):
        """Every review's product_id exists in products."""
        reviews = star_dfs["reviews"]
        products = star_dfs["products"]
        non_null = reviews.filter(F.col("product_id").isNotNull())
        orphans = non_null.join(
            products,
            non_null["product_id"] == products["product_id"],
            "left_anti",
        )
        assert orphans.count() == 0

    def test_reviews_customer_join_non_null(self, star_dfs):
        """Non-null review customer_ids all exist in customers (nullable FK)."""
        reviews = star_dfs["reviews"]
        customers = star_dfs["customers"]
        non_null = reviews.filter(F.col("customer_id").isNotNull())
        orphans = non_null.join(
            customers,
            non_null["customer_id"] == customers["customer_id"],
            "left_anti",
        )
        assert orphans.count() == 0


# ---------------------------------------------------------------------------
# Nullable FK
# ---------------------------------------------------------------------------


class TestNullableFK:
    def test_reviews_customer_has_nulls(self, star_dfs):
        """reviews.customer_id has null_fraction=0.2, expect roughly 20% nulls."""
        reviews = star_dfs["reviews"]
        null_count = reviews.filter(F.col("customer_id").isNull()).count()
        fraction = null_count / 3000
        assert 0.1 < fraction < 0.35, f"Null fraction {fraction} outside expected range"

    def test_non_nullable_fks_have_no_nulls(self, star_dfs):
        """FKs without nullable=True should have zero nulls."""
        for tbl, col in [
            ("customers", "region_id"),
            ("orders", "customer_id"),
            ("order_items", "order_id"),
            ("order_items", "product_id"),
            ("reviews", "product_id"),
        ]:
            null_count = star_dfs[tbl].filter(F.col(col).isNull()).count()
            assert null_count == 0, f"{tbl}.{col} has {null_count} unexpected nulls"


# ---------------------------------------------------------------------------
# FK distribution skew (Zipf)
# ---------------------------------------------------------------------------


class TestFKDistribution:
    def test_orders_customer_zipf_skew(self, star_dfs):
        """orders -> customers uses Zipf(1.5); top customer gets far more orders."""
        counts = star_dfs["orders"].groupBy("customer_id").count().orderBy(F.col("count").desc()).collect()
        top = counts[0]["count"]
        bottom = counts[-1]["count"]
        assert top > bottom * 3, f"Expected strong Zipf skew, got top={top} bottom={bottom}"

    def test_order_items_product_zipf_skew(self, star_dfs):
        """order_items -> products uses Zipf(1.8); heavy skew toward popular products."""
        counts = star_dfs["order_items"].groupBy("product_id").count().orderBy(F.col("count").desc()).collect()
        top = counts[0]["count"]
        median_idx = len(counts) // 2
        median = counts[median_idx]["count"]
        assert top > median * 2, f"Expected Zipf skew, got top={top} median={median}"


# ---------------------------------------------------------------------------
# Multi-hop join (regions -> customers -> orders -> order_items)
# ---------------------------------------------------------------------------


class TestMultiHopJoin:
    def test_four_table_join(self, star_dfs):
        """Full chain join: regions -> customers -> orders -> order_items succeeds."""
        regions = star_dfs["regions"]
        customers = star_dfs["customers"]
        orders = star_dfs["orders"]
        items = star_dfs["order_items"]

        joined = (
            items.join(orders, items["order_id"] == orders["order_id"])
            .join(customers, orders["customer_id"] == customers["customer_id"])
            .join(regions, customers["region_id"] == regions["region_id"])
        )
        # every order_item should resolve all the way to a region
        assert joined.count() == items.count()

    def test_region_coverage(self, star_dfs):
        """With 500 customers across 10 regions, all regions should have customers."""
        distinct_regions = star_dfs["customers"].select("region_id").distinct().count()
        assert distinct_regions == 10


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_full_star_determinism(self, spark, star_plan):
        """Generating the star schema twice produces identical DataFrames."""
        dfs1 = generate(spark, star_plan)
        dfs2 = generate(spark, star_plan)

        for name in ["orders", "order_items", "reviews"]:
            pk_map = {"orders": "order_id", "order_items": "item_id"}
            pk = pk_map.get(name, "review_id")
            rows1 = [tuple(r) for r in dfs1[name].orderBy(pk).collect()]
            rows2 = [tuple(r) for r in dfs2[name].orderBy(pk).collect()]
            assert rows1 == rows2, f"{name} not deterministic"


# ---------------------------------------------------------------------------
# PK uniqueness across all tables
# ---------------------------------------------------------------------------


class TestPKUniqueness:
    @pytest.mark.parametrize(
        "table,pk_col",
        [
            ("regions", "region_id"),
            ("customers", "customer_id"),
            ("products", "product_id"),
            ("orders", "order_id"),
            ("order_items", "item_id"),
        ],
    )
    def test_pk_unique(self, star_dfs, table, pk_col):
        df = star_dfs[table]
        assert df.count() == df.select(pk_col).distinct().count(), f"Duplicate PKs in {table}"
