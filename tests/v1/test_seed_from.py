"""Tests for the seed_from parameter on ColumnSpec.

seed_from allows a column to derive its randomness from another column's
value instead of the row ID.  Same source value → same derived value,
regardless of row position.
"""

import pytest
from pyspark.sql import SparkSession

from dbldatagen.v1 import generate
from dbldatagen.v1.dsl import (
    decimal,
    fk,
    integer,
    pk_auto,
    text,
    timestamp,
)
from dbldatagen.v1.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    PrimaryKey,
    RangeColumn,
    TableSpec,
    TimestampColumn,
    ValuesColumn,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_seed_from").getOrCreate()


def _make_plan_with_seed_from():
    """Build a plan where country is seed_from device_id (FK)."""
    return DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="devices",
                rows=50,
                columns=[pk_auto("id")],
                primary_key=PrimaryKey(columns=["id"]),
            ),
            TableSpec(
                name="events",
                rows=200,
                columns=[
                    pk_auto("id"),
                    fk("device_id", ref="devices.id"),
                    text("country", values=["US", "DE", "JP", "BR"], seed_from="device_id"),
                    text(
                        "manufacturer",
                        values=["Apple", "Samsung", "Google"],
                        seed_from="device_id",
                    ),
                    integer("value", min_val=1, max_val=1000),  # no seed_from — varies per row
                ],
                primary_key=PrimaryKey(columns=["id"]),
            ),
        ],
    )


def test_correlated_values(spark):
    """Two rows with the same device_id get the same country."""
    plan = _make_plan_with_seed_from()
    results = generate(spark, plan)
    events = results["events"]

    # Group by device_id + country — each device should map to exactly 1 country
    distinct_per_device = events.select("device_id", "country").distinct()
    device_count = events.select("device_id").distinct().count()
    distinct_count = distinct_per_device.count()

    assert distinct_count == device_count, (
        f"Expected {device_count} device→country pairs, got {distinct_count}. "
        f"seed_from should produce the same country for every row with the same device_id."
    )


def test_independence_across_columns(spark):
    """country and manufacturer both seed_from device_id but produce different values."""
    plan = _make_plan_with_seed_from()
    results = generate(spark, plan)
    events = results["events"]

    # Since column_seed differs per column, each column should produce
    # different mappings. Just verify both columns have variety.
    n_countries = events.select("country").distinct().count()
    n_manufacturers = events.select("manufacturer").distinct().count()
    assert n_countries > 1, "country column has no variety"
    assert n_manufacturers > 1, "manufacturer column has no variety"


def test_determinism(spark):
    """Same spec + seed → same output."""
    plan1 = _make_plan_with_seed_from()
    plan2 = _make_plan_with_seed_from()

    r1 = generate(spark, plan1)["events"].orderBy("id").collect()
    r2 = generate(spark, plan2)["events"].orderBy("id").collect()

    assert len(r1) == len(r2)
    for row1, row2 in zip(r1, r2):
        assert row1.asDict() == row2.asDict()


def test_non_seed_from_columns_vary(spark):
    """Columns without seed_from still vary per row."""
    plan = _make_plan_with_seed_from()
    results = generate(spark, plan)
    events = results["events"]

    # value column (no seed_from) should have many distinct values
    n_values = events.select("value").distinct().count()
    assert n_values > 10, f"value column should vary per row, got only {n_values} distinct values"


def test_seed_from_with_values_column(spark):
    """ValuesColumn with seed_from works."""
    plan = DataGenPlan(
        seed=99,
        tables=[
            TableSpec(
                name="t",
                rows=100,
                columns=[
                    pk_auto("id"),
                    ColumnSpec(
                        name="group_id",
                        dtype=DataType.INT,
                        gen=RangeColumn(min=1, max=5),
                    ),
                    ColumnSpec(
                        name="label",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["A", "B", "C", "D"]),
                        seed_from="group_id",
                    ),
                ],
                primary_key=PrimaryKey(columns=["id"]),
            ),
        ],
    )
    results = generate(spark, plan)
    df = results["t"]

    # Same group_id → same label
    distinct = df.select("group_id", "label").distinct()
    groups = df.select("group_id").distinct().count()
    assert distinct.count() == groups


def test_seed_from_with_range_column(spark):
    """RangeColumn with seed_from produces correlated values."""
    plan = DataGenPlan(
        seed=77,
        tables=[
            TableSpec(
                name="t",
                rows=100,
                columns=[
                    pk_auto("id"),
                    ColumnSpec(
                        name="category",
                        dtype=DataType.INT,
                        gen=RangeColumn(min=1, max=10),
                    ),
                    ColumnSpec(
                        name="score",
                        dtype=DataType.INT,
                        gen=RangeColumn(min=0, max=100),
                        seed_from="category",
                    ),
                ],
                primary_key=PrimaryKey(columns=["id"]),
            ),
        ],
    )
    results = generate(spark, plan)
    df = results["t"]

    # Same category → same score
    distinct = df.select("category", "score").distinct()
    categories = df.select("category").distinct().count()
    assert distinct.count() == categories


def test_seed_from_with_timestamp_column(spark):
    """TimestampColumn with seed_from produces correlated values."""
    plan = DataGenPlan(
        seed=55,
        tables=[
            TableSpec(
                name="t",
                rows=100,
                columns=[
                    pk_auto("id"),
                    ColumnSpec(
                        name="region",
                        dtype=DataType.INT,
                        gen=RangeColumn(min=1, max=5),
                    ),
                    ColumnSpec(
                        name="created_at",
                        dtype=DataType.TIMESTAMP,
                        gen=TimestampColumn(start="2020-01-01", end="2025-12-31"),
                        seed_from="region",
                    ),
                ],
                primary_key=PrimaryKey(columns=["id"]),
            ),
        ],
    )
    results = generate(spark, plan)
    df = results["t"]

    # Same region → same created_at
    distinct = df.select("region", "created_at").distinct()
    regions = df.select("region").distinct().count()
    assert distinct.count() == regions


def test_seed_from_nonexistent_column_raises(spark):
    """seed_from referencing a non-existent column raises at generation time."""
    plan = DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="t",
                rows=10,
                columns=[
                    pk_auto("id"),
                    ColumnSpec(
                        name="label",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["A", "B"]),
                        seed_from="nonexistent",
                    ),
                ],
                primary_key=PrimaryKey(columns=["id"]),
            ),
        ],
    )
    with pytest.raises(Exception):
        # Spark will raise AnalysisException when the column doesn't exist
        results = generate(spark, plan)
        results["t"].collect()


def test_seed_from_null_fraction_correlated(spark):
    """Null masking with seed_from is also correlated (same source → consistent nullability)."""
    plan = DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="t",
                rows=200,
                columns=[
                    pk_auto("id"),
                    ColumnSpec(
                        name="group_id",
                        dtype=DataType.INT,
                        gen=RangeColumn(min=1, max=5),
                    ),
                    ColumnSpec(
                        name="optional_val",
                        dtype=DataType.STRING,
                        gen=ValuesColumn(values=["X", "Y", "Z"]),
                        nullable=True,
                        null_fraction=0.4,
                        seed_from="group_id",
                    ),
                ],
                primary_key=PrimaryKey(columns=["id"]),
            ),
        ],
    )
    results = generate(spark, plan)
    df = results["t"]

    # Same group_id should have the same null/non-null status for optional_val
    # Check: for each group_id, optional_val is either always null or always the same value
    from pyspark.sql import functions as F

    checks = df.groupBy("group_id").agg(
        F.countDistinct(F.when(F.col("optional_val").isNull(), "__NULL__").otherwise(F.col("optional_val"))).alias(
            "n_distinct"
        ),
    )
    # Each group_id should map to exactly 1 distinct value (including NULL)
    max_distinct = checks.agg(F.max("n_distinct")).collect()[0][0]
    assert max_distinct == 1, (
        f"Expected each group_id to have exactly 1 distinct optional_val, " f"but found up to {max_distinct}"
    )


def test_dsl_seed_from_passthrough(spark):
    """DSL helpers correctly pass seed_from to ColumnSpec."""
    plan = DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="t",
                rows=50,
                columns=[
                    pk_auto("id"),
                    integer("group_id", min_val=1, max_val=5),
                    text("label", values=["A", "B", "C"], seed_from="group_id"),
                    integer("score", min_val=0, max_val=100, seed_from="group_id"),
                    decimal("amount", min_val=0.0, max_val=100.0, seed_from="group_id"),
                    timestamp("ts", seed_from="group_id"),
                ],
                primary_key=PrimaryKey(columns=["id"]),
            ),
        ],
    )
    results = generate(spark, plan)
    df = results["t"]

    # All seed_from columns should be correlated with group_id
    for col_name in ["label", "score", "amount", "ts"]:
        distinct = df.select("group_id", col_name).distinct()
        groups = df.select("group_id").distinct().count()
        assert distinct.count() == groups, (
            f"Column {col_name} with seed_from='group_id' should produce "
            f"1 value per group_id, got {distinct.count()} for {groups} groups"
        )
