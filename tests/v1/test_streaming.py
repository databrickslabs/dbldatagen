"""Tests for streaming DataFrame generation.

Uses Spark's memory sink to capture streaming output and assert on it.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from dbldatagen.v1 import TableSpec, generate_stream
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataType,
    FakerColumn,
    ForeignKeyRef,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
    Zipf,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collect_streaming(spark: SparkSession, sdf, query_name: str, wait_seconds: float = 3.0):
    """Write streaming DF to memory sink, wait for data, return collected rows."""
    import time

    query = sdf.writeStream.format("memory").queryName(query_name).outputMode("append").start()
    try:
        # Give the rate source time to emit rows and trigger a micro-batch
        time.sleep(wait_seconds)
        query.processAllAvailable()
    finally:
        query.stop()
    return spark.sql(f"SELECT * FROM {query_name}")


# ---------------------------------------------------------------------------
# Basic tests
# ---------------------------------------------------------------------------


def test_streaming_returns_streaming_df(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    assert sdf.isStreaming


def test_streaming_range_column(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="val", dtype=DataType.INT, gen=RangeColumn(min=1, max=100))],
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_range")
    assert result.count() > 0
    # All values should be in [1, 100]
    stats = result.agg({"val": "min"}).collect()[0][0]
    assert stats >= 1
    stats = result.agg({"val": "max"}).collect()[0][0]
    assert stats <= 100


def test_streaming_values_column(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[
            ColumnSpec(
                name="color",
                dtype=DataType.STRING,
                gen=ValuesColumn(values=["red", "green", "blue"]),
            )
        ],
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_values")
    assert result.count() > 0
    distinct = {row.color for row in result.select("color").distinct().collect()}
    assert distinct.issubset({"red", "green", "blue"})


def test_streaming_pattern_column(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="code", dtype=DataType.STRING, gen=PatternColumn(template="ORD-{digit:4}"))],
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_pattern")
    assert result.count() > 0
    sample = result.first().code
    assert sample.startswith("ORD-")
    assert len(sample) == 8  # "ORD-" + 4 digits


def test_streaming_timestamp_column(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[
            ColumnSpec(
                name="ts",
                dtype=DataType.TIMESTAMP,
                gen=TimestampColumn(start="2024-01-01", end="2024-12-31"),
            )
        ],
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_timestamp")
    assert result.count() > 0
    assert result.schema["ts"].dataType.simpleString() == "timestamp"


def test_streaming_constant_column(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="region", gen=ConstantColumn(value="us-east-1"))],
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_constant")
    assert result.count() > 0
    vals = {row.region for row in result.collect()}
    assert vals == {"us-east-1"}


# ---------------------------------------------------------------------------
# PK strategies in streaming
# ---------------------------------------------------------------------------


def test_streaming_sequence_pk(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn(start=1, step=1))],
        primary_key=PrimaryKey(columns=["id"]),
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_seq_pk")
    assert result.count() > 0


def test_streaming_uuid_pk(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="id", dtype=DataType.STRING, gen=UUIDColumn())],
        primary_key=PrimaryKey(columns=["id"]),
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_uuid_pk")
    assert result.count() > 0
    sample = result.first().id
    # UUID format: 8-4-4-4-12
    assert len(sample) == 36
    assert sample.count("-") == 4


def test_streaming_pattern_pk(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="id", dtype=DataType.STRING, gen=PatternColumn(template="USR-{digit:6}"))],
        primary_key=PrimaryKey(columns=["id"]),
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_pattern_pk")
    assert result.count() > 0
    assert result.first().id.startswith("USR-")


# ---------------------------------------------------------------------------
# Faker in streaming
# ---------------------------------------------------------------------------


def test_streaming_faker_column(spark):
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="city", dtype=DataType.STRING, gen=FakerColumn(provider="city"))],
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    result = _collect_streaming(spark, sdf, "test_faker")
    assert result.count() > 0
    assert result.first().city is not None
    assert len(result.first().city) > 0


# ---------------------------------------------------------------------------
# Validation: reject incompatible strategies
# ---------------------------------------------------------------------------


def test_streaming_rejects_feistel_pk(spark):
    """Feistel (random-unique) PK requires fixed N — should fail in streaming."""
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[ColumnSpec(name="id", dtype=DataType.LONG, gen=RangeColumn(min=0, max=1000))],
        primary_key=PrimaryKey(columns=["id"]),
        seed=42,
    )
    with pytest.raises(ValueError, match="random-unique"):
        generate_stream(spark, spec, rows_per_second=100)


def test_streaming_rejects_fk_without_parent_specs(spark):
    """FK columns without parent_specs should raise ValueError."""
    spec = TableSpec(
        name="t",
        rows=0,
        columns=[
            ColumnSpec(
                name="parent_id",
                dtype=DataType.LONG,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="parents.id"),
            )
        ],
        seed=42,
    )
    with pytest.raises(ValueError, match="parent_specs"):
        generate_stream(spark, spec, rows_per_second=100)


# ---------------------------------------------------------------------------
# Multiple columns together
# ---------------------------------------------------------------------------


def test_streaming_multi_column(spark):
    """Streaming with mixed column types."""
    spec = TableSpec(
        name="events",
        rows=0,
        columns=[
            ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ColumnSpec(name="score", dtype=DataType.INT, gen=RangeColumn(min=0, max=100)),
            ColumnSpec(
                name="status",
                dtype=DataType.STRING,
                gen=ValuesColumn(values=["active", "inactive", "pending"]),
            ),
            ColumnSpec(
                name="ts",
                dtype=DataType.TIMESTAMP,
                gen=TimestampColumn(start="2024-01-01", end="2024-12-31"),
            ),
        ],
        primary_key=PrimaryKey(columns=["id"]),
        seed=42,
    )
    sdf = generate_stream(spark, spec, rows_per_second=100)
    assert sdf.isStreaming
    result = _collect_streaming(spark, sdf, "test_multi")
    assert result.count() > 0
    # Check all columns are present
    col_names = set(result.columns)
    assert col_names == {"id", "score", "status", "ts"}


# ---------------------------------------------------------------------------
# FK columns in streaming (Phase 2)
# ---------------------------------------------------------------------------

# Reusable parent specs for FK tests

_PARENT_SEQ = TableSpec(
    name="customers",
    rows=1000,
    primary_key=PrimaryKey(columns=["id"]),
    columns=[ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn(start=1, step=1))],
    seed=42,
)

_PARENT_UUID = TableSpec(
    name="users",
    rows=500,
    primary_key=PrimaryKey(columns=["uid"]),
    columns=[ColumnSpec(name="uid", dtype=DataType.STRING, gen=UUIDColumn())],
    seed=42,
)

_PARENT_PATTERN = TableSpec(
    name="products",
    rows=200,
    primary_key=PrimaryKey(columns=["sku"]),
    columns=[ColumnSpec(name="sku", dtype=DataType.STRING, gen=PatternColumn(template="SKU-{digit:6}"))],
    seed=42,
)


def test_streaming_fk_sequential_pk(spark):
    """Stream child with FK to sequential PK parent."""
    child = TableSpec(
        name="orders",
        rows=0,
        primary_key=PrimaryKey(columns=["id"]),
        columns=[
            ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn(start=1, step=1)),
            ColumnSpec(
                name="customer_id",
                dtype=DataType.LONG,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="customers.id"),
            ),
        ],
        seed=42,
    )
    sdf = generate_stream(spark, child, rows_per_second=100, parent_specs={"customers": _PARENT_SEQ})
    assert sdf.isStreaming
    result = _collect_streaming(spark, sdf, "test_fk_seq")
    assert result.count() > 0
    # FK values should be in parent PK range [1, 1000]
    fk_min = result.agg({"customer_id": "min"}).collect()[0][0]
    fk_max = result.agg({"customer_id": "max"}).collect()[0][0]
    assert fk_min >= 1
    assert fk_max <= 1000


def test_streaming_fk_uuid_pk(spark):
    """Stream child with FK to UUID PK parent."""
    child = TableSpec(
        name="sessions",
        rows=0,
        primary_key=PrimaryKey(columns=["id"]),
        columns=[
            ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ColumnSpec(
                name="user_id",
                dtype=DataType.STRING,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="users.uid"),
            ),
        ],
        seed=42,
    )
    sdf = generate_stream(spark, child, rows_per_second=100, parent_specs={"users": _PARENT_UUID})
    result = _collect_streaming(spark, sdf, "test_fk_uuid")
    assert result.count() > 0
    sample = result.first().user_id
    # UUID format: 8-4-4-4-12
    assert len(sample) == 36
    assert sample.count("-") == 4


def test_streaming_fk_pattern_pk(spark):
    """Stream child with FK to pattern PK parent."""
    child = TableSpec(
        name="order_items",
        rows=0,
        primary_key=PrimaryKey(columns=["id"]),
        columns=[
            ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ColumnSpec(
                name="product_sku",
                dtype=DataType.STRING,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="products.sku"),
            ),
        ],
        seed=42,
    )
    sdf = generate_stream(spark, child, rows_per_second=100, parent_specs={"products": _PARENT_PATTERN})
    result = _collect_streaming(spark, sdf, "test_fk_pattern")
    assert result.count() > 0
    sample = result.first().product_sku
    assert sample.startswith("SKU-")
    assert len(sample) == 10  # "SKU-" + 6 digits


def test_streaming_fk_nullable(spark):
    """FK with null_fraction should produce some NULLs."""
    child = TableSpec(
        name="orders",
        rows=0,
        primary_key=PrimaryKey(columns=["id"]),
        columns=[
            ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ColumnSpec(
                name="customer_id",
                dtype=DataType.LONG,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="customers.id", nullable=True, null_fraction=0.5),
            ),
        ],
        seed=42,
    )
    sdf = generate_stream(spark, child, rows_per_second=100, parent_specs={"customers": _PARENT_SEQ})
    result = _collect_streaming(spark, sdf, "test_fk_null")
    assert result.count() > 0
    null_count = result.filter("customer_id IS NULL").count()
    non_null_count = result.filter("customer_id IS NOT NULL").count()
    # With 50% null fraction, we should see some of each
    assert null_count > 0
    assert non_null_count > 0


def test_streaming_fk_zipf_distribution(spark):
    """FK with Zipf distribution should skew toward low parent indices."""
    child = TableSpec(
        name="orders",
        rows=0,
        primary_key=PrimaryKey(columns=["id"]),
        columns=[
            ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ColumnSpec(
                name="customer_id",
                dtype=DataType.LONG,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="customers.id", distribution=Zipf(exponent=2.0)),
            ),
        ],
        seed=42,
    )
    sdf = generate_stream(spark, child, rows_per_second=100, parent_specs={"customers": _PARENT_SEQ})
    result = _collect_streaming(spark, sdf, "test_fk_zipf")
    assert result.count() > 0
    # With Zipf(2.0) over 1000 parents, FK values should skew toward lower indices.
    # The bottom half of parents should get more than 50% of references.
    low_count = result.filter("customer_id <= 500").count()
    total = result.count()
    assert low_count / total > 0.5  # Bottom half of parents gets majority


def test_streaming_fk_rejects_missing_parent(spark):
    """FK referencing a parent not in parent_specs should raise ValueError."""
    child = TableSpec(
        name="orders",
        rows=0,
        columns=[
            ColumnSpec(
                name="customer_id",
                dtype=DataType.LONG,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="customers.id"),
            ),
        ],
        seed=42,
    )
    with pytest.raises(ValueError, match="not found in parent_specs"):
        generate_stream(
            spark,
            child,
            rows_per_second=100,
            parent_specs={"other_table": _PARENT_SEQ},
        )


def test_streaming_fk_multi_parent(spark):
    """Stream child with FK columns to multiple parent tables."""
    child = TableSpec(
        name="order_items",
        rows=0,
        primary_key=PrimaryKey(columns=["id"]),
        columns=[
            ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
            ColumnSpec(
                name="customer_id",
                dtype=DataType.LONG,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="customers.id"),
            ),
            ColumnSpec(
                name="product_sku",
                dtype=DataType.STRING,
                gen=ConstantColumn(value=None),
                foreign_key=ForeignKeyRef(ref="products.sku"),
            ),
            ColumnSpec(name="qty", dtype=DataType.INT, gen=RangeColumn(min=1, max=10)),
        ],
        seed=42,
    )
    sdf = generate_stream(
        spark,
        child,
        rows_per_second=100,
        parent_specs={"customers": _PARENT_SEQ, "products": _PARENT_PATTERN},
    )
    assert sdf.isStreaming
    result = _collect_streaming(spark, sdf, "test_fk_multi")
    assert result.count() > 0
    assert set(result.columns) == {"id", "customer_id", "product_sku", "qty"}
    # Verify FK values
    fk_min = result.agg({"customer_id": "min"}).collect()[0][0]
    assert fk_min >= 1
    sample_sku = result.first().product_sku
    assert sample_sku.startswith("SKU-")
