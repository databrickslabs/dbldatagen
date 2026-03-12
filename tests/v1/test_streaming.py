"""Tests for streaming DataFrame generation.

Covers:
- _validate_streaming_spec: all validation branches
- _resolve_streaming_fk: FK metadata resolution
- generate_stream: end-to-end streaming DataFrame creation
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from dbldatagen.v1 import TableSpec, generate_stream
from dbldatagen.v1.engine.streaming import _resolve_streaming_fk, _validate_streaming_spec
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
# Reusable parent specs
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collect_streaming(spark: SparkSession, sdf, query_name: str, wait_seconds: float = 3.0):
    """Write streaming DF to memory sink, wait for data, return collected rows."""
    import time

    query = sdf.writeStream.format("memory").queryName(query_name).outputMode("append").start()
    try:
        time.sleep(wait_seconds)
        query.processAllAvailable()
    finally:
        query.stop()
    return spark.sql(f"SELECT * FROM {query_name}")


# ===========================================================================
# _validate_streaming_spec tests
# ===========================================================================


class TestValidateStreamingSpec:
    """Direct tests for _validate_streaming_spec covering all branches."""

    def test_valid_spec_no_pk(self):
        """Non-PK columns pass validation."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
        )
        _validate_streaming_spec(spec)  # should not raise

    def test_valid_spec_sequence_pk(self):
        """Sequence PK passes validation."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn(start=1, step=1))],
            primary_key=PrimaryKey(columns=["id"]),
        )
        _validate_streaming_spec(spec)  # should not raise

    def test_valid_spec_uuid_pk(self):
        """UUID PK passes validation."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="id", dtype=DataType.STRING, gen=UUIDColumn())],
            primary_key=PrimaryKey(columns=["id"]),
        )
        _validate_streaming_spec(spec)  # should not raise

    def test_valid_spec_pattern_pk(self):
        """Pattern PK passes validation."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="id", dtype=DataType.STRING, gen=PatternColumn(template="ID-{digit:6}"))],
            primary_key=PrimaryKey(columns=["id"]),
        )
        _validate_streaming_spec(spec)  # should not raise

    def test_rejects_feistel_pk(self):
        """RangeColumn as PK (Feistel/random-unique) is rejected."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="id", dtype=DataType.LONG, gen=RangeColumn(min=0, max=1000))],
            primary_key=PrimaryKey(columns=["id"]),
        )
        with pytest.raises(ValueError, match="random-unique"):
            _validate_streaming_spec(spec)

    def test_rejects_fk_without_parent_specs(self):
        """FK column without parent_specs raises ValueError."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[
                ColumnSpec(
                    name="parent_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="parents.id"),
                ),
            ],
        )
        with pytest.raises(ValueError, match="parent_specs"):
            _validate_streaming_spec(spec, parent_specs=None)

    def test_rejects_fk_missing_parent_table(self):
        """FK referencing a parent table not in parent_specs raises ValueError."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.id"),
                ),
            ],
        )
        with pytest.raises(ValueError, match="not found in parent_specs"):
            _validate_streaming_spec(spec, parent_specs={"other_table": _PARENT_SEQ})

    def test_rejects_fk_missing_parent_column(self):
        """FK referencing a non-existent column in parent raises ValueError."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.nonexistent"),
                ),
            ],
        )
        with pytest.raises(ValueError, match="not found in table"):
            _validate_streaming_spec(spec, parent_specs={"customers": _PARENT_SEQ})

    def test_rejects_fk_referencing_non_pk_column(self):
        """FK referencing a column that is not a PK raises ValueError."""
        parent_with_non_pk = TableSpec(
            name="customers",
            rows=100,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[
                ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="email", dtype=DataType.STRING, gen=ConstantColumn(value="a@b.com")),
            ],
            seed=42,
        )
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[
                ColumnSpec(
                    name="email_ref",
                    dtype=DataType.STRING,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.email"),
                ),
            ],
        )
        with pytest.raises(ValueError, match="is not a primary key"):
            _validate_streaming_spec(spec, parent_specs={"customers": parent_with_non_pk})

    def test_valid_fk_with_correct_parent_specs(self):
        """FK with correct parent_specs passes validation."""
        spec = TableSpec(
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
        )
        _validate_streaming_spec(spec, parent_specs={"customers": _PARENT_SEQ})  # should not raise

    def test_rejects_fk_parent_without_pk(self):
        """FK referencing a parent that has no primary_key at all raises ValueError."""
        parent_no_pk = TableSpec(
            name="customers",
            rows=100,
            columns=[
                ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn(start=1, step=1)),
            ],
            seed=42,
        )
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.id"),
                ),
            ],
        )
        with pytest.raises(ValueError, match="is not a primary key"):
            _validate_streaming_spec(spec, parent_specs={"customers": parent_no_pk})


# ===========================================================================
# _resolve_streaming_fk tests
# ===========================================================================


class TestResolveStreamingFK:
    """Direct tests for _resolve_streaming_fk."""

    def test_resolve_single_sequential_fk(self):
        """Resolves a single FK to a sequential PK parent."""
        child = TableSpec(
            name="orders",
            rows=0,
            columns=[
                ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.id"),
                ),
            ],
            primary_key=PrimaryKey(columns=["id"]),
            seed=42,
        )
        result = _resolve_streaming_fk(child, {"customers": _PARENT_SEQ}, global_seed=42)

        assert ("orders", "customer_id") in result
        fk_res = result[("orders", "customer_id")]
        assert fk_res.child_table == "orders"
        assert fk_res.child_column == "customer_id"
        assert fk_res.parent_meta.table_name == "customers"
        assert fk_res.parent_meta.pk_column == "id"
        assert fk_res.parent_meta.row_count == 1000
        assert fk_res.parent_meta.pk_type == "sequence"

    def test_resolve_uuid_fk(self):
        """Resolves a FK to a UUID PK parent."""
        child = TableSpec(
            name="sessions",
            rows=0,
            columns=[
                ColumnSpec(
                    name="user_id",
                    dtype=DataType.STRING,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="users.uid"),
                ),
            ],
            seed=42,
        )
        result = _resolve_streaming_fk(child, {"users": _PARENT_UUID}, global_seed=42)

        assert ("sessions", "user_id") in result
        fk_res = result[("sessions", "user_id")]
        assert fk_res.parent_meta.pk_type == "uuid"
        assert fk_res.parent_meta.row_count == 500

    def test_resolve_pattern_fk(self):
        """Resolves a FK to a pattern PK parent."""
        child = TableSpec(
            name="items",
            rows=0,
            columns=[
                ColumnSpec(
                    name="product_sku",
                    dtype=DataType.STRING,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="products.sku"),
                ),
            ],
            seed=42,
        )
        result = _resolve_streaming_fk(child, {"products": _PARENT_PATTERN}, global_seed=42)

        assert ("items", "product_sku") in result
        fk_res = result[("items", "product_sku")]
        assert fk_res.parent_meta.pk_type == "pattern"
        assert fk_res.parent_meta.pk_template == "SKU-{digit:6}"

    def test_resolve_multi_fk(self):
        """Resolves multiple FK columns to different parents."""
        child = TableSpec(
            name="order_items",
            rows=0,
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
            ],
            primary_key=PrimaryKey(columns=["id"]),
            seed=42,
        )
        result = _resolve_streaming_fk(
            child,
            {"customers": _PARENT_SEQ, "products": _PARENT_PATTERN},
            global_seed=42,
        )

        assert len(result) == 2
        assert ("order_items", "customer_id") in result
        assert ("order_items", "product_sku") in result

    def test_resolve_skips_non_fk_columns(self):
        """Non-FK columns are not included in the resolution."""
        child = TableSpec(
            name="t",
            rows=0,
            columns=[
                ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10)),
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.id"),
                ),
            ],
            seed=42,
        )
        result = _resolve_streaming_fk(child, {"customers": _PARENT_SEQ}, global_seed=42)
        assert len(result) == 1  # only the FK column

    def test_resolve_preserves_distribution(self):
        """FK distribution (e.g., Zipf) is preserved in the resolution."""
        child = TableSpec(
            name="orders",
            rows=0,
            columns=[
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.id", distribution=Zipf(exponent=2.0)),
                ),
            ],
            seed=42,
        )
        result = _resolve_streaming_fk(child, {"customers": _PARENT_SEQ}, global_seed=42)
        fk_res = result[("orders", "customer_id")]
        assert isinstance(fk_res.distribution, Zipf)
        assert fk_res.distribution.exponent == 2.0

    def test_resolve_preserves_null_fraction(self):
        """FK null_fraction is preserved in the resolution."""
        child = TableSpec(
            name="orders",
            rows=0,
            columns=[
                ColumnSpec(
                    name="customer_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="customers.id", nullable=True, null_fraction=0.3),
                ),
            ],
            seed=42,
        )
        result = _resolve_streaming_fk(child, {"customers": _PARENT_SEQ}, global_seed=42)
        fk_res = result[("orders", "customer_id")]
        assert fk_res.null_fraction == 0.3

    def test_resolve_no_fk_columns_returns_empty(self):
        """Spec with no FK columns returns an empty dict."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
            seed=42,
        )
        result = _resolve_streaming_fk(spec, {"customers": _PARENT_SEQ}, global_seed=42)
        assert result == {}


# ===========================================================================
# generate_stream tests (require Spark)
# ===========================================================================


class TestGenerateStream:
    """End-to-end tests for generate_stream."""

    def test_returns_streaming_dataframe(self, spark):
        """generate_stream returns a streaming DataFrame."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
        )
        sdf = generate_stream(spark, spec, rows_per_second=100)
        assert sdf.isStreaming

    def test_default_seed_42(self, spark):
        """When spec has no seed, default seed 42 is used (no error)."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
        )
        sdf = generate_stream(spark, spec, rows_per_second=100)
        assert sdf.isStreaming

    def test_custom_seed(self, spark):
        """Spec with explicit seed produces a streaming DataFrame."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
            seed=99,
        )
        sdf = generate_stream(spark, spec, rows_per_second=100)
        assert sdf.isStreaming

    def test_num_partitions_option(self, spark):
        """num_partitions is accepted without error."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
            seed=42,
        )
        sdf = generate_stream(spark, spec, rows_per_second=100, num_partitions=2)
        assert sdf.isStreaming

    def test_schema_matches_spec(self, spark):
        """Streaming DataFrame has the correct columns."""
        spec = TableSpec(
            name="events",
            rows=0,
            columns=[
                ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
                ColumnSpec(name="score", dtype=DataType.INT, gen=RangeColumn(min=0, max=100)),
                ColumnSpec(name="status", dtype=DataType.STRING, gen=ValuesColumn(values=["a", "b"])),
            ],
            primary_key=PrimaryKey(columns=["id"]),
            seed=42,
        )
        sdf = generate_stream(spark, spec, rows_per_second=100)
        assert sdf.isStreaming
        assert set(sdf.columns) == {"id", "score", "status"}

    def test_rejects_feistel_pk(self, spark):
        """Feistel/random-unique PK raises ValueError via validation."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="id", dtype=DataType.LONG, gen=RangeColumn(min=0, max=1000))],
            primary_key=PrimaryKey(columns=["id"]),
            seed=42,
        )
        with pytest.raises(ValueError, match="random-unique"):
            generate_stream(spark, spec, rows_per_second=100)

    def test_rejects_fk_without_parent_specs(self, spark):
        """FK columns without parent_specs raise ValueError."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[
                ColumnSpec(
                    name="parent_id",
                    dtype=DataType.LONG,
                    gen=ConstantColumn(value=None),
                    foreign_key=ForeignKeyRef(ref="parents.id"),
                ),
            ],
            seed=42,
        )
        with pytest.raises(ValueError, match="parent_specs"):
            generate_stream(spark, spec, rows_per_second=100)

    def test_fk_with_parent_specs_returns_streaming(self, spark):
        """FK columns with valid parent_specs produce a streaming DataFrame."""
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
        assert "customer_id" in sdf.columns

    def test_no_parent_specs_skips_fk_resolution(self, spark):
        """When no FK columns and no parent_specs, fk_resolutions is empty."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(min=0, max=10))],
            seed=42,
        )
        sdf = generate_stream(spark, spec, rows_per_second=100)
        assert sdf.isStreaming

    def test_streaming_range_column_data(self, spark):
        """Streaming range column produces bounded values."""
        spec = TableSpec(
            name="t",
            rows=0,
            columns=[ColumnSpec(name="val", dtype=DataType.INT, gen=RangeColumn(min=1, max=100))],
            seed=42,
        )
        sdf = generate_stream(spark, spec, rows_per_second=100)
        result = _collect_streaming(spark, sdf, "test_range_data")
        assert result.count() > 0
        stats = result.agg({"val": "min"}).collect()[0][0]
        assert stats >= 1
        stats = result.agg({"val": "max"}).collect()[0][0]
        assert stats <= 100

    def test_streaming_fk_sequential_pk_data(self, spark):
        """FK to sequential PK produces valid parent key values."""
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
        result = _collect_streaming(spark, sdf, "test_fk_seq_data")
        assert result.count() > 0
        fk_min = result.agg({"customer_id": "min"}).collect()[0][0]
        fk_max = result.agg({"customer_id": "max"}).collect()[0][0]
        assert fk_min >= 1
        assert fk_max <= 1000

    def test_streaming_multi_fk_parents(self, spark):
        """Streaming with FK columns to multiple parents."""
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
        assert set(sdf.columns) == {"id", "customer_id", "product_sku"}
