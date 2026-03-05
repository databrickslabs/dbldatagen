"""Tests for nested column types: StructColumn and ArrayColumn.

Covers struct generation, array generation, nesting, JSON round-trip,
CDC compatibility, and determinism.
"""

from __future__ import annotations

import os
import tempfile

from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.v1 import (
    DataGenPlan,
    PrimaryKey,
    TableSpec,
    array,
    generate,
    integer,
    pk_auto,
    struct,
    text,
)
from dbldatagen.v1.schema import (
    ArrayColumn,
    ColumnSpec,
    RangeColumn,
    StructColumn,
    ValuesColumn,
)


def _nested_plan(rows=100, seed=42):
    return DataGenPlan(
        seed=seed,
        tables=[
            TableSpec(
                name="items",
                rows=rows,
                primary_key=PrimaryKey(columns=["item_id"]),
                columns=[
                    pk_auto("item_id"),
                    struct(
                        "address",
                        [
                            text("city", ["Austin", "NYC", "LA", "Chicago"]),
                            text("state", ["TX", "NY", "CA", "IL"]),
                            integer("zip", min=10000, max=99999),
                        ],
                    ),
                    array(
                        "tags",
                        ValuesColumn(values=["sale", "new", "popular", "clearance"]),
                        min_length=1,
                        max_length=4,
                    ),
                    integer("price", min=1, max=500),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# StructColumn
# ---------------------------------------------------------------------------


class TestStructColumn:
    def test_struct_schema(self, spark):
        dfs = generate(spark, _nested_plan(rows=10))
        schema = dfs["items"].schema
        addr_field = schema["address"]
        assert isinstance(addr_field.dataType, T.StructType)
        field_names = [f.name for f in addr_field.dataType.fields]
        assert "city" in field_names
        assert "state" in field_names
        assert "zip" in field_names

    def test_struct_field_access(self, spark):
        dfs = generate(spark, _nested_plan(rows=10))
        cities = [r.city for r in dfs["items"].select("address.city").collect()]
        assert all(c in {"Austin", "NYC", "LA", "Chicago"} for c in cities)

    def test_struct_no_nulls(self, spark):
        dfs = generate(spark, _nested_plan(rows=50))
        null_count = dfs["items"].filter(F.col("address").isNull()).count()
        assert null_count == 0

    def test_struct_row_count(self, spark):
        dfs = generate(spark, _nested_plan(rows=100))
        assert dfs["items"].count() == 100


# ---------------------------------------------------------------------------
# ArrayColumn
# ---------------------------------------------------------------------------


class TestArrayColumn:
    def test_array_schema(self, spark):
        dfs = generate(spark, _nested_plan(rows=10))
        schema = dfs["items"].schema
        tags_field = schema["tags"]
        assert isinstance(tags_field.dataType, T.ArrayType)

    def test_array_length_bounds(self, spark):
        dfs = generate(spark, _nested_plan(rows=200))
        lengths = [r[0] for r in dfs["items"].select(F.size("tags")).collect()]
        assert all(1 <= length <= 4 for length in lengths)

    def test_array_values_from_pool(self, spark):
        dfs = generate(spark, _nested_plan(rows=50))
        rows = dfs["items"].select(F.explode("tags").alias("tag")).collect()
        valid = {"sale", "new", "popular", "clearance"}
        assert all(r.tag in valid for r in rows)

    def test_fixed_length_array(self, spark):
        plan = DataGenPlan(
            seed=42,
            tables=[
                TableSpec(
                    name="t",
                    rows=50,
                    columns=[
                        pk_auto("tid"),
                        array("nums", RangeColumn(min=1, max=100), min_length=3, max_length=3),
                    ],
                ),
            ],
        )
        dfs = generate(spark, plan)
        lengths = [r[0] for r in dfs["t"].select(F.size("nums")).collect()]
        assert all(length == 3 for length in lengths)


# ---------------------------------------------------------------------------
# Nested struct (struct in struct)
# ---------------------------------------------------------------------------


class TestNestedStruct:
    def test_struct_in_struct(self, spark):
        plan = DataGenPlan(
            seed=42,
            tables=[
                TableSpec(
                    name="t",
                    rows=20,
                    columns=[
                        pk_auto("tid"),
                        ColumnSpec(
                            name="contact",
                            gen=StructColumn(
                                fields=[
                                    text("name", ["Alice", "Bob"]),
                                    ColumnSpec(
                                        name="location",
                                        gen=StructColumn(
                                            fields=[
                                                text("city", ["Austin", "NYC"]),
                                                integer("zip", min=10000, max=99999),
                                            ]
                                        ),
                                    ),
                                ]
                            ),
                        ),
                    ],
                ),
            ],
        )
        dfs = generate(spark, plan)
        # Access nested field
        cities = [r[0] for r in dfs["t"].select("contact.location.city").collect()]
        assert all(c in {"Austin", "NYC"} for c in cities)

    def test_array_of_integers(self, spark):
        plan = DataGenPlan(
            seed=42,
            tables=[
                TableSpec(
                    name="t",
                    rows=30,
                    columns=[
                        pk_auto("tid"),
                        array("scores", RangeColumn(min=0, max=100), min_length=2, max_length=5),
                    ],
                ),
            ],
        )
        dfs = generate(spark, plan)
        rows = dfs["t"].select(F.explode("scores").alias("s")).collect()
        assert all(0 <= r.s <= 100 for r in rows)


# ---------------------------------------------------------------------------
# JSON round-trip
# ---------------------------------------------------------------------------


class TestJSONRoundTrip:
    def test_write_read_json_preserves_struct(self, spark):
        dfs = generate(spark, _nested_plan(rows=50))
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "items_json")
            dfs["items"].write.json(path)
            df_back = spark.read.json(path)
            assert "address" in df_back.columns
            addr_type = df_back.schema["address"].dataType
            assert isinstance(addr_type, T.StructType)

    def test_write_read_json_preserves_array(self, spark):
        dfs = generate(spark, _nested_plan(rows=50))
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "items_json")
            dfs["items"].write.json(path)
            df_back = spark.read.json(path)
            assert "tags" in df_back.columns
            tags_type = df_back.schema["tags"].dataType
            assert isinstance(tags_type, T.ArrayType)

    def test_json_row_count_preserved(self, spark):
        dfs = generate(spark, _nested_plan(rows=50))
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "items_json")
            dfs["items"].write.json(path)
            df_back = spark.read.json(path)
            assert df_back.count() == 50


# ---------------------------------------------------------------------------
# CDC with nested columns
# ---------------------------------------------------------------------------


class TestCDCWithNested:
    def test_cdc_initial_has_struct(self, spark):
        from dbldatagen.v1.cdc import generate_cdc

        stream = generate_cdc(spark, _nested_plan(rows=50), num_batches=1)
        schema = stream.initial["items"].schema
        addr_field = schema["address"]
        assert isinstance(addr_field.dataType, T.StructType)

    def test_cdc_batch_has_struct(self, spark):
        from dbldatagen.v1.cdc import generate_cdc

        stream = generate_cdc(spark, _nested_plan(rows=50), num_batches=1)
        batch_df = stream.batches[0]["items"]
        assert "address" in batch_df.columns
        # Verify struct fields are accessible
        cities = [r[0] for r in batch_df.filter("_op = 'I'").select("address.city").collect()]
        assert len(cities) > 0

    def test_cdc_batch_has_array(self, spark):
        from dbldatagen.v1.cdc import generate_cdc

        stream = generate_cdc(spark, _nested_plan(rows=50), num_batches=1)
        batch_df = stream.batches[0]["items"]
        assert "tags" in batch_df.columns


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestNestedDeterminism:
    def test_struct_deterministic(self, spark):
        dfs1 = generate(spark, _nested_plan(rows=20, seed=42))
        dfs2 = generate(spark, _nested_plan(rows=20, seed=42))
        rows1 = dfs1["items"].orderBy("item_id").collect()
        rows2 = dfs2["items"].orderBy("item_id").collect()
        for r1, r2 in zip(rows1, rows2):
            assert r1.address == r2.address

    def test_array_deterministic(self, spark):
        dfs1 = generate(spark, _nested_plan(rows=20, seed=42))
        dfs2 = generate(spark, _nested_plan(rows=20, seed=42))
        rows1 = dfs1["items"].orderBy("item_id").collect()
        rows2 = dfs2["items"].orderBy("item_id").collect()
        for r1, r2 in zip(rows1, rows2):
            assert list(r1.tags) == list(r2.tags)

    def test_different_seed_different_data(self, spark):
        dfs1 = generate(spark, _nested_plan(rows=50, seed=42))
        dfs2 = generate(spark, _nested_plan(rows=50, seed=999))
        rows1 = dfs1["items"].orderBy("item_id").collect()
        rows2 = dfs2["items"].orderBy("item_id").collect()
        some_differ = any(r1.address != r2.address for r1, r2 in zip(rows1, rows2))
        assert some_differ


# ---------------------------------------------------------------------------
# Schema serialization round-trip
# ---------------------------------------------------------------------------


class TestSchemaSerialize:
    def test_struct_column_serializes(self):
        plan = _nested_plan(rows=10)
        data = plan.model_dump(mode="json")
        plan2 = DataGenPlan.model_validate(data)
        struct_col = next(c for c in plan2.tables[0].columns if c.name == "address")
        assert isinstance(struct_col.gen, StructColumn)
        assert len(struct_col.gen.fields) == 3

    def test_array_column_serializes(self):
        plan = _nested_plan(rows=10)
        data = plan.model_dump(mode="json")
        plan2 = DataGenPlan.model_validate(data)
        arr_col = next(c for c in plan2.tables[0].columns if c.name == "tags")
        assert isinstance(arr_col.gen, ArrayColumn)
        assert arr_col.gen.min_length == 1
        assert arr_col.gen.max_length == 4
