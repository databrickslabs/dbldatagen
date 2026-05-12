"""Tests for nested column types: StructColumn and ArrayColumn.

Covers struct generation, array generation, nesting, JSON round-trip,
and determinism.
"""

from __future__ import annotations

import os
import tempfile

from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.core import DataGenPlan, PrimaryKey, TableSpec, generate
from dbldatagen.core.spec import dsl as dg
from dbldatagen.core.spec.schema import (
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
                    dg.pk_auto("item_id"),
                    dg.struct(
                        "address",
                        [
                            dg.text("city", ["Austin", "NYC", "LA", "Chicago"]),
                            dg.text("state", ["TX", "NY", "CA", "IL"]),
                            dg.integer("zip", min=10000, max=99999),
                        ],
                    ),
                    dg.array(
                        "tags",
                        ValuesColumn(values=["sale", "new", "popular", "clearance"]),
                        min_length=1,
                        max_length=4,
                    ),
                    dg.integer("price", min=1, max=500),
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
                        dg.pk_auto("tid"),
                        dg.array("nums", RangeColumn(min=1, max=100), min_length=3, max_length=3),
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
                        dg.pk_auto("tid"),
                        ColumnSpec(
                            name="contact",
                            gen=StructColumn(
                                fields=[
                                    dg.text("name", ["Alice", "Bob"]),
                                    ColumnSpec(
                                        name="location",
                                        gen=StructColumn(
                                            fields=[
                                                dg.text("city", ["Austin", "NYC"]),
                                                dg.integer("zip", min=10000, max=99999),
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
                        dg.pk_auto("tid"),
                        dg.array("scores", RangeColumn(min=0, max=100), min_length=2, max_length=5),
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


class TestNullFractionOnNested:
    """Pins value-level behavior at ``null_fraction=1.0`` on struct
    fields and array columns.  Element-level ``null_fraction`` isn't
    expressible (``ArrayColumn.element`` is a raw ``ColumnStrategy``
    with no per-element null_fraction field), so coverage stops at
    the field-on-struct and column-on-array boundary.  Both paths had
    previously slipped through the main null-fraction tests which
    only exercised ~0.3."""

    def test_struct_field_null_fraction_one(self, spark):
        """A struct field with null_fraction=1.0 should produce all NULLs
        in that field without breaking the surrounding struct type."""
        plan = DataGenPlan(
            seed=11,
            tables=[
                TableSpec(
                    name="items",
                    rows=30,
                    primary_key=PrimaryKey(columns=["item_id"]),
                    columns=[
                        dg.pk_auto("item_id"),
                        ColumnSpec(
                            name="addr",
                            gen=StructColumn(
                                fields=[
                                    dg.text("city", values=["Austin", "NYC"]),
                                    ColumnSpec(
                                        name="zip",
                                        gen=RangeColumn(min=10000, max=99999),
                                        null_fraction=1.0,
                                    ),
                                ]
                            ),
                        ),
                    ],
                ),
            ],
        )
        df = generate(spark, plan)["items"]
        rows = df.collect()
        assert all(r.addr.zip is None for r in rows), "every zip should be NULL at null_fraction=1.0"
        # The sibling field must still carry non-null values -- the
        # null injection should be field-local, not leak to the struct.
        assert any(r.addr.city is not None for r in rows)

    def test_array_column_null_fraction_one(self, spark):
        """An array *column* with ``null_fraction=1.0`` produces NULL
        array values on every row -- distinct from an empty array in
        Spark (``null`` vs ``[]``).  Element-level null_fraction isn't
        a schema concept, so this pins the column-level boundary."""
        plan = DataGenPlan(
            seed=13,
            tables=[
                TableSpec(
                    name="items",
                    rows=20,
                    primary_key=PrimaryKey(columns=["item_id"]),
                    columns=[
                        dg.pk_auto("item_id"),
                        ColumnSpec(
                            name="tags",
                            gen=ArrayColumn(
                                element=RangeColumn(min=1, max=100),
                                min_length=2,
                                max_length=2,
                            ),
                            null_fraction=1.0,
                        ),
                    ],
                ),
            ],
        )
        df = generate(spark, plan)["items"]
        rows = df.collect()
        # At null_fraction=1.0 on the array column itself, every
        # row's ``tags`` is NULL -- NOT an empty array, which is
        # distinct in Spark.
        assert all(r.tags is None for r in rows), f"expected all NULL tags, got {[r.tags for r in rows[:3]]}"
