"""Tests for nested column types: StructColumn and ArrayColumn.

Covers struct generation, array generation, nesting, JSON round-trip,
CDC compatibility, and determinism.
"""

from __future__ import annotations

import os
import tempfile

from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.core import (
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
        from dbldatagen.core.engine.cdc import generate_cdc

        stream = generate_cdc(spark, _nested_plan(rows=50), num_batches=1)
        schema = stream.initial["items"].schema
        addr_field = schema["address"]
        assert isinstance(addr_field.dataType, T.StructType)

    def test_cdc_batch_has_struct(self, spark):
        from dbldatagen.core.engine.cdc import generate_cdc

        stream = generate_cdc(spark, _nested_plan(rows=50), num_batches=1)
        batch_df = stream.batches[0]["items"]
        assert "address" in batch_df.columns
        # Verify struct fields are accessible
        cities = [r[0] for r in batch_df.filter("_op = 'I'").select("address.city").collect()]
        assert len(cities) > 0

    def test_cdc_batch_has_array(self, spark):
        from dbldatagen.core.engine.cdc import generate_cdc

        stream = generate_cdc(spark, _nested_plan(rows=50), num_batches=1)
        batch_df = stream.batches[0]["items"]
        assert "tags" in batch_df.columns

    def test_bulk_struct_pre_image_matches_per_batch(self, spark):
        """Pre-image struct values must agree between bulk and per-batch paths.

        Per-batch (``generate_cdc``) passes a scalar int seed into
        ``_build_struct_column``, which polynomial-hashes each field via
        ``derive_column_seed(parent_seed, "", field)``.  Bulk
        (``generate_cdc_bulk`` with chunk_size > 1) goes through the
        fused multi-batch path, which previously XOR'd the field hash
        against the parent column seed — not equivalent to the
        polynomial-hash the scalar path uses.  Result: UB (update
        before-image) and D rows carried different struct field values
        on the two paths.  This test pins the cross-path equality
        invariant on pre-image rows.
        """
        from dbldatagen.core.engine.cdc import generate_cdc, generate_cdc_bulk

        num_b = 5
        # Larger row count + more batches so the workload produces UB/D events.
        per = generate_cdc(spark, _nested_plan(rows=200), num_batches=num_b)
        bulk = generate_cdc_bulk(spark, _nested_plan(rows=200), num_batches=num_b, chunk_size=num_b)

        def pre_image_map(stream) -> dict[tuple[int, int, str], tuple]:
            out: dict[tuple[int, int, str], tuple] = {}
            for b in stream.batches:
                df = b["items"].filter("_op IN ('UB', 'D')")
                for r in df.collect():
                    key = (r.item_id, r._batch_id, r._op)
                    out[key] = (r.address.city, r.address.state, r.address.zip)
            return out

        per_pre = pre_image_map(per)
        bulk_pre = pre_image_map(bulk)

        assert per_pre, "no UB/D rows collected from per-batch path — test setup too small"
        assert set(per_pre.keys()) == set(
            bulk_pre.keys()
        ), "bulk and per-batch paths emitted different pre-image (pk, batch, op) tuples"
        for key in per_pre:
            assert bulk_pre[key] == per_pre[key], (
                f"struct pre-image values diverge for {key}: " f"per-batch={per_pre[key]}, bulk={bulk_pre[key]}"
            )


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


# ---------------------------------------------------------------------------
# Regression: sibling struct fields must NOT collapse on the Column-seed path
# ---------------------------------------------------------------------------


class TestStructFieldSeedIndependence:
    """Regression: the Column-seed path must mix in field names so
    sibling struct fields derive independent per-cell seeds.  An earlier
    bug had it passing the parent seed unchanged (100% field agreement);
    a later bug XOR'd with a per-field constant (independent from each
    other, but divergent from the scalar path's polynomial hash).  The
    current implementation precomputes polynomial-hashed child seeds on
    the driver via ``struct_field_seed_map`` so the Column path matches
    the scalar path byte-for-byte and siblings stay independent.
    """

    def test_struct_fields_independent_on_column_seed(self, spark):
        from dbldatagen.core.engine.generator import _build_struct_column
        from dbldatagen.core.engine.seed import column_seed_lookup, column_seed_map

        gen = StructColumn(
            fields=[
                text("a", values=["x", "y", "z", "w", "v"]),
                text("b", values=["x", "y", "z", "w", "v"]),
            ]
        )
        # Synthetic dyn_ctx mimicking the fused multi-batch path: one
        # write_batch value, a ``_write_batch`` column, and a parent seed
        # sourced from ``column_seed_map`` — the exact shape
        # ``_build_exprs_dynamic`` hands in.
        df = spark.range(500).withColumn("_write_batch", F.lit(0).cast("long"))
        parent_map = column_seed_map(global_seed=1, unique_wbs=[0], table_name="t", column_name="parent")
        parent_seed = column_seed_lookup(parent_map, F.col("_write_batch"))
        struct_col = _build_struct_column(
            gen,
            F.col("id"),
            parent_seed,
            row_count=500,
            global_seed=1,
            parent_col_name="parent",
            dyn_ctx=("t", [0], F.col("_write_batch")),
        )
        rows = df.select(struct_col.alias("s")).collect()
        matches = sum(1 for r in rows if r.s.a == r.s.b)
        # Independent fields with 5 values each should match on ~1/5 of rows.
        # The buggy "pass through unchanged" code would give 100% matches.
        assert matches / len(rows) < 0.5, (
            f"Sibling struct fields 'a' and 'b' agreed on {matches}/{len(rows)} rows "
            f"(~100% indicates the Column-seed path is not mixing field names)."
        )

    def test_struct_fields_independent_in_multi_batch_cdc(self, spark):
        """End-to-end check via generate_cdc with num_batches=3, which routes
        through the fused multi-batch path (``_build_exprs_dynamic``).
        """
        from dbldatagen.core.engine.cdc import generate_cdc

        plan = DataGenPlan(
            seed=7,
            tables=[
                TableSpec(
                    name="items",
                    rows=300,
                    primary_key=PrimaryKey(columns=["item_id"]),
                    columns=[
                        pk_auto("item_id"),
                        ColumnSpec(
                            name="payload",
                            gen=StructColumn(
                                fields=[
                                    text("left", values=["x", "y", "z", "w", "v"]),
                                    text("right", values=["x", "y", "z", "w", "v"]),
                                ]
                            ),
                        ),
                    ],
                ),
            ],
        )
        stream = generate_cdc(spark, plan, num_batches=3)
        # Gather all insert rows across batches (batches use the Column-seed path)
        matches = 0
        total = 0
        for batch in stream.batches:
            df = batch["items"].filter("_op = 'I'")
            for row in df.collect():
                total += 1
                if row.payload.left == row.payload.right:
                    matches += 1
        assert total > 0, "no insert rows collected from fused CDC batches"
        assert matches / total < 0.5, (
            f"Sibling struct fields in fused CDC agreed on {matches}/{total} inserts "
            f"(~100% indicates struct-field seeds collapse on the Column path)."
        )


class TestNullFractionOnNested:
    """Pins behavior at the null_fraction=1.0 boundary on struct fields
    and array elements.

    The naive engine builds each nested child via ``F.when(null_mask,
    F.lit(None)).otherwise(expr)``.  At ``null_fraction=1.0`` the
    short-circuit shifts to ``F.lit(True)`` and every row gets
    ``F.lit(None)`` from the non-null branch -- if the null-branch
    literal isn't typed to match the element's expected Spark type,
    struct field types collide with the parent ``StructType`` and the
    generator raises an obscure type-mismatch at Catalyst compile.
    """

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
                        pk_auto("item_id"),
                        ColumnSpec(
                            name="addr",
                            gen=StructColumn(
                                fields=[
                                    text("city", values=["Austin", "NYC"]),
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

    def test_array_element_struct_null_fraction_one(self, spark):
        """An array of structs where one struct field has
        null_fraction=1.0: every element's that-field is NULL, but the
        array itself is not empty and other fields carry data."""
        plan = DataGenPlan(
            seed=13,
            tables=[
                TableSpec(
                    name="items",
                    rows=20,
                    primary_key=PrimaryKey(columns=["item_id"]),
                    columns=[
                        pk_auto("item_id"),
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
