"""Tests for FK column generation with Spark.

Verifies that FK values are valid parent PKs, deterministic, and support
different PK types and distributions.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from dbldatagen.core.engine.generator import generate_table
from dbldatagen.core.engine.planner import resolve_plan
from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataGenPlan,
    ForeignKeyColumn,
    ForeignKeyRef,
    PatternColumn,
    PrimaryKey,
    SequenceColumn,
    TableSpec,
    Uniform,
    UUIDColumn,
    Zipf,
)


def _gen_table(spark, plan, table_name):
    """Helper to generate a single table from a plan."""
    resolved = resolve_plan(plan)
    table_spec = next(t for t in plan.tables if t.name == table_name)
    return generate_table(spark, table_spec, resolved)


class TestFKValuesAreValidPKs:
    def test_fk_sequential_pk(self, spark):
        """FK values referencing a sequential PK all exist in parent table."""
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
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.customer_id", distribution=Uniform()),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)

        cust_df = generate_table(spark, customers, resolved)
        order_df = generate_table(spark, orders, resolved)

        # Inner join: should match all non-null FK values
        joined = order_df.join(
            cust_df,
            order_df["customer_id"] == cust_df["customer_id"],
            "inner",
        )
        assert joined.count() == order_df.count()

    def test_fk_pattern_pk(self, spark):
        """FK values referencing a pattern PK join correctly."""
        customers = TableSpec(
            name="customers",
            rows=50,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[
                ColumnSpec(name="cid", gen=PatternColumn(template="CUST-{digit:6}")),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=200,
            primary_key=PrimaryKey(columns=["oid"]),
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.cid", distribution=Uniform()),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)

        cust_df = generate_table(spark, customers, resolved)
        order_df = generate_table(spark, orders, resolved)

        # Left anti join: should be zero orphans
        orphans = order_df.join(cust_df, order_df["cid"] == cust_df["cid"], "left_anti")
        assert orphans.count() == 0

    def test_fk_uuid_pk(self, spark):
        """FK values referencing a UUID PK join correctly."""
        customers = TableSpec(
            name="customers",
            rows=50,
            primary_key=PrimaryKey(columns=["uid"]),
            columns=[
                ColumnSpec(name="uid", gen=UUIDColumn()),
            ],
        )
        orders = TableSpec(
            name="orders",
            rows=200,
            primary_key=PrimaryKey(columns=["oid"]),
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="uid",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.uid", distribution=Uniform()),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)

        cust_df = generate_table(spark, customers, resolved)
        order_df = generate_table(spark, orders, resolved)

        # Left anti join: should be zero orphans
        orphans = order_df.join(cust_df, order_df["uid"] == cust_df["uid"], "left_anti")
        assert orphans.count() == 0


class TestFKNullable:
    def test_fk_nullable(self, spark):
        """Nullable FK produces approximately the expected null fraction."""
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
            rows=5000,
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(
                        ref="customers.cid",
                        nullable=True,
                        null_fraction=0.3,
                    ),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)
        order_df = generate_table(spark, orders, resolved)

        null_count = order_df.filter(F.col("cid").isNull()).count()
        fraction = null_count / 5000
        assert 0.2 < fraction < 0.4, f"Null fraction {fraction} outside expected range"

    def test_generate_table_without_resolution_raises(self, spark):
        """Calling generate_table directly on an FK-bearing spec without
        a ResolvedPlan must raise, not silently emit an all-NULL column.

        The ForeignKeyColumn strategy was added (a78597b) to close the
        silent-NULL class; an earlier branch in build_fk_column_expr
        re-introduced it by returning None on missing resolution, which
        the caller translated into ``F.lit(None)``.
        """
        import pytest

        orders = TableSpec(
            name="orders",
            rows=5,
            # ``seed`` is required on a direct ``generate_table`` call —
            # normally ``DataGenPlan.propagate_seeds`` fills it in, but
            # we're skipping the plan to exercise the FK-raise path.
            seed=42,
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        with pytest.raises(TypeError, match="no FKResolution"):
            generate_table(spark, orders)

    def test_fk_top_level_null_fraction_applied(self, spark):
        """``ColumnSpec.null_fraction`` on an FK column must actually be applied.

        Prior bug: the FK path read only ``foreign_key.null_fraction``,
        silently ignoring ``ColumnSpec.null_fraction``.  Users who set
        the same field they use on every other column kind got 0% nulls.
        """
        customers = TableSpec(
            name="customers",
            rows=100,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1))],
        )
        orders = TableSpec(
            name="orders",
            rows=5000,
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ForeignKeyColumn(),
                    null_fraction=0.3,
                    foreign_key=ForeignKeyRef(ref="customers.cid", nullable=True),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)
        order_df = generate_table(spark, orders, resolved)

        fraction = order_df.filter(F.col("cid").isNull()).count() / 5000
        assert 0.2 < fraction < 0.4, (
            f"ColumnSpec.null_fraction=0.3 on FK column produced {fraction} — "
            f"top-level null_fraction was silently ignored before the fix"
        )


class TestFKDeterminism:
    def test_fk_determinism(self, spark):
        """Same seed produces identical FK values."""
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
            rows=200,
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)

        df1 = generate_table(spark, orders, resolved).orderBy("oid").collect()
        df2 = generate_table(spark, orders, resolved).orderBy("oid").collect()

        vals1 = [r.cid for r in df1]
        vals2 = [r.cid for r in df2]
        assert vals1 == vals2


class TestFKZipfDistribution:
    def test_fk_zipf_distribution(self, spark):
        """Zipf-distributed FKs show skew: top parent gets more refs than bottom."""
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
            rows=10000,
            columns=[
                ColumnSpec(name="oid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="cid",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.cid", distribution=Zipf(exponent=1.5)),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)
        order_df = generate_table(spark, orders, resolved)

        # Count references per customer
        counts = order_df.groupBy("cid").count().orderBy(F.col("count").desc()).collect()

        # With Zipf, the most popular customer should have significantly more
        # references than the average (10000/100 = 100)
        top_count = counts[0]["count"]
        bottom_count = counts[-1]["count"]
        assert top_count > bottom_count, "Zipf should create skew"


class TestReconstructParentPkBackstop:
    """_reconstruct_parent_pk raises on unknown pk_type — defense-in-depth
    against a hand-constructed PKMetadata that bypasses the planner."""

    def test_unknown_pk_type_raises(self, spark):
        import pytest

        from dbldatagen.core.engine.fk import _reconstruct_parent_pk
        from dbldatagen.core.engine.planner import PKMetadata

        meta = PKMetadata(
            table_name="t",
            pk_column="id",
            row_count=10,
            pk_type="bogus",  # planner would never emit this
            pk_seed=42,
            pk_start=0,
            pk_step=1,
            pk_template=None,
        )
        with pytest.raises(ValueError, match="unknown pk_type='bogus'"):
            # parent_index_col can be anything; the raise fires before we use it
            _reconstruct_parent_pk(F.col("id"), meta)
