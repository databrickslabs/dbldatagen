"""Integration tests for CDC generation with Spark.

Tests the full generate_cdc pipeline: initial snapshots, batches,
determinism, batch independence, PK continuity, before/after images,
FK integrity, and parent delete guard.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.engine.cdc import (
    generate_cdc,
    generate_cdc_batch,
    generate_cdc_bulk,
    generate_expected_state,
)
from dbldatagen.core.spec.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights
from dbldatagen.core.spec.schema import (
    ColumnSpec,
    DataGenPlan,
    DataType,
    FakerColumn,
    ForeignKeyColumn,
    ForeignKeyRef,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    TimestampColumn,
    ValuesColumn,
)


def _simple_plan(rows=100, seed=42):
    return DataGenPlan(
        seed=seed,
        tables=[
            TableSpec(
                name="products",
                rows=rows,
                primary_key=PrimaryKey(columns=["product_id"]),
                columns=[
                    ColumnSpec(name="product_id", gen=SequenceColumn()),
                    ColumnSpec(
                        name="name",
                        gen=ValuesColumn(values=["Widget", "Gadget", "Doohickey", "Thingamajig"]),
                    ),
                    ColumnSpec(name="price", dtype=DataType.INT, gen=RangeColumn(min=10, max=500)),
                ],
            ),
        ],
    )


def _fk_plan(seed=42):
    return DataGenPlan(
        seed=seed,
        tables=[
            TableSpec(
                name="customers",
                rows=50,
                primary_key=PrimaryKey(columns=["cust_id"]),
                columns=[
                    ColumnSpec(name="cust_id", gen=SequenceColumn()),
                    ColumnSpec(name="name", gen=ValuesColumn(values=["A", "B", "C", "D"])),
                ],
            ),
            TableSpec(
                name="orders",
                rows=200,
                primary_key=PrimaryKey(columns=["order_id"]),
                columns=[
                    ColumnSpec(name="order_id", gen=SequenceColumn()),
                    ColumnSpec(
                        name="cust_id",
                        gen=ForeignKeyColumn(),
                        foreign_key=ForeignKeyRef(ref="customers.cust_id"),
                    ),
                    ColumnSpec(name="amount", dtype=DataType.INT, gen=RangeColumn(min=1, max=1000)),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Basic generation
# ---------------------------------------------------------------------------


class TestCDCBasicGeneration:
    def test_returns_cdc_stream(self, spark):
        stream = generate_cdc(spark, _simple_plan(), num_batches=3)
        assert "products" in stream.initial
        assert len(stream.batches) == 3

    def test_initial_snapshot_count(self, spark):
        stream = generate_cdc(spark, _simple_plan(rows=100), num_batches=1)
        assert stream.initial["products"].count() == 100

    def test_initial_has_op_column(self, spark):
        stream = generate_cdc(spark, _simple_plan(), num_batches=1)
        cols = stream.initial["products"].columns
        assert "_op" in cols
        assert "_batch_id" in cols

    def test_initial_all_inserts(self, spark):
        stream = generate_cdc(spark, _simple_plan(), num_batches=1)
        ops = [r._op for r in stream.initial["products"].select("_op").distinct().collect()]
        assert ops == ["I"]

    def test_batch_has_multiple_ops(self, spark):
        # Stateless engine with min_life=3: deletes appear at batch 3+.
        # Use 100 rows with batch_size=10 so all three op types appear.
        plan = CDCPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=5,
            table_configs={
                "products": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=3, update=5, delete=2),
                    min_life=1,
                ),
            },
        )
        stream = generate_cdc(spark, plan)
        ops = {r._op for r in stream.batches[0]["products"].select("_op").distinct().collect()}
        assert "I" in ops
        assert "U" in ops
        assert "D" in ops

    def test_accepts_data_gen_plan(self, spark):
        """generate_cdc should accept DataGenPlan directly."""
        stream = generate_cdc(spark, _simple_plan(), num_batches=2)
        assert len(stream.batches) == 2

    def test_accepts_cdc_plan(self, spark):
        plan = CDCPlan(base_plan=_simple_plan(), num_batches=2)
        stream = generate_cdc(spark, plan)
        assert len(stream.batches) == 2


# ---------------------------------------------------------------------------
# Row counts
# ---------------------------------------------------------------------------


class TestCDCRowCounts:
    def test_batch_size_fraction(self, spark):
        plan = CDCPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=1,
            table_configs={
                "products": CDCTableConfig(
                    batch_size=0.1,
                    operations=OperationWeights(insert=3, update=5, delete=2),
                    min_life=1,
                ),
            },
        )
        stream = generate_cdc(spark, plan)
        # batch_size = 10 (0.1 * 100), with UB rows: 3 I + 5 U + 5 UB + 2 D = 15
        batch_count = stream.batches[0]["products"].count()
        assert batch_count == 15

    def test_batch_size_absolute(self, spark):
        plan = CDCPlan(
            base_plan=_simple_plan(rows=100),
            num_batches=1,
            table_configs={
                "products": CDCTableConfig(
                    batch_size=20,
                    operations=OperationWeights(insert=3, update=5, delete=2),
                    min_life=1,
                ),
            },
        )
        stream = generate_cdc(spark, plan)
        batch_count = stream.batches[0]["products"].count()
        # 6 I + 10 U + 10 UB + 4 D = 30
        assert batch_count == 30


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestCDCDeterminism:
    def test_same_seed_same_data(self, spark):
        s1 = generate_cdc(spark, _simple_plan(seed=42), num_batches=2)
        s2 = generate_cdc(spark, _simple_plan(seed=42), num_batches=2)

        # Initial snapshots match
        d1 = s1.initial["products"].orderBy("product_id").collect()
        d2 = s2.initial["products"].orderBy("product_id").collect()
        assert d1 == d2

        # Batches match
        for i in range(2):
            b1 = s1.batches[i]["products"].orderBy("product_id", "_op").collect()
            b2 = s2.batches[i]["products"].orderBy("product_id", "_op").collect()
            assert b1 == b2

    def test_different_seed_different_data(self, spark):
        # In the stateless model, update targets are structurally determined
        # (same row count + weights = same indices), but VALUES differ by seed.
        # Use initial snapshots which always have data at batch 0.
        s1 = generate_cdc(spark, _simple_plan(seed=42), num_batches=1)
        s2 = generate_cdc(spark, _simple_plan(seed=999), num_batches=1)

        # Initial snapshots should have different values for non-PK columns
        i1 = s1.initial["products"].orderBy("product_id").collect()
        i2 = s2.initial["products"].orderBy("product_id").collect()
        vals1 = [(r.name, r.price) for r in i1]
        vals2 = [(r.name, r.price) for r in i2]
        assert vals1 != vals2


# ---------------------------------------------------------------------------
# Batch independence
# ---------------------------------------------------------------------------


class TestCDCBatchIndependence:
    def test_independent_batch_matches_stream(self, spark):
        plan = _simple_plan(rows=50, seed=42)
        stream = generate_cdc(spark, plan, num_batches=5)

        # Generate batch 3 independently
        batch3_ind = generate_cdc_batch(spark, plan, batch_id=3)

        stream_rows = stream.batches[2]["products"].orderBy("product_id", "_op").collect()
        ind_rows = batch3_ind["products"].orderBy("product_id", "_op").collect()
        assert len(stream_rows) == len(ind_rows)
        for sr, ir in zip(stream_rows, ind_rows):
            assert sr.product_id == ir.product_id
            assert sr._op == ir._op

    @pytest.mark.parametrize("bad_batch_id", [0, -1, 6, 10**6])
    def test_out_of_range_batch_id_rejected(self, spark, bad_batch_id):
        """``batch_id`` must be in ``[1, num_batches]``.  ``0`` is the
        initial snapshot, values outside the range used to produce an
        empty-but-formatted DataFrame; now raise ValueError with a clear
        hint pointing at ``.initial`` for the snapshot case.
        """
        plan = CDCPlan(base_plan=_simple_plan(rows=10), num_batches=5)
        with pytest.raises(ValueError, match=r"batch_id must be in \[1, 5\]"):
            generate_cdc_batch(spark, plan, batch_id=bad_batch_id)


# ---------------------------------------------------------------------------
# PK continuity
# ---------------------------------------------------------------------------


class TestCDCInsertPKContinuity:
    def test_no_pk_collision(self, spark):
        stream = generate_cdc(spark, _simple_plan(rows=50), num_batches=3)

        all_pks = set()
        for r in stream.initial["products"].select("product_id").collect():
            all_pks.add(r.product_id)

        for batch in stream.batches:
            inserts = batch["products"].filter("_op = 'I'")
            for r in inserts.select("product_id").collect():
                assert r.product_id not in all_pks, f"PK collision: {r.product_id}"
                all_pks.add(r.product_id)

    def test_descending_sequence_pk_no_collision(self, spark):
        """``SequenceColumn(step=-1)`` as a PK produces a descending
        sequence.  Pin that CDC insert arithmetic
        (``start + first_start_index * step``) correctly extends the
        sequence downward without re-using initial-snapshot PKs.

        Regression scope: no prior test covered negative-step as a PK
        under CDC; the reviewer flagged it as a gap.
        """
        plan = DataGenPlan(
            seed=7,
            tables=[
                TableSpec(
                    name="items",
                    rows=40,
                    primary_key=PrimaryKey(columns=["item_id"]),
                    columns=[
                        ColumnSpec(name="item_id", gen=SequenceColumn(start=1000, step=-1)),
                        ColumnSpec(name="v", gen=RangeColumn(min=1, max=100)),
                    ],
                ),
            ],
        )
        stream = generate_cdc(spark, plan, num_batches=2)

        all_pks = set()
        for r in stream.initial["items"].select("item_id").collect():
            all_pks.add(r.item_id)
        assert min(all_pks) == 1000 - 39  # start=1000, step=-1, 40 rows: [1000..961]

        for batch in stream.batches:
            inserts = batch["items"].filter("_op = 'I'")
            for r in inserts.select("item_id").collect():
                assert r.item_id not in all_pks, f"PK collision at {r.item_id}"
                # Inserted PKs must continue the descending run.
                assert r.item_id < 1000 - 39, f"insert PK {r.item_id} overlaps initial range " f"[{1000 - 39}, 1000]"
                all_pks.add(r.item_id)


# ---------------------------------------------------------------------------
# Before / after images
# ---------------------------------------------------------------------------


class TestCDCUpdateImages:
    def test_before_image_matches_initial(self, spark):
        """For batch 1, before-images should match the initial snapshot values."""
        stream = generate_cdc(spark, _simple_plan(rows=50), num_batches=1)

        initial = {r.product_id: r for r in stream.initial["products"].drop("_op", "_batch_id", "_ts").collect()}
        before_rows = stream.batches[0]["products"].filter("_op = 'UB'").collect()

        for row in before_rows:
            pk = row.product_id
            assert pk in initial, f"Before-image PK {pk} not in initial"
            init_row = initial[pk]
            assert row.name == init_row.name
            assert row.price == init_row.price

    def test_after_image_differs(self, spark):
        """After-images should differ from before-images in at least some rows."""
        stream = generate_cdc(spark, _simple_plan(rows=100, seed=42), num_batches=1)
        batch = stream.batches[0]["products"]

        before_map = {r.product_id: (r.name, r.price) for r in batch.filter("_op = 'UB'").collect()}
        after_map = {r.product_id: (r.name, r.price) for r in batch.filter("_op = 'U'").collect()}

        assert len(before_map) > 0
        some_differ = any(after_map[pk] != before_map[pk] for pk in before_map if pk in after_map)
        assert some_differ, "All after-images identical to before-images"


# ---------------------------------------------------------------------------
# Expected state
# ---------------------------------------------------------------------------


class TestExpectedState:
    def test_live_row_count(self, spark):
        plan = _simple_plan(rows=100)
        state = generate_expected_state(spark, plan, "products", batch_id=3)
        assert state.count() > 0
        assert state.count() < 200  # not more than initial + inserts

    def test_unique_pks(self, spark):
        plan = _simple_plan(rows=100)
        state = generate_expected_state(spark, plan, "products", batch_id=3)
        assert state.select("product_id").distinct().count() == state.count()

    def test_deleted_not_in_state(self, spark):
        """Rows deleted in batches should not appear in expected state."""
        plan = CDCPlan(
            base_plan=_simple_plan(rows=50),
            num_batches=3,
            table_configs={
                "products": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=0, update=0, delete=1),
                ),
            },
        )
        stream = generate_cdc(spark, plan)
        state = generate_expected_state(spark, plan, "products", batch_id=3)

        # Collect deleted PKs across all batches
        deleted_pks = set()
        for batch in stream.batches:
            for r in batch["products"].filter("_op = 'D'").select("product_id").collect():
                deleted_pks.add(r.product_id)

        live_pks = {r.product_id for r in state.select("product_id").collect()}
        assert len(deleted_pks & live_pks) == 0


# ---------------------------------------------------------------------------
# FK integrity
# ---------------------------------------------------------------------------


class TestCDCFKIntegrity:
    """CDC with FK requires the parent to be a static dimension (not in
    ``cdc_tables``) — ``CDCPlan`` rejects cross-CDC foreign keys at
    construction because the FK generator uses plan-time parent row
    count and would dangle under parent mutation.  These tests pin the
    supported case: static parent + CDC child.
    """

    def test_initial_fk_valid(self, spark):
        from dbldatagen.core import generate

        base = _fk_plan()
        plan = CDCPlan(base_plan=base, cdc_tables=["orders"])
        stream = generate_cdc(spark, plan, num_batches=1)

        # Static parent isn't part of the CDC stream — materialize it directly.
        customers = generate(spark, base)["customers"]
        cust_pks = {r.cust_id for r in customers.select("cust_id").collect()}
        order_fks = {r.cust_id for r in stream.initial["orders"].select("cust_id").collect()}
        assert order_fks.issubset(cust_pks)

    def test_child_deletes_allowed(self, spark):
        """CDC child with FK to a static parent is allowed to have deletes."""
        plan = CDCPlan(
            base_plan=_fk_plan(),
            cdc_tables=["orders"],
            num_batches=5,
            table_configs={
                "orders": CDCTableConfig(
                    batch_size=20,
                    operations=OperationWeights(insert=1, update=3, delete=6),
                ),
            },
        )
        stream = generate_cdc(spark, plan)
        total_deletes = sum(b["orders"].filter("_op = 'D'").count() for b in stream.batches)
        assert total_deletes > 0


# ---------------------------------------------------------------------------
# Append-only
# ---------------------------------------------------------------------------


class TestCDCAppendOnly:
    def test_insert_only(self, spark):
        plan = CDCPlan(
            base_plan=_simple_plan(rows=50),
            num_batches=3,
            table_configs={
                "products": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=1, update=0, delete=0),
                ),
            },
        )
        stream = generate_cdc(spark, plan)
        for batch in stream.batches:
            ops = {r._op for r in batch["products"].select("_op").distinct().collect()}
            assert ops == {"I"}, f"Expected only inserts, got {ops}"


# ---------------------------------------------------------------------------
# Bulk CDC generation (generate_cdc_bulk)
# ---------------------------------------------------------------------------


class TestCDCBulkGeneration:
    def test_bulk_returns_cdc_stream(self, spark):
        stream = generate_cdc_bulk(spark, _simple_plan(), num_batches=5, chunk_size=3)
        assert "products" in stream.initial
        # 5 batches / chunk_size 3 = 2 chunks (ceil division)
        assert len(stream.batches) == 2

    def test_bulk_matches_per_batch(self, spark):
        """Bulk output should contain the same rows as per-batch generation."""
        plan = _simple_plan(rows=50, seed=42)
        num_b = 5

        per_batch = generate_cdc(spark, plan, num_batches=num_b)
        bulk = generate_cdc_bulk(spark, plan, num_batches=num_b, chunk_size=num_b)

        # Initial snapshots should match
        init_per = per_batch.initial["products"].orderBy("product_id").collect()
        init_bulk = bulk.initial["products"].orderBy("product_id").collect()
        assert init_per == init_bulk

        # Collect all batch rows from per-batch
        all_per_rows = []
        for b in per_batch.batches:
            all_per_rows.extend(b["products"].orderBy("product_id", "_op", "_batch_id").collect())

        # Collect all rows from bulk (single chunk covering all 5 batches)
        all_bulk_rows = []
        for chunk in bulk.batches:
            all_bulk_rows.extend(chunk["products"].orderBy("product_id", "_op", "_batch_id").collect())

        # Sort both by PK + op + batch_id for comparison
        all_per_rows.sort(key=lambda r: (r.product_id, r._op, r._batch_id))
        all_bulk_rows.sort(key=lambda r: (r.product_id, r._op, r._batch_id))

        assert len(all_per_rows) == len(all_bulk_rows)
        for pr, br in zip(all_per_rows, all_bulk_rows):
            assert pr.product_id == br.product_id
            assert pr._op == br._op
            assert pr._batch_id == br._batch_id

    def test_bulk_determinism(self, spark):
        plan = _simple_plan(rows=50, seed=42)
        s1 = generate_cdc_bulk(spark, plan, num_batches=3, chunk_size=2)
        s2 = generate_cdc_bulk(spark, plan, num_batches=3, chunk_size=2)

        for i in range(len(s1.batches)):
            r1 = s1.batches[i]["products"].orderBy("product_id", "_op").collect()
            r2 = s2.batches[i]["products"].orderBy("product_id", "_op").collect()
            assert r1 == r2

    def test_bulk_chunk_size_one_matches_per_batch(self, spark):
        """chunk_size=1 should degrade to per-batch behavior."""
        plan = _simple_plan(rows=50, seed=42)
        per_batch = generate_cdc(spark, plan, num_batches=3)
        bulk = generate_cdc_bulk(spark, plan, num_batches=3, chunk_size=1)

        assert len(bulk.batches) == 3
        for i in range(3):
            pb = per_batch.batches[i]["products"].orderBy("product_id", "_op").collect()
            bk = bulk.batches[i]["products"].orderBy("product_id", "_op").collect()
            assert pb == bk

    def test_bulk_fk_integrity(self, spark):
        """FK integrity should hold in bulk mode with a static parent."""
        from dbldatagen.core import generate

        base = _fk_plan()
        plan = CDCPlan(base_plan=base, cdc_tables=["orders"])
        stream = generate_cdc_bulk(spark, plan, num_batches=3, chunk_size=2)

        customers = generate(spark, base)["customers"]
        cust_pks = {r.cust_id for r in customers.select("cust_id").collect()}
        order_fks = {r.cust_id for r in stream.initial["orders"].select("cust_id").collect()}
        assert order_fks.issubset(cust_pks)

    def test_bulk_with_deletes(self, spark):
        """Deletes should work correctly in bulk mode."""
        plan = CDCPlan(
            base_plan=_simple_plan(rows=50),
            num_batches=4,
            table_configs={
                "products": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=1, update=1, delete=8),
                ),
            },
        )
        stream = generate_cdc_bulk(spark, plan, chunk_size=2)
        total_deletes = 0
        for chunk in stream.batches:
            total_deletes += chunk["products"].filter("_op = 'D'").count()
        assert total_deletes > 0

    def test_auto_chunk_size(self):
        """Auto chunk_size should produce reasonable values."""
        from dbldatagen.core.engine.cdc._common import _auto_chunk_size

        plan = CDCPlan(
            base_plan=_simple_plan(rows=50_000_000),
            num_batches=1460,
            table_configs={
                "products": CDCTableConfig(batch_size=500_000),
            },
        )
        chunk = _auto_chunk_size(plan)
        # 20M target / 550K per batch ≈ 36
        assert 10 <= chunk <= 100
        assert chunk <= plan.num_batches

    def test_bulk_format_override(self, spark):
        """Format override should work with bulk generation."""
        stream = generate_cdc_bulk(
            spark,
            _simple_plan(),
            num_batches=2,
            format="delta_cdf",
            chunk_size=2,
        )
        cols = stream.initial["products"].columns
        assert "_change_type" in cols
        assert "_commit_version" in cols

    def test_bulk_fk_matches_per_batch(self, spark):
        """Bulk with FK (static parent) should produce same rows as per-batch."""
        plan = CDCPlan(base_plan=_fk_plan(seed=42), cdc_tables=["orders"])
        num_b = 3

        per_batch = generate_cdc(spark, plan, num_batches=num_b)
        bulk = generate_cdc_bulk(spark, plan, num_batches=num_b, chunk_size=num_b)

        # Only orders is CDC; customers is static (initial only, no batch events).
        all_per = []
        for b in per_batch.batches:
            all_per.extend(b["orders"].orderBy("_batch_id", "_op").collect())
        all_bulk = []
        for chunk in bulk.batches:
            all_bulk.extend(chunk["orders"].orderBy("_batch_id", "_op").collect())
        all_per.sort(key=lambda r: (r._batch_id, r._op, r[0]))
        all_bulk.sort(key=lambda r: (r._batch_id, r._op, r[0]))
        assert len(all_per) == len(all_bulk), "orders: row count mismatch"
        for pr, br in zip(all_per, all_bulk):
            assert pr[0] == br[0], "orders: PK mismatch"
            assert pr._op == br._op, "orders: _op mismatch"

    def test_bulk_insert_only_plan(self, spark):
        """Insert-only plan should work with bulk fusion (no updates/deletes)."""
        plan = CDCPlan(
            base_plan=_simple_plan(rows=50),
            num_batches=4,
            table_configs={
                "products": CDCTableConfig(
                    batch_size=10,
                    operations=OperationWeights(insert=1, update=0, delete=0),
                ),
            },
        )
        per_batch = generate_cdc(spark, plan)
        bulk = generate_cdc_bulk(spark, plan, chunk_size=4)

        all_per = []
        for b in per_batch.batches:
            all_per.extend(b["products"].collect())
        all_bulk = []
        for chunk in bulk.batches:
            all_bulk.extend(chunk["products"].collect())

        all_per.sort(key=lambda r: r.product_id)
        all_bulk.sort(key=lambda r: r.product_id)
        assert len(all_per) == len(all_bulk)
        for pr, br in zip(all_per, all_bulk):
            assert pr.product_id == br.product_id

    def test_bulk_faker_fallback(self, spark):
        """Tables with Faker columns should fall back to per-batch union."""
        faker_plan = DataGenPlan(
            seed=42,
            tables=[
                TableSpec(
                    name="people",
                    rows=20,
                    primary_key=PrimaryKey(columns=["id"]),
                    columns=[
                        ColumnSpec(name="id", gen=SequenceColumn()),
                        ColumnSpec(name="full_name", gen=FakerColumn(provider="name")),
                    ],
                ),
            ],
        )
        # Should not crash — falls back to per-batch
        stream = generate_cdc_bulk(spark, faker_plan, num_batches=2, chunk_size=2)
        assert stream.batches[0]["people"].count() > 0

    def test_bulk_multi_column_types(self, spark):
        """Bulk fusion should work with pattern, timestamp, values columns."""
        plan = DataGenPlan(
            seed=42,
            tables=[
                TableSpec(
                    name="txns",
                    rows=50,
                    primary_key=PrimaryKey(columns=["txn_id"]),
                    columns=[
                        ColumnSpec(name="txn_id", gen=SequenceColumn(start=1, step=1)),
                        ColumnSpec(
                            name="account_id",
                            gen=PatternColumn(template="ACCT-{digit:8}"),
                        ),
                        ColumnSpec(
                            name="amount",
                            dtype=DataType.DOUBLE,
                            gen=RangeColumn(min=0.01, max=25000.0),
                        ),
                        ColumnSpec(
                            name="status",
                            gen=ValuesColumn(values=["completed", "pending", "failed"]),
                        ),
                        ColumnSpec(
                            name="ts",
                            dtype=DataType.TIMESTAMP,
                            gen=TimestampColumn(start="2022-01-01", end="2025-12-31"),
                        ),
                    ],
                ),
            ],
        )
        per_batch = generate_cdc(spark, plan, num_batches=3)
        bulk = generate_cdc_bulk(spark, plan, num_batches=3, chunk_size=3)

        # Collect all insert rows (where bulk fusion applies)
        per_inserts = []
        for b in per_batch.batches:
            per_inserts.extend(b["txns"].filter("_op = 'I'").collect())
        bulk_inserts = []
        for chunk in bulk.batches:
            bulk_inserts.extend(chunk["txns"].filter("_op = 'I'").collect())

        per_inserts.sort(key=lambda r: r.txn_id)
        bulk_inserts.sort(key=lambda r: r.txn_id)

        assert len(per_inserts) == len(bulk_inserts)
        for pr, br in zip(per_inserts, bulk_inserts):
            assert pr.txn_id == br.txn_id
            assert pr.account_id == br.account_id
            assert pr.status == br.status


def _multi_table_seed_plan():
    """Two-table plan where the seed-mismatch bug only manifests for table index > 0."""
    base = DataGenPlan(
        seed=42,
        tables=[
            TableSpec(
                name="customers",
                rows=50,
                primary_key=PrimaryKey(columns=["cid"]),
                columns=[
                    ColumnSpec(name="cid", gen=SequenceColumn()),
                    ColumnSpec(name="tier", gen=ValuesColumn(values=["gold", "silver", "bronze"])),
                ],
            ),
            TableSpec(
                name="orders",
                rows=100,
                primary_key=PrimaryKey(columns=["oid"]),
                columns=[
                    ColumnSpec(name="oid", gen=SequenceColumn()),
                    ColumnSpec(name="amount", dtype=DataType.INT, gen=RangeColumn(min=10, max=500)),
                ],
            ),
        ],
    )
    return CDCPlan(
        base_plan=base,
        num_batches=5,
        table_configs={
            "orders": CDCTableConfig(
                batch_size=20,
                operations=OperationWeights(insert=1, update=0, delete=5),
            ),
        },
    )


class TestMultiTableSeedConsistency:
    """Verify that CDC pre-images match initial snapshot for multi-table plans.

    Regression test for seed mismatch: generate_initial_snapshot uses
    table_spec.seed (plan.seed + i) but generate_cdc_batch_for_table,
    generate_cdc_bulk, and generate_expected_state were all using
    plan.base_plan.seed for all tables.
    """

    def test_second_table_delete_preimage_matches_initial(self, spark):
        """generate_cdc: delete pre-image for 2nd table must match its initial snapshot."""
        plan = _multi_table_seed_plan()
        stream = generate_cdc(spark, plan)

        initial_map = {r.oid: r.amount for r in stream.initial["orders"].collect()}

        for batch_id in range(1, 4):
            batch = stream.batches[batch_id - 1]
            if "orders" not in batch:
                continue
            deletes = batch["orders"].filter("_op = 'D'")
            if deletes.count() == 0:
                continue
            for row in deletes.collect():
                if row.oid in initial_map:
                    assert row.amount == initial_map[row.oid], (
                        f"Delete pre-image mismatch for oid={row.oid} at batch {batch_id}: "
                        f"initial={initial_map[row.oid]}, delete pre-image={row.amount}. "
                        f"This indicates a seed mismatch between initial snapshot and CDC generation."
                    )
            return
        raise AssertionError("No deletes found for orders table in first 3 batches")

    def test_bulk_path_matches_non_bulk_for_second_table(self, spark):
        """generate_cdc_bulk must produce the same insert values as generate_cdc.

        Covers the bulk path at cdc.py:_generate_chunk_for_table, which was
        silently using plan.base_plan.seed instead of table_spec.seed.
        """
        plan = _multi_table_seed_plan()
        per_batch = generate_cdc(spark, plan)
        bulk = generate_cdc_bulk(spark, plan, chunk_size=5)

        per_inserts = {}
        for b in per_batch.batches:
            if "orders" not in b:
                continue
            for r in b["orders"].filter("_op = 'I'").collect():
                per_inserts[r.oid] = r.amount

        bulk_inserts = {}
        for chunk in bulk.batches:
            if "orders" not in chunk:
                continue
            for r in chunk["orders"].filter("_op = 'I'").collect():
                bulk_inserts[r.oid] = r.amount

        assert per_inserts, "No inserts collected from non-bulk path"
        assert set(per_inserts.keys()) == set(
            bulk_inserts.keys()
        ), "Bulk path emitted different insert PKs than the non-bulk path"
        for oid, amount in per_inserts.items():
            assert bulk_inserts[oid] == amount, (
                f"Bulk vs non-bulk mismatch for oid={oid}: "
                f"non-bulk={amount}, bulk={bulk_inserts[oid]}. "
                f"This indicates the bulk path is using the wrong seed for table index > 0."
            )

    def test_expected_state_matches_initial_for_second_table(self, spark):
        """generate_expected_state at batch_id=0 must match the initial snapshot.

        Covers the oracle at cdc.py:_generate_expected_state_driver, which was
        silently using plan.base_plan.seed instead of table_spec.seed.
        """
        plan = _multi_table_seed_plan()
        stream = generate_cdc(spark, plan)

        initial_map = {r.oid: r.amount for r in stream.initial["orders"].collect()}
        state = generate_expected_state(spark, plan, "orders", batch_id=0)
        state_map = {r.oid: r.amount for r in state.collect()}

        assert state_map == initial_map, (
            "generate_expected_state(batch_id=0) disagrees with the initial snapshot "
            "for the 2nd table. This indicates the oracle is using the wrong seed "
            "for table index > 0."
        )


class TestBulkChunkOmitsEmptyTables:
    """Pin the contract that ``generate_cdc_bulk`` OMITS a table from a
    chunk dict when that table had zero CDC rows in that chunk.  An
    earlier ``.limit(0)`` fallback filled every slot with an empty
    DataFrame, which made the ``if table_name in chunk`` guard in
    ``write_cdc_to_delta`` a no-op: every chunk wrote an empty append
    for every table, inflating the Delta log with meaningless commits
    and confusing CDF consumers reading by version.
    """

    def test_empty_chunk_omits_table(self, spark):
        """Construct a chunk where the delete-only config has no deletes
        to emit yet (chunk's batches are all pre-``min_life``).  That
        chunk's ``_generate_chunk_for_table`` returns None and the
        table must be absent from the chunk dict -- not present with
        a ``.limit(0)`` DataFrame.
        """
        plan = CDCPlan(
            base_plan=DataGenPlan(
                seed=42,
                tables=[
                    TableSpec(
                        name="items",
                        rows=20,
                        primary_key=PrimaryKey(columns=["pk"]),
                        columns=[
                            ColumnSpec(name="pk", gen=SequenceColumn(start=1)),
                            ColumnSpec(name="v", dtype=DataType.INT, gen=RangeColumn(min=1, max=100)),
                        ],
                    ),
                ],
            ),
            num_batches=12,
            table_configs={
                "items": CDCTableConfig(
                    batch_size=4,
                    operations=OperationWeights(insert=0, update=0, delete=5),
                    # min_life=10 means deletes don't fire until batch 10,
                    # so chunks covering batches 1..4 and 5..8 have zero
                    # CDC rows (no inserts, no updates, no deletes).
                    min_life=10,
                ),
            },
        )
        stream = generate_cdc_bulk(spark, plan, chunk_size=4)
        # Three chunks: batches [1..4], [5..8], [9..12].  Only the
        # third should contain "items" (deletes fire at batch >= 10).
        assert len(stream.batches) == 3
        assert "items" not in stream.batches[0], (
            "chunk 0 has zero CDC rows but chunk dict still contains " "'items' -- empty-chunk fallback reintroduced?"
        )
        assert "items" not in stream.batches[1], "chunk 1 has zero CDC rows but chunk dict still contains 'items'."
        assert "items" in stream.batches[2], (
            "chunk 2 spans batches 9..12 where deletes should fire; " "items should be present."
        )
        # Sanity: the non-empty chunk actually has rows.
        assert stream.batches[2]["items"].count() > 0
