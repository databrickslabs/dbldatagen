"""Tests for the plan resolution engine: FK validation, topological sort, metadata extraction."""

from __future__ import annotations

import pytest

from dbldatagen.core.engine.planner import (
    resolve_plan,
)
from dbldatagen.core.spec.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    ExpressionColumn,
    FakerColumn,
    ForeignKeyColumn,
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
                gen=ForeignKeyColumn(),
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
        assert fk_res.parent_metadata.table_name == "customers"
        assert fk_res.parent_metadata.pk_column == "customer_id"
        assert fk_res.parent_metadata.row_count == 1000


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
                    gen=ForeignKeyColumn(),
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
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="orders.order_id"),
                ),
                ColumnSpec(
                    name="product_id",
                    gen=ForeignKeyColumn(),
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
                    gen=ForeignKeyColumn(),
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
                    gen=ForeignKeyColumn(),
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
                    gen=ForeignKeyColumn(),
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
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.nonexistent"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        with pytest.raises(ValueError, match="does not exist"):
            resolve_plan(plan)


class TestCompositePKFKRejection:
    """``ForeignKeyRef.ref`` is single-column; ``PKMetadata`` and
    ``_extract_pk_metadata`` are single-column by construction.  An FK
    targeting one sub-column of a composite parent PK silently produced
    a single ``FKResolution`` keyed on ``(table, sub_column)`` and the
    FK reconstruction path emitted values from a single-column synthesis
    that may not uniquely identify a parent row.  Reject at plan time
    so the failure mode is visible instead of silently producing
    join-ambiguous FKs.
    """

    @staticmethod
    def _parent_with_composite_pk() -> TableSpec:
        return TableSpec(
            name="parent",
            rows=100,
            primary_key=PrimaryKey(columns=["tenant_id", "user_id"]),
            columns=[
                ColumnSpec(name="tenant_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="user_id", gen=SequenceColumn(start=1, step=1)),
            ],
        )

    def test_fk_to_composite_pk_first_subcol_rejected(self):
        child = TableSpec(
            name="child",
            rows=100,
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="t_ref",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="parent.tenant_id"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[self._parent_with_composite_pk(), child], seed=42)
        with pytest.raises(ValueError, match="composite primary key"):
            resolve_plan(plan)

    def test_fk_to_composite_pk_second_subcol_rejected(self):
        """Rejection fires regardless of which sub-column is targeted —
        the composite check runs before the column-membership check, so
        any FK ref into a composite-PK table is rejected with the same
        message."""
        child = TableSpec(
            name="child",
            rows=100,
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="u_ref",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="parent.user_id"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[self._parent_with_composite_pk(), child], seed=42)
        with pytest.raises(ValueError, match="composite primary key"):
            resolve_plan(plan)

    def test_fk_to_composite_pk_non_member_subcol_also_rejected(self):
        """If the user targets a column that exists on the parent but
        isn't part of the composite PK, the composite-PK rejection
        still fires first.  Better signal: the root issue is that the
        parent's PK shape doesn't support single-column FKs; the
        column-membership error would be a less informative red herring.
        """
        parent = TableSpec(
            name="parent",
            rows=100,
            primary_key=PrimaryKey(columns=["tenant_id", "user_id"]),
            columns=[
                ColumnSpec(name="tenant_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="user_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="name", gen=SequenceColumn(start=1, step=1)),
            ],
        )
        child = TableSpec(
            name="child",
            rows=100,
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="n_ref",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="parent.name"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[parent, child], seed=42)
        with pytest.raises(ValueError, match="composite primary key"):
            resolve_plan(plan)

    def test_fk_to_single_column_pk_still_works(self):
        """Negative regression: a normal single-column-PK FK must
        continue to resolve cleanly with the new composite check in
        place."""
        parent = TableSpec(
            name="parent",
            rows=100,
            primary_key=PrimaryKey(columns=["pid"]),
            columns=[ColumnSpec(name="pid", gen=SequenceColumn(start=1, step=1))],
        )
        child = TableSpec(
            name="child",
            rows=100,
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="p_ref",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="parent.pid"),
                ),
            ],
        )
        # Must resolve cleanly with no raise.
        resolve_plan(DataGenPlan(tables=[parent, child], seed=42))


class TestPKMetadataExtraction:
    def test_sequence_pk_metadata(self):
        """Verify correct metadata for sequence PK."""
        plan = _make_simple_plan()
        resolved = resolve_plan(plan)
        meta = resolved.fk_resolutions[("orders", "customer_id")].parent_metadata

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
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.cid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)
        meta = resolved.fk_resolutions[("orders", "cid")].parent_metadata

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
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.uid"),
                ),
            ],
        )
        plan = DataGenPlan(tables=[customers, orders], seed=42)
        resolved = resolve_plan(plan)
        meta = resolved.fk_resolutions[("orders", "uid")].parent_metadata

        assert meta.pk_type == "uuid"
        assert meta.row_count == 300


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestExpressionColumnValidation:
    @staticmethod
    def _plan(expr: str, extra_cols: list[ColumnSpec] | None = None) -> DataGenPlan:
        cols = [ColumnSpec(name="a", gen=RangeColumn(min=1, max=10))]
        if extra_cols:
            cols.extend(extra_cols)
        cols.append(ColumnSpec(name="b", gen=ExpressionColumn(expr=expr)))
        return DataGenPlan(tables=[TableSpec(name="t", rows=10, columns=cols)])

    def test_undefined_reference_raises(self):
        with pytest.raises(ValueError, match="unknown_col"):
            resolve_plan(self._plan("unknown_col + 1"))

    def test_error_lists_available_columns(self):
        with pytest.raises(ValueError, match=r"Available columns: \['a', 'b'\]"):
            resolve_plan(self._plan("typo + 1"))

    def test_valid_reference_ok(self):
        resolved = resolve_plan(self._plan("a + 1"))
        assert "t" in resolved.generation_order

    def test_spark_function_not_flagged(self):
        """Spark builtins like year() are function calls, not column refs."""
        cols = [ColumnSpec(name="d", gen=RangeColumn(min=1, max=10))]
        resolve_plan(self._plan("year(d) + 1", extra_cols=cols))

    def test_nested_function_calls_not_flagged(self):
        resolve_plan(self._plan("coalesce(cast(a as string), 'x')"))

    def test_string_literal_contents_not_flagged(self):
        """Identifiers inside single-quoted strings should not trigger validation."""
        resolve_plan(self._plan("case when a > 0 then 'unknown_text_here' else 'other' end"))

    def test_struct_field_access_not_flagged(self):
        """``struct_col.field`` should not flag ``field`` as an unknown column."""
        resolve_plan(self._plan("a.some_field + 1"))

    def test_cast_with_type_name_not_flagged(self):
        resolve_plan(self._plan("cast(a as bigint) + 1"))

    def test_keywords_and_literals_not_flagged(self):
        resolve_plan(self._plan("case when a is not null and a between 1 and 10 then true else false end"))

    def test_multiple_unknowns_reported(self):
        """Error message must list ALL unknowns, not just the first."""
        with pytest.raises(ValueError) as exc:
            resolve_plan(self._plan("typo1 + typo2"))
        msg = str(exc.value)
        assert "['typo1', 'typo2']" in msg, f"expected sorted list, got: {msg}"

    def test_interval_expression_not_flagged(self):
        """``interval 1 day`` — unit keywords must not false-positive."""
        resolve_plan(self._plan("a + interval 1 day"))
        resolve_plan(self._plan("a - interval 3 months"))
        resolve_plan(self._plan("a + interval 2 quarters"))

    def test_current_date_bare_not_flagged(self):
        """``current_date`` / ``current_timestamp`` are legal without parens."""
        resolve_plan(self._plan("current_date"))
        resolve_plan(self._plan("case when a > 0 then current_timestamp else current_date end"))

    def test_double_quoted_string_contents_not_flagged(self):
        """Double-quoted strings are Spark string literals (ANSI default off)."""
        resolve_plan(self._plan('concat(cast(a as string), "unknown_label")'))

    def test_backtick_identifier_contents_not_flagged(self):
        """Backtick-quoted identifier contents must be stripped before tokenizing."""
        resolve_plan(self._plan("a + `some external col with space`"))

    def test_sql_escaped_quote_in_string_not_flagged(self):
        """SQL doubles a quote to escape: ``'it''s'`` is one literal, not two."""
        resolve_plan(self._plan("case when a > 0 then 'it''s unknown_word here' else 'x' end"))

    def test_window_function_not_flagged(self):
        """``rank() over (partition by a order by a)`` — the bare tokens
        ``over``, ``partition``, ``by``, ``order`` are window-function
        keywords, not unknown column references.  A regression here would
        break valid v0-style expressions in core.
        """
        resolve_plan(self._plan("rank() over (partition by a order by a)"))

    def test_window_function_with_frame_not_flagged(self):
        """``rows between unbounded preceding and current row`` — frame-bound
        keywords (``rows``, ``unbounded``, ``preceding``, ``current``,
        ``row``, ``following``) must also pass."""
        resolve_plan(self._plan("sum(a) over (order by a rows between unbounded preceding and current row)"))

    def test_aggregation_with_asc_desc_not_flagged(self):
        """``order by a desc`` / ``... asc`` keywords must pass."""
        resolve_plan(self._plan("first(a) over (order by a desc)"))
        resolve_plan(self._plan("first(a) over (order by a asc)"))

    def test_nulls_first_last_not_flagged(self):
        """``order by a nulls first`` / ``... nulls last`` — the bare
        tokens ``nulls``, ``first``, ``last`` must pass."""
        resolve_plan(self._plan("first(a) over (order by a nulls first)"))
        resolve_plan(self._plan("last(a) over (order by a nulls last)"))

    def test_filter_where_clause_not_flagged(self):
        """``sum(a) filter (where a > 0)`` — the bare token ``where``
        must pass."""
        resolve_plan(self._plan("sum(a) filter (where a > 0)"))

    def test_reference_to_fk_column_rejected(self):
        """ExpressionColumn cannot reference an FK column (phase-2 column).

        The expression runs in phase-1 ``select``; the FK column isn't
        added to the DataFrame until phase 2's ``withColumn`` step, so
        the ``F.expr()`` reference would fail at Spark plan time with
        ``UNRESOLVED_COLUMN``.  Reject at validation with a clearer message.
        """
        parent = TableSpec(
            name="customers",
            rows=10,
            columns=[ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1))],
            primary_key=PrimaryKey(columns=["customer_id"]),
        )
        child = TableSpec(
            name="orders",
            rows=20,
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="customer_id",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(ref="customers.customer_id"),
                ),
                ColumnSpec(name="customer_label", gen=ExpressionColumn(expr="concat('cid-', customer_id)")),
            ],
            primary_key=PrimaryKey(columns=["order_id"]),
        )
        with pytest.raises(ValueError, match="FK / Faker / seed_from columns applied in a later phase"):
            resolve_plan(DataGenPlan(tables=[parent, child]))

    def test_reference_to_faker_column_rejected(self):
        """ExpressionColumn cannot reference a FakerColumn (phase-2 column)."""
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    columns=[
                        ColumnSpec(name="a", gen=RangeColumn(min=1, max=10)),
                        ColumnSpec(name="full_name", gen=FakerColumn(provider="name")),
                        ColumnSpec(name="greeting", gen=ExpressionColumn(expr="concat('hi ', full_name)")),
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="FK / Faker / seed_from columns applied in a later phase"):
            resolve_plan(plan)

    def test_reference_to_seed_from_column_rejected(self):
        """ExpressionColumn cannot reference a seed_from-derived column (phase 3)."""
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    columns=[
                        ColumnSpec(name="group_id", gen=RangeColumn(min=1, max=5)),
                        ColumnSpec(
                            name="country",
                            gen=ExpressionColumn(expr="concat('country_', group_id)"),  # group_id is OK (regular)
                        ),
                        ColumnSpec(
                            name="label",
                            gen=RangeColumn(min=1, max=2),
                            seed_from="group_id",
                        ),
                        ColumnSpec(name="bad", gen=ExpressionColumn(expr="label + 1")),  # label is phase-3
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="FK / Faker / seed_from columns applied in a later phase"):
            resolve_plan(plan)


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
        # ``TableSpec.validate_primary_key_columns_exist`` now catches
        # this at model construction (one validation layer earlier than
        # the planner).  Direct ``generate_table`` callers also get the
        # signal now -- see tests/core/spec/test_table_spec_validators.py
        # for the model-layer coverage.  Pinned here so the planner
        # rejection assertion stays correct if the validator order ever
        # shifts.
        with pytest.raises(ValueError, match="don't exist"):
            TableSpec(
                name="t",
                rows=10,
                primary_key=PrimaryKey(columns=["id", "missing"]),
                columns=[
                    ColumnSpec(name="id", gen=SequenceColumn()),
                    ColumnSpec(name="val", gen=RangeColumn(min=1, max=10)),
                ],
            )

    def test_pk_with_range_column_strategy_rejected(self):
        """A PK column with a RangeColumn strategy is rejected at plan time.

        FK reconstruction expects sequence / pattern / uuid PK shapes.
        Without this validator a RangeColumn PK would produce FK values
        that don't match the parent's actual PK values; reject up
        front so the error names the offender.
        """
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    primary_key=PrimaryKey(columns=["id"]),
                    columns=[
                        ColumnSpec(name="id", gen=RangeColumn(min=1, max=100)),
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="not a supported PK strategy"):
            resolve_plan(plan)

    def test_pk_with_constant_column_strategy_rejected(self):
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t",
                    rows=10,
                    primary_key=PrimaryKey(columns=["id"]),
                    columns=[
                        ColumnSpec(name="id", gen=ConstantColumn(value=1)),
                    ],
                )
            ]
        )
        with pytest.raises(ValueError, match="not a supported PK strategy"):
            resolve_plan(plan)

    def test_pk_strategy_check_fires_before_fk_loop(self):
        """Bad-PK + child-FK: the FRIENDLY ValueError fires, not the engine RuntimeError.

        Before the validator-ordering fix, the FK loop's
        ``_extract_pk_metadata`` call hit its RuntimeError backstop
        ("validator-ordering regression") before
        ``_validate_primary_keys`` ran, surfacing a confusing message.
        Pin the ordering so the friendly ValueError fires first.
        """
        parent = TableSpec(
            name="parent",
            rows=10,
            primary_key=PrimaryKey(columns=["id"]),
            columns=[ColumnSpec(name="id", gen=RangeColumn(min=1, max=100))],
        )
        child = TableSpec(
            name="child",
            rows=20,
            primary_key=PrimaryKey(columns=["cid"]),
            columns=[
                ColumnSpec(name="cid", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="pid", gen=ForeignKeyColumn(), foreign_key=ForeignKeyRef(ref="parent.id")),
            ],
        )
        with pytest.raises(ValueError, match="not a supported PK strategy"):
            resolve_plan(DataGenPlan(tables=[parent, child]))
