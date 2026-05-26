"""Tests for dbldatagen.core.spec.schema Pydantic models."""

import pytest

from dbldatagen.core.spec.schema import (
    ArrayColumn,
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    DataType,
    Exponential,
    ExpressionColumn,
    FakerColumn,
    ForeignKeyColumn,
    ForeignKeyRef,
    LogNormal,
    Normal,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    TimestampColumn,
    Uniform,
    UUIDColumn,
    ValuesColumn,
    WeightedValues,
    Zipf,
)


# ---------------------------------------------------------------------------
# Distribution types
# ---------------------------------------------------------------------------


class TestDistributions:
    def test_uniform(self):
        d = Uniform()
        assert d.type == "uniform"

    def test_normal(self):
        d = Normal(mean=10.0, stddev=2.5)
        assert d.type == "normal"
        assert d.mean == 10.0
        assert d.stddev == 2.5

    def test_normal_defaults(self):
        d = Normal()
        assert d.mean == 0.0
        assert d.stddev == 1.0

    def test_lognormal(self):
        d = LogNormal(mean=1.0, stddev=0.5)
        assert d.type == "lognormal"
        assert d.mean == 1.0
        assert d.stddev == 0.5

    def test_zipf(self):
        d = Zipf(exponent=2.0)
        assert d.type == "zipf"
        assert d.exponent == 2.0

    def test_zipf_default(self):
        d = Zipf()
        assert d.exponent == 1.5

    def test_exponential(self):
        d = Exponential(rate=0.5)
        assert d.type == "exponential"
        assert d.rate == 0.5

    def test_exponential_default(self):
        d = Exponential()
        assert d.rate == 1.0

    def test_weighted_values(self):
        d = WeightedValues(weights={"a": 0.7, "b": 0.3})
        assert d.type == "weighted"
        assert d.weights == {"a": 0.7, "b": 0.3}


# ---------------------------------------------------------------------------
# Column strategy types
# ---------------------------------------------------------------------------


class TestColumnStrategies:
    def test_range_column(self):
        s = RangeColumn(min=10, max=50, step=5)
        assert s.strategy == "range"
        assert s.min == 10
        assert s.max == 50
        assert s.step == 5
        assert isinstance(s.distribution, Uniform)

    def test_range_column_defaults(self):
        s = RangeColumn()
        assert s.min == 0
        assert s.max == 100
        assert s.step is None

    def test_range_column_with_distribution(self):
        s = RangeColumn(min=0, max=100, distribution=Normal(mean=50, stddev=10))
        assert isinstance(s.distribution, Normal)

    def test_values_column(self):
        s = ValuesColumn(values=["a", "b", "c"])
        assert s.strategy == "values"
        assert s.values == ["a", "b", "c"]

    def test_values_column_with_distribution(self):
        w = WeightedValues(weights={"x": 0.5, "y": 0.5})
        s = ValuesColumn(values=["x", "y"], distribution=w)
        assert isinstance(s.distribution, WeightedValues)

    def test_faker_column(self):
        s = FakerColumn(provider="name")
        assert s.strategy == "faker"
        assert s.provider == "name"
        assert s.kwargs == {}
        assert s.locale is None

    def test_faker_column_with_kwargs_and_locale(self):
        s = FakerColumn(provider="date_between", kwargs={"start_date": "-5y"}, locale="de_DE")
        assert s.kwargs == {"start_date": "-5y"}
        assert s.locale == "de_DE"

    def test_faker_kwargs_default_is_independent_dict(self):
        """``kwargs`` uses ``Field(default_factory=dict)``: each
        instance gets its own dict, not a shared one.  Pydantic v2
        already deep-copies the literal ``{}`` default, but the
        ``default_factory`` spelling makes the intent unambiguous
        and pins it against a future regression that swaps the
        default for a mutable literal."""
        a = FakerColumn(provider="name")
        b = FakerColumn(provider="email")
        a.kwargs["mutated"] = 1
        assert "mutated" not in b.kwargs

    def test_pattern_column(self):
        s = PatternColumn(template="ORD-{digit:4}-{alpha:3}")
        assert s.strategy == "pattern"
        assert s.template == "ORD-{digit:4}-{alpha:3}"

    def test_sequence_column(self):
        s = SequenceColumn(start=100, step=10)
        assert s.strategy == "sequence"
        assert s.start == 100
        assert s.step == 10

    def test_sequence_column_defaults(self):
        s = SequenceColumn()
        assert s.start == 1
        assert s.step == 1

    def test_uuid_column(self):
        s = UUIDColumn()
        assert s.strategy == "uuid"

    def test_expression_column(self):
        s = ExpressionColumn(expr="quantity * unit_price")
        assert s.strategy == "expression"
        assert s.expr == "quantity * unit_price"

    def test_expression_column_empty_expr_rejected(self):
        """``ExpressionColumn(expr="")`` would crash at ``F.expr("")``;
        reject at construction so the failure names the column."""
        with pytest.raises(ValueError, match="at least 1 character"):
            ExpressionColumn(expr="")

    def test_timestamp_column(self):
        s = TimestampColumn(start="2023-01-01", end="2024-12-31")
        assert s.strategy == "timestamp"
        assert s.start == "2023-01-01"
        assert s.end == "2024-12-31"

    def test_timestamp_column_requires_start_and_end(self):
        """``TimestampColumn`` has no universal default — both bounds
        must be supplied.  Past defaults (``"2020-01-01"`` /
        ``"2025-12-31"``) were vestigial demo values that went stale."""
        with pytest.raises(ValueError, match="Field required"):
            TimestampColumn()  # type: ignore[call-arg]

    def test_timestamp_column_with_distribution(self):
        s = TimestampColumn(start="2023-01-01", end="2023-12-31", distribution=Normal(mean=0.5, stddev=0.1))
        assert isinstance(s.distribution, Normal)

    def test_constant_column(self):
        s = ConstantColumn(value="fixed")
        assert s.strategy == "constant"
        assert s.value == "fixed"

    def test_constant_column_none_value(self):
        s = ConstantColumn(value=None)
        assert s.value is None


# ---------------------------------------------------------------------------
# ColumnSpec
# ---------------------------------------------------------------------------


class TestColumnSpec:
    def test_basic_column_with_range(self):
        col = ColumnSpec(name="age", dtype=DataType.INT, gen=RangeColumn(min=18, max=90))
        assert col.name == "age"
        assert col.dtype == DataType.INT
        assert isinstance(col.gen, RangeColumn)
        assert col.nullable is False
        assert col.null_fraction == 0.0
        assert col.foreign_key is None

    def test_column_with_faker(self):
        col = ColumnSpec(name="email", dtype=DataType.STRING, gen=FakerColumn(provider="email"))
        assert isinstance(col.gen, FakerColumn)
        assert col.gen.provider == "email"

    def test_column_with_sequence(self):
        col = ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn())
        assert isinstance(col.gen, SequenceColumn)

    def test_column_with_uuid(self):
        col = ColumnSpec(name="id", dtype=DataType.STRING, gen=UUIDColumn())
        assert isinstance(col.gen, UUIDColumn)

    def test_column_with_pattern(self):
        col = ColumnSpec(name="code", gen=PatternColumn(template="X-{digit:4}"))
        assert isinstance(col.gen, PatternColumn)

    def test_column_with_expression(self):
        col = ColumnSpec(name="total", gen=ExpressionColumn(expr="a + b"))
        assert isinstance(col.gen, ExpressionColumn)

    def test_column_with_timestamp(self):
        col = ColumnSpec(name="ts", gen=TimestampColumn(start="2023-01-01", end="2023-12-31"))
        assert isinstance(col.gen, TimestampColumn)

    def test_column_with_values(self):
        col = ColumnSpec(name="tier", gen=ValuesColumn(values=["a", "b"]))
        assert isinstance(col.gen, ValuesColumn)

    def test_column_with_constant(self):
        col = ColumnSpec(name="version", gen=ConstantColumn(value=1))
        assert isinstance(col.gen, ConstantColumn)

    @pytest.mark.parametrize(
        "reserved_name",
        ["_write_batch", "_synth_row_id", "_anything"],
    )
    def test_leading_underscore_name_rejected(self, reserved_name):
        """Names with a leading underscore collide with engine-internal
        metadata columns (``_write_batch``, ``_synth_row_id``).
        Rejected at construction so the collision fails loudly instead
        of silently shadowing."""
        with pytest.raises(ValueError, match="reserved for engine-internal"):
            ColumnSpec(name=reserved_name, gen=RangeColumn())

    @pytest.mark.parametrize(
        "bad_name",
        ["1col", "col-with-dash", "col name", "col.dot", "col`tick", "", "col\n"],
    )
    def test_non_identifier_name_rejected(self, bad_name):
        """Names must be valid identifiers so they round-trip through
        Spark without backtick quoting."""
        with pytest.raises(ValueError, match="not a valid identifier"):
            ColumnSpec(name=bad_name, gen=RangeColumn())

    def test_null_fraction_and_nullable_are_orthogonal(self):
        """``null_fraction`` drives runtime NULL injection; ``nullable``
        is separate schema metadata.  The validator used to mutate
        ``nullable=True`` when ``null_fraction > 0`` -- a side-effect
        that broke ``dump -> validate`` round-tripping (the dumped
        model picked up a ``nullable=True`` the user never typed).
        The fields are now fully orthogonal.
        """
        col = ColumnSpec(name="x", gen=RangeColumn(), null_fraction=0.1)
        assert col.nullable is False
        assert col.null_fraction == 0.1

    def test_zero_null_fraction_keeps_nullable_false(self):
        col = ColumnSpec(name="x", gen=RangeColumn(), null_fraction=0.0)
        assert col.nullable is False

    def test_explicit_nullable_with_zero_fraction(self):
        col = ColumnSpec(name="x", gen=RangeColumn(), nullable=True, null_fraction=0.0)
        assert col.nullable is True

    def test_column_spec_is_pure_under_roundtrip(self):
        """Construct -> model_dump -> re-construct must be idempotent:
        nothing the user didn't type appears in the re-constructed
        spec.  Pins the fix that removed the ``nullable`` auto-flip
        (which broke this round-trip).
        """
        col1 = ColumnSpec(name="x", gen=RangeColumn(), null_fraction=0.25)
        dumped = col1.model_dump()
        col2 = ColumnSpec.model_validate(dumped)
        assert col1.model_dump() == col2.model_dump()

    def test_column_with_foreign_key(self):
        fk_ref = ForeignKeyRef(ref="customers.id")
        col = ColumnSpec(name="customer_id", gen=ForeignKeyColumn(), foreign_key=fk_ref)
        assert col.foreign_key is not None
        assert col.foreign_key.ref == "customers.id"

    def test_decimal_precision_scale_stored(self):
        col = ColumnSpec(
            name="rate",
            dtype=DataType.DECIMAL,
            gen=RangeColumn(min=0, max=1),
            precision=10,
            scale=4,
        )
        assert col.precision == 10
        assert col.scale == 4

    def test_decimal_without_precision_scale_defaults_to_none(self):
        """Both unset on a DECIMAL column → engine falls back to Spark's DecimalType() default of (10, 0)."""
        col = ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn())
        assert col.precision is None
        assert col.scale is None

    def test_precision_without_scale_rejected(self):
        with pytest.raises(ValueError, match="precision and scale must be set together"):
            ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn(), precision=10)

    def test_scale_without_precision_rejected(self):
        with pytest.raises(ValueError, match="precision and scale must be set together"):
            ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn(), scale=4)

    def test_precision_scale_on_non_decimal_rejected(self):
        with pytest.raises(ValueError, match="precision/scale are only valid when dtype=DECIMAL"):
            ColumnSpec(name="x", dtype=DataType.INT, gen=RangeColumn(), precision=10, scale=4)

    def test_precision_out_of_range_rejected(self):
        with pytest.raises(ValueError, match=r"precision must be in \[1, 38\]"):
            ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn(), precision=39, scale=2)
        with pytest.raises(ValueError, match=r"precision must be in \[1, 38\]"):
            ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn(), precision=0, scale=0)

    def test_scale_greater_than_precision_rejected(self):
        with pytest.raises(ValueError, match=r"scale must be in \[0, precision\]"):
            ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn(), precision=5, scale=6)

    def test_scale_negative_rejected(self):
        with pytest.raises(ValueError, match=r"scale must be in \[0, precision\]"):
            ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn(), precision=10, scale=-1)

    def test_range_exceeds_precision_rejected(self):
        """decimal(5,2) holds up to 999.99; max=10000 must fail at plan time."""
        with pytest.raises(ValueError, match="does not fit in decimal"):
            ColumnSpec(
                name="x",
                dtype=DataType.DECIMAL,
                gen=RangeColumn(min=0, max=10000),
                precision=5,
                scale=2,
            )

    def test_range_negative_exceeds_precision_rejected(self):
        with pytest.raises(ValueError, match="does not fit in decimal"):
            ColumnSpec(
                name="x",
                dtype=DataType.DECIMAL,
                gen=RangeColumn(min=-10000, max=0),
                precision=5,
                scale=2,
            )

    def test_range_at_max_representable_ok(self):
        """999.99 is the max representable in decimal(5, 2); must not false-fail."""
        col = ColumnSpec(
            name="x",
            dtype=DataType.DECIMAL,
            gen=RangeColumn(min=0, max=999.99),
            precision=5,
            scale=2,
        )
        assert col.precision == 5

    def test_range_check_skipped_when_precision_unset(self):
        """Plans without explicit precision/scale fall through to Spark's
        ``DecimalType()`` default of ``(10, 0)``; range fit is not enforced
        at plan time so any overflow surfaces at materialization."""
        # Max 10**17 would overflow DecimalType(10, 0), but with no explicit
        # precision/scale we defer to Spark's runtime behaviour.
        col = ColumnSpec(name="x", dtype=DataType.DECIMAL, gen=RangeColumn(min=0, max=10**17))
        assert col.precision is None

    def test_values_column_exceeds_precision_rejected(self):
        """``ValuesColumn(values=[100000])`` on ``decimal(5, 0)`` (max 99999)
        overflows after cast; previously this passed Pydantic and surfaced
        as Spark's ``NUMERIC_VALUE_OUT_OF_RANGE`` deep in the job."""
        with pytest.raises(ValueError, match="does not fit in decimal"):
            ColumnSpec(
                name="x",
                dtype=DataType.DECIMAL,
                gen=ValuesColumn(values=[1, 50, 100000]),
                precision=5,
                scale=0,
            )

    def test_values_column_negative_exceeds_precision_rejected(self):
        """Magnitude check covers negative outliers too."""
        with pytest.raises(ValueError, match="does not fit in decimal"):
            ColumnSpec(
                name="x",
                dtype=DataType.DECIMAL,
                gen=ValuesColumn(values=[-1000000, 0, 5]),
                precision=5,
                scale=0,
            )

    def test_values_column_at_max_representable_ok(self):
        """99999 is the max representable in decimal(5, 0); must not false-fail."""
        col = ColumnSpec(
            name="x",
            dtype=DataType.DECIMAL,
            gen=ValuesColumn(values=[0, 1, 99999]),
            precision=5,
            scale=0,
        )
        assert col.precision == 5

    def test_values_column_with_floats_checked(self):
        """Float values are walked too; 999.99 fits in decimal(5, 2),
        1000.0 does not (limit = 1000)."""
        with pytest.raises(ValueError, match="does not fit in decimal"):
            ColumnSpec(
                name="x",
                dtype=DataType.DECIMAL,
                gen=ValuesColumn(values=[1.5, 999.99, 1000.0]),
                precision=5,
                scale=2,
            )

    def test_values_column_non_numeric_skipped(self):
        """Non-numeric values are skipped (a string ValuesColumn paired
        with dtype=DECIMAL is a different misconfiguration -- the
        decimal-fit validator must not crash on the list walk)."""
        # No numeric values → no check → no false-positive raise.
        col = ColumnSpec(
            name="x",
            dtype=DataType.DECIMAL,
            gen=ValuesColumn(values=["a", "b"]),
            precision=5,
            scale=0,
        )
        assert col.precision == 5

    def test_dtype_can_be_none(self):
        col = ColumnSpec(name="x", gen=RangeColumn())
        assert col.dtype is None


# ---------------------------------------------------------------------------
# DataType enum
# ---------------------------------------------------------------------------


class TestDataType:
    def test_values(self):
        assert DataType.INT.value == "int"
        assert DataType.LONG.value == "long"
        assert DataType.FLOAT.value == "float"
        assert DataType.DOUBLE.value == "double"
        assert DataType.STRING.value == "string"
        assert DataType.BOOLEAN.value == "boolean"
        assert DataType.DATE.value == "date"
        assert DataType.TIMESTAMP.value == "timestamp"
        assert DataType.DECIMAL.value == "decimal"

    def test_integer_alias(self):
        assert DataType.INTEGER.value == "int"
        assert DataType.INTEGER == DataType.INT  # type: ignore[comparison-overlap]


# ---------------------------------------------------------------------------
# PrimaryKey
# ---------------------------------------------------------------------------


class TestPrimaryKey:
    def test_single_column(self):
        pk = PrimaryKey(columns=["id"])
        assert pk.columns == ["id"]

    def test_composite_columns(self):
        pk = PrimaryKey(columns=["tenant_id", "user_id"])
        assert pk.columns == ["tenant_id", "user_id"]
        assert len(pk.columns) == 2

    def test_empty_columns_rejected(self):
        """An empty PK is meaningless — would silently disable FK
        targeting downstream.  Reject at construction so the spec
        never carries a zero-column PK."""
        with pytest.raises(ValueError, match="at least 1 item"):
            PrimaryKey(columns=[])

    def test_duplicate_columns_rejected(self):
        """``PrimaryKey(columns=["a", "a"])`` has no well-defined tuple
        identity; the downstream FK planner keys resolutions on
        ``(table, column)``, so a typo'd duplicate silently produced
        a single FK resolution and no signal to the user.  Reject at
        plan time."""
        with pytest.raises(ValueError, match=r"duplicate column names: \['a'\]"):
            PrimaryKey(columns=["a", "a"])

    def test_duplicate_columns_in_composite_rejected(self):
        """Mid-list duplicates in a longer composite PK also caught."""
        with pytest.raises(ValueError, match=r"duplicate column names: \['a'\]"):
            PrimaryKey(columns=["a", "b", "a"])

    def test_multiple_duplicates_listed(self):
        """All duplicated names appear in the error so the user can
        fix every typo in one round."""
        with pytest.raises(ValueError, match=r"duplicate column names: \['a', 'b'\]"):
            PrimaryKey(columns=["a", "b", "a", "b"])


# ---------------------------------------------------------------------------
# ForeignKeyRef
# ---------------------------------------------------------------------------


class TestForeignKeyRef:
    def test_string_ref(self):
        fk = ForeignKeyRef(ref="orders.order_id")
        assert fk.ref == "orders.order_id"
        assert isinstance(fk.distribution, Uniform)
        assert fk.nullable is False
        assert fk.null_fraction == 0.0

    def test_cardinality_field_removed(self):
        """``cardinality`` was declared on ``ForeignKeyRef`` but never
        consumed by the engine — keeping it would have been a
        load-bearing lie (users set it, nothing happened).  Assert the
        field is gone from the model definition so future refactors
        can't accidentally resurrect it.
        """
        assert "cardinality" not in ForeignKeyRef.model_fields

    def test_with_distribution(self):
        fk = ForeignKeyRef(ref="t.c", distribution=Zipf(exponent=1.3))
        assert isinstance(fk.distribution, Zipf)
        assert fk.distribution.exponent == 1.3


# ---------------------------------------------------------------------------
# TableSpec
# ---------------------------------------------------------------------------


class TestTableSpec:
    def _make_columns(self):
        return [ColumnSpec(name="id", gen=SequenceColumn())]

    def test_rows_int(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows=1000)
        assert t.rows == 1000

    def test_rows_string_million(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows="1M")
        assert t.rows == 1_000_000

    def test_rows_string_billion(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows="1B")
        assert t.rows == 1_000_000_000

    def test_rows_string_thousand(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows="500K")
        assert t.rows == 500_000

    def test_rows_string_fractional(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows="1.5M")
        assert t.rows == 1_500_000

    def test_rows_string_plain_number(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows="5000")
        assert t.rows == 5000

    def test_rows_string_lowercase(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows="10m")
        assert t.rows == 10_000_000

    def test_rows_string_empty_mantissa_friendly_error(self):
        """``"K"`` has no digits before the suffix; the float()
        conversion previously leaked a raw "could not convert string
        to float: ''" message.  Now wrapped by the same friendly
        guard as the plain-int path."""
        with pytest.raises(ValueError, match=r"Invalid row count 'K'"):
            TableSpec(name="t", columns=self._make_columns(), rows="K")

    def test_rows_string_malformed_mantissa_friendly_error(self):
        """``"1.5.0K"`` has a malformed mantissa; same wrap as the
        empty-mantissa case."""
        with pytest.raises(ValueError, match=r"Invalid row count '1\.5\.0K'"):
            TableSpec(name="t", columns=self._make_columns(), rows="1.5.0K")

    def test_rows_string_garbage_friendly_error(self):
        """Plain garbage with no suffix still hits the friendly wrap
        — pin that the int()-path branch of the unified try/except
        still raises with the same message shape."""
        with pytest.raises(ValueError, match=r"Invalid row count 'abc'"):
            TableSpec(name="t", columns=self._make_columns(), rows="abc")

    def test_primary_key(self):
        t = TableSpec(
            name="t",
            columns=self._make_columns(),
            rows=100,
            primary_key=PrimaryKey(columns=["id"]),
        )
        assert t.primary_key is not None
        assert t.primary_key.columns == ["id"]

    def test_seed_default_none(self):
        t = TableSpec(name="t", columns=self._make_columns(), rows=100)
        # seed is None before plan propagation
        assert t.seed is None


# ---------------------------------------------------------------------------
# DataGenPlan
# ---------------------------------------------------------------------------


class TestDataGenPlan:
    def _make_table(self, name, seed=None):
        cols = [ColumnSpec(name="id", gen=SequenceColumn())]
        return TableSpec(name=name, columns=cols, rows=100, seed=seed)

    def test_seed_propagation(self):
        plan = DataGenPlan(
            tables=[self._make_table("a"), self._make_table("b"), self._make_table("c")],
            seed=100,
        )
        assert plan.tables[0].seed == 100  # 100 + 0
        assert plan.tables[1].seed == 101  # 100 + 1
        assert plan.tables[2].seed == 102  # 100 + 2

    def test_explicit_seed_not_overwritten(self):
        plan = DataGenPlan(
            tables=[self._make_table("a", seed=999), self._make_table("b")],
            seed=100,
        )
        assert plan.tables[0].seed == 999  # explicit seed preserved
        assert plan.tables[1].seed == 101  # derived from plan seed

    def test_default_plan_seed(self):
        plan = DataGenPlan(tables=[self._make_table("a")])
        assert plan.seed == 42
        assert plan.tables[0].seed == 42

    def test_default_locale(self):
        plan = DataGenPlan(tables=[self._make_table("a")])
        assert plan.default_locale == "en_US"

    def test_custom_locale(self):
        plan = DataGenPlan(tables=[self._make_table("a")], default_locale="de_DE")
        assert plan.default_locale == "de_DE"


# ---------------------------------------------------------------------------
# JSON round-trip
# ---------------------------------------------------------------------------


class TestJsonRoundTrip:
    def test_column_spec_round_trip(self):
        col = ColumnSpec(
            name="age",
            dtype=DataType.INT,
            gen=RangeColumn(min=18, max=90, distribution=Normal(mean=40, stddev=15)),
            nullable=True,
            null_fraction=0.05,
        )
        json_str = col.model_dump_json()
        restored = ColumnSpec.model_validate_json(json_str)
        assert restored == col

    def test_table_spec_round_trip(self):
        table = TableSpec(
            name="users",
            columns=[
                ColumnSpec(name="id", dtype=DataType.LONG, gen=SequenceColumn()),
                ColumnSpec(name="name", dtype=DataType.STRING, gen=FakerColumn(provider="name")),
            ],
            rows=1000,
            primary_key=PrimaryKey(columns=["id"]),
            seed=42,
        )
        json_str = table.model_dump_json()
        restored = TableSpec.model_validate_json(json_str)
        assert restored == table

    def test_plan_round_trip(self):
        plan = DataGenPlan(
            tables=[
                TableSpec(
                    name="t1",
                    columns=[ColumnSpec(name="id", gen=SequenceColumn())],
                    rows=100,
                ),
                TableSpec(
                    name="t2",
                    columns=[
                        ColumnSpec(name="id", gen=UUIDColumn()),
                        ColumnSpec(
                            name="t1_id",
                            gen=ForeignKeyColumn(),
                            foreign_key=ForeignKeyRef(ref="t1.id"),
                        ),
                    ],
                    rows=500,
                ),
            ],
            seed=123,
        )
        json_str = plan.model_dump_json()
        restored = DataGenPlan.model_validate_json(json_str)
        assert restored == plan

    def test_all_strategies_round_trip(self):
        """Every column strategy type survives JSON serialization."""
        strategies = [
            RangeColumn(min=0, max=10),
            ValuesColumn(values=["a", "b"]),
            FakerColumn(provider="email"),
            PatternColumn(template="X-{digit:3}"),
            SequenceColumn(),
            UUIDColumn(),
            ExpressionColumn(expr="a + b"),
            TimestampColumn(start="2023-01-01", end="2023-12-31"),
            ConstantColumn(value=42),
        ]
        for i, strat in enumerate(strategies):
            col = ColumnSpec(name=f"col_{i}", gen=strat)  # type: ignore[arg-type]
            json_str = col.model_dump_json()
            restored = ColumnSpec.model_validate_json(json_str)
            assert restored == col, f"Round-trip failed for {type(strat).__name__}"

    def test_all_distributions_round_trip(self):
        """Every continuous distribution survives JSON serialization inside a
        RangeColumn.  ``WeightedValues`` is excluded because RangeColumn
        validators now reject it (no discrete value list to weight) --
        the WeightedValues round-trip lives in
        ``test_weighted_values_round_trip_on_values_column``."""
        distributions = [
            Uniform(),
            Normal(mean=5, stddev=2),
            LogNormal(mean=1, stddev=0.5),
            Zipf(exponent=2.0),
            Exponential(rate=0.3),
        ]
        for dist in distributions:
            col = ColumnSpec(name="x", gen=RangeColumn(distribution=dist))  # type: ignore[arg-type]
            json_str = col.model_dump_json()
            restored = ColumnSpec.model_validate_json(json_str)
            assert restored == col, f"Round-trip failed for {type(dist).__name__}"

    def test_weighted_values_round_trip_on_values_column(self):
        """``WeightedValues`` is the one distribution that's only valid on
        ``ValuesColumn`` (where the discrete list is in scope for the
        weighted-CASE-WHEN dispatch).  Pin the round-trip there."""
        col = ColumnSpec(
            name="tier",
            gen=ValuesColumn(
                values=["x", "y"],
                distribution=WeightedValues(weights={"x": 0.6, "y": 0.4}),
            ),
        )
        restored = ColumnSpec.model_validate_json(col.model_dump_json())
        assert restored == col

    def test_weighted_values_rejected_on_range_column(self):
        with pytest.raises(ValueError, match="RangeColumn does not support WeightedValues"):
            RangeColumn(distribution=WeightedValues(weights={"a": 1.0}))

    def test_weighted_values_rejected_on_timestamp_column(self):
        with pytest.raises(ValueError, match="TimestampColumn does not support WeightedValues"):
            TimestampColumn(start="2023-01-01", end="2023-12-31", distribution=WeightedValues(weights={"a": 1.0}))

    def test_weighted_values_rejected_on_foreign_key_ref(self):
        with pytest.raises(ValueError, match="ForeignKeyRef does not support WeightedValues"):
            ForeignKeyRef(ref="t.c", distribution=WeightedValues(weights={"a": 1.0}))


# ---------------------------------------------------------------------------
# Input validation tests
# ---------------------------------------------------------------------------


class TestDistributionValidation:
    def test_normal_negative_stddev(self):
        with pytest.raises(ValueError, match="stddev must be >= 0"):
            Normal(stddev=-1.0)

    def test_lognormal_negative_stddev(self):
        with pytest.raises(ValueError, match="stddev must be >= 0"):
            LogNormal(stddev=-0.5)

    def test_zipf_zero_exponent(self):
        # Power-law CDF diverges for exponent <= 1 (not just <= 0), so the
        # validator now rejects both -- the engine used to silently fall
        # back to ``exp(log(n) * u)``, which is neither Zipf nor uniform.
        with pytest.raises(ValueError, match="must be > 1"):
            Zipf(exponent=0)

    def test_zipf_negative_exponent(self):
        with pytest.raises(ValueError, match="must be > 1"):
            Zipf(exponent=-1.0)

    def test_zipf_exponent_exactly_one_rejected(self):
        """``exponent == 1`` is the harmonic-series boundary where the
        normalizing sum diverges.  Reject at validation so no caller
        lands on wrong-shaped data."""
        with pytest.raises(ValueError, match="must be > 1"):
            Zipf(exponent=1.0)

    def test_zipf_exponent_just_above_one_accepted(self):
        """Just above 1 is valid -- covers the ``1.2`` default used by
        ``fk()`` and nearby callers."""
        Zipf(exponent=1.01)

    def test_exponential_zero_rate(self):
        with pytest.raises(ValueError, match="rate must be > 0"):
            Exponential(rate=0)

    def test_exponential_negative_rate(self):
        with pytest.raises(ValueError, match="rate must be > 0"):
            Exponential(rate=-0.5)

    def test_weighted_empty(self):
        with pytest.raises(ValueError, match="weights must not be empty"):
            WeightedValues(weights={})

    def test_weighted_negative(self):
        with pytest.raises(ValueError, match="weights must be non-negative"):
            WeightedValues(weights={"a": 0.5, "b": -0.3})

    def test_normal_zero_stddev_ok(self):
        n = Normal(stddev=0.0)
        assert n.stddev == 0.0


class TestRangeColumnValidation:
    def test_min_greater_than_max(self):
        with pytest.raises(ValueError, match=r"min.*must be <= max"):
            RangeColumn(min=100, max=50)

    def test_min_equals_max_ok(self):
        r = RangeColumn(min=5, max=5)
        assert r.min == r.max

    def test_step_zero(self):
        with pytest.raises(ValueError, match="step must be > 0"):
            RangeColumn(step=0)

    def test_step_negative(self):
        with pytest.raises(ValueError, match="step must be > 0"):
            RangeColumn(step=-1)


class TestValuesColumnValidation:
    def test_empty_values(self):
        with pytest.raises(ValueError, match="values list must not be empty"):
            ValuesColumn(values=[])


class TestSequenceColumnValidation:
    def test_step_zero(self):
        with pytest.raises(ValueError, match="step must not be 0"):
            SequenceColumn(step=0)

    def test_negative_step_ok(self):
        s = SequenceColumn(step=-1)
        assert s.step == -1


class TestTimestampColumnValidation:
    def test_start_after_end(self):
        with pytest.raises(ValueError, match=r"start.*must be <= end"):
            TimestampColumn(start="2025-01-01", end="2020-01-01")

    def test_equal_dates_ok(self):
        t = TimestampColumn(start="2025-01-01", end="2025-01-01")
        assert t.start == t.end


class TestArrayColumnValidation:
    def test_negative_min_length(self):
        with pytest.raises(ValueError, match="min_length must be >= 0"):
            ArrayColumn(element=RangeColumn(), min_length=-1, max_length=5)

    def test_zero_min_length_ok(self):
        a = ArrayColumn(element=RangeColumn(), min_length=0, max_length=3)
        assert a.min_length == 0


class TestNullFractionValidation:
    def test_column_spec_above_one(self):
        with pytest.raises(ValueError, match="null_fraction must be in"):
            ColumnSpec(name="x", gen=RangeColumn(), null_fraction=1.5)

    def test_column_spec_negative(self):
        with pytest.raises(ValueError, match="null_fraction must be in"):
            ColumnSpec(name="x", gen=RangeColumn(), null_fraction=-0.1)

    def test_fk_null_fraction_above_one(self):
        with pytest.raises(ValueError, match="null_fraction must be in"):
            ForeignKeyRef(ref="t.c", null_fraction=1.5)

    def test_fk_null_fraction_negative(self):
        with pytest.raises(ValueError, match="null_fraction must be in"):
            ForeignKeyRef(ref="t.c", null_fraction=-0.1)

    def test_fk_null_fraction_on_both_disagreeing_rejected(self):
        """ColumnSpec.null_fraction and ForeignKeyRef.null_fraction set to
        different non-zero values must be rejected at schema validation
        so the user's intent is unambiguous."""
        with pytest.raises(ValueError, match="null_fraction is set on both"):
            ColumnSpec(
                name="cid",
                gen=ForeignKeyColumn(),
                null_fraction=0.3,
                foreign_key=ForeignKeyRef(ref="t.c", null_fraction=0.7),
            )

    def test_fk_null_fraction_on_both_agreeing_ok(self):
        col = ColumnSpec(
            name="cid",
            gen=ForeignKeyColumn(),
            null_fraction=0.3,
            foreign_key=ForeignKeyRef(ref="t.c", null_fraction=0.3),
        )
        assert col.null_fraction == 0.3
        assert col.foreign_key is not None
        assert col.foreign_key.null_fraction == 0.3


class TestTableSpecValidation:
    def test_rows_zero(self):
        with pytest.raises(ValueError, match="rows must be > 0"):
            TableSpec(name="t", columns=[ColumnSpec(name="x", gen=RangeColumn())], rows=0)

    def test_rows_negative(self):
        with pytest.raises(ValueError, match="rows must be > 0"):
            TableSpec(name="t", columns=[ColumnSpec(name="x", gen=RangeColumn())], rows=-10)


class TestSequenceColumnOverflowGuard:
    """``SequenceColumn`` PKs overflow int64 at ``start + (rows-1)*step >
    2**63 - 1``.  Engine computes this expression in Spark and ANSI mode
    raises ``ARITHMETIC_OVERFLOW`` mid-job with no pointer back to the
    column.  The TableSpec-level validator catches it at plan time."""

    def test_overflow_at_large_step_times_rows(self):
        """A user who picks ``step = 10**18`` with ``rows = 20`` gets
        ``0 + 19 * 10**18 = 1.9e19`` which exceeds int64 max (~9.22e18)."""
        with pytest.raises(ValueError, match="overflows int64"):
            TableSpec(
                name="t",
                columns=[ColumnSpec(name="pk", gen=SequenceColumn(start=0, step=10**18))],
                rows=20,
            )

    def test_overflow_at_large_start(self):
        """Even with step=1, a near-Long.MAX ``start`` overflows at the
        last row."""
        with pytest.raises(ValueError, match="overflows int64"):
            TableSpec(
                name="t",
                columns=[ColumnSpec(name="pk", gen=SequenceColumn(start=2**63 - 5, step=1))],
                rows=100,
            )

    def test_negative_overflow_with_descending_step(self):
        """``step = -big`` can underflow Long.MIN at the last row."""
        with pytest.raises(ValueError, match="overflows int64"):
            TableSpec(
                name="t",
                columns=[ColumnSpec(name="pk", gen=SequenceColumn(start=0, step=-(10**18)))],
                rows=20,
            )

    def test_realistic_plan_accepted(self):
        """The 500M-3B row target with typical steps stays well within
        int64 -- guard must not flag realistic plans."""
        TableSpec(
            name="t",
            columns=[ColumnSpec(name="pk", gen=SequenceColumn(start=1, step=1))],
            rows=3_000_000_000,
        )
        TableSpec(
            name="t",
            columns=[ColumnSpec(name="pk", gen=SequenceColumn(start=10**6, step=1000))],
            rows=10_000_000,
        )

    def test_non_pk_sequence_also_checked(self):
        """SequenceColumn can appear on any column, not just PKs.  The
        arithmetic overflow hits the same ``id * step + start`` engine
        path regardless; validate across all columns."""
        with pytest.raises(ValueError, match="overflows int64"):
            TableSpec(
                name="t",
                columns=[
                    ColumnSpec(name="pk", gen=SequenceColumn()),
                    ColumnSpec(name="counter", gen=SequenceColumn(start=0, step=10**18)),
                ],
                rows=100,
            )

    def test_sequence_overflows_decimal_precision(self):
        """``SequenceColumn(start=1, step=1)`` over 10**6 rows reaches
        last_val = 1_000_000, which does not fit in decimal(5, 0)
        (max 99999).  Previously surfaced as Spark's
        ``NUMERIC_VALUE_OUT_OF_RANGE`` at cast time; now caught at
        plan time."""
        with pytest.raises(ValueError, match=r"does not fit in decimal\(5, 0\)"):
            TableSpec(
                name="t",
                columns=[
                    ColumnSpec(
                        name="c",
                        dtype=DataType.DECIMAL,
                        gen=SequenceColumn(start=1, step=1),
                        precision=5,
                        scale=0,
                    )
                ],
                rows=10**6,
            )

    def test_sequence_at_decimal_max_ok(self):
        """``SequenceColumn(start=1, step=1)`` over 99999 rows hits
        last_val = 99999, exactly the max representable in
        decimal(5, 0).  Must not false-fail at the boundary."""
        TableSpec(
            name="t",
            columns=[
                ColumnSpec(
                    name="c",
                    dtype=DataType.DECIMAL,
                    gen=SequenceColumn(start=1, step=1),
                    precision=5,
                    scale=0,
                )
            ],
            rows=99999,
        )

    def test_sequence_negative_step_overflows_decimal(self):
        """With a descending step, ``start`` is the magnitude-max; the
        check must catch overflow at row 0, not just at last_val."""
        with pytest.raises(ValueError, match=r"does not fit in decimal\(5, 0\)"):
            TableSpec(
                name="t",
                columns=[
                    ColumnSpec(
                        name="c",
                        dtype=DataType.DECIMAL,
                        gen=SequenceColumn(start=200_000, step=-1),
                        precision=5,
                        scale=0,
                    )
                ],
                rows=10,
            )

    def test_sequence_decimal_check_skipped_when_precision_unset(self):
        """Mirrors RangeColumn behavior: without explicit precision/scale
        the DECIMAL check defers to Spark's DecimalType(10, 0) default
        and is not enforced at plan time."""
        TableSpec(
            name="t",
            columns=[
                ColumnSpec(
                    name="c",
                    dtype=DataType.DECIMAL,
                    gen=SequenceColumn(start=1, step=1),
                )
            ],
            rows=10**6,  # exceeds DecimalType(10,0) max but plan-time check is off
        )


class TestExtrasForbid:
    """Every public plan model inherits from ``_StrictModel``, which sets
    ``extra="forbid"``.  A YAML plan with a typo'd key round-trips
    silently under the default ``extra="ignore"`` behaviour; the strict
    base catches typos at load time with a clear pointer to the
    offending key."""

    def test_columnspec_rejects_unknown_field(self):
        with pytest.raises(ValueError, match=r"[Ee]xtra"):
            ColumnSpec(name="x", gen=RangeColumn(), niull_fraction=0.1)  # type: ignore[call-arg]

    def test_tablespec_rejects_unknown_field(self):
        with pytest.raises(ValueError, match=r"[Ee]xtra"):
            TableSpec(  # type: ignore[call-arg]
                name="t",
                columns=[ColumnSpec(name="x", gen=RangeColumn())],
                rows=10,
                unknown_key="whoops",
            )

    def test_datagenplan_rejects_unknown_field(self):
        with pytest.raises(ValueError, match=r"[Ee]xtra"):
            DataGenPlan(  # type: ignore[call-arg]
                tables=[TableSpec(name="t", columns=[ColumnSpec(name="x", gen=RangeColumn())], rows=10)],
                typo_in_root="x",
            )


class TestStructColumnUniqueness:
    def test_duplicate_field_names_rejected(self):
        """StructColumn with two fields named ``x`` would build an
        invalid Spark StructType and ambiguate every downstream
        ``addr.x`` select.  Reject at plan time."""
        from dbldatagen.core.spec.schema import StructColumn

        with pytest.raises(ValueError, match="duplicate field names"):
            StructColumn(
                fields=[
                    ColumnSpec(name="x", gen=RangeColumn()),
                    ColumnSpec(name="x", gen=ValuesColumn(values=["a", "b"])),
                ]
            )


class TestLogNormalMeanBound:
    def test_mean_above_cap_rejected(self):
        """``math.exp(710)`` overflows the double range; the validator
        stops the plan well before Python raises at plan time."""
        with pytest.raises(ValueError, match=r"must be in \[-100, 100\]"):
            LogNormal(mean=150.0)

    def test_mean_below_cap_rejected(self):
        with pytest.raises(ValueError, match=r"must be in \[-100, 100\]"):
            LogNormal(mean=-200.0)

    def test_mean_at_bound_accepted(self):
        LogNormal(mean=100.0)
        LogNormal(mean=-100.0)


class TestRangeColumnBounds:
    def test_full_int64_domain_rejected(self):
        """``max - min + 1`` for the full int64 range overflows F.lit's
        signed-64 bound.  Reject at plan time instead of failing at
        query build with an opaque Py4J error."""
        with pytest.raises(ValueError, match=r"integer range size .* >= 2\*\*63"):
            RangeColumn(min=-(2**63), max=2**63 - 1)

    def test_range_just_under_cap_accepted(self):
        # (2**63 - 1) - 0 + 1 = 2**63 -- still over, must be < 2**63.
        RangeColumn(min=0, max=2**63 - 2)


class TestArrayMaxLengthZero:
    def test_max_length_zero_rejected(self):
        """Always-empty arrays produce ``array<nothing>`` which confuses
        downstream schema inference.  Omit the column or set
        ``max_length >= 1``."""
        with pytest.raises(ValueError, match=r"max_length must be >= 1"):
            ArrayColumn(element=RangeColumn(min=1, max=5), min_length=0, max_length=0)


class TestDateDtypeGuard:
    @pytest.mark.parametrize(
        ("bad_gen", "type_word"),
        [
            (RangeColumn(min=0, max=100), "integers"),
            (SequenceColumn(), "integers"),
            (PatternColumn(template="X-{digit:4}"), "strings"),
            (UUIDColumn(), "strings"),
            (FakerColumn(provider="date_of_birth"), "strings"),
        ],
    )
    def test_date_with_non_date_strategy_rejected(self, bad_gen, type_word):
        """Strategies whose output type is fixed and non-date can't
        honour ``dtype=DATE``; the dtype hint would be silently dropped
        and the column would carry integers or strings where the user
        asked for a date.  FakerColumn joins the list: the Faker pool
        pandas_udf hardcodes StringType and ``str(val)`` stringifies
        every pool entry, so even ``date_of_birth`` returns a string
        column regardless of declared dtype.
        """
        with pytest.raises(ValueError, match=f"not compatible.*{type_word}"):
            ColumnSpec(name="d", dtype=DataType.DATE, gen=bad_gen)

    def test_date_with_timestamp_column_ok(self):
        ColumnSpec(name="d", dtype=DataType.DATE, gen=TimestampColumn(start="2023-01-01", end="2023-12-31"))

    def test_faker_without_explicit_dtype_ok(self):
        """``dtype`` is allowed to be ``None`` (engine infers StringType
        from the Faker pool) or explicit ``STRING``.  Only a mismatch
        like ``dtype=DATE`` / ``INT`` etc. is rejected."""
        ColumnSpec(name="name", gen=FakerColumn(provider="name"))
        ColumnSpec(name="name", dtype=DataType.STRING, gen=FakerColumn(provider="name"))


class TestNullFractionGranularityBinding:
    """``_MIN_NULL_FRACTION`` in ``spec/schema.py`` must track
    ``NULL_PRECISION`` in ``engine/seed.py`` -- if they drift, the
    schema validator rejects fractions the engine would have accepted
    (or vice versa).  Layering forbids schema importing from engine,
    so the two are duplicated -- pin the correspondence with this
    test so either edit is caught in CI."""

    def test_constants_agree(self):
        from dbldatagen.core.engine.seed import NULL_PRECISION
        from dbldatagen.core.spec.schema import _MIN_NULL_FRACTION

        assert _MIN_NULL_FRACTION * NULL_PRECISION == 1.0, (
            f"_MIN_NULL_FRACTION ({_MIN_NULL_FRACTION}) and NULL_PRECISION "
            f"({NULL_PRECISION}) have drifted -- update one to match the other."
        )
