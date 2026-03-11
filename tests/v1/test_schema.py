"""Tests for dbldatagen.v1.schema Pydantic models."""

from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataGenPlan,
    DataType,
    Exponential,
    ExpressionColumn,
    FakerColumn,
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

    def test_timestamp_column(self):
        s = TimestampColumn(start="2023-01-01", end="2024-12-31")
        assert s.strategy == "timestamp"
        assert s.start == "2023-01-01"
        assert s.end == "2024-12-31"

    def test_timestamp_column_defaults(self):
        s = TimestampColumn()
        assert s.start == "2020-01-01"
        assert s.end == "2025-12-31"

    def test_timestamp_column_with_distribution(self):
        s = TimestampColumn(distribution=Normal(mean=0.5, stddev=0.1))
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
        col = ColumnSpec(name="ts", gen=TimestampColumn())
        assert isinstance(col.gen, TimestampColumn)

    def test_column_with_values(self):
        col = ColumnSpec(name="tier", gen=ValuesColumn(values=["a", "b"]))
        assert isinstance(col.gen, ValuesColumn)

    def test_column_with_constant(self):
        col = ColumnSpec(name="version", gen=ConstantColumn(value=1))
        assert isinstance(col.gen, ConstantColumn)

    def test_null_fraction_auto_sets_nullable(self):
        col = ColumnSpec(name="x", gen=RangeColumn(), null_fraction=0.1)
        assert col.nullable is True
        assert col.null_fraction == 0.1

    def test_zero_null_fraction_keeps_nullable_false(self):
        col = ColumnSpec(name="x", gen=RangeColumn(), null_fraction=0.0)
        assert col.nullable is False

    def test_explicit_nullable_with_zero_fraction(self):
        col = ColumnSpec(name="x", gen=RangeColumn(), nullable=True, null_fraction=0.0)
        assert col.nullable is True

    def test_column_with_foreign_key(self):
        fk_ref = ForeignKeyRef(ref="customers.id")
        col = ColumnSpec(name="customer_id", gen=ConstantColumn(value=None), foreign_key=fk_ref)
        assert col.foreign_key is not None
        assert col.foreign_key.ref == "customers.id"

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


# ---------------------------------------------------------------------------
# ForeignKeyRef
# ---------------------------------------------------------------------------


class TestForeignKeyRef:
    def test_string_ref(self):
        fk = ForeignKeyRef(ref="orders.order_id")
        assert fk.ref == "orders.order_id"
        assert fk.cardinality is None
        assert isinstance(fk.distribution, Uniform)
        assert fk.nullable is False
        assert fk.null_fraction == 0.0

    def test_with_int_cardinality(self):
        fk = ForeignKeyRef(ref="t.c", cardinality=5)
        assert fk.cardinality == 5

    def test_with_tuple_cardinality(self):
        fk = ForeignKeyRef(ref="t.c", cardinality=(1, 10))
        assert fk.cardinality == (1, 10)

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
                            gen=ConstantColumn(value=None),
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
            TimestampColumn(),
            ConstantColumn(value=42),
        ]
        for i, strat in enumerate(strategies):
            col = ColumnSpec(name=f"col_{i}", gen=strat)  # type: ignore[arg-type]
            json_str = col.model_dump_json()
            restored = ColumnSpec.model_validate_json(json_str)
            assert restored == col, f"Round-trip failed for {type(strat).__name__}"

    def test_all_distributions_round_trip(self):
        """Every distribution type survives JSON serialization inside a RangeColumn."""
        distributions = [
            Uniform(),
            Normal(mean=5, stddev=2),
            LogNormal(mean=1, stddev=0.5),
            Zipf(exponent=2.0),
            Exponential(rate=0.3),
            WeightedValues(weights={"x": 0.6, "y": 0.4}),
        ]
        for dist in distributions:
            col = ColumnSpec(name="x", gen=RangeColumn(distribution=dist))  # type: ignore[arg-type]
            json_str = col.model_dump_json()
            restored = ColumnSpec.model_validate_json(json_str)
            assert restored == col, f"Round-trip failed for {type(dist).__name__}"
