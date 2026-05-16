"""Tests for dbldatagen.core.spec.dsl convenience constructors."""

import pytest

from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import (
    ColumnSpec,
    ConstantColumn,
    DataType,
    ExpressionColumn,
    FakerColumn,
    ForeignKeyColumn,
    ForeignKeyRef,
    PatternColumn,
    RangeColumn,
    SequenceColumn,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
    Zipf,
)


# ---------------------------------------------------------------------------
# Primary key helpers
# ---------------------------------------------------------------------------


class TestPkAuto:
    def test_returns_column_spec(self):
        col = datagendg.pk_auto()
        assert isinstance(col, ColumnSpec)

    def test_default_name(self):
        col = datagendg.pk_auto()
        assert col.name == "id"

    def test_custom_name(self):
        col = datagendg.pk_auto("user_id")
        assert col.name == "user_id"

    def test_dtype_is_long(self):
        col = datagendg.pk_auto()
        assert col.dtype == DataType.LONG

    def test_strategy_is_sequence(self):
        col = datagendg.pk_auto()
        assert isinstance(col.gen, SequenceColumn)
        assert col.gen.start == 1
        assert col.gen.step == 1


class TestPkUuid:
    def test_returns_column_spec(self):
        col = datagendg.pk_uuid()
        assert isinstance(col, ColumnSpec)

    def test_default_name(self):
        col = datagendg.pk_uuid()
        assert col.name == "id"

    def test_custom_name(self):
        col = datagendg.pk_uuid("order_id")
        assert col.name == "order_id"

    def test_dtype_is_string(self):
        col = datagendg.pk_uuid()
        assert col.dtype == DataType.STRING

    def test_strategy_is_uuid(self):
        col = datagendg.pk_uuid()
        assert isinstance(col.gen, UUIDColumn)


class TestPkPattern:
    def test_returns_column_spec(self):
        col = datagendg.pk_pattern("code", "X-{digit:4}")
        assert isinstance(col, ColumnSpec)

    def test_name_and_template(self):
        col = datagendg.pk_pattern("customer_id", "CUST-{digit:8}")
        assert col.name == "customer_id"
        assert isinstance(col.gen, PatternColumn)
        assert col.gen.template == "CUST-{digit:8}"

    def test_dtype_is_string(self):
        col = datagendg.pk_pattern("id", "ID-{seq}")
        assert col.dtype == DataType.STRING


# ---------------------------------------------------------------------------
# Foreign key helper
# ---------------------------------------------------------------------------


class TestFk:
    def test_returns_column_spec(self):
        col = datagendg.fk("customer_id", "customers.customer_id")
        assert isinstance(col, ColumnSpec)

    def test_foreign_key_ref_set(self):
        col = datagendg.fk("customer_id", "customers.customer_id")
        assert col.foreign_key is not None
        assert isinstance(col.foreign_key, ForeignKeyRef)
        assert col.foreign_key.ref == "customers.customer_id"

    def test_default_distribution_is_zipf(self):
        col = datagendg.fk("cid", "c.id")
        assert col.foreign_key is not None
        assert isinstance(col.foreign_key.distribution, Zipf)
        assert col.foreign_key.distribution.exponent == 1.2

    def test_custom_distribution(self):
        col = datagendg.fk("cid", "c.id", distribution=Zipf(exponent=2.0))
        assert col.foreign_key is not None
        assert isinstance(col.foreign_key.distribution, Zipf)
        assert col.foreign_key.distribution.exponent == 2.0

    def test_gen_is_foreign_key_column(self):
        col = datagendg.fk("cid", "c.id")
        assert isinstance(col.gen, ForeignKeyColumn)

    def test_nullable_passthrough(self):
        col = datagendg.fk("cid", "c.id", nullable=True)
        assert col.foreign_key is not None
        assert col.foreign_key.nullable is True

    def test_null_fraction_passthrough(self):
        col = datagendg.fk("cid", "c.id", null_fraction=0.05)
        assert col.foreign_key is not None
        assert col.foreign_key.null_fraction == 0.05


# ---------------------------------------------------------------------------
# Common column shorthands
# ---------------------------------------------------------------------------


class TestInteger:
    def test_returns_column_spec(self):
        col = datagendg.integer("age")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_int(self):
        col = datagendg.integer("age")
        assert col.dtype == DataType.INT

    def test_strategy_is_range(self):
        col = datagendg.integer("age", min=18, max=90)
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 18
        assert col.gen.max == 90

    def test_defaults(self):
        col = datagendg.integer("x")
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 0
        assert col.gen.max == 100


class TestDecimal:
    def test_returns_column_spec(self):
        col = datagendg.decimal("price")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_decimal(self):
        col = datagendg.decimal("price")
        assert col.dtype == DataType.DECIMAL

    def test_strategy_is_range(self):
        col = datagendg.decimal("price", min=1.99, max=999.99)
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 1.99
        assert col.gen.max == 999.99

    def test_defaults(self):
        col = datagendg.decimal("x")
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 0.0
        assert col.gen.max == 1000.0

    def test_precision_scale_default_none(self):
        """Unset precision/scale stay None so the engine falls back to Spark's DecimalType() default of (10, 0)."""
        col = datagendg.decimal("price")
        assert col.precision is None
        assert col.scale is None

    def test_precision_scale_passed_through(self):
        col = datagendg.decimal("rate", precision=10, scale=4)
        assert col.precision == 10
        assert col.scale == 4


class TestText:
    def test_returns_column_spec(self):
        col = datagendg.text("tier", values=["a", "b"])
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_string(self):
        col = datagendg.text("tier", values=["a"])
        assert col.dtype == DataType.STRING

    def test_strategy_is_values(self):
        col = datagendg.text("tier", values=["free", "pro"])
        assert isinstance(col.gen, ValuesColumn)
        assert col.gen.values == ["free", "pro"]


class TestFaker:
    def test_returns_column_spec(self):
        col = datagendg.faker("email", "email")
        assert isinstance(col, ColumnSpec)

    def test_dtype_default_string(self):
        col = datagendg.faker("email", "email")
        assert col.dtype == DataType.STRING

    def test_custom_dtype(self):
        # Faker emits StringType regardless of provider (the pool
        # stringifies every value), so STRING is the only dtype that
        # survives validation -- anything else would lie about the
        # column's runtime type.
        col = datagendg.faker("name", "name", dtype=DataType.STRING)
        assert col.dtype == DataType.STRING

    def test_strategy_is_faker(self):
        col = datagendg.faker("name", "name")
        assert isinstance(col.gen, FakerColumn)
        assert col.gen.provider == "name"

    def test_locale(self):
        col = datagendg.faker("name", "name", locale="de_DE")
        assert isinstance(col.gen, FakerColumn)
        assert col.gen.locale == "de_DE"

    def test_kwargs_passthrough(self):
        col = datagendg.faker("dob", "date_of_birth", minimum_age=18, maximum_age=80)
        assert isinstance(col.gen, FakerColumn)
        assert col.gen.kwargs == {"minimum_age": 18, "maximum_age": 80}


class TestTimestamp:
    def test_returns_column_spec(self):
        col = datagendg.timestamp("created_at", start="2020-01-01", end="2025-12-31")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_timestamp(self):
        col = datagendg.timestamp("created_at", start="2020-01-01", end="2025-12-31")
        assert col.dtype == DataType.TIMESTAMP

    def test_strategy_is_timestamp(self):
        col = datagendg.timestamp("created_at", start="2023-01-01", end="2024-12-31")
        assert isinstance(col.gen, TimestampColumn)
        assert col.gen.start == "2023-01-01"
        assert col.gen.end == "2024-12-31"

    def test_start_and_end_required(self):
        """Past defaults (``"2020-01-01"`` / ``"2025-12-31"``) were
        vestigial demo values; the helper now requires explicit
        bounds so the call site documents the intended range."""
        with pytest.raises(TypeError, match="required"):
            datagendg.timestamp("ts")  # type: ignore[call-arg]


class TestPattern:
    def test_returns_column_spec(self):
        col = datagendg.pattern("code", "X-{digit:4}")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_string(self):
        col = datagendg.pattern("code", "X-{digit:4}")
        assert col.dtype == DataType.STRING

    def test_strategy_is_pattern(self):
        col = datagendg.pattern("code", "ORD-{digit:4}-{alpha:3}")
        assert isinstance(col.gen, PatternColumn)
        assert col.gen.template == "ORD-{digit:4}-{alpha:3}"


class TestExpression:
    def test_returns_column_spec(self):
        col = datagendg.expression("total", "a * b")
        assert isinstance(col, ColumnSpec)

    def test_strategy_is_expression(self):
        col = datagendg.expression("total", "quantity * unit_price")
        assert isinstance(col.gen, ExpressionColumn)
        assert col.gen.expr == "quantity * unit_price"

    def test_dtype_default_none(self):
        col = datagendg.expression("total", "a + b")
        assert col.dtype is None

    def test_custom_dtype(self):
        col = datagendg.expression("total", "a + b", dtype=DataType.DOUBLE)
        assert col.dtype == DataType.DOUBLE


class TestConstant:
    def test_returns_column_spec(self):
        col = datagendg.constant("env", "prod")
        assert isinstance(col, ColumnSpec)

    def test_strategy_is_constant_and_value_preserved(self):
        col = datagendg.constant("version", 42)
        assert isinstance(col.gen, ConstantColumn)
        assert col.gen.value == 42

    def test_dtype_default_none(self):
        col = datagendg.constant("flag", True)
        assert col.dtype is None

    def test_explicit_dtype(self):
        col = datagendg.constant("score", 0, dtype=DataType.LONG)
        assert col.dtype == DataType.LONG
