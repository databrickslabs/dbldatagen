"""Tests for dbldatagen.v1.dsl convenience constructors."""

from dbldatagen.v1.dsl import (
    decimal,
    expression,
    faker,
    fk,
    integer,
    pattern,
    pk_auto,
    pk_pattern,
    pk_uuid,
    text,
    timestamp,
)
from dbldatagen.v1.schema import (
    ColumnSpec,
    ConstantColumn,
    DataType,
    ExpressionColumn,
    FakerColumn,
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
        col = pk_auto()
        assert isinstance(col, ColumnSpec)

    def test_default_name(self):
        col = pk_auto()
        assert col.name == "id"

    def test_custom_name(self):
        col = pk_auto("user_id")
        assert col.name == "user_id"

    def test_dtype_is_long(self):
        col = pk_auto()
        assert col.dtype == DataType.LONG

    def test_strategy_is_sequence(self):
        col = pk_auto()
        assert isinstance(col.gen, SequenceColumn)
        assert col.gen.start == 1
        assert col.gen.step == 1


class TestPkUuid:
    def test_returns_column_spec(self):
        col = pk_uuid()
        assert isinstance(col, ColumnSpec)

    def test_default_name(self):
        col = pk_uuid()
        assert col.name == "id"

    def test_custom_name(self):
        col = pk_uuid("order_id")
        assert col.name == "order_id"

    def test_dtype_is_string(self):
        col = pk_uuid()
        assert col.dtype == DataType.STRING

    def test_strategy_is_uuid(self):
        col = pk_uuid()
        assert isinstance(col.gen, UUIDColumn)


class TestPkPattern:
    def test_returns_column_spec(self):
        col = pk_pattern("code", "X-{digit:4}")
        assert isinstance(col, ColumnSpec)

    def test_name_and_template(self):
        col = pk_pattern("customer_id", "CUST-{digit:8}")
        assert col.name == "customer_id"
        assert isinstance(col.gen, PatternColumn)
        assert col.gen.template == "CUST-{digit:8}"

    def test_dtype_is_string(self):
        col = pk_pattern("id", "ID-{seq}")
        assert col.dtype == DataType.STRING


# ---------------------------------------------------------------------------
# Foreign key helper
# ---------------------------------------------------------------------------


class TestFk:
    def test_returns_column_spec(self):
        col = fk("customer_id", "customers.customer_id")
        assert isinstance(col, ColumnSpec)

    def test_foreign_key_ref_set(self):
        col = fk("customer_id", "customers.customer_id")
        assert col.foreign_key is not None
        assert isinstance(col.foreign_key, ForeignKeyRef)
        assert col.foreign_key.ref == "customers.customer_id"

    def test_default_distribution_is_zipf(self):
        col = fk("cid", "c.id")
        assert isinstance(col.foreign_key.distribution, Zipf)
        assert col.foreign_key.distribution.exponent == 1.2

    def test_custom_distribution(self):
        col = fk("cid", "c.id", distribution=Zipf(exponent=2.0))
        assert col.foreign_key.distribution.exponent == 2.0

    def test_placeholder_gen_is_constant(self):
        col = fk("cid", "c.id")
        assert isinstance(col.gen, ConstantColumn)
        assert col.gen.value is None

    def test_nullable_passthrough(self):
        col = fk("cid", "c.id", nullable=True)
        assert col.foreign_key.nullable is True

    def test_null_fraction_passthrough(self):
        col = fk("cid", "c.id", null_fraction=0.05)
        assert col.foreign_key.null_fraction == 0.05


# ---------------------------------------------------------------------------
# Common column shorthands
# ---------------------------------------------------------------------------


class TestInteger:
    def test_returns_column_spec(self):
        col = integer("age")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_int(self):
        col = integer("age")
        assert col.dtype == DataType.INT

    def test_strategy_is_range(self):
        col = integer("age", min=18, max=90)
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 18
        assert col.gen.max == 90

    def test_defaults(self):
        col = integer("x")
        assert col.gen.min == 0
        assert col.gen.max == 100


class TestDecimal:
    def test_returns_column_spec(self):
        col = decimal("price")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_double(self):
        col = decimal("price")
        assert col.dtype == DataType.DOUBLE

    def test_strategy_is_range(self):
        col = decimal("price", min=1.99, max=999.99)
        assert isinstance(col.gen, RangeColumn)
        assert col.gen.min == 1.99
        assert col.gen.max == 999.99

    def test_defaults(self):
        col = decimal("x")
        assert col.gen.min == 0.0
        assert col.gen.max == 1000.0


class TestText:
    def test_returns_column_spec(self):
        col = text("tier", values=["a", "b"])
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_string(self):
        col = text("tier", values=["a"])
        assert col.dtype == DataType.STRING

    def test_strategy_is_values(self):
        col = text("tier", values=["free", "pro"])
        assert isinstance(col.gen, ValuesColumn)
        assert col.gen.values == ["free", "pro"]


class TestFaker:
    def test_returns_column_spec(self):
        col = faker("email", "email")
        assert isinstance(col, ColumnSpec)

    def test_dtype_default_string(self):
        col = faker("email", "email")
        assert col.dtype == DataType.STRING

    def test_custom_dtype(self):
        col = faker("dob", "date_of_birth", dtype=DataType.DATE)
        assert col.dtype == DataType.DATE

    def test_strategy_is_faker(self):
        col = faker("name", "name")
        assert isinstance(col.gen, FakerColumn)
        assert col.gen.provider == "name"

    def test_locale(self):
        col = faker("name", "name", locale="de_DE")
        assert col.gen.locale == "de_DE"

    def test_kwargs_passthrough(self):
        col = faker("dob", "date_of_birth", minimum_age=18, maximum_age=80)
        assert col.gen.kwargs == {"minimum_age": 18, "maximum_age": 80}


class TestTimestamp:
    def test_returns_column_spec(self):
        col = timestamp("created_at")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_timestamp(self):
        col = timestamp("created_at")
        assert col.dtype == DataType.TIMESTAMP

    def test_strategy_is_timestamp(self):
        col = timestamp("created_at", start="2023-01-01", end="2024-12-31")
        assert isinstance(col.gen, TimestampColumn)
        assert col.gen.start == "2023-01-01"
        assert col.gen.end == "2024-12-31"

    def test_defaults(self):
        col = timestamp("ts")
        assert col.gen.start == "2020-01-01"
        assert col.gen.end == "2025-12-31"


class TestPattern:
    def test_returns_column_spec(self):
        col = pattern("code", "X-{digit:4}")
        assert isinstance(col, ColumnSpec)

    def test_dtype_is_string(self):
        col = pattern("code", "X-{digit:4}")
        assert col.dtype == DataType.STRING

    def test_strategy_is_pattern(self):
        col = pattern("code", "ORD-{digit:4}-{alpha:3}")
        assert isinstance(col.gen, PatternColumn)
        assert col.gen.template == "ORD-{digit:4}-{alpha:3}"


class TestExpression:
    def test_returns_column_spec(self):
        col = expression("total", "a * b")
        assert isinstance(col, ColumnSpec)

    def test_strategy_is_expression(self):
        col = expression("total", "quantity * unit_price")
        assert isinstance(col.gen, ExpressionColumn)
        assert col.gen.expr == "quantity * unit_price"

    def test_dtype_default_none(self):
        col = expression("total", "a + b")
        assert col.dtype is None

    def test_custom_dtype(self):
        col = expression("total", "a + b", dtype=DataType.DOUBLE)
        assert col.dtype == DataType.DOUBLE
