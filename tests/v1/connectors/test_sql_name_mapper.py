"""Exhaustive tests for dbldatagen.v1.connectors.sql.name_mapper."""

from dbldatagen.v1.connectors.sql.name_mapper import map_column_name
from dbldatagen.v1.schema import (
    DataType,
    FakerColumn,
    PatternColumn,
    RangeColumn,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
)


# ---------------------------------------------------------------------------
# Contact info (Faker)
# ---------------------------------------------------------------------------


class TestContactFaker:
    def test_email(self):
        spec = map_column_name("email")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "email"

    def test_email_suffix(self):
        spec = map_column_name("user_email")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "email"

    def test_name(self):
        spec = map_column_name("name")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "name"

    def test_name_suffix(self):
        spec = map_column_name("customer_name")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "name"

    def test_full_name(self):
        spec = map_column_name("full_name")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "name"

    def test_first_name(self):
        spec = map_column_name("first_name")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "first_name"

    def test_last_name(self):
        spec = map_column_name("last_name")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "last_name"

    def test_phone(self):
        spec = map_column_name("phone")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "phone_number"

    def test_mobile(self):
        spec = map_column_name("mobile")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "phone_number"

    def test_address(self):
        spec = map_column_name("address")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "street_address"

    def test_street(self):
        spec = map_column_name("street_address")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "street_address"

    def test_city(self):
        spec = map_column_name("city")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "city"

    def test_state(self):
        spec = map_column_name("state")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "state"

    def test_country(self):
        spec = map_column_name("country")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "country"

    def test_zip(self):
        spec = map_column_name("zip")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "zipcode"

    def test_postal_code(self):
        spec = map_column_name("postal_code")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "zipcode"

    def test_company(self):
        spec = map_column_name("company")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "company"

    def test_organization(self):
        spec = map_column_name("organization")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "company"

    def test_url(self):
        spec = map_column_name("url")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "url"

    def test_website(self):
        spec = map_column_name("website")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "url"

    def test_ip_address(self):
        spec = map_column_name("ip_address")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "ipv4"

    def test_user_agent(self):
        spec = map_column_name("user_agent")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "user_agent"

    def test_username(self):
        spec = map_column_name("username")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "user_name"


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------


class TestUUID:
    def test_uuid(self):
        spec = map_column_name("uuid")
        assert isinstance(spec.gen, UUIDColumn)

    def test_guid(self):
        spec = map_column_name("guid")
        assert isinstance(spec.gen, UUIDColumn)


# ---------------------------------------------------------------------------
# Temporal
# ---------------------------------------------------------------------------


class TestTemporal:
    def test_at_suffix(self):
        spec = map_column_name("created_at")
        assert isinstance(spec.gen, TimestampColumn)
        assert spec.dtype == DataType.TIMESTAMP

    def test_date_suffix(self):
        spec = map_column_name("order_date")
        assert isinstance(spec.gen, TimestampColumn)

    def test_time_suffix(self):
        spec = map_column_name("login_time")
        assert isinstance(spec.gen, TimestampColumn)

    def test_created_prefix(self):
        spec = map_column_name("created")
        assert isinstance(spec.gen, TimestampColumn)

    def test_updated_prefix(self):
        spec = map_column_name("updated")
        assert isinstance(spec.gen, TimestampColumn)

    def test_timestamp(self):
        spec = map_column_name("timestamp")
        assert isinstance(spec.gen, TimestampColumn)

    def test_date(self):
        spec = map_column_name("date")
        assert isinstance(spec.gen, TimestampColumn)

    def test_on_suffix(self):
        spec = map_column_name("completed_on")
        assert isinstance(spec.gen, TimestampColumn)


# ---------------------------------------------------------------------------
# Money / decimal
# ---------------------------------------------------------------------------


class TestMoney:
    def test_price(self):
        spec = map_column_name("price")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.dtype == DataType.DOUBLE

    def test_cost(self):
        spec = map_column_name("cost")
        assert isinstance(spec.gen, RangeColumn)

    def test_amount(self):
        spec = map_column_name("amount")
        assert isinstance(spec.gen, RangeColumn)

    def test_total(self):
        spec = map_column_name("total")
        assert isinstance(spec.gen, RangeColumn)

    def test_revenue(self):
        spec = map_column_name("revenue")
        assert isinstance(spec.gen, RangeColumn)

    def test_salary(self):
        spec = map_column_name("salary")
        assert isinstance(spec.gen, RangeColumn)

    def test_tax(self):
        spec = map_column_name("tax")
        assert isinstance(spec.gen, RangeColumn)

    def test_discount(self):
        spec = map_column_name("discount")
        assert isinstance(spec.gen, RangeColumn)

    def test_unit_price_contains_price(self):
        spec = map_column_name("unit_price")
        assert isinstance(spec.gen, RangeColumn)


# ---------------------------------------------------------------------------
# Counts / integers
# ---------------------------------------------------------------------------


class TestCounts:
    def test_quantity(self):
        spec = map_column_name("quantity")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.dtype == DataType.INT

    def test_qty(self):
        spec = map_column_name("qty")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.dtype == DataType.INT

    def test_age(self):
        spec = map_column_name("age")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == 18
        assert spec.gen.max == 90

    def test_year(self):
        spec = map_column_name("year")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == 2000

    def test_count_suffix(self):
        spec = map_column_name("order_count")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.dtype == DataType.INT

    def test_num_prefix(self):
        spec = map_column_name("num_items")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.dtype == DataType.INT


# ---------------------------------------------------------------------------
# Scores / ratios
# ---------------------------------------------------------------------------


class TestScores:
    def test_rating(self):
        spec = map_column_name("rating")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == 1.0
        assert spec.gen.max == 5.0

    def test_score(self):
        spec = map_column_name("score")
        assert isinstance(spec.gen, RangeColumn)

    def test_percent(self):
        spec = map_column_name("percent")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == 0.0
        assert spec.gen.max == 1.0

    def test_ratio(self):
        spec = map_column_name("ratio")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.max == 1.0


# ---------------------------------------------------------------------------
# Geo
# ---------------------------------------------------------------------------


class TestGeo:
    def test_latitude(self):
        spec = map_column_name("latitude")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == -90.0
        assert spec.gen.max == 90.0

    def test_lat(self):
        spec = map_column_name("lat")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == -90.0

    def test_longitude(self):
        spec = map_column_name("longitude")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == -180.0
        assert spec.gen.max == 180.0

    def test_lng(self):
        spec = map_column_name("lng")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.gen.min == -180.0

    def test_lon(self):
        spec = map_column_name("lon")
        assert isinstance(spec.gen, RangeColumn)


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------


class TestDimensions:
    def test_weight(self):
        spec = map_column_name("weight")
        assert isinstance(spec.gen, RangeColumn)
        assert spec.dtype == DataType.DOUBLE

    def test_height(self):
        spec = map_column_name("height")
        assert isinstance(spec.gen, RangeColumn)

    def test_length(self):
        spec = map_column_name("length")
        assert isinstance(spec.gen, RangeColumn)

    def test_width(self):
        spec = map_column_name("width")
        assert isinstance(spec.gen, RangeColumn)


# ---------------------------------------------------------------------------
# Boolean
# ---------------------------------------------------------------------------


class TestBoolean:
    def test_is_prefix(self):
        spec = map_column_name("is_active")
        assert isinstance(spec.gen, ValuesColumn)
        assert spec.dtype == DataType.BOOLEAN
        assert set(spec.gen.values) == {True, False}

    def test_has_prefix(self):
        spec = map_column_name("has_email")
        assert isinstance(spec.gen, ValuesColumn)
        assert spec.dtype == DataType.BOOLEAN

    def test_flag_suffix(self):
        spec = map_column_name("deleted_flag")
        assert isinstance(spec.gen, ValuesColumn)
        assert spec.dtype == DataType.BOOLEAN

    def test_active(self):
        spec = map_column_name("active")
        assert isinstance(spec.gen, ValuesColumn)
        assert spec.dtype == DataType.BOOLEAN

    def test_enabled(self):
        spec = map_column_name("enabled")
        assert isinstance(spec.gen, ValuesColumn)
        assert spec.dtype == DataType.BOOLEAN

    def test_verified(self):
        spec = map_column_name("verified")
        assert isinstance(spec.gen, ValuesColumn)
        assert spec.dtype == DataType.BOOLEAN

    def test_deleted(self):
        spec = map_column_name("deleted")
        assert isinstance(spec.gen, ValuesColumn)
        assert spec.dtype == DataType.BOOLEAN


# ---------------------------------------------------------------------------
# Enum-like
# ---------------------------------------------------------------------------


class TestEnumLike:
    def test_status(self):
        spec = map_column_name("status")
        assert isinstance(spec.gen, ValuesColumn)
        assert "active" in spec.gen.values

    def test_status_suffix(self):
        spec = map_column_name("order_status")
        # order_status matches the ^order_status$ rule specifically
        assert isinstance(spec.gen, ValuesColumn)

    def test_type(self):
        spec = map_column_name("type")
        assert isinstance(spec.gen, ValuesColumn)

    def test_category(self):
        spec = map_column_name("category")
        assert isinstance(spec.gen, ValuesColumn)

    def test_tier(self):
        spec = map_column_name("tier")
        assert isinstance(spec.gen, ValuesColumn)
        assert "free" in spec.gen.values

    def test_gender(self):
        spec = map_column_name("gender")
        assert isinstance(spec.gen, ValuesColumn)
        assert "M" in spec.gen.values

    def test_currency(self):
        spec = map_column_name("currency")
        assert isinstance(spec.gen, ValuesColumn)
        assert "USD" in spec.gen.values

    def test_payment_method(self):
        spec = map_column_name("payment_method")
        assert isinstance(spec.gen, ValuesColumn)
        assert "credit_card" in spec.gen.values


# ---------------------------------------------------------------------------
# Text / description (Faker)
# ---------------------------------------------------------------------------


class TestText:
    def test_description(self):
        spec = map_column_name("description")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "paragraph"

    def test_notes(self):
        spec = map_column_name("notes")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "paragraph"

    def test_comment(self):
        spec = map_column_name("comment")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "paragraph"

    def test_bio(self):
        spec = map_column_name("bio")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "paragraph"

    def test_title(self):
        spec = map_column_name("title")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "sentence"

    def test_subject(self):
        spec = map_column_name("subject")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "sentence"

    def test_headline(self):
        spec = map_column_name("headline")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "sentence"


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------


class TestFallback:
    def test_unknown_gets_pattern(self):
        spec = map_column_name("xyzzy_code")
        assert isinstance(spec.gen, PatternColumn)
        assert spec.dtype == DataType.STRING
        assert "xyzzy_code" in spec.gen.template

    def test_random_name(self):
        spec = map_column_name("foobar")
        assert isinstance(spec.gen, PatternColumn)

    def test_name_preserved(self):
        spec = map_column_name("my_column")
        assert spec.name == "my_column"


# ---------------------------------------------------------------------------
# Case insensitivity
# ---------------------------------------------------------------------------


class TestCaseInsensitivity:
    def test_uppercase_email(self):
        spec = map_column_name("EMAIL")
        assert isinstance(spec.gen, FakerColumn)
        assert spec.gen.provider == "email"

    def test_mixed_case_price(self):
        spec = map_column_name("Unit_Price")
        assert isinstance(spec.gen, RangeColumn)

    def test_uppercase_status(self):
        spec = map_column_name("STATUS")
        assert isinstance(spec.gen, ValuesColumn)
