"""Exhaustive tests for dbldatagen.v1.connectors.sql.inference."""

from dbldatagen.v1.connectors.sql.inference import (
    detect_foreign_keys,
    detect_pk,
    infer_column_type,
    infer_schema,
    infer_type_from_name,
)
from dbldatagen.v1.connectors.sql.parser import (
    ExtractedColumn,
    ExtractedJoin,
    ExtractedSchema,
    ExtractedTable,
)
from dbldatagen.v1.schema import DataType

# ---------------------------------------------------------------------------
# infer_type_from_name
# ---------------------------------------------------------------------------


class TestInferTypeFromName:
    def test_id_column(self):
        dtype, conf = infer_type_from_name("id")
        assert dtype == DataType.LONG
        assert conf >= 0.7

    def test_suffix_id(self):
        dtype, _ = infer_type_from_name("customer_id")
        assert dtype == DataType.LONG

    def test_boolean_is_prefix(self):
        dtype, conf = infer_type_from_name("is_active")
        assert dtype == DataType.BOOLEAN
        assert conf >= 0.8

    def test_boolean_has_prefix(self):
        dtype, _ = infer_type_from_name("has_subscription")
        assert dtype == DataType.BOOLEAN

    def test_boolean_flag_suffix(self):
        dtype, _ = infer_type_from_name("email_flag")
        assert dtype == DataType.BOOLEAN

    def test_boolean_active(self):
        dtype, _ = infer_type_from_name("active")
        assert dtype == DataType.BOOLEAN

    def test_boolean_verified(self):
        dtype, _ = infer_type_from_name("verified")
        assert dtype == DataType.BOOLEAN

    def test_boolean_deleted(self):
        dtype, _ = infer_type_from_name("deleted")
        assert dtype == DataType.BOOLEAN

    def test_timestamp_at_suffix(self):
        dtype, _ = infer_type_from_name("created_at")
        assert dtype == DataType.TIMESTAMP

    def test_timestamp_date_suffix(self):
        dtype, _ = infer_type_from_name("order_date")
        assert dtype == DataType.TIMESTAMP

    def test_timestamp_created(self):
        dtype, _ = infer_type_from_name("created")
        assert dtype == DataType.TIMESTAMP

    def test_timestamp_updated(self):
        dtype, _ = infer_type_from_name("updated")
        assert dtype == DataType.TIMESTAMP

    def test_timestamp_date(self):
        dtype, _ = infer_type_from_name("date")
        assert dtype == DataType.TIMESTAMP

    def test_money_price(self):
        dtype, _ = infer_type_from_name("price")
        assert dtype == DataType.DOUBLE

    def test_money_amount(self):
        dtype, _ = infer_type_from_name("amount")
        assert dtype == DataType.DOUBLE

    def test_money_salary(self):
        dtype, _ = infer_type_from_name("salary")
        assert dtype == DataType.DOUBLE

    def test_money_total(self):
        dtype, _ = infer_type_from_name("total")
        assert dtype == DataType.DOUBLE

    def test_money_balance(self):
        dtype, _ = infer_type_from_name("balance")
        assert dtype == DataType.DOUBLE

    def test_count_quantity(self):
        dtype, _ = infer_type_from_name("quantity")
        assert dtype == DataType.INT

    def test_count_age(self):
        dtype, _ = infer_type_from_name("age")
        assert dtype == DataType.INT

    def test_count_year(self):
        dtype, _ = infer_type_from_name("year")
        assert dtype == DataType.INT

    def test_count_num_prefix(self):
        dtype, _ = infer_type_from_name("num_items")
        assert dtype == DataType.INT

    def test_count_suffix(self):
        dtype, _ = infer_type_from_name("order_count")
        assert dtype == DataType.INT

    def test_score(self):
        dtype, _ = infer_type_from_name("rating")
        assert dtype == DataType.DOUBLE

    def test_latitude(self):
        dtype, _ = infer_type_from_name("latitude")
        assert dtype == DataType.DOUBLE

    def test_lat(self):
        dtype, _ = infer_type_from_name("lat")
        assert dtype == DataType.DOUBLE

    def test_longitude(self):
        dtype, _ = infer_type_from_name("longitude")
        assert dtype == DataType.DOUBLE

    def test_unknown_defaults_string(self):
        dtype, conf = infer_type_from_name("foobar")
        assert dtype == DataType.STRING
        assert conf <= 0.3

    def test_case_insensitive(self):
        dtype, _ = infer_type_from_name("IS_ACTIVE")
        assert dtype == DataType.BOOLEAN


# ---------------------------------------------------------------------------
# infer_column_type  (multi-signal)
# ---------------------------------------------------------------------------


class TestInferColumnType:
    def test_cast_wins(self):
        col = ExtractedColumn(name="amount", cast_type="DECIMAL(10,2)")
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.DOUBLE
        assert conf == 0.95

    def test_cast_int(self):
        col = ExtractedColumn(name="x", cast_type="INTEGER")
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.INT
        assert conf == 0.95

    def test_cast_bigint(self):
        col = ExtractedColumn(name="x", cast_type="BIGINT")
        dtype, _ = infer_column_type(col)
        assert dtype == DataType.LONG

    def test_cast_boolean(self):
        col = ExtractedColumn(name="x", cast_type="BOOLEAN")
        dtype, _ = infer_column_type(col)
        assert dtype == DataType.BOOLEAN

    def test_cast_timestamp(self):
        col = ExtractedColumn(name="x", cast_type="TIMESTAMP")
        dtype, _ = infer_column_type(col)
        assert dtype == DataType.TIMESTAMP

    def test_cast_varchar(self):
        col = ExtractedColumn(name="x", cast_type="VARCHAR(255)")
        dtype, _ = infer_column_type(col)
        assert dtype == DataType.STRING

    def test_literal_int(self):
        col = ExtractedColumn(name="x", compared_to_literal=42)
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.LONG
        assert conf == 0.8

    def test_literal_float(self):
        col = ExtractedColumn(name="x", compared_to_literal=3.14)
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.DOUBLE
        assert conf == 0.8

    def test_literal_string(self):
        col = ExtractedColumn(name="x", compared_to_literal="active")
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.STRING
        assert conf == 0.8

    def test_aggregate_context(self):
        col = ExtractedColumn(name="x", aggregated=True)
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.DOUBLE
        assert conf == 0.6

    def test_name_fallback(self):
        col = ExtractedColumn(name="customer_id")
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.LONG
        assert conf >= 0.7

    def test_cast_overrides_name(self):
        """CAST takes priority over name heuristic."""
        col = ExtractedColumn(name="is_active", cast_type="INTEGER")
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.INT
        assert conf == 0.95

    def test_literal_overrides_name(self):
        """Literal comparison takes priority over name."""
        col = ExtractedColumn(name="price", compared_to_literal="expensive")
        dtype, conf = infer_column_type(col)
        assert dtype == DataType.STRING
        assert conf == 0.8


# ---------------------------------------------------------------------------
# detect_pk
# ---------------------------------------------------------------------------


class TestDetectPK:
    def test_id_column(self):
        assert detect_pk("customers", ["id", "name", "email"]) == "id"

    def test_table_name_id(self):
        assert detect_pk("customers", ["customer_id", "name"]) == "customer_id"

    def test_singular_table_name_id(self):
        assert detect_pk("orders", ["order_id", "amount"]) == "order_id"

    def test_first_id_column(self):
        assert detect_pk("data", ["row_id", "value"]) == "row_id"

    def test_fallback_synth_id(self):
        assert detect_pk("products", ["name", "price"]) == "_synth_id"

    def test_id_takes_priority_over_table_id(self):
        result = detect_pk("customers", ["id", "customer_id", "name"])
        assert result == "id"

    def test_table_id_takes_priority_over_other_id(self):
        result = detect_pk("customers", ["customer_id", "order_id", "name"])
        assert result == "customer_id"

    def test_case_insensitive(self):
        result = detect_pk("customers", ["ID", "name"])
        assert result == "ID"

    def test_empty_columns(self):
        assert detect_pk("customers", []) == "_synth_id"


# ---------------------------------------------------------------------------
# detect_foreign_keys
# ---------------------------------------------------------------------------


class TestDetectForeignKeys:
    def test_simple_fk(self):
        joins = [ExtractedJoin("orders", "customer_id", "customers", "id", "inner")]
        pk_columns = {"orders": "id", "customers": "id"}
        fks = detect_foreign_keys(joins, pk_columns)
        assert len(fks) == 1
        child_t, child_c, parent_t, parent_c = fks[0]
        assert child_t == "orders"
        assert child_c == "customer_id"
        assert parent_t == "customers"
        assert parent_c == "id"

    def test_pk_orientation(self):
        """When one side is the PK, it's the parent."""
        joins = [ExtractedJoin("customers", "id", "orders", "customer_id", "inner")]
        pk_columns = {"customers": "id", "orders": "id"}
        fks = detect_foreign_keys(joins, pk_columns)
        child_t, child_c, parent_t, parent_c = fks[0]
        assert parent_t == "customers"
        assert child_t == "orders"

    def test_id_suffix_orientation(self):
        """_id suffix column is the child."""
        joins = [ExtractedJoin("a", "b_id", "b", "code", "inner")]
        pk_columns = {"a": "id", "b": "id"}  # Neither side is the PK
        fks = detect_foreign_keys(joins, pk_columns)
        child_t, child_c, parent_t, parent_c = fks[0]
        assert child_t == "a"
        assert child_c == "b_id"
        assert parent_t == "b"

    def test_multiple_fks(self):
        joins = [
            ExtractedJoin("orders", "customer_id", "customers", "id", "inner"),
            ExtractedJoin("line_items", "order_id", "orders", "id", "inner"),
        ]
        pk_columns = {"orders": "id", "customers": "id", "line_items": "id"}
        fks = detect_foreign_keys(joins, pk_columns)
        assert len(fks) == 2

    def test_dedup_same_column(self):
        """Same child column in multiple joins should be deduped."""
        joins = [
            ExtractedJoin("orders", "customer_id", "customers", "id", "inner"),
            ExtractedJoin("orders", "customer_id", "customers", "id", "left"),
        ]
        pk_columns = {"orders": "id", "customers": "id"}
        fks = detect_foreign_keys(joins, pk_columns)
        assert len(fks) == 1

    def test_empty_table_skipped(self):
        joins = [
            ExtractedJoin("", "x", "b", "y", "inner"),
        ]
        fks = detect_foreign_keys(joins, {})
        assert len(fks) == 0

    def test_alphabetical_tiebreak(self):
        """When neither side is PK and neither has _id suffix, use alpha."""
        joins = [ExtractedJoin("b", "code", "a", "code", "inner")]
        pk_columns = {"a": "id", "b": "id"}  # Neither column is a PK
        fks = detect_foreign_keys(joins, pk_columns)
        child_t, _, parent_t, _ = fks[0]
        # Alphabetical: a < b, so a is parent, b is child
        assert parent_t == "a"
        assert child_t == "b"


# ---------------------------------------------------------------------------
# infer_schema  (integration)
# ---------------------------------------------------------------------------


class TestInferSchema:
    def _make_extracted(
        self,
        tables: list[ExtractedTable],
        joins: list[ExtractedJoin] | None = None,
    ) -> ExtractedSchema:
        return ExtractedSchema(
            tables=tables,
            joins=joins or [],
            original_sql="SELECT ...",
            dialect=None,
        )

    def test_single_table_basic(self):
        tables = [
            ExtractedTable(
                name="customers",
                columns=[
                    ExtractedColumn(name="id", table_name="customers"),
                    ExtractedColumn(name="name", table_name="customers"),
                    ExtractedColumn(name="email", table_name="customers"),
                ],
            )
        ]
        result = infer_schema(self._make_extracted(tables))

        assert "customers" in result.tables
        cols = result.tables["customers"]
        col_names = {c.name for c in cols}
        assert "id" in col_names
        assert "name" in col_names
        assert "email" in col_names

        # PK detection
        assert result.pk_columns["customers"] == "id"
        pk_col = next(c for c in cols if c.name == "id")
        assert pk_col.is_pk is True
        assert pk_col.nullable is False

    def test_pk_injected_when_missing(self):
        """If no 'id' column, synth PK should be injected."""
        tables = [
            ExtractedTable(
                name="products",
                columns=[
                    ExtractedColumn(name="name", table_name="products"),
                    ExtractedColumn(name="price", table_name="products"),
                ],
            )
        ]
        result = infer_schema(self._make_extracted(tables))
        assert result.pk_columns["products"] == "_synth_id"
        pk = next(c for c in result.tables["products"] if c.is_pk)
        assert pk.name == "_synth_id"
        assert pk.inferred_type == DataType.LONG
        assert pk.nullable is False

    def test_fk_from_join(self):
        tables = [
            ExtractedTable(
                name="customers",
                columns=[
                    ExtractedColumn(name="id", table_name="customers"),
                    ExtractedColumn(name="name", table_name="customers"),
                ],
            ),
            ExtractedTable(
                name="orders",
                columns=[
                    ExtractedColumn(name="id", table_name="orders"),
                    ExtractedColumn(name="customer_id", table_name="orders"),
                    ExtractedColumn(name="amount", table_name="orders"),
                ],
            ),
        ]
        joins = [ExtractedJoin("orders", "customer_id", "customers", "id", "inner")]
        result = infer_schema(self._make_extracted(tables, joins))

        # FK edges
        assert len(result.fk_edges) == 1
        child_t, child_c, parent_t, parent_c = result.fk_edges[0]
        assert child_t == "orders"
        assert child_c == "customer_id"
        assert parent_t == "customers"

        # FK column should have fk_ref set
        order_cols = result.tables["orders"]
        fk_col = next(c for c in order_cols if c.name == "customer_id")
        assert fk_col.fk_ref == "customers.id"

    def test_fk_column_injected_if_missing(self):
        """FK column from JOIN but not in SELECT should still appear."""
        tables = [
            ExtractedTable(
                name="customers",
                columns=[
                    ExtractedColumn(name="id", table_name="customers"),
                ],
            ),
            ExtractedTable(
                name="orders",
                columns=[
                    ExtractedColumn(name="id", table_name="orders"),
                    # customer_id NOT in columns
                ],
            ),
        ]
        joins = [ExtractedJoin("orders", "customer_id", "customers", "id", "inner")]
        result = infer_schema(self._make_extracted(tables, joins))
        order_col_names = {c.name for c in result.tables["orders"]}
        assert "customer_id" in order_col_names

    def test_type_inference_applied(self):
        tables = [
            ExtractedTable(
                name="orders",
                columns=[
                    ExtractedColumn(name="id", table_name="orders"),
                    ExtractedColumn(
                        name="amount",
                        table_name="orders",
                        cast_type="DECIMAL(10,2)",
                    ),
                    ExtractedColumn(
                        name="status",
                        table_name="orders",
                        compared_to_literal="active",
                    ),
                ],
            )
        ]
        result = infer_schema(self._make_extracted(tables))
        cols = {c.name: c for c in result.tables["orders"]}

        assert cols["amount"].inferred_type == DataType.DOUBLE
        assert cols["amount"].type_confidence == 0.95
        assert cols["status"].inferred_type == DataType.STRING
        assert cols["status"].type_confidence == 0.8

    def test_no_duplicate_columns(self):
        tables = [
            ExtractedTable(
                name="customers",
                columns=[
                    ExtractedColumn(name="id", table_name="customers"),
                    ExtractedColumn(name="id", table_name="customers"),
                    ExtractedColumn(name="name", table_name="customers"),
                ],
            )
        ]
        result = infer_schema(self._make_extracted(tables))
        col_names = [c.name for c in result.tables["customers"]]
        assert col_names.count("id") == 1

    def test_multi_table_star_schema(self):
        tables = [
            ExtractedTable(
                name="customers",
                columns=[
                    ExtractedColumn(name="id", table_name="customers"),
                    ExtractedColumn(name="name", table_name="customers"),
                ],
            ),
            ExtractedTable(
                name="orders",
                columns=[
                    ExtractedColumn(name="id", table_name="orders"),
                    ExtractedColumn(name="customer_id", table_name="orders"),
                ],
            ),
            ExtractedTable(
                name="products",
                columns=[
                    ExtractedColumn(name="id", table_name="products"),
                    ExtractedColumn(name="product_name", table_name="products"),
                ],
            ),
            ExtractedTable(
                name="line_items",
                columns=[
                    ExtractedColumn(name="id", table_name="line_items"),
                    ExtractedColumn(name="order_id", table_name="line_items"),
                    ExtractedColumn(name="product_id", table_name="line_items"),
                ],
            ),
        ]
        joins = [
            ExtractedJoin("orders", "customer_id", "customers", "id", "inner"),
            ExtractedJoin("line_items", "order_id", "orders", "id", "inner"),
            ExtractedJoin("line_items", "product_id", "products", "id", "inner"),
        ]
        result = infer_schema(self._make_extracted(tables, joins))
        assert len(result.fk_edges) == 3
        assert len(result.tables) == 4
        assert all(t in result.pk_columns for t in ["customers", "orders", "products", "line_items"])
