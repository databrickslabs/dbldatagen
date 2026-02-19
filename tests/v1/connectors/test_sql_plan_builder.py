"""Exhaustive tests for dbldatagen.v1.connectors.sql.plan_builder."""

from dbldatagen.v1.connectors.sql.inference import InferredColumn, InferredSchema
from dbldatagen.v1.connectors.sql.plan_builder import (
    CHILD_MULTIPLIER,
    DEFAULT_BASE_ROWS,
    _compute_row_counts,
    _resolve_row_count,
    _topo_sort,
    build_plan,
)
from dbldatagen.v1.schema import (
    DataGenPlan,
    DataType,
    SequenceColumn,
)

# ---------------------------------------------------------------------------
# _resolve_row_count
# ---------------------------------------------------------------------------


class TestResolveRowCount:
    def test_int_passthrough(self):
        assert _resolve_row_count(500) == 500

    def test_string_int(self):
        assert _resolve_row_count("500") == 500

    def test_k_suffix(self):
        assert _resolve_row_count("10K") == 10_000

    def test_k_suffix_lower(self):
        assert _resolve_row_count("10k") == 10_000

    def test_m_suffix(self):
        assert _resolve_row_count("2M") == 2_000_000

    def test_b_suffix(self):
        assert _resolve_row_count("1B") == 1_000_000_000

    def test_fractional_k(self):
        assert _resolve_row_count("1.5K") == 1500

    def test_fractional_m(self):
        assert _resolve_row_count("0.5M") == 500_000

    def test_whitespace_handling(self):
        assert _resolve_row_count("  10K  ") == 10_000


# ---------------------------------------------------------------------------
# _topo_sort
# ---------------------------------------------------------------------------


class TestTopoSort:
    def test_no_edges(self):
        order = _topo_sort([], ["a", "b", "c"])
        assert set(order) == {"a", "b", "c"}

    def test_simple_chain(self):
        # customers -> orders -> line_items
        edges = [
            ("orders", "customer_id", "customers", "id"),
            ("line_items", "order_id", "orders", "id"),
        ]
        order = _topo_sort(edges, ["customers", "orders", "line_items"])
        assert order.index("customers") < order.index("orders")
        assert order.index("orders") < order.index("line_items")

    def test_diamond(self):
        # customers and products -> line_items
        edges = [
            ("line_items", "customer_id", "customers", "id"),
            ("line_items", "product_id", "products", "id"),
        ]
        order = _topo_sort(edges, ["customers", "products", "line_items"])
        assert order.index("customers") < order.index("line_items")
        assert order.index("products") < order.index("line_items")

    def test_self_reference_ignored(self):
        edges = [("employees", "manager_id", "employees", "id")]
        order = _topo_sort(edges, ["employees"])
        assert order == ["employees"]

    def test_cycle_handled(self):
        # Cycles shouldn't cause infinite loop — remaining nodes appended
        edges = [
            ("a", "b_id", "b", "id"),
            ("b", "a_id", "a", "id"),
        ]
        order = _topo_sort(edges, ["a", "b"])
        assert set(order) == {"a", "b"}

    def test_disconnected_tables(self):
        edges = [("orders", "customer_id", "customers", "id")]
        order = _topo_sort(edges, ["customers", "orders", "products", "regions"])
        assert order.index("customers") < order.index("orders")
        assert "products" in order
        assert "regions" in order


# ---------------------------------------------------------------------------
# _compute_row_counts
# ---------------------------------------------------------------------------


class TestComputeRowCounts:
    def _make_schema(
        self,
        table_names: list[str],
        fk_edges: list[tuple[str, str, str, str]] | None = None,
    ) -> InferredSchema:
        tables = {name: [] for name in table_names}
        pk_columns = {name: "id" for name in table_names}
        return InferredSchema(
            tables=tables,
            fk_edges=fk_edges or [],
            pk_columns=pk_columns,
        )

    def test_default_no_fks(self):
        schema = self._make_schema(["customers", "products"])
        counts = _compute_row_counts(schema, None)
        assert counts["customers"] == DEFAULT_BASE_ROWS
        assert counts["products"] == DEFAULT_BASE_ROWS

    def test_child_multiplier(self):
        schema = self._make_schema(
            ["customers", "orders"],
            fk_edges=[("orders", "customer_id", "customers", "id")],
        )
        counts = _compute_row_counts(schema, None)
        assert counts["customers"] == DEFAULT_BASE_ROWS
        assert counts["orders"] == DEFAULT_BASE_ROWS * CHILD_MULTIPLIER

    def test_user_override(self):
        schema = self._make_schema(["customers"])
        counts = _compute_row_counts(schema, {"customers": 500})
        assert counts["customers"] == 500

    def test_user_override_string(self):
        schema = self._make_schema(["customers"])
        counts = _compute_row_counts(schema, {"customers": "10K"})
        assert counts["customers"] == 10_000

    def test_override_propagates_to_children(self):
        schema = self._make_schema(
            ["customers", "orders"],
            fk_edges=[("orders", "customer_id", "customers", "id")],
        )
        counts = _compute_row_counts(schema, {"customers": 200})
        assert counts["customers"] == 200
        assert counts["orders"] == 200 * CHILD_MULTIPLIER

    def test_child_of_multiple_parents(self):
        schema = self._make_schema(
            ["customers", "products", "line_items"],
            fk_edges=[
                ("line_items", "customer_id", "customers", "id"),
                ("line_items", "product_id", "products", "id"),
            ],
        )
        counts = _compute_row_counts(schema, {"customers": 100, "products": 200})
        # Child gets min(parent counts) * multiplier
        assert counts["line_items"] == 100 * CHILD_MULTIPLIER

    def test_three_level_chain(self):
        schema = self._make_schema(
            ["regions", "customers", "orders"],
            fk_edges=[
                ("customers", "region_id", "regions", "id"),
                ("orders", "customer_id", "customers", "id"),
            ],
        )
        counts = _compute_row_counts(schema, None)
        assert counts["regions"] == DEFAULT_BASE_ROWS
        assert counts["customers"] == DEFAULT_BASE_ROWS * CHILD_MULTIPLIER
        assert counts["orders"] == DEFAULT_BASE_ROWS * CHILD_MULTIPLIER * CHILD_MULTIPLIER


# ---------------------------------------------------------------------------
# build_plan
# ---------------------------------------------------------------------------


class TestBuildPlan:
    def _make_schema(
        self,
        tables: dict[str, list[InferredColumn]],
        fk_edges: list[tuple[str, str, str, str]] | None = None,
        pk_columns: dict[str, str] | None = None,
    ) -> InferredSchema:
        if pk_columns is None:
            pk_columns = {t: "id" for t in tables}
        return InferredSchema(
            tables=tables,
            fk_edges=fk_edges or [],
            pk_columns=pk_columns,
        )

    def test_single_table(self):
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="name", table_name="customers", inferred_type=DataType.STRING, type_confidence=0.3
                    ),
                    InferredColumn(
                        name="email", table_name="customers", inferred_type=DataType.STRING, type_confidence=0.3
                    ),
                ],
            },
        )
        plan = build_plan(schema)
        assert isinstance(plan, DataGenPlan)
        assert len(plan.tables) == 1
        assert plan.tables[0].name == "customers"
        assert plan.tables[0].rows == DEFAULT_BASE_ROWS
        assert plan.seed == 42

    def test_pk_column_is_sequence(self):
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                ],
            },
        )
        plan = build_plan(schema)
        pk_col = plan.tables[0].columns[0]
        assert pk_col.name == "id"
        assert isinstance(pk_col.gen, SequenceColumn)
        assert pk_col.dtype == DataType.LONG

    def test_fk_column_has_reference(self):
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                ],
                "orders": [
                    InferredColumn(
                        name="id", table_name="orders", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="customer_id",
                        table_name="orders",
                        fk_ref="customers.id",
                        inferred_type=DataType.LONG,
                        type_confidence=0.7,
                    ),
                ],
            },
            fk_edges=[("orders", "customer_id", "customers", "id")],
        )
        plan = build_plan(schema)
        order_table = next(t for t in plan.tables if t.name == "orders")
        fk_col = next(c for c in order_table.columns if c.name == "customer_id")
        assert fk_col.foreign_key is not None
        assert fk_col.foreign_key.ref == "customers.id"

    def test_regular_column_mapped(self):
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="email", table_name="customers", inferred_type=DataType.STRING, type_confidence=0.3
                    ),
                ],
            },
        )
        plan = build_plan(schema)
        email_col = next(c for c in plan.tables[0].columns if c.name == "email")
        # email should be mapped to FakerColumn by name_mapper
        from dbldatagen.v1.schema import FakerColumn

        assert isinstance(email_col.gen, FakerColumn)

    def test_primary_key_set(self):
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                ],
            },
        )
        plan = build_plan(schema)
        assert plan.tables[0].primary_key is not None
        assert plan.tables[0].primary_key.columns == ["id"]

    def test_custom_seed(self):
        schema = self._make_schema(
            tables={
                "t": [
                    InferredColumn(
                        name="id", table_name="t", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                ]
            },
        )
        plan = build_plan(schema, seed=99)
        assert plan.seed == 99

    def test_custom_row_counts(self):
        schema = self._make_schema(
            tables={
                "t": [
                    InferredColumn(
                        name="id", table_name="t", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                ]
            },
        )
        plan = build_plan(schema, row_counts={"t": 500})
        assert plan.tables[0].rows == 500

    def test_topological_order(self):
        """Parent tables should appear before children."""
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                ],
                "orders": [
                    InferredColumn(
                        name="id", table_name="orders", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="customer_id",
                        table_name="orders",
                        fk_ref="customers.id",
                        inferred_type=DataType.LONG,
                        type_confidence=0.7,
                    ),
                ],
            },
            fk_edges=[("orders", "customer_id", "customers", "id")],
        )
        plan = build_plan(schema)
        names = [t.name for t in plan.tables]
        assert names.index("customers") < names.index("orders")

    def test_child_row_count_multiplier(self):
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                ],
                "orders": [
                    InferredColumn(
                        name="id", table_name="orders", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="customer_id",
                        table_name="orders",
                        fk_ref="customers.id",
                        inferred_type=DataType.LONG,
                        type_confidence=0.7,
                    ),
                ],
            },
            fk_edges=[("orders", "customer_id", "customers", "id")],
        )
        plan = build_plan(schema)
        cust = next(t for t in plan.tables if t.name == "customers")
        orders = next(t for t in plan.tables if t.name == "orders")
        assert orders.rows == cust.rows * CHILD_MULTIPLIER

    def test_high_confidence_dtype_override(self):
        """High-confidence non-string types override name_mapper dtype."""
        schema = self._make_schema(
            tables={
                "orders": [
                    InferredColumn(
                        name="id", table_name="orders", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="amount", table_name="orders", inferred_type=DataType.DOUBLE, type_confidence=0.95
                    ),
                ],
            },
        )
        plan = build_plan(schema)
        amount_col = next(c for c in plan.tables[0].columns if c.name == "amount")
        assert amount_col.dtype == DataType.DOUBLE

    def test_star_schema_plan(self):
        schema = self._make_schema(
            tables={
                "customers": [
                    InferredColumn(
                        name="id", table_name="customers", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="name", table_name="customers", inferred_type=DataType.STRING, type_confidence=0.3
                    ),
                ],
                "products": [
                    InferredColumn(
                        name="id", table_name="products", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="product_name", table_name="products", inferred_type=DataType.STRING, type_confidence=0.3
                    ),
                ],
                "orders": [
                    InferredColumn(
                        name="id", table_name="orders", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="customer_id",
                        table_name="orders",
                        fk_ref="customers.id",
                        inferred_type=DataType.LONG,
                        type_confidence=0.7,
                    ),
                ],
                "line_items": [
                    InferredColumn(
                        name="id", table_name="line_items", is_pk=True, inferred_type=DataType.LONG, type_confidence=0.7
                    ),
                    InferredColumn(
                        name="order_id",
                        table_name="line_items",
                        fk_ref="orders.id",
                        inferred_type=DataType.LONG,
                        type_confidence=0.7,
                    ),
                    InferredColumn(
                        name="product_id",
                        table_name="line_items",
                        fk_ref="products.id",
                        inferred_type=DataType.LONG,
                        type_confidence=0.7,
                    ),
                ],
            },
            fk_edges=[
                ("orders", "customer_id", "customers", "id"),
                ("line_items", "order_id", "orders", "id"),
                ("line_items", "product_id", "products", "id"),
            ],
        )
        plan = build_plan(schema)
        names = [t.name for t in plan.tables]

        # Parents before children
        assert names.index("customers") < names.index("orders")
        assert names.index("orders") < names.index("line_items")
        assert names.index("products") < names.index("line_items")

        # Row counts
        cust = next(t for t in plan.tables if t.name == "customers")
        orders = next(t for t in plan.tables if t.name == "orders")
        li = next(t for t in plan.tables if t.name == "line_items")
        assert orders.rows == cust.rows * CHILD_MULTIPLIER
        # line_items uses min(parent counts) * multiplier; min(orders=5000, products=1000) = 1000
        assert li.rows == min(cust.rows, orders.rows) * CHILD_MULTIPLIER

    def test_no_pk_column_means_no_primary_key(self):
        schema = self._make_schema(
            tables={
                "data": [
                    InferredColumn(name="value", table_name="data", inferred_type=DataType.STRING, type_confidence=0.3),
                ],
            },
            pk_columns={"data": None},
        )
        plan = build_plan(schema)
        assert plan.tables[0].primary_key is None
