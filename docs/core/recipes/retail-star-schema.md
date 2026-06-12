# Recipe: a retail star schema with multi-FK facts

This recipe builds a small but realistic **retail star schema** on the
`dbldatagen.core` engine: two dimensions (`products`, `customers`) and
two facts (`orders`, `order_items`). The interesting piece is
`order_items`, which carries **two foreign keys at once** — one to
`orders` and one to `products` — and computes a `quantity × unit_price`
line-total measure. It's the natural next step up from the two-table
[foreign-key recipe](multi-table.md).

The shape:

```
products (dimension)        customers (dimension)
  product_id  PK              customer_id  PK
  category                    region
  unit_price                  segment
        ^                           ^
        |                           |
        |                    orders (fact / header)
        |                      order_id    PK
        |                      customer_id FK -> customers.customer_id
        |                      order_date
        |                      status
        |                           ^
        |                           |
        +------------------- order_items (fact / lines)
                               item_id     PK
                               order_id    FK -> orders.order_id
                               product_id  FK -> products.product_id   (Zipf)
                               quantity
                               unit_price                (local copy)
                               line_total  = quantity * unit_price
```

## What this builds

- A **`products`** dimension: a patterned `product_id` PK, a weighted
  `category` (`ValuesColumn` + `WeightedValues`), and a `unit_price`
  drawn from a `decimal(10,2)` `RangeColumn`.
- A **`customers`** dimension: a sequence `customer_id` PK plus a
  `region` and a weighted `segment`.
- An **`orders`** header fact: a sequence `order_id` PK, a
  `customer_id` FK into `customers`, an `order_date` timestamp, and a
  weighted `status`.
- An **`order_items`** line fact: a sequence `item_id` PK and **two
  FKs** — `order_id → orders.order_id` and `product_id →
  products.product_id`. The product FK uses a `Zipf` distribution so a
  few products sell far more than the rest. A `quantity` integer range
  and a `line_total = quantity * unit_price` measure round it out.

`order_items` is where the multi-FK, multi-level dependency lives:
`products` and `customers` build first, then `orders` (which depends on
`customers`), then `order_items` (which depends on both `orders` and
`products`). You list the tables in any order — `resolve_plan`
topologically sorts the whole graph and rejects cycles, so the
generation order is always parents-before-children.

### Where the `line_total` price comes from — read this first

An `ExpressionColumn` can reference **only other columns on the same
table** (the planner enforces same-table membership at `resolve_plan`
time). The "real" unit price lives on the **`products`** dimension, and
an expression on `order_items` **cannot reach into the parent table** to
read it. So a naive `line_total = quantity * products.unit_price` is not
expressible.

There are two honest ways to handle this; this recipe uses **(a)** so
every block runs end-to-end:

- **(a) Carry a `unit_price` column on `order_items` itself.** Give
  `order_items` its own `unit_price` `RangeColumn` (the price *as
  captured on the line*, the way a real order-line snapshots the price
  at purchase time). Then `line_total = quantity * unit_price` is a
  same-table expression and works. This is what the code below does.
- **(b) Join to `products` after generation.** Keep the canonical price
  only on `products` and recover it with a join
  (`order_items ⋈ products ON product_id`), computing the line total in
  the query rather than in the plan. This mirrors how a star schema is
  actually queried; see *Notes / variations*.

The two `unit_price` columns (one on `products`, one on `order_items`)
are independent draws — they are **not** correlated by construction. If
you need the order-line price to match the catalog price exactly, use
approach (b) and take the price from `products` in the join.

## Build the plan

### Schema form

```python
from dbldatagen.core import (
    generate, DataGenPlan, TableSpec, ColumnSpec, PrimaryKey, ForeignKeyRef,
)
from dbldatagen.core.spec.schema import (
    SequenceColumn, PatternColumn, RangeColumn, ValuesColumn, TimestampColumn,
    ExpressionColumn, ForeignKeyColumn, DataType, Zipf, WeightedValues,
)

products = TableSpec(
    name="products",
    rows=200,
    primary_key=PrimaryKey(columns=["product_id"]),       # parent MUST declare a PK
    columns=[
        ColumnSpec(name="product_id", gen=PatternColumn(template="SKU-{digit:6}")),
        ColumnSpec(
            name="category",
            gen=ValuesColumn(
                values=["electronics", "home", "grocery", "apparel", "toys"],
                distribution=WeightedValues(
                    weights={"electronics": 0.30, "home": 0.25, "grocery": 0.25,
                             "apparel": 0.15, "toys": 0.05},
                ),
            ),
        ),
        # The catalog price.  RangeColumn honors precision/scale.
        ColumnSpec(
            name="unit_price",
            dtype=DataType.DECIMAL, precision=10, scale=2,
            gen=RangeColumn(min=1.0, max=500.0),
        ),
    ],
)

customers = TableSpec(
    name="customers",
    rows=500,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
        ColumnSpec(
            name="region",
            gen=ValuesColumn(values=["North", "South", "East", "West"]),
        ),
        ColumnSpec(
            name="segment",
            gen=ValuesColumn(
                values=["consumer", "smb", "enterprise"],
                distribution=WeightedValues(
                    weights={"consumer": 0.7, "smb": 0.25, "enterprise": 0.05},
                ),
            ),
        ),
    ],
)

orders = TableSpec(
    name="orders",
    rows=2000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
        ColumnSpec(
            name="customer_id",
            gen=ForeignKeyColumn(),
            foreign_key=ForeignKeyRef(
                ref="customers.customer_id",
                distribution=Zipf(exponent=1.3),     # a few customers place most orders
            ),
        ),
        ColumnSpec(
            name="order_date",
            gen=TimestampColumn(start="2024-01-01", end="2024-12-31"),
        ),
        ColumnSpec(
            name="status",
            gen=ValuesColumn(
                values=["placed", "shipped", "delivered", "returned", "cancelled"],
                distribution=WeightedValues(
                    weights={"placed": 0.15, "shipped": 0.25, "delivered": 0.50,
                             "returned": 0.05, "cancelled": 0.05},
                ),
            ),
        ),
    ],
)

order_items = TableSpec(
    name="order_items",
    rows=8000,
    primary_key=PrimaryKey(columns=["item_id"]),
    columns=[
        ColumnSpec(name="item_id", gen=SequenceColumn(start=1, step=1)),
        # FK #1 -> orders.order_id
        ColumnSpec(
            name="order_id",
            gen=ForeignKeyColumn(),
            foreign_key=ForeignKeyRef(ref="orders.order_id"),   # bare ref -> Uniform
        ),
        # FK #2 -> products.product_id, skewed so a few SKUs dominate sales.
        ColumnSpec(
            name="product_id",
            gen=ForeignKeyColumn(),
            foreign_key=ForeignKeyRef(
                ref="products.product_id",
                distribution=Zipf(exponent=1.5),
            ),
        ),
        ColumnSpec(
            name="quantity",
            dtype=DataType.INT,
            gen=RangeColumn(min=1, max=10),
        ),
        # The line's captured price.  Lives on order_items (NOT a reach
        # into products) so the line_total expression can see it.
        ColumnSpec(
            name="unit_price",
            dtype=DataType.DECIMAL, precision=10, scale=2,
            gen=RangeColumn(min=1.0, max=500.0),
        ),
        # Measure: quantity * unit_price, both same-table columns declared
        # above.  ExpressionColumn carries no dtype -- cast inside the SQL.
        ColumnSpec(
            name="line_total",
            gen=ExpressionColumn(expr="cast(quantity * unit_price as decimal(11,2))"),
        ),
    ],
)

plan = DataGenPlan(
    tables=[order_items, orders, products, customers],   # any order; resolve_plan sorts it
    seed=42,
)
```

The pieces that wire the star together:

- **Every parent declares a single-column `PrimaryKey`.** `products`,
  `customers`, and `orders` each carry a `PrimaryKey`, because each is
  the target of an FK. The PK column itself is a `SequenceColumn`,
  `PatternColumn`, or `UUIDColumn` — those are the only PK strategies
  the planner accepts (an `ExpressionColumn` PK is rejected).
- **`order_items` carries two FK columns.** `order_id` and `product_id`
  are each a `ForeignKeyColumn()` + `ForeignKeyRef(...)` pair. A table
  can hold as many FKs as you like, to different parents — the planner
  resolves each independently and the dtype of each FK column is
  inferred from the parent PK it points at (a `long` for the sequence
  PKs, a `string` for the `SKU-{digit:6}` pattern PK).
- **Two distribution defaults.** `order_items.order_id` uses a **bare
  `ForeignKeyRef(ref=...)`**, which defaults the sampling distribution
  to **`Uniform`** (orders get a flat spread of line items).
  `order_items.product_id` passes `Zipf(exponent=1.5)` explicitly, so a
  small set of SKUs accounts for most sales. (The `datagendg.fk()`
  helper in the DSL form below defaults to `Zipf(exponent=1.2)` instead
  — see the note there.)
- **`line_total` is an `ExpressionColumn`.** Its SQL references
  `quantity` and `unit_price`, both regular (non-FK, non-Faker,
  non-`seed_from`) columns on the same table. An `ExpressionColumn`
  carries **no** `dtype`/`precision`/`scale` (the planner rejects them —
  the engine passes the SQL straight to `F.expr()` with no cast), so the
  `decimal(11,2)` is pinned with `cast(... as decimal(11,2))` **inside**
  the SQL. Declare the referenced columns *above* the expression:
  expressions evaluate in the first projection pass, so a forward
  reference fails at Spark eval time (the planner checks same-table
  membership and phase, not declaration order).

### DSL form

The `datagendg` helpers collapse the PK / FK / typed-column boilerplate.
The `line_total` measure still uses `datagendg.expression` with the
`cast(... as decimal(11,2))` inside the SQL — there is no helper that
both writes an expression and pins a decimal type, so the cast in the
string is the single source of truth for that column's type:

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import Zipf, Uniform, WeightedValues

products = TableSpec(
    name="products",
    rows=200,
    primary_key=PrimaryKey(columns=["product_id"]),
    columns=[
        datagendg.pk_pattern("product_id", "SKU-{digit:6}"),
        datagendg.text(
            "category",
            ["electronics", "home", "grocery", "apparel", "toys"],
            distribution=WeightedValues(
                weights={"electronics": 0.30, "home": 0.25, "grocery": 0.25,
                         "apparel": 0.15, "toys": 0.05},
            ),
        ),
        datagendg.decimal("unit_price", 1.0, 500.0, precision=10, scale=2),
    ],
)

customers = TableSpec(
    name="customers",
    rows=500,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        datagendg.pk_auto("customer_id"),
        datagendg.text("region", ["North", "South", "East", "West"]),
        datagendg.text(
            "segment",
            ["consumer", "smb", "enterprise"],
            distribution=WeightedValues(
                weights={"consumer": 0.7, "smb": 0.25, "enterprise": 0.05},
            ),
        ),
    ],
)

orders = TableSpec(
    name="orders",
    rows=2000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        datagendg.pk_auto("order_id"),
        datagendg.fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=1.3)),
        datagendg.timestamp("order_date", "2024-01-01", "2024-12-31"),
        datagendg.text(
            "status",
            ["placed", "shipped", "delivered", "returned", "cancelled"],
            distribution=WeightedValues(
                weights={"placed": 0.15, "shipped": 0.25, "delivered": 0.50,
                         "returned": 0.05, "cancelled": 0.05},
            ),
        ),
    ],
)

order_items = TableSpec(
    name="order_items",
    rows=8000,
    primary_key=PrimaryKey(columns=["item_id"]),
    columns=[
        datagendg.pk_auto("item_id"),
        # Match the schema form's Uniform on the orders FK (fk() defaults to Zipf).
        datagendg.fk("order_id", "orders.order_id", distribution=Uniform()),
        datagendg.fk("product_id", "products.product_id", distribution=Zipf(exponent=1.5)),
        datagendg.integer("quantity", 1, 10),
        datagendg.decimal("unit_price", 1.0, 500.0, precision=10, scale=2),
        datagendg.expression("line_total", "cast(quantity * unit_price as decimal(11,2))"),
    ],
)

plan = DataGenPlan(tables=[order_items, orders, products, customers], seed=42)
```

One difference worth calling out between the two forms:
`datagendg.fk(...)` defaults its `distribution` to **`Zipf(exponent=1.2)`**,
whereas a bare `ForeignKeyRef(ref=...)` in the schema form defaults to
**`Uniform`**. To keep the DSL `order_id` FK flat (matching the schema
form above), pass `distribution=Uniform()` explicitly, as shown. See
[../relationships/foreign-keys.md](../relationships/foreign-keys.md).

## Generate

```python
dfs = generate(spark, plan)
```

`generate` resolves the plan — topologically ordering the tables so
`products` and `customers` build before `orders`, and `orders` builds
before `order_items` — and returns a `dict` keyed by table name, in
generation order (parents first):

```python
for name in ["products", "customers", "orders", "order_items"]:
    dfs[name].show(5)
```

## Verify

Three things worth checking on a star schema: row counts match the
spec, primary keys are unique, and every FK resolves through the join
graph with no orphans.

### Row counts

```python
assert dfs["products"].count() == 200
assert dfs["customers"].count() == 500
assert dfs["orders"].count() == 2000
assert dfs["order_items"].count() == 8000
```

### Primary keys are unique

```python
for tbl, pk in [("products", "product_id"), ("customers", "customer_id"),
                ("orders", "order_id"), ("order_items", "item_id")]:
    df = dfs[tbl]
    assert df.count() == df.select(pk).distinct().count(), f"Duplicate PKs in {tbl}"
```

### Star-join referential integrity — no orphans on either FK

`order_items` has to join cleanly **both ways**: every line item points
at a real order (which points at a real customer), and every line item
points at a real product. An inner join down each path should preserve
the line-item count.

```python
items   = dfs["order_items"]
orders  = dfs["orders"]
custs   = dfs["customers"]
prods   = dfs["products"]

# order_items -> orders -> customers (the header path)
items_orders = items.join(orders, "order_id", "inner")
assert items_orders.count() == items.count()            # no orphan order_id

items_orders_custs = items_orders.join(custs, "customer_id", "inner")
assert items_orders_custs.count() == items.count()      # orders all have a customer

# order_items -> products (the product path)
items_prods = items.join(prods, "product_id", "inner")
assert items_prods.count() == items.count()             # no orphan product_id
```

Equivalently, a **left-anti** join surfaces orphans directly — any FK
value with no matching parent PK shows up in the result:

```python
assert items.join(orders, "order_id", "left_anti").count() == 0
assert items.join(prods, "product_id", "left_anti").count() == 0
assert orders.join(custs, "customer_id", "left_anti").count() == 0
```

You shouldn't ever see orphans: `resolve_plan` validates every FK
reference at plan time, and the engine deterministically reconstructs
parent PK values, so every emitted FK row hits a real parent by
construction. The joins above are a regression check that the guarantee
held on the actual `DataFrame`s. (The test suite ships an internal
`validate_referential_integrity(dfs, plan)` helper that runs exactly
this left-anti check over every FK column in a plan — it lives under
`tests/` and is not a public API, but the joins above are what it does.)

### The line-total measure

`line_total` lands on `decimal(11,2)` and equals `quantity *
unit_price` on every row:

```python
from pyspark.sql import functions as F

types = dict(dfs["order_items"].dtypes)
assert types["line_total"] == "decimal(11,2)", types["line_total"]

bad = dfs["order_items"].filter(
    F.col("line_total") != F.expr("cast(quantity * unit_price as decimal(11,2))")
)
assert bad.count() == 0
```

The Zipf skew on `product_id` shows up as a long-tailed sales
distribution — a handful of SKUs carry most of the line items:

```python
dfs["order_items"].groupBy("product_id").count().orderBy(F.desc("count")).show(10)
```

## Notes / variations

- **Take the price from `products` instead of the line (approach b).**
  Drop `unit_price` and `line_total` from `order_items`, keep the
  catalog price only on `products`, and compute the measure in a join
  after generation:

  ```python
  star = (
      dfs["order_items"]
      .join(dfs["products"], "product_id", "inner")
      .withColumn("line_total",
                  F.expr("cast(quantity * unit_price as decimal(11,2))"))
  )
  ```

  This matches how a star schema is actually queried (the fact joins
  out to its dimensions) and guarantees the line total uses the true
  catalog price. The trade-off: the measure isn't materialised in the
  generated `order_items` table — it's computed at read time.

- **Skew knobs.** `Zipf(exponent=...)` controls how concentrated the
  child→parent mapping is: smaller exponents (closer to `1.0`) give
  heavier tails (a few parents dominate harder), larger exponents
  approach `Uniform`. The product FK here uses `1.5`; the customer FK on
  `orders` uses `1.3`. `Zipf.exponent` must be strictly `> 1`.

- **Optional / nullable FKs.** To model line items not yet linked to a
  product (e.g. freeform charges), make the product FK nullable:
  `datagendg.fk("product_id", "products.product_id", nullable=True,
  null_fraction=0.1)`. The integrity joins above already exclude NULL
  FK values from the orphan check.

- **Scaling rows.** `rows` accepts human-readable strings, so
  `rows="1M"` on `order_items` is equivalent to `rows=1000000`. The
  topological generation order is unchanged at any scale.

- **More dimensions / deeper levels.** Add another dimension (e.g.
  `stores`) with its own `PrimaryKey` and an FK on `orders`, or another
  level of facts — the verification approach is identical. `resolve_plan`
  re-sorts the whole graph each time.

## See also

- [recipes/multi-table.md](multi-table.md) — the two-table foreign-key
  starting point this recipe builds on
- [relationships/foreign-keys.md](../relationships/foreign-keys.md) —
  FK distributions, nullable FKs, `null_fraction`, supported parent PK
  types
- [column-strategies/numeric-ranges.md](../column-strategies/numeric-ranges.md)
  — `RangeColumn`, decimals, and skewed numeric distributions
- [column-strategies/categorical-values.md](../column-strategies/categorical-values.md)
  — `ValuesColumn` and `WeightedValues` for the weighted `category` /
  `status` / `segment` columns
- [column-strategies/expressions.md](../column-strategies/expressions.md)
  — `ExpressionColumn` and the cast-inside-the-SQL rule for the
  `line_total` measure
- [../persisting-output.md](../persisting-output.md) — writing the star
  schema to Delta / Unity Catalog (iterate `dfs.items()` to write
  parents before children)
- [../api-reference.md](../api-reference.md) — `generate`,
  `resolve_plan`, `generate_table`
```
