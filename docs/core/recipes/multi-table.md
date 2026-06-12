# Recipe: a two-table dataset with a foreign key

This recipe builds the canonical `customers` / `orders` dataset end to
end: two tables joined by a foreign key, generated in one call, then
verified for row counts and referential integrity. It's the smallest
complete multi-table example.

The shape:

```
customers (parent)          orders (child)
  customer_id  PK   <---------  customer_id  FK -> customers.customer_id
  name                          order_id     PK
                                amount
```

## Build the plan

### Schema form

```python
from dbldatagen.core import (
    generate, DataGenPlan, TableSpec, ColumnSpec, PrimaryKey, ForeignKeyRef,
)
from dbldatagen.core.spec.schema import (
    SequenceColumn, ConstantColumn, RangeColumn, ForeignKeyColumn, DataType,
)

customers = TableSpec(
    name="customers",
    rows=200,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
        ColumnSpec(name="name", gen=ConstantColumn(value="test")),
    ],
)

orders = TableSpec(
    name="orders",
    rows=1000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
        ColumnSpec(
            name="customer_id",
            gen=ForeignKeyColumn(),
            foreign_key=ForeignKeyRef(ref="customers.customer_id"),
        ),
        ColumnSpec(
            name="amount",
            dtype=DataType.INT,
            gen=RangeColumn(min=10, max=500),
        ),
    ],
)

plan = DataGenPlan(tables=[customers, orders], seed=42)
```

The two pieces that wire the tables together:

- **`PrimaryKey(columns=["customer_id"])`** on `customers` marks the
  key the child will reference. A parent that something FKs into
  *must* have a primary key.
- **`gen=ForeignKeyColumn()` plus `foreign_key=ForeignKeyRef(ref="customers.customer_id")`**
  on the child column. The two go together: the `gen` marks the column
  as an FK, and the `ForeignKeyRef` says which `"table.column"` it
  points at. The FK column's type is inferred from the referenced PK,
  so you don't set `dtype` on it.

### DSL form

The same plan with the `datagendg` helpers, which collapse the
PK and FK boilerplate into one call each:

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg

customers = TableSpec(
    name="customers",
    rows=200,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        datagendg.pk_auto("customer_id"),
        datagendg.constant("name", "test"),
    ],
)

orders = TableSpec(
    name="orders",
    rows=1000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        datagendg.pk_auto("order_id"),
        datagendg.fk("customer_id", "customers.customer_id"),
        datagendg.integer("amount", 10, 500),
    ],
)

plan = DataGenPlan(tables=[customers, orders], seed=42)
```

`datagendg.pk_auto` is the sequence PK, and `datagendg.fk` builds the
`ForeignKeyColumn` + `ForeignKeyRef` pair in one call. Note one
difference: `datagendg.fk` defaults the sampling distribution to
`Zipf(exponent=1.2)` (a few customers get most of the orders), whereas
the schema form above leaves `distribution` unset, which falls back to
`Uniform`. Pass `distribution=` to `datagendg.fk` to match the schema
form, or vice versa. See
[relationships/foreign-keys.md](../relationships/foreign-keys.md).

## Generate

```python
dfs = generate(spark, plan)
```

`generate` resolves the plan — ordering tables so `customers` builds
before `orders` resolves its FK — and returns a `dict` keyed by table
name:

```python
dfs["customers"].show(5)
dfs["orders"].show(5)
```

Tables come back in dependency order: `customers` exists before
`orders` samples its `customer_id` values from it.

## Verify

Two things worth checking on any multi-table dataset: the row counts
match the spec, and every FK value points at a real parent.

### Row counts

```python
assert dfs["customers"].count() == 200
assert dfs["orders"].count() == 1000
```

### Primary keys are unique

```python
for tbl, pk in [("customers", "customer_id"), ("orders", "order_id")]:
    df = dfs[tbl]
    assert df.count() == df.select(pk).distinct().count(), f"Duplicate PKs in {tbl}"
```

### Referential integrity — every order has a real customer

The cleanest check is an inner join: every order should match a
customer, so the join count equals the order count.

```python
cust_df = dfs["customers"]
order_df = dfs["orders"]

joined = order_df.join(
    cust_df,
    order_df["customer_id"] == cust_df["customer_id"],
    "inner",
)
assert joined.count() == order_df.count()   # no orphan orders
```

Equivalently, a **left-anti** join surfaces orphans directly — any FK
value with no matching parent PK shows up in the result:

```python
orphans = order_df.join(
    cust_df,
    order_df["customer_id"] == cust_df["customer_id"],
    "left_anti",
)
assert orphans.count() == 0
```

You shouldn't ever see orphans: `resolve_plan` validates every FK
reference at plan time, and the engine deterministically reconstructs
parent PK values, so every emitted FK row hits a real parent by
construction. The join above is a regression check that the guarantee
held on the actual `DataFrame`. (The test suite ships an internal
`validate_referential_integrity(dfs, plan)` helper that runs exactly
this left-anti check over every FK column in a plan — it lives under
`tests/` and is not a public API, but the join above is what it does.)

## Determinism

The same plan and seed produce byte-identical data every time:

```python
dfs1 = generate(spark, plan)
dfs2 = generate(spark, plan)

for name in ["customers", "orders"]:
    rows1 = [tuple(r) for r in dfs1[name].orderBy("customer_id" if name == "customers" else "order_id").collect()]
    rows2 = [tuple(r) for r in dfs2[name].orderBy("customer_id" if name == "customers" else "order_id").collect()]
    assert rows1 == rows2
```

To get *different* data, vary the seed at construction time — see
[concepts/determinism-and-seeds.md](../concepts/determinism-and-seeds.md).

## Scaling up

This same pattern extends to a full star schema — multiple parents,
multiple FK levels (e.g. `order_items` referencing both `orders` and
`products`). `resolve_plan` topologically sorts the whole graph, so the
generation order is correct no matter how you list the tables. Add more
`TableSpec`s and more FK columns; the verification approach is
unchanged.

## Next steps

- [relationships/foreign-keys.md](../relationships/foreign-keys.md) —
  FK distributions, nullable FKs, `null_fraction`
- [api-reference.md](../api-reference.md) — `generate`, `resolve_plan`,
  `generate_table`
- [recipes/sequential-timestamps.md](sequential-timestamps.md) —
  evenly-spaced timestamps
