# Getting started

This walkthrough builds a small two-table dataset — `customers` and
`orders`, joined by a foreign key — and generates it with Spark. It's
the canonical example referenced throughout these docs.

## Prerequisites

```bash
pip install 'dbldatagen[core]'
```

You need an active `SparkSession` named `spark` (on Databricks this is
provided for you).

## 1. Describe the plan

A plan is a `DataGenPlan` holding one or more `TableSpec`s. Each
`TableSpec` lists its `ColumnSpec`s, and each column carries a
generation strategy in its `gen` field.

```python
from dbldatagen.core import (
    generate, DataGenPlan, TableSpec, ColumnSpec, PrimaryKey, ForeignKeyRef,
)
from dbldatagen.core.spec.schema import (
    SequenceColumn, ValuesColumn, TimestampColumn, RangeColumn,
    ForeignKeyColumn, WeightedValues, Zipf, DataType,
)

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="customers",
            rows=200,
            primary_key=PrimaryKey(columns=["customer_id"]),
            columns=[
                ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="name",
                    dtype=DataType.STRING,
                    gen=ValuesColumn(values=["Alice", "Bob", "Carol", "Dave"]),
                ),
                ColumnSpec(
                    name="signup_date",
                    gen=TimestampColumn(start="2022-01-01", end="2024-12-31"),
                ),
            ],
        ),
        TableSpec(
            name="orders",
            rows=1000,
            primary_key=PrimaryKey(columns=["order_id"]),
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(
                    name="customer_id",
                    gen=ForeignKeyColumn(),
                    foreign_key=ForeignKeyRef(
                        ref="customers.customer_id",
                        distribution=Zipf(exponent=1.2),
                    ),
                ),
                ColumnSpec(
                    name="amount",
                    dtype=DataType.DOUBLE,
                    gen=RangeColumn(min=5.0, max=500.0),
                ),
                ColumnSpec(
                    name="status",
                    dtype=DataType.STRING,
                    gen=ValuesColumn(
                        values=["pending", "shipped", "delivered", "cancelled"],
                        distribution=WeightedValues(
                            weights={
                                "pending": 0.1,
                                "shipped": 0.2,
                                "delivered": 0.6,
                                "cancelled": 0.1,
                            }
                        ),
                    ),
                ),
            ],
        ),
    ],
)
```

What this plan says:

- **`customer_id`** is a monotonic integer sequence (`1, 2, 3, …`) and
  the table's primary key — required because `orders` references it.
- **`name`** picks uniformly from four strings.
- **`signup_date`** is a random timestamp in the given range.
- **`orders.customer_id`** is a foreign key into
  `customers.customer_id`. The `Zipf` distribution makes a few
  customers place disproportionately many orders, like real data.
- **`amount`** is a continuous double in `[5.0, 500.0]`.
- **`status`** is a weighted categorical — 60% `delivered`, etc.

## 2. Generate

```python
dfs = generate(spark, plan)
```

`generate` resolves the plan (ordering tables so parents build before
children), then returns a `dict` mapping each table name to its
`DataFrame`:

```python
dfs["customers"].show(5)
dfs["orders"].show(5)

dfs["customers"].count()   # 200
dfs["orders"].count()      # 1000
```

Tables are generated in dependency order, so `customers` exists before
`orders` resolves its foreign key.

## 3. The same plan can come from a file

That exact plan can also live in YAML or JSON and be loaded with
`DataGenPlan.model_validate(...)`. See
[loading-plans.md](loading-plans.md).

## 4. Or write it in the compact DSL

The same plan in DSL form is shorter. See
[concepts/authoring-styles.md](concepts/authoring-styles.md) for the
side-by-side comparison.

## Next steps

- [concepts/plans-and-tables.md](concepts/plans-and-tables.md) — the
  three building blocks in depth
- [column-strategies/index.md](column-strategies/index.md) — every way
  to generate a column
- [concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md)
  — how reproducibility works and how to vary it
