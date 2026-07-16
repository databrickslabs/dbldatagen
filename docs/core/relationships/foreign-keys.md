# Foreign keys

A foreign key column draws its values from another table's primary key,
with **guaranteed referential integrity**: every non-NULL FK value is a
real parent PK, so child and parent always join cleanly. The engine
reconstructs what the parent PK generator would have produced for a
chosen row — it never has to materialize the parent table to look the
value up.

This is the `orders.customer_id → customers.customer_id` relationship
from the canonical plan.

## The three moving parts

An FK column is a `ColumnSpec` with two cooperating pieces:

1. `gen=ForeignKeyColumn()` — a marker strategy that tells the engine
   "this column's value comes from FK resolution, not from a normal
   generator."
2. `foreign_key=ForeignKeyRef(ref="parent.pk_column", ...)` — the
   reference itself: which parent PK, how to skew, whether it's
   nullable.

Both must be set together. Setting `ForeignKeyColumn()` without a
`foreign_key` (or vice-versa) is rejected at plan construction — the
validator refuses the half-configured shape rather than silently
emitting an all-NULL column.

## Schema form

```python
from dbldatagen.core import (
    DataGenPlan, TableSpec, ColumnSpec, PrimaryKey, ForeignKeyRef,
)
from dbldatagen.core.spec.schema import SequenceColumn, ForeignKeyColumn, Zipf

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="customers",
            rows=200,
            primary_key=PrimaryKey(columns=["customer_id"]),   # parent MUST declare a PK
            columns=[
                ColumnSpec(name="customer_id", gen=SequenceColumn(start=1, step=1)),
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
            ],
        ),
    ],
)
```

## DSL form

The `fk()` helper builds the same `ColumnSpec` — `ForeignKeyColumn()`
strategy plus an attached `ForeignKeyRef` — in one call:

```python
from dbldatagen.core import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="customers",
            rows=200,
            primary_key=PrimaryKey(columns=["customer_id"]),
            columns=[datagendg.pk_auto("customer_id")],
        ),
        TableSpec(
            name="orders",
            rows=1000,
            primary_key=PrimaryKey(columns=["order_id"]),
            columns=[
                datagendg.pk_auto("order_id"),
                datagendg.fk("customer_id", ref="customers.customer_id"),
            ],
        ),
    ],
)
```

Note the default distribution differs by authoring style:

- `fk(...)` defaults `distribution` to **`Zipf(exponent=1.2)`** — a
  small set of parents gets disproportionately many children, matching
  typical production data.
- A bare `ForeignKeyRef(...)` defaults to **`Uniform`** (every parent
  equally likely).

Pass `distribution=` explicitly whenever you want a specific shape.

## YAML form

```yaml
seed: 42

tables:
  - name: customers
    rows: 200
    primary_key:
      columns: [customer_id]
    columns:
      - name: customer_id
        gen:
          strategy: sequence
          start: 1
          step: 1

  - name: orders
    rows: 1000
    primary_key:
      columns: [order_id]
    columns:
      - name: order_id
        gen:
          strategy: sequence
          start: 1
          step: 1
      - name: customer_id
        gen:
          strategy: foreign_key
        foreign_key:
          ref: customers.customer_id
          distribution:
            type: zipf
            exponent: 1.2
```

## `ForeignKeyRef` fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `ref` | `str` | — | Parent PK in `"table.column"` form, e.g. `"customers.customer_id"`. The parent table must exist in the same plan, the named column must be part of that table's `primary_key`, and that PK must be **single-column** (composite parent PKs are rejected). |
| `distribution` | `Distribution` | `Uniform()` | Sampling distribution over the parent-row index range. `WeightedValues` is rejected here (it weights a discrete value list, not an index range). The `fk()` DSL helper overrides this default to `Zipf(exponent=1.2)`. |
| `nullable` | `bool` | `False` | Whether the FK column is declared nullable in the resulting Spark schema. Independent of `null_fraction`. |
| `null_fraction` | `float` | `0.0` | Probability in `[0.0, 1.0]` that a child row leaves the FK unresolved (emits NULL). Use `0.0` for mandatory FKs. Non-zero values below the engine's null-mask granularity (`1/10000`) raise at validation. |

`null_fraction` may also be set on the enclosing `ColumnSpec` instead of
on the `ForeignKeyRef` — both are legal. If set in both places with
different non-zero values, the plan is rejected; set it in exactly one
place (or use the same value in both).

```python
# A mostly-mandatory FK with 30% of orders unassigned to any customer.
datagendg.fk("customer_id", ref="customers.customer_id",
             nullable=True, null_fraction=0.3)
```

## How resolution works

Calling `generate(spark, plan)` (or `resolve_plan(plan)` directly)
performs FK resolution before any data is produced:

- **dtype is inferred from the parent PK.** The FK column always
  matches the parent column's type — a long for a sequence/UUID PK, a
  string for a pattern PK. You don't declare a `dtype` on an FK column.
- **The parent must declare a `PrimaryKey`,** the referenced column
  must belong to it, and that PK must be single-column. Pointing at a
  missing table, a missing column, a non-PK column, or one sub-column
  of a composite key all raise a `ValueError` naming the offending
  reference.
- **Generation order is parents-before-children.** Tables are
  topologically sorted from the FK dependency graph so a parent always
  exists before a child resolves against it.
- **Cycles are rejected.** If the FK graph contains a cycle,
  `resolve_plan` raises a `ValueError` — there is no valid generation
  order.

Because resolution is reproducible, the FK assignment is deterministic:
the same plan and seed produce the same child-to-parent mapping every
run.

## Supported parent PK types

Any single-column PK strategy works as an FK target, and the FK column
reconstructs the matching value type:

| Parent PK strategy | FK column type | Reconstruction |
| --- | --- | --- |
| `SequenceColumn` | long | `index * step + start` |
| `PatternColumn` | string | re-applies the parent's pattern template |
| `UUIDColumn` | string | re-derives the deterministic UUID |

These are exactly the cases verified end-to-end in
`tests/core/engine/test_fk.py` (sequential, pattern, and UUID parents
all join with zero orphans).

## See also

- [distributions.md](../distributions.md) — `Zipf` and the other
  distributions you can pass to `ForeignKeyRef`
- [concepts/plans-and-tables.md](../concepts/plans-and-tables.md) —
  `PrimaryKey`, `TableSpec`, and how the pieces connect
- [API reference](../reference/api/api.md) — `resolve_plan` /
  `generate`
- [getting-started.md](../getting-started.md) — the full
  customers/orders walkthrough
