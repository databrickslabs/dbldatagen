# API reference

Core has three generation entry points, all importable from
`dbldatagen.core`:

| Function | Purpose |
| --- | --- |
| `generate(spark, plan)` | Materialize a whole plan — the usual entry point. |
| `resolve_plan(plan)` | Resolve a plan once, ahead of time, into a `ResolvedPlan`. |
| `generate_table(spark, table_spec, resolved_plan=None)` | Materialize one table. |

Most code only needs `generate`. The other two exist for the
resolve-once-then-iterate pattern and for building a single table on
its own.

## `generate`

```python
generate(
    spark: SparkSession,
    plan: DataGenPlan,
    resolved_plan: ResolvedPlan | None = None,
) -> dict[str, DataFrame]
```

Generates every table in a `DataGenPlan` and returns a `dict` keyed by
table name (`TableSpec.name`). Tables are built in dependency order —
parents before children — so a foreign-key child can reference the
parent rows that were just built. The dict's iteration order follows
`resolved.generation_order`.

```python
from dbldatagen.core import generate

dfs = generate(spark, plan)     # -> {"customers": DataFrame, "orders": DataFrame}
dfs["orders"].show()
```

**Arguments**

- `spark` — the active `SparkSession` used to construct the
  `DataFrame`s.
- `plan` — the `DataGenPlan` to materialize. Its `seed` must be set
  (it defaults to `42`, and Pydantic propagates it to each table at
  construction time).
- `resolved_plan` — *optional* pre-resolved plan from
  `resolve_plan(plan)`. When omitted, `generate` calls
  `resolve_plan(plan)` once on entry. Pass a pre-computed
  `ResolvedPlan` to skip re-resolution when generating the same plan
  many times.

**The identity-check rule.** If you pass `resolved_plan`, it must have
been produced from the *exact same* `plan` object — `generate` checks
`resolved_plan.plan is plan` (object identity, not equality). A
mismatch raises `ValueError`:

```python
plan_a = build_plan()
plan_b = build_plan()              # identical contents, different object
resolved_b = resolve_plan(plan_b)

generate(spark, plan_a, resolved_plan=resolved_b)
# ValueError: resolved_plan was produced from a different DataGenPlan ...
```

The check exists because a mismatch would silently combine one plan's
table seeds with another's FK topology and corrupt FK child columns —
producing orphan references with no error. Re-resolve with
`resolve_plan(plan)`, or just drop the `resolved_plan` argument and let
`generate` resolve internally.

## `resolve_plan`

```python
resolve_plan(plan: DataGenPlan) -> ResolvedPlan
```

Resolves a `DataGenPlan` into a generation-ready `ResolvedPlan`. This
is where the plan-time validation and graph work happens:

- every foreign-key reference is validated (parent table exists,
  referenced column exists and is a single-column primary key);
- the table-dependency graph is built and topologically sorted, so
  each parent comes before its children (cycles raise `ValueError`);
- the PK metadata each FK child needs at materialization is extracted.

It raises `ValueError` when an FK `ref` is malformed, points at a
missing/non-PK column, or the FK graph contains a cycle (and surfaces
the related expression-column / `seed_from` / primary-key validator
failures too).

### `ResolvedPlan`

A `@dataclass` carrying the resolution result. Its attributes:

| Attribute | Type | What it holds |
| --- | --- | --- |
| `generation_order` | `list[str]` | Table names sorted so each parent is built before any of its children. |
| `fk_resolutions` | `dict[tuple[str, str], FKResolution]` | Per FK column, keyed by `(table_name, column_name)`: the parent PK metadata, sampling distribution, and null fraction. |
| `plan` | `DataGenPlan` | Back-pointer to the plan this resolution was built from. `generate()` identity-checks against it. |

### Resolve once, then iterate

When you generate the *same* plan repeatedly — across seeds, batches,
or partitions — resolution is wasted work if it runs every time.
Resolve once and thread the result through:

```python
from dbldatagen.core import generate, resolve_plan

resolved = resolve_plan(plan)
dfs = generate(spark, plan, resolved_plan=resolved)
```

Because `generate` identity-checks `resolved_plan.plan is plan`, the
`resolved` you pass must come from this same `plan` object. If you want
genuinely *different* data per iteration, vary the seed by rebuilding
the plan (and re-resolving) — see
[concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md).

## `generate_table`

```python
generate_table(
    spark: SparkSession,
    table_spec: TableSpec,
    resolved_plan: ResolvedPlan | None = None,
) -> DataFrame
```

Generates a single table as a `DataFrame`. This is the lower-level
helper `generate` calls per table; reach for it directly when you want
just one table.

```python
from dbldatagen.core import generate_table, TableSpec, ColumnSpec
from dbldatagen.core.spec.schema import SequenceColumn, RangeColumn

spec = TableSpec(
    name="events",
    rows=1000,
    seed=42,                       # required when used standalone
    columns=[
        ColumnSpec(name="id", gen=SequenceColumn(start=1, step=1)),
        ColumnSpec(name="value", gen=RangeColumn(min=0, max=100)),
    ],
)
df = generate_table(spark, spec)
```

**Arguments**

- `spark` — the active `SparkSession`.
- `table_spec` — the `TableSpec` describing the schema, row count, and
  per-table seed.
- `resolved_plan` — *optional* `ResolvedPlan`. Supply it when the
  table has FK columns: FK children resolve against the parent
  metadata it carries. Without it, an FK column raises at
  materialization. For an FK-free table you can omit it (as above).

**`table_spec.seed` must be set.** Inside a plan, `plan.seed` is
propagated to each `TableSpec` during Pydantic validation, so you never
set it by hand. But a standalone `TableSpec` passed straight to
`generate_table` has `seed=None` unless you set it — and that raises
`ValueError`. Set `seed=` explicitly on the `TableSpec` (as in the
example above).

**`resolved_plan` is name-checked, not identity-checked.** Unlike
`generate`, `generate_table` only verifies that `resolved_plan` contains
a table named `table_spec.name` — the weaker check lets callers that
legitimately build fresh per-call `TableSpec` objects still reuse a
`ResolvedPlan`. If you need the full cross-plan guarantee, use
`generate`. Passing a `TableSpec` whose name happens to match a
same-named table in an *unrelated* resolved plan is **not** caught here.

**Column order is not strictly the declared order.** The engine
projects columns in three phases for performance (regular columns,
then FK / Faker columns, then `seed_from`-derived columns; declaration
order is preserved within each phase). Schema-level operations
(`df.schema`, `df.columns`, `df.select("name")`) are unaffected. If you
need byte-exact declared order in the final DataFrame, append a
projection:

```python
df = df.select(*[c.name for c in table_spec.columns])
```

## Imports at a glance

```python
# top-level objects
from dbldatagen.core import (
    generate, resolve_plan, generate_table, ResolvedPlan,
    DataGenPlan, TableSpec, ColumnSpec, PrimaryKey, ForeignKeyRef, DataType,
)

# strategy models live in the schema module
from dbldatagen.core.spec.schema import (
    SequenceColumn, RangeColumn, ValuesColumn, TimestampColumn,
    PatternColumn, ExpressionColumn, ConstantColumn, UUIDColumn,
    ForeignKeyColumn, StructColumn, ArrayColumn, FakerColumn,
)

# or the compact DSL helpers
from dbldatagen.core.spec import dsl as datagendg
```

## Next steps

- [recipes/multi-table.md](recipes/multi-table.md) — build and verify
  a two-table FK dataset
- [loading-plans.md](loading-plans.md) — load plans from JSON / YAML
- [relationships/foreign-keys.md](relationships/foreign-keys.md) —
  cross-table FKs in depth
