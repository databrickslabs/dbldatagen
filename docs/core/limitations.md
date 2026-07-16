# Limitations

What `dbldatagen.core` can't do *today*, and the recommended workaround
for each. These are real boundaries of the current engine, not bugs —
several have a clean composition workaround, and a couple are **silent**
(no error, just behavior you didn't ask for), which we flag explicitly.

Where a limitation is a silent no-op or a silent-wrong-data trap, it's
called out in bold — those are the ones that bite hardest because
nothing fails loudly.

## Distributions

### ⚠️ Float ranges silently fold `Zipf` / `Exponential` / `LogNormal` into uniform

This is a **silent wrong-data trap**, the single sharpest edge in core.
On a **float/double** `RangeColumn`, `Zipf`, `Exponential`, and
`LogNormal` are accepted *without any error* but the float path produces
a **uniform** draw. You ask for a skewed column and get uniform data,
silently. Only **integer** ranges shape all five non-weighted
distributions; on a float range only `Uniform` and `Normal` do anything.

- **Workaround** — shape an *integer* `RangeColumn` and scale it in an
  [`ExpressionColumn`](column-strategies/expressions.md), e.g.
  `RangeColumn(min=0, max=10000, distribution=Zipf(...))` with
  `expr="col / 100.0"`.
- See [distributions.md](distributions.md) (the prominent warning box).

### `Normal` mean/stddev are value-space and numeric-`RangeColumn`-only

`Normal(mean=…, stddev=…)` is honored only on a numeric `RangeColumn`.
On `TimestampColumn`, `ForeignKeyRef`, and `ValuesColumn` a parametrized
`Normal` is **rejected at plan time** (a bare `Normal()` still
auto-centers there).

- **Workaround** — bare `Normal()` on those hosts, or put the
  parametrized `Normal` on a numeric range.
- See [distributions.md](distributions.md#normal).

## Column strategies

### No `MapType` / map-column strategy

Core has no map-column strategy, so a `MapType` column is not something
it can generate.

- **Workaround** — model the relationship as a child table (one row per
  key) joined back, rather than a single wide map column.
- See [column-strategies/nested.md](column-strategies/nested.md).

### `ExpressionColumn`: type always inferred, narrow reference rules, no PK

An expression's output type is **always inferred** from the SQL —
setting `dtype`, `precision`, or `scale` is rejected. It also **can't
reference** FK / Faker / `seed_from` columns (those are added in a later
phase), and an `ExpressionColumn` **can't be a primary key**.

- **Workaround** — cast inside the SQL (`cast(... as decimal(11,2))`);
  reference only regular columns declared earlier; use
  `SequenceColumn` / `PatternColumn` / `UUIDColumn` for PKs.
- See [column-strategies/expressions.md](column-strategies/expressions.md).

### `FakerColumn`: needs the extra, always `StringType`, scalar-only

`FakerColumn` requires the `core-faker` extra, always emits a
`StringType`, and **can't be a struct field or array element**
(Faker needs a column-level `pandas_udf`).

- **Workaround** — `pip install 'dbldatagen[core-faker]'`; `cast(...)`
  in an expression if you need a non-string type; nest a `StructColumn`
  if you need Faker inside an array.
- See
  [column-strategies/text-and-patterns.md](column-strategies/text-and-patterns.md).

### `ForeignKeyColumn` can't be a struct field or array element

Like Faker, an FK keys on the top-level `(table, column)` pair, which
has no meaning inside a struct field or array element — it's rejected
there.

- **Workaround** — nest a `StructColumn` containing the FK inside the
  array if you need an array of FK records.
- See [column-strategies/nested.md](column-strategies/nested.md).

### Pattern placeholder width caps

`PatternColumn` placeholder widths are capped: **digit 18, hex 15, alpha
64, seq 24**. The digit/hex caps are int64-fit limits (`10**18` /
`16**15` are the largest that fit a signed int64); alpha/seq caps bound
emitted string length and per-row plan size.

- **Workaround** — for wider random tokens than the caps allow, generate
  via an `ExpressionColumn` with a UDF.
- See
  [column-strategies/text-and-patterns.md](column-strategies/text-and-patterns.md).

### int64 range-size cap on integer ranges

An integer `RangeColumn` needs `(max - min + 1) < 2**63` — a range
spanning the full int64 domain overflows `F.lit`'s int64 bound and is
rejected at plan time.

- **Workaround** — narrow `[min, max]`.
- See [column-strategies/numeric-ranges.md](column-strategies/numeric-ranges.md).

## Keys and relationships

### FK parents must declare a single-column primary key

A `ForeignKeyRef` target table must declare a `PrimaryKey`, and that PK
must be **single-column** — a composite (multi-column) PK **cannot** be
an FK target (a single-column FK can't deterministically resolve to one
row of a composite key). Both are rejected at `resolve_plan`.

- **Workaround** — give the parent a single-column PK, or split the
  relationship into a derived single-column key.
- See [relationships/foreign-keys.md](relationships/foreign-keys.md).

### `seed_from` gives a constant value per source key — no per-row jitter

`seed_from` correlates a column to another column's value: for a given
source-key value you get a single, constant derived value. There's no
built-in per-row jitter around a per-key base.

- **Workaround** — combine the `seed_from` base with an independent
  noise column via an `ExpressionColumn`.
- See [relationships/correlated-columns.md](relationships/correlated-columns.md).

## Missing conveniences (compose instead)

### No exposed per-row index

There's no implicit "row number" column.

- **Workaround** — add a [`SequenceColumn`](column-strategies/sequences-and-ids.md)
  (`start=0, step=1`) to materialize a per-row index.

### No fixed-interval timestamp strategy

There's no first-class "every N seconds" timestamp strategy.

- **Workaround** — compose a `SequenceColumn` (the step index) with an
  `ExpressionColumn` that adds `index * interval` to a base time.

## See also

- [troubleshooting.md](troubleshooting.md) — the exact error each of
  the rejected cases above raises
- [distributions.md](distributions.md) — distribution support matrix
- [column-strategies/index.md](column-strategies/index.md) — all
  strategies
