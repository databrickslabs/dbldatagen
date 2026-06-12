# Troubleshooting

Common errors and warnings from `dbldatagen.core`, with the cause and
the fix for each. Every message below is paraphrased from a real
`raise` / `warnings.warn` in the engine, so search for the quoted
fragment if you hit something not listed here.

Errors are grouped by the phase they surface in: **plan construction**
(Pydantic validation, when you build the models), **`resolve_plan`**
(the planner's cross-column / cross-table checks), and **generation**
(Spark evaluation, the latest and least localized point).

---

## Plan construction

### "DataGenPlan constructed without an explicit `seed` — defaulting to 42"

- **Symptom** — a `UserWarning` the moment you build a `DataGenPlan`
  without passing `seed`.
- **Cause** — an unseeded plan silently defaults to `seed=42`, so every
  unseeded plan produces *identical* output across independent runs and
  callers. The warning exists so that coincidence isn't mistaken for
  real randomness.
- **Fix** — pass `seed=<int>` explicitly. Any integer works; the same
  seed always reproduces the same data. See
  [concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md).

### "null_fraction … is below the engine's 0.0001 granularity"

- **Symptom** — `ValueError` at plan time when `null_fraction` is a tiny
  positive number (e.g. `1e-6`).
- **Cause** — the engine's NULL mask has a granularity of `1/10000`, so
  a fraction strictly between `0.0` and `0.0001` would round to zero
  rows. It's rejected rather than silently nulling nothing.
- **Fix** — use `0.0` (no nulls) or a fraction `>= 0.0001`. See
  [nullable-and-dirty-data.md](nullable-and-dirty-data.md).

### "null_fraction must be in [0.0, 1.0]"

- **Symptom** — `ValueError` on a negative or `> 1.0` `null_fraction`
  (on a `ColumnSpec` or a `ForeignKeyRef`).
- **Cause** — `null_fraction` is a probability.
- **Fix** — set it within `[0.0, 1.0]`.

### "has duplicate column names" / "duplicate TableSpec names" / "duplicate entries"

- **Symptom** — `ValueError` naming the duplicated identifiers, in one
  of three forms:
  - `Table '<t>' has duplicate column names` — two `ColumnSpec`s share a
    `name` in the same table.
  - `DataGenPlan has duplicate TableSpec names` — `resolve_plan` dedupes
    on name and would silently drop all but the last.
  - `ValuesColumn has duplicate entries` — duplicates double the
    probability mass under `Uniform`.
- **Fix** — rename so each table / column is unique; for `ValuesColumn`
  use distinct values, or switch to
  [`WeightedValues`](distributions.md#weightedvalues) with explicit
  weights.

### "PrimaryKey has duplicate column names"

- **Symptom** — `ValueError` building a `PrimaryKey`.
- **Cause** — a column appears more than once in a composite key.
- **Fix** — list each PK column at most once.

---

## Columns and expressions

### "… cannot be set on an ExpressionColumn — its output type is always inferred"

- **Symptom** — `ValueError` at plan time when a `ColumnSpec` sets
  `dtype`, `precision`, or `scale` and its `gen` is an
  `ExpressionColumn`.
- **Cause** — an expression's type comes from the SQL; the engine
  evaluates it as-is with no cast, so a declared type would be silently
  ignored.
- **Fix** — cast inside the expression instead:
  `cast(quantity * unit_price as decimal(11,2))`. See
  [column-strategies/expressions.md](column-strategies/expressions.md).

### "ExpressionColumn.expr=… is whitespace-only"

- **Symptom** — `ValueError` at plan time for an empty or blank `expr`.
- **Cause** — `expr` is validated `min_length=1` and rejected if it's
  whitespace-only, so a blank string fails at the declaration rather
  than as a confusing Spark parse error later.
- **Fix** — supply a real Spark SQL expression.

### "ExpressionColumn '…' references … which are not columns in this table"

- **Symptom** — `ValueError` at `resolve_plan` listing the unknown
  names and the available columns.
- **Cause** — the expression references a name that is not a column on
  the same table at all.
- **Fix** — correct the name, or declare the referenced column.

### "ExpressionColumn '…' references …, which are FK / Faker / seed_from columns applied in a later phase"

- **Symptom** — `ValueError` at `resolve_plan`.
- **Cause** — the referenced column *exists* but is a foreign-key,
  `FakerColumn`, or `seed_from` column. Those are added in a later phase
  (via `withColumn`), after the expression's projection pass, so the
  reference would fail at Spark plan time with `UNRESOLVED_COLUMN`.
- **Fix** — reference a regular column, or compute the value at the
  source (e.g. inline the FK derivation in the expression).

### `UNRESOLVED_COLUMN` at generation time (forward reference)

- **Symptom** — a Spark `UNRESOLVED_COLUMN` error during generation, not
  at plan time.
- **Cause** — an `ExpressionColumn` references a **regular** column that
  is declared *later* in the column list. The planner does **not** check
  ordering among regular columns, so this slips past `resolve_plan` and
  surfaces only when Spark evaluates the single flat `select`.
- **Fix** — declare every referenced regular column *before* the
  expression. See
  [column-strategies/expressions.md](column-strategies/expressions.md).

### "precision must be in [1, 38]" / "scale must be in [0, precision]"

- **Symptom** — `ValueError` on a `DECIMAL` column's `precision` /
  `scale`.
- **Cause** — these mirror Spark's `DecimalType` limits.
- **Fix** — keep `precision` in `[1, 38]` and `scale` in `[0, precision]`.

### "… does not fit in decimal(p, s)"

- **Symptom** — `ValueError` at plan time: a range, a `ValuesColumn`
  entry, a `ConstantColumn` value, or a `SequenceColumn`'s last row
  exceeds the declared decimal's max representable magnitude.
- **Cause** — the declared `decimal(precision, scale)` is too narrow for
  the values the column would emit.
- **Fix** — widen `precision` (or reduce `scale`), or narrow the
  range / values.

### "FakerColumn / ForeignKeyColumn … is not supported" inside a struct or array

- **Symptom** — `ValueError` when a `StructColumn` field's `gen` or an
  `ArrayColumn.element` is a `FakerColumn` or `ForeignKeyColumn`.
- **Cause** — Faker needs a column-level `pandas_udf` and an FK keys on
  the top-level `(table, column)` pair — neither works inside a struct
  field or array element.
- **Fix** — use a scalar strategy (`RangeColumn`, `ValuesColumn`,
  `PatternColumn`, `ConstantColumn`, `ExpressionColumn`) for the
  field/element; to get an *array of FK records*, nest a `StructColumn`
  containing the FK inside the array. See
  [column-strategies/nested.md](column-strategies/nested.md).

### "uses strategy …, which is not a supported PK strategy"

- **Symptom** — `ValueError` at `resolve_plan` when a primary-key column
  uses an expression, range, values, or other non-key strategy.
- **Cause** — only `SequenceColumn`, `PatternColumn`, and `UUIDColumn`
  are valid PK strategies; the engine has to be able to reconstruct PK
  values deterministically when resolving foreign keys.
- **Fix** — give the PK column a `SequenceColumn`, `PatternColumn`, or
  `UUIDColumn` (DSL: `pk_auto` / `pk_pattern` / `pk_uuid`). See
  [relationships/foreign-keys.md](relationships/foreign-keys.md).

### "has width-cap violations" on a `PatternColumn`

- **Symptom** — `ValueError` listing per-placeholder violations.
- **Cause** — a `{digit:N}` / `{hex:N}` / `{alpha:N}` / `{seq:N}`
  placeholder exceeds its width cap (digit 18, hex 15, alpha 64, seq
  24), is `< 1`, or `{uuid}` carries a width modifier. The digit/hex
  caps come from int64-fit math (`10**18` / `16**15` are the largest
  that fit a signed int64).
- **Fix** — keep widths within the caps; for a short random token use
  `{hex:N}` / `{alpha:N}`; for very wide values use an expression with a
  UDF. See
  [column-strategies/text-and-patterns.md](column-strategies/text-and-patterns.md).

---

## Distributions

### "… does not support Normal with explicit mean/stddev"

- **Symptom** — `ValueError` at plan time when a `TimestampColumn`,
  `ForeignKeyRef`, or `ValuesColumn` carries a `Normal(mean=…)` or
  `Normal(stddev=…)`.
- **Cause** — `Normal`'s `mean`/`stddev` are *value-space* parameters,
  honored only on a numeric `RangeColumn`. On a timestamp (epoch
  seconds), an FK (parent-row index), or a value list (list position)
  there's no usable float value space, so `Normal` only ever
  auto-centers there.
- **Fix** — use a bare `Normal()` on those hosts, or move the
  parametrized `Normal` to a numeric `RangeColumn`. See
  [distributions.md](distributions.md#normal).

### "… does not support WeightedValues"

- **Symptom** — `ValueError` at plan time when `WeightedValues` is
  attached to a `RangeColumn`, `TimestampColumn`, or `ForeignKeyRef`.
- **Cause** — `WeightedValues` weights a *discrete value list*; a
  continuous range or a parent-row index range has no list to weight.
- **Fix** — use [`ValuesColumn`](column-strategies/categorical-values.md)
  for weighted discrete selection, or `Zipf` / `Normal` / `LogNormal` /
  `Exponential` to skew a range. See
  [distributions.md](distributions.md#weightedvalues).

### A skewed float range comes out uniform (silent)

- **Symptom** — no error, but a float/double `RangeColumn` with `Zipf`,
  `Exponential`, or `LogNormal` produces uniform-looking data.
- **Cause** — on a float range, those three distributions are accepted
  but the float path folds them into a uniform draw. Only `Uniform` and
  `Normal` actually shape a float range; full skew requires an
  **integer** range.
- **Fix** — skew an integer `RangeColumn` and scale it in an expression
  (e.g. `RangeColumn(min=0, max=10000, distribution=Zipf(...))` then
  `expr="col / 100.0"`). See the prominent warning in
  [distributions.md](distributions.md) and
  [limitations.md](limitations.md).

---

## Foreign keys

### "table '…' has no primary key defined"

- **Symptom** — `ValueError` at `resolve_plan` on an FK whose parent
  table declares no `primary_key`.
- **Cause** — an FK reconstructs the parent PK, so the parent must
  declare one.
- **Fix** — add a single-column `PrimaryKey` to the parent table.

### "table '…' has a composite primary key …; single-column ForeignKeyRef cannot deterministically resolve to one parent row"

- **Symptom** — `ValueError` at `resolve_plan` when the FK's parent has
  a multi-column PK.
- **Cause** — a single-column `ForeignKeyRef` can't map to one row of a
  composite key.
- **Fix** — give the parent a single-column PK, or split the
  relationship into a derived single-column key. See
  [relationships/foreign-keys.md](relationships/foreign-keys.md).

### "column '…' is not a primary key of '…'" / "does not exist"

- **Symptom** — `ValueError` at `resolve_plan` on a malformed
  `ref="table.column"`.
- **Cause** — the parent table or column doesn't exist, or the
  referenced column isn't part of the parent's PK.
- **Fix** — point `ref` at an existing parent column that *is* the
  parent's single-column primary key.

### "Circular FK dependency detected among tables"

- **Symptom** — `ValueError` at `resolve_plan` naming the tables in the
  cycle.
- **Cause** — the FK graph contains a cycle (A references B references
  A), so no topological generation order exists.
- **Fix** — break the cycle; FK relationships must form a DAG.

### `seed_from` errors ("does not exist" / "referencing itself" / chains rejected)

- **Symptom** — `ValueError` at `resolve_plan` on a bad `seed_from`.
- **Cause** — the target column doesn't exist, points at itself, or
  *itself* has `seed_from` set (chains are rejected — phase-3 columns
  apply in declaration order, not dependency order, so a chain breaks at
  Spark plan time).
- **Fix** — point `seed_from` at a different, non-derived column. See
  [relationships/correlated-columns.md](relationships/correlated-columns.md).

---

## Generation

### "TableSpec '…'.seed is None"

- **Symptom** — `ValueError` calling `generate_table` on a standalone
  `TableSpec`.
- **Cause** — a `TableSpec` built on its own has no seed; a
  `DataGenPlan` propagates `plan.seed` to each table during validation,
  but a bare `TableSpec` doesn't get one.
- **Fix** — set `TableSpec.seed` explicitly, or generate through a
  `DataGenPlan` (which propagates the plan seed). See
  [api-reference.md](api-reference.md).

### Faker `ImportError` — "the 'faker' package is required"

- **Symptom** — `ImportError` at generation time (pool build), not at
  plan time, when a plan uses `FakerColumn`.
- **Cause** — the `faker` library isn't installed.
- **Fix** — `pip install 'dbldatagen[core-faker]'`. See
  [column-strategies/text-and-patterns.md](column-strategies/text-and-patterns.md).

### "Unknown Faker provider method: '…'"

- **Symptom** — `ValueError` at generation time when the `provider` name
  isn't a real Faker method. (A whitespace-only `provider` is caught
  earlier, at plan time.)
- **Cause** — `provider` must name a Faker method such as `first_name`,
  `company`, or `ipv4`.
- **Fix** — use a valid Faker provider method name.

### "integer range size (…) >= 2\*\*63 overflows F.lit's int64 bound"

- **Symptom** — `ValueError` at plan time on an integer `RangeColumn`
  spanning nearly the whole int64 domain.
- **Cause** — the integer path computes `range_size = max - min + 1` and
  needs it to fit a signed int64.
- **Fix** — narrow `[min, max]` so `(max - min + 1) < 2**63`.

### `CANNOT_PARSE_TIMESTAMP` on a sequential timestamp

- **Symptom** — a Spark `CANNOT_PARSE_TIMESTAMP` error during generation
  when building timestamps from a string literal.
- **Cause** — under ANSI mode, `unix_timestamp` on a date-only literal
  like `'2024-01-01'` (no `HH:mm:ss`) fails to parse. This is Spark's
  error, not the engine's.
- **Fix** — always include a full `HH:mm:ss` in the literal
  (`'2024-01-01 00:00:00'`). Note also that `unix_timestamp` parses in
  `spark.sql.session.timeZone`, not UTC — see
  [recipes/sequential-timestamps.md](recipes/sequential-timestamps.md)
  for the time-zone caveat and the UTC-pinning workaround.

---

## See also

- [limitations.md](limitations.md) — what core can't do yet, with
  workarounds
- [distributions.md](distributions.md) — which distributions each
  strategy accepts
- [relationships/foreign-keys.md](relationships/foreign-keys.md) — FK
  rules in full
- [api-reference.md](api-reference.md) — `generate` / `generate_table` /
  `resolve_plan`
