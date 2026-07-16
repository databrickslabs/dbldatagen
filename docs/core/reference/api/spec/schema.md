---
sidebar_label: schema
title: dbldatagen.core.spec.schema
---

Pydantic models that describe a data-generation plan.

Defines the `Distribution` and `ColumnStrategy` discriminated unions and the
three composition models (`ColumnSpec`, `TableSpec`, `DataGenPlan`). Each model
forbids unknown fields and validates its parameters at construction.

### parse\_fk\_ref

```python
def parse_fk_ref(ref: str) -> tuple[str, str]
```

Parses and validates a `"table.column"` reference string.

**Arguments**:

- `ref` - A reference in `"table.column"` form; each part must be a valid
  identifier.
  

**Returns**:

  A `(table_name, column_name)` tuple.
  

**Raises**:

- `ValueError` - If `ref` is not two identifier-shaped parts separated by a
  single `.`.

## Uniform Objects

```python
class Uniform(_StrictModel)
```

Uniform distribution: every value in the range is equally likely.

This is the default for every strategy that accepts a `distribution`.

**Attributes**:

- `type` - Discriminator literal; always "uniform".

## Normal Objects

```python
class Normal(_StrictModel)
```

Normal (Gaussian) distribution over the strategy's value range.

`mean` and `stddev` are in the range's value units; samples are drawn from
`N(mean, stddev)` and clamped to the bounds. When unset, the bell
auto-centers on the range midpoint with a spread of `span / 6`.

**Attributes**:

- `type` - Discriminator literal; always "normal".
- `mean` - Optional peak in value units (default None, meaning the midpoint).
- `stddev` - Optional spread in value units; must be non-negative
  (default None, meaning span / 6).
  

**Notes**:

  Explicit `mean`/`stddev` are honored only on numeric `RangeColumn`; on
  timestamp, foreign-key, and value-list columns, use a bare `Normal()`.

### validate\_params

```python
@model_validator(mode="after")
def validate_params() -> Normal
```

Rejects non-finite parameters and a negative `stddev`.

**Raises**:

- `ValueError` - If `mean` or `stddev` is NaN/infinite, or `stddev` < 0.

## LogNormal Objects

```python
class LogNormal(_StrictModel)
```

Log-normal distribution: the log of the samples is normally distributed.

Useful for heavy-tailed positive quantities (e.g. incomes, file sizes,
response times).

**Attributes**:

- `type` - Discriminator literal; always "lognormal".
- `mean` - Optional mean of the underlying normal distribution; must be in
  [-100.0, 100.0] (default 0.0).
- `stddev` - Optional standard deviation of the underlying normal
  distribution; must be non-negative (default 1.0).

### validate\_params

```python
@model_validator(mode="after")
def validate_params() -> LogNormal
```

Rejects non-finite parameters, negative `stddev`, or out-of-range `mean`.

**Raises**:

- `ValueError` - If `mean`/`stddev` is NaN/infinite, `stddev` < 0, or
  `mean` is outside [-100, 100].

## Zipf Objects

```python
class Zipf(_StrictModel)
```

Zipfian / power-law distribution for realistic cardinality skew.

A small fraction of values account for most of the mass. Sampled via inverse
power-law CDF, which converges only for `exponent > 1`.

**Attributes**:

- `type` - Discriminator literal; always "zipf".
- `exponent` - Optional power-law exponent; must be > 1.0. Smaller values give
  heavier tails; larger values approach uniform (default 1.5).

### validate\_params

```python
@model_validator(mode="after")
def validate_params() -> Zipf
```

Rejects a non-finite or non-converging `exponent`.

**Raises**:

- `ValueError` - If `exponent` is NaN/infinite or <= 1.0.

## Exponential Objects

```python
class Exponential(_StrictModel)
```

Exponential distribution (e.g. for memoryless inter-arrival times).

**Attributes**:

- `type` - Discriminator literal; always "exponential".
- `rate` - Optional rate parameter (lambda); the reciprocal is the mean; must
  be positive (default 1.0).

### validate\_params

```python
@model_validator(mode="after")
def validate_params() -> Exponential
```

Rejects a non-finite or non-positive `rate`.

**Raises**:

- `ValueError` - If `rate` is NaN/infinite or <= 0.

## WeightedValues Objects

```python
class WeightedValues(_StrictModel)
```

Explicit weighted selection from a list of values.

Pair with `ValuesColumn` to give each value its own probability mass. Weights
are normalized and do not need to sum to 1.0. Not accepted by range,
timestamp, or foreign-key strategies, which have no value list to weight.

**Attributes**:

- `type` - Discriminator literal; always "weighted".
- `weights` - Mapping of values (as `str`) to non-negative weights. Must be
  non-empty.

### validate\_weights

```python
@model_validator(mode="after")
def validate_weights() -> WeightedValues
```

Rejects empty, non-finite, or negative weights.

**Raises**:

- `ValueError` - If `weights` is empty, or any weight is NaN/infinite or
  negative.

## RangeColumn Objects

```python
class RangeColumn(_StrictModel)
```

Generates values from a numeric range.

Both `min` and `max` are inclusive. When `step` is set, output snaps to
`min, min + step, ...` within the range; otherwise it is continuous.

**Attributes**:

- `strategy` - Discriminator literal; always "range".
- `min` - Optional inclusive lower bound (default 0).
- `max` - Optional inclusive upper bound; must be >= min (default 100).
- `step` - Optional step size; must be positive when set (default None,
  meaning continuous).
- `distribution` - Optional sampling distribution; `WeightedValues` is not
  accepted (default Uniform).

### validate\_range

```python
@model_validator(mode="after")
def validate_range() -> RangeColumn
```

Validates the range bounds, step, and distribution.

**Raises**:

- `ValueError` - If the distribution is `WeightedValues`, a bound or step
  is NaN/infinite, `min` > `max`, `step` <= 0, or the integer range
  size exceeds the int64 bound.

## ValuesColumn Objects

```python
class ValuesColumn(_StrictModel)
```

Selects from an explicit list of allowed values.

The element type is whatever the caller puts in `values` (strings, ints,
booleans, etc.).

**Attributes**:

- `strategy` - Discriminator literal; always "values".
- `values` - Non-empty list of allowed values.
- `distribution` - Optional sampling distribution. Use `WeightedValues` for
  per-value probabilities (default Uniform).

### validate\_values

```python
@model_validator(mode="after")
def validate_values() -> ValuesColumn
```

Validates the value list and any weighted distribution.

**Raises**:

- `ValueError` - If `values` is empty or has duplicates, the distribution
  is a parametrized `Normal`, or `WeightedValues` is missing a
  weight for any value or sums to zero.

## FakerColumn Objects

```python
class FakerColumn(_StrictModel)
```

Generates data using a Faker provider.

Requires the `faker` extra (`pip install 'dbldatagen[core-faker]'`). Always
produces StringType output.

**Attributes**:

- `strategy` - Discriminator literal; always "faker".
- `provider` - Faker provider method name (e.g. "first_name", "company").
- `kwargs` - Optional keyword arguments forwarded to the provider (default
  empty).
- `locale` - Optional Faker locale, e.g. "de_DE" (default None, meaning the
  plan's default locale).

## PatternColumn Objects

```python
class PatternColumn(_StrictModel)
```

Generates strings from a pattern template.

Placeholders:
{seq}     - row sequence number
{uuid}    - deterministic UUID
{alpha:N} - N random letters
{digit:N} - N random digits
{hex:N}   - N random hex characters

Example: `"ORD-{digit:4}-{alpha:3}"` -> `"ORD-3847-KMX"`.

**Attributes**:

- `strategy` - Discriminator literal; always "pattern".
- `template` - Pattern string containing literal text and placeholders.
  `{kind:N}` widths are capped per kind (see `_constants.py`); `{uuid}`
  takes no width.

### validate\_template

```python
@model_validator(mode="after")
def validate_template() -> PatternColumn
```

Validates the template's content and placeholder widths.

**Raises**:

- `ValueError` - If the template is whitespace-only, has no placeholders,
  gives `{uuid}` a width, or a `{kind:N}` width is < 1 or exceeds
  its cap.

## SequenceColumn Objects

```python
class SequenceColumn(_StrictModel)
```

Monotonic integer sequence. The value at row `i` is `start + i * step`.

**Attributes**:

- `strategy` - Discriminator literal; always "sequence".
- `start` - Optional starting value (default 1).
- `step` - Optional increment value per row; may be negative but not 0
  (default 1).

### validate\_step

```python
@model_validator(mode="after")
def validate_step() -> SequenceColumn
```

Ensures a valid step value is provided.

**Raises**:

- `ValueError` - If `step` is 0.

## UUIDColumn Objects

```python
class UUIDColumn(_StrictModel)
```

Deterministic UUID generation. The same seed and row always yield the same UUID.

**Attributes**:

- `strategy` - Discriminator literal; always "uuid".

## ExpressionColumn Objects

```python
class ExpressionColumn(_StrictModel)
```

A Spark SQL expression over other columns in the same table.

The expression may reference any column in the same table as long as the
referenced column is declared before this one, e.g. `"quantity * unit_price"`.

**Attributes**:

- `strategy` - Discriminator literal; always "expression".
- `expr` - A Spark SQL expression string. Referenced columns must exist in the
  same table. References are checked at resolution time.
  

**Notes**:

  `expr` is passed directly to `F.expr()` and can execute arbitrary Spark
  SQL. Do not use it with untrusted plan input in multi-tenant environments.

## TimestampColumn Objects

```python
class TimestampColumn(_StrictModel)
```

Generates timestamps within a range. Bounds are inclusive, and output is
independent of the session time zone.

**Attributes**:

- `strategy` - Discriminator literal; always "timestamp".
- `start` - Inclusive lower bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS").
- `end` - Inclusive upper bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS"); must be >= start.
- `distribution` - Optional sampling distribution; `WeightedValues` is not
  accepted (default Uniform).

### validate\_timestamps

```python
@model_validator(mode="after")
def validate_timestamps() -> TimestampColumn
```

Validates that timestamp bounds parse, agree in tz-awareness, and are ordered.

**Raises**:

- `ValueError` - If a bound is not a valid ISO timestamp, the bounds mix
  naive and tz-aware values, `start` > `end`, or the distribution
  is `WeightedValues`.

## ConstantColumn Objects

```python
class ConstantColumn(_StrictModel)
```

Emits the same literal value on every row.

**Attributes**:

- `strategy` - Discriminator literal; always "constant".
- `value` - The literal value emitted on every row.

## ForeignKeyColumn Objects

```python
class ForeignKeyColumn(_StrictModel)
```

Marker strategy for a column generated from its `foreign_key`.

Generation (type, distribution, null fraction, and the parent-key lookup) is
driven by `ColumnSpec.foreign_key` at resolution time.

**Attributes**:

- `strategy` - Discriminator literal; always "foreign_key".

## StructColumn Objects

```python
class StructColumn(_StrictModel)
```

Groups child columns into a Spark struct.

Each field is a full `ColumnSpec`; fields may be nested structs or arrays.
Field names must be unique within the struct.

Example JSON output::

{"address": {"street": "123 Main St", "city": "Austin", "zip": "78701"}}

**Attributes**:

- `strategy` - Discriminator literal; always "struct".
- `fields` - Non-empty, ordered list of field column specs with unique names.

### validate\_unique\_field\_names

```python
@model_validator(mode="after")
def validate_unique_field_names() -> StructColumn
```

Rejects duplicate field names within the struct.

**Raises**:

- `ValueError` - If two fields share a name.

### validate\_field\_strategies

```python
@model_validator(mode="after")
def validate_field_strategies() -> StructColumn
```

Rejects FakerColumn and ForeignKeyColumn strategies that can't be generated as struct fields.

**Raises**:

- `ValueError` - If a field uses `FakerColumn` or `ForeignKeyColumn`.

## ArrayColumn Objects

```python
class ArrayColumn(_StrictModel)
```

Generates a variable-length array of values.

The array holds a random number of values generated from `element`, each with
a unique seed. `FakerColumn` and `ForeignKeyColumn` are not allowed as
elements (wrap them in a `StructColumn` for an array of records).

Example JSON output::

{"tags": ["electronics", "sale", "new"]}

**Attributes**:

- `strategy` - Discriminator literal; always "array".
- `element` - Element-generation strategy; any `ColumnStrategy` except
  `FakerColumn` and `ForeignKeyColumn`. May itself be a `StructColumn`
  or `ArrayColumn`.
- `min_length` - Optional inclusive minimum length per row; must be >= 0
  (default 1).
- `max_length` - Optional inclusive maximum length per row; must be >= 1,
  >= min_length, and <= 1000 (default 5).

#### element

forward ref resolved by model_rebuild() below

### validate\_lengths

```python
@model_validator(mode="after")
def validate_lengths() -> ArrayColumn
```

Validates the array length bounds and element strategy.

**Raises**:

- `ValueError` - If `min_length` < 0, `min_length` > `max_length`,
  `max_length` is 0 or exceeds the cap, or `element` is a
  `FakerColumn` or `ForeignKeyColumn`.

## DataType Objects

```python
class DataType(str, Enum)
```

Column data types, mapped to the equivalent Spark SQL types.

`INTEGER` is an alias for `INT`. Alternate spellings `"integer"`, `"bool"`,
and `"str"` are also accepted when deserializing plans.

### \_missing\_

```python
@classmethod
def _missing_(cls, value: object) -> DataType | None
```

Accepts common alternate spellings (`"integer"` -> INT, etc.).

## PrimaryKey Objects

```python
class PrimaryKey(_StrictModel)
```

Marks one or more columns as the table's primary key.

Composite (multi-column) primary keys are allowed, but a `ForeignKeyRef` can
only target a single-column primary key.

**Attributes**:

- `columns` - Non-empty, ordered list of primary-key column names with no
  duplicates. Each must match a column on the table.

### validate\_unique\_columns

```python
@model_validator(mode="after")
def validate_unique_columns() -> PrimaryKey
```

Rejects duplicate column names in the key.

**Raises**:

- `ValueError` - If a column name appears more than once.

## ForeignKeyRef Objects

```python
class ForeignKeyRef(_StrictModel)
```

Defines a foreign-key relationship to another table's primary key.

`ref` uses `"table.column"` syntax. Use `distribution` (e.g. `Zipf`) to skew
children toward a small set of parents, and `null_fraction` for optional FKs.

**Attributes**:

- `ref` - Reference to the parent primary key in `"table.column"` form. The
  parent table must exist and the column must be its single-column
  primary key.
- `distribution` - Optional sampling distribution over parent rows;
  `WeightedValues` is not accepted (default Uniform).
- `nullable` - Optional flag for whether the column is declared nullable in
  the schema; independent of `null_fraction` (default False).
- `null_fraction` - Optional probability in [0.0, 1.0] that a row is NULL
  instead of a parent reference (default 0.0).

### validate\_ref\_format

```python
@model_validator(mode="after")
def validate_ref_format() -> ForeignKeyRef
```

Validates the reference format, distribution, and null fraction.

**Raises**:

- `ValueError` - If `ref` is not `"table.column"`, the distribution is a
  parametrized `Normal` or `WeightedValues`, or `null_fraction` is
  outside [0.0, 1.0].

## ColumnSpec Objects

```python
class ColumnSpec(_StrictModel)
```

A single column in a table.

A name and generation strategy must be specified. `dtype` is inferred from
`gen` when not set. The DSL helpers in `dbldatagen.core.spec.dsl` are the
recommended way to build a `ColumnSpec`.

**Attributes**:

- `name` - Column name; must be a valid identifier and must not be a reserved
  internal name (e.g. "_synth_row_id").
- `dtype` - Optional output type; inferred from `gen` when None (default None).
- `gen` - The value-generation strategy for this column.
- `nullable` - Optional flag for whether the column is nullable; independent of
  `null_fraction` (default False).
- `null_fraction` - Optional probability in [0.0, 1.0] that a row is NULL
  (default 0.0).
- `foreign_key` - Optional foreign-key reference; when set, the column is
  generated from it and `gen` must be `ForeignKeyColumn` (default None).
- `seed_from` - Optional name of another column to derive the per-cell seed
  from, for correlating values across columns (default None).
- `precision` - Optional total number of digits for DECIMAL columns; set with
  `scale`; rejected on non-DECIMAL types (default None).
- `scale` - Optional number of fractional digits for DECIMAL columns; set with
  `precision` (default None).

### validate\_column\_name

```python
@model_validator(mode="after")
def validate_column_name() -> ColumnSpec
```

Validates the column name is a valid, unreserved identifier.

**Raises**:

- `ValueError` - If `name` is not a valid identifier or collides with an
  engine-internal name.

### validate\_expression\_column\_type

```python
@model_validator(mode="after")
def validate_expression_column_type() -> ColumnSpec
```

Rejects a declared type on an `ExpressionColumn`. Expression types are
always inferred by Spark. Add a cast statement in the passed expression to
control the output data type.

**Raises**:

- `ValueError` - If `dtype`, `precision`, or `scale` is set on an
  `ExpressionColumn`.

### validate\_decimal\_precision\_scale

```python
@model_validator(mode="after")
def validate_decimal_precision_scale() -> ColumnSpec
```

Validates decimal precision/scale and that values fit the type.

`precision` and `scale` are valid only on decimal columns and must be set
together; when both are unset, the engine uses Spark's `DecimalType()`
default of (10, 0).

**Raises**:

- `ValueError` - If precision/scale are set on a non-decimal column, only
  one is set, either is out of range, or a value does not fit.

### validate\_date\_dtype\_strategy

```python
@model_validator(mode="after")
def validate_date_dtype_strategy() -> ColumnSpec
```

Rejects `dtype=DATE` on strategies that can't produce a date.

Use `TimestampColumn` for a random date, or drop `dtype=DATE` to keep the
strategy's native type.

**Raises**:

- `ValueError` - If `dtype` is DATE and `gen` is a range, pattern,
  sequence, UUID, Faker, struct, or array strategy.

### validate\_faker\_dtype\_compatibility

```python
@model_validator(mode="after")
def validate_faker_dtype_compatibility() -> ColumnSpec
```

Rejects a FakerColumn paired with a non-string dtype.

Faker always produces StringType, so any other declared dtype is invalid.

**Raises**:

- `ValueError` - If `gen` is a `FakerColumn` and `dtype` is set to anything
  other than STRING.

### validate\_null\_fraction

```python
@model_validator(mode="after")
def validate_null_fraction() -> ColumnSpec
```

Validates the null fraction is within range.

**Raises**:

- `ValueError` - If `null_fraction` is outside [0.0, 1.0].

### validate\_foreign\_key\_strategy

```python
@model_validator(mode="after")
def validate_foreign_key_strategy() -> ColumnSpec
```

Validates the ForeignKeyColumn/foreign_key pairing and null fraction.

**Raises**:

- `ValueError` - If `gen` is `ForeignKeyColumn` without a `foreign_key`,
  `foreign_key` is set with a non-FK `gen`, or the column and FK
  set conflicting and non-zero null fractions.

## TableSpec Objects

```python
class TableSpec(_StrictModel)
```

Defines the generation specification for a single table.

The number of rows can be passed as an integer count or a human-readable
string like "10M".

**Attributes**:

- `name` - Table name; must be a valid identifier. Used as the result dict key
  and as the parent name in `ForeignKeyRef.ref`.
- `columns` - Non-empty, ordered list of `ColumnSpec` with unique names.
- `rows` - Number of rows to generate. Can be an integer row count or a string
  like "10K", "5M", or "1B". Must be positive.
- `primary_key` - Optional `PrimaryKey`; required when another table
  references this one (default None).
- `seed` - Optional per-table seed; usually propagated from `DataGenPlan.seed`
  (default None).

### resolve\_row\_count

```python
@model_validator(mode="after")
def resolve_row_count() -> TableSpec
```

Normalizes the row count to a positive integer.

Accepts an integer count or a string like `"10K"` and stores the parsed
int back on `self.rows`.

**Raises**:

- `ValueError` - If `rows` does not parse to a positive integer.

### validate\_name

```python
@model_validator(mode="after")
def validate_name() -> TableSpec
```

Validates the table name is a valid identifier.

**Raises**:

- `ValueError` - If `name` is not a valid identifier.

### validate\_unique\_column\_names

```python
@model_validator(mode="after")
def validate_unique_column_names() -> TableSpec
```

Rejects duplicate column names within the table.

**Raises**:

- `ValueError` - If two columns share a name.

### validate\_primary\_key\_columns\_exist

```python
@model_validator(mode="after")
def validate_primary_key_columns_exist() -> TableSpec
```

Rejects primary-key columns that don't exist on the table.

**Raises**:

- `ValueError` - If a `primary_key` column is not among the table's
  columns.

### parse\_human\_count

```python
@staticmethod
def parse_human_count(value: int | str) -> int
```

Parses a human-readable row count into an integer.

Accepts an integer (returned unchanged) or a string with a `K`, `M`, or
`B` suffix (case-insensitive), such as `"10K"` or `"1.5B"`.

**Arguments**:

- `value` - An integer row count, or a string like `"10K"`, `"5M"`, or
  `"1.5B"`.
  

**Returns**:

  The row count as an integer.
  

**Raises**:

- `ValueError` - If `value` does not parse to an integer.

### validate\_sequence\_column\_overflow

```python
@model_validator(mode="after")
def validate_sequence_column_overflow() -> TableSpec
```

Rejects SequenceColumn configurations that overflow the available range of values.

For each sequence column, the last value `start + (rows - 1) * step` must
fit in int64 and, when precision/scale are set, in the declared decimal.

**Raises**:

- `ValueError` - If a sequence column's last value overflows int64 or does
  not fit its decimal type.

## DataGenPlan Objects

```python
class DataGenPlan(_StrictModel)
```

Top-level plan describing all tables to generate. Tables are generated in
dependency order; foreign-key references are resolved automatically.

**Attributes**:

- `tables` - Non-empty list of `TableSpec` instances to generate.
- `seed` - Global seed propagated to each table without a set table seed
  (table `i` gets `seed + i`). Defaults to 42 with a `UserWarning`; set
  it explicitly in production.
- `default_locale` - Default Faker locale for `FakerColumn`s that don't set
  one (default "en_US").

### validate\_unique\_table\_names

```python
@model_validator(mode="after")
def validate_unique_table_names() -> DataGenPlan
```

Rejects duplicate table names in the plan.

**Raises**:

- `ValueError` - If two tables share a name.

### propagate\_seeds

```python
@model_validator(mode="after")
def propagate_seeds() -> DataGenPlan
```

Assigns each table a deterministic seed from the global seed when unset.

Table `i` gets `seed + i`. Tables that already have a seed are left
unchanged, so re-validating a dumped plan is stable.

