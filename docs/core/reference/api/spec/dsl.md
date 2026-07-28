---
sidebar_label: dsl
title: dbldatagen.core.spec.dsl
---

Lowercase factory helpers for building `ColumnSpec` objects.

The helpers should be accessed by importing this module with an alias (e.g.
from dbldatagen.core.spec import dsl as datagendg).

Individual methods are not re-exported from `dbldatagen.core` or
`dbldatagen.core.spec`. Several lowercase names would shadow stdlib modules or
the `faker` package when imported flat.

### pk\_auto

```python
def pk_auto(name: str = "id") -> ColumnSpec
```

Builds an auto-incrementing integer primary key column spec (1, 2, 3, ...).

**Arguments**:

- `name` - Optional column name (default "id").
  

**Returns**:

  A `ColumnSpec` with `DataType.LONG` and a `SequenceColumn` generator.
  Pair it with `PrimaryKey(columns=[name])` on the `TableSpec` to make it
  the table's PK.

### pk\_uuid

```python
def pk_uuid(name: str = "id") -> ColumnSpec
```

Builds a deterministic UUID primary key column spec.

**Arguments**:

- `name` - Optional column name (default "id").
  

**Returns**:

  A `ColumnSpec` with `DataType.STRING` and a `UUIDColumn` generator.
  Output is reproducible: the same plan seed and row always yield the same
  UUID.

### pk\_pattern

```python
def pk_pattern(name: str, template: str) -> ColumnSpec
```

Builds a patterned string primary key column spec.

**Arguments**:

- `name` - Column name.
- `template` - Pattern template, e.g. `"CUST-{digit:6}"`. Supported
- `placeholders` - `{digit:N}`, `{alpha:N}`, `{hex:N}`, `{seq}`, `{uuid}`.
  

**Returns**:

  A `ColumnSpec` with `DataType.STRING` and a `PatternColumn` generator.

### fk

```python
def fk(name: str,
       ref: str,
       *,
       nullable: bool = False,
       null_fraction: float = 0.0,
       distribution: Distribution | None = None) -> ColumnSpec
```

Builds a foreign-key column spec referencing a parent table's primary key.

The data type and generation strategy are inferred from the referenced
primary key at resolution time, so the FK column always matches the parent's
type.

**Arguments**:

- `name` - Column name on the child table.
- `ref` - Column reference as `"table.column"`, e.g. `"customers.id"`.
- `nullable` - Optional flag for whether the column may be NULL (default False).
- `null_fraction` - Optional fraction of rows in [0.0, 1.0] to set NULL when
  nullable (default 0.0).
- `distribution` - Optional sampling distribution over parent rows (default
  None, meaning `Zipf(exponent=1.2)`, which skews children toward a
  small set of parents).
  

**Returns**:

  A `ColumnSpec` with a `ForeignKeyColumn` generator and an attached
  `ForeignKeyRef`.

### integer

```python
def integer(name: str,
            min: float | int = 0,
            max: float | int = 100,
            seed_from: str | None = None,
            **kw) -> ColumnSpec
```

Builds an integer column spec that draws values from `[min, max]`.

**Arguments**:

- `name` - Column name.
- `min` - Optional inclusive lower bound (default 0).
- `max` - Optional inclusive upper bound (default 100).
- `seed_from` - Optional name of another column to derive the cell seed from
  (default None).
- `**kw` - Extra `RangeColumn` fields, e.g. `distribution` or `step`.
  

**Returns**:

  A `ColumnSpec` with `DataType.INT` and a `RangeColumn` generator.

### double

```python
def double(name: str,
           min: float | int = 0.0,
           max: float | int = 1.0,
           seed_from: str | None = None,
           **kw) -> ColumnSpec
```

Builds a double column spec that draws values from `[min, max]`.

**Arguments**:

- `name` - Column name.
- `min` - Optional inclusive lower bound (default 0.0).
- `max` - Optional inclusive upper bound (default 1.0).
- `seed_from` - Optional name of another column to derive the cell seed from
  (default None).
- `**kw` - Extra `RangeColumn` fields, e.g. `distribution`.
  

**Returns**:

  A `ColumnSpec` with `DataType.DOUBLE` and a `RangeColumn` generator.
  

**Notes**:

  Use `decimal()` for fixed-precision values.

### decimal

```python
def decimal(name: str,
            min: float | int = 0.0,
            max: float | int = 1000.0,
            seed_from: str | None = None,
            precision: int | None = None,
            scale: int | None = None,
            **kw) -> ColumnSpec
```

Builds a decimal column spec that draws values from `[min, max]`.

**Arguments**:

- `name` - Column name.
- `min` - Optional inclusive lower bound (default 0.0).
- `max` - Optional inclusive upper bound (default 1000.0).
- `seed_from` - Optional name of another column to derive the cell seed from
  (default None).
- `precision` - Optional total number of digits (default None, meaning 10).
- `scale` - Optional number of fractional digits (default None, meaning 0).
- `**kw` - Extra `RangeColumn` fields, e.g. `distribution`.
  

**Returns**:

  A `ColumnSpec` with `DataType.DECIMAL` and a `RangeColumn` generator.
  

**Notes**:

  Use `double()` for floating point values.

### text

```python
def text(name: str,
         values: list[str],
         seed_from: str | None = None,
         **kw) -> ColumnSpec
```

Builds a string column spec that selects from a list of values.

**Arguments**:

- `name` - Column name.
- `values` - Candidate string values; each row selects one.
- `seed_from` - Optional name of another column to derive the cell seed from
  (default None).
- `**kw` - Extra `ValuesColumn` fields, e.g.
  `distribution=WeightedValues(weights={...})` for biased selection.
  

**Returns**:

  A `ColumnSpec` with `DataType.STRING` and a `ValuesColumn` generator.

### faker

```python
def faker(name: str,
          provider: str,
          *,
          dtype: DataType = DataType.STRING,
          locale: str | None = None,
          seed_from: str | None = None,
          **kwargs) -> ColumnSpec
```

Builds a column spec backed by a Faker provider.

Requires the `faker` extra (`pip install 'dbldatagen[core-faker]'`).

**Arguments**:

- `name` - Column name.
- `provider` - Faker method name, e.g. "email", "name", "street_address".
- `dtype` - Optional output type (default `DataType.STRING`).
- `locale` - Optional Faker locale, e.g. "de_DE" (default None, meaning
  "en_US").
- `seed_from` - Optional name of another column to derive the cell seed from
  (default None).
- `**kwargs` - Extra keyword arguments forwarded to the Faker provider method.
  

**Returns**:

  A `ColumnSpec` with the requested dtype and a `FakerColumn` generator.

### timestamp

```python
def timestamp(name: str,
              start: str,
              end: str,
              seed_from: str | None = None,
              **kw) -> ColumnSpec
```

Builds a timestamp column spec that draws values from `[start, end]`.

**Arguments**:

- `name` - Column name.
- `start` - Inclusive lower bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS").
- `end` - Inclusive upper bound as an ISO-8601 string ("YYYY-MM-DD" or
  "YYYY-MM-DD HH:MM:SS").
- `seed_from` - Optional name of another column to derive the cell seed from
  (default None).
- `**kw` - Extra `TimestampColumn` fields, e.g. `distribution`.
  

**Returns**:

  A `ColumnSpec` with `DataType.TIMESTAMP` and a `TimestampColumn`
  generator. Output is independent of the session time zone.

### pattern

```python
def pattern(name: str,
            template: str,
            seed_from: str | None = None) -> ColumnSpec
```

Builds a string column spec from a placeholder template.

**Arguments**:

- `name` - Column name.
- `template` - Pattern template, e.g. `"ORD-{digit:4}"`. Supported
- `placeholders` - `{digit:N}`, `{alpha:N}`, `{hex:N}`, `{seq}`, `{uuid}`.
- `seed_from` - Optional name of another column to derive the cell seed from
  (default None).
  

**Returns**:

  A `ColumnSpec` with `DataType.STRING` and a `PatternColumn` generator.

### expression

```python
def expression(name: str, expr: str) -> ColumnSpec
```

Builds a column spec computed from a Spark SQL expression.

The output type is always inferred by Spark from the expression; there is no
`dtype` parameter. To control the type, cast inside the expression, e.g.
`cast(quantity * unit_price as decimal(11, 2))`.

**Arguments**:

- `name` - Column name.
- `expr` - Spark SQL expression referencing other columns in the same table,
  e.g. `"quantity * unit_price"`.
  

**Returns**:

  A `ColumnSpec` with an `ExpressionColumn` generator.

### constant

```python
def constant(name: str,
             value: object,
             dtype: DataType | None = None) -> ColumnSpec
```

Builds a column spec where every row gets the same literal value.

**Arguments**:

- `name` - Column name.
- `value` - The literal value emitted on every row. Any type `F.lit` accepts.
- `dtype` - Optional type override (default None, meaning Spark infers it from
  the value).
  

**Returns**:

  A `ColumnSpec` with a `ConstantColumn` generator.

### struct

```python
def struct(name: str, fields: list[ColumnSpec]) -> ColumnSpec
```

Builds a nested struct column spec.

**Arguments**:

- `name` - Column name.
- `fields` - A list of column specs for each nested field. Each field is
  generated like a top-level column, with its own seed.
  

**Returns**:

  A `ColumnSpec` with a `StructColumn` generator; its type is derived from
  the fields.
  

**Notes**:

  `FakerColumn` and foreign-key columns are not allowed as struct fields;
  use them as top-level columns.
  
  Example::
  
  struct("address", [
  pattern("street", "{digit:3} {alpha:6} St"),
  text("city", ["Springfield", "Riverton", "Fairview"]),
  pattern("zip", "{digit:5}"),
  ])

### array

```python
def array(name: str,
          element: ColumnStrategy,
          min_length: int = 1,
          max_length: int = 5) -> ColumnSpec
```

Builds a variable-length array column spec.

**Arguments**:

- `name` - Column name.
- `element` - Generation strategy for a single element, e.g.
  `ValuesColumn(...)` or `RangeColumn(...)`.
- `min_length` - Optional inclusive minimum elements per row (default 1).
- `max_length` - Optional inclusive maximum elements per row (default 5).
  

**Returns**:

  A `ColumnSpec` with an `ArrayColumn` generator; array length is uniform
  in `[min_length, max_length]`.
  
  Example::
  
  array("tags", ValuesColumn(values=["sale", "new", "popular", "clearance"]),
  min_length=1, max_length=4)

