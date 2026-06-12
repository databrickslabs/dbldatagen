# Nested columns: structs and arrays

Two strategies add nesting once a row stops being flat: `StructColumn`
groups child columns into a Spark struct (a nested object in JSON), and
`ArrayColumn` produces a variable-length array of generated elements.

## When to use

- **`StructColumn`** — a grouped sub-record, like an `address` made of
  `street` / `city` / `zip`. Renders as a nested object in JSON.
- **`ArrayColumn`** — a repeated value, like a list of `tags` or
  `scores`. Each row's array has a random length within
  `[min_length, max_length]`.

Structs can nest other structs (and arrays), and arrays can hold structs
— so you can build arrays of records by combining the two.

## StructColumn

Each field is a full `ColumnSpec`, generated like a top-level column but
with its seed isolated per field. The struct's Spark type is derived
from its fields.

### Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import (
    StructColumn, ValuesColumn, RangeColumn, PatternColumn, DataType,
)

ColumnSpec(
    name="address",
    gen=StructColumn(
        fields=[
            ColumnSpec(name="street", dtype=DataType.STRING,
                       gen=PatternColumn(template="{digit:3} Main St")),
            ColumnSpec(name="city", dtype=DataType.STRING,
                       gen=ValuesColumn(values=["Austin", "NYC", "LA", "Chicago"])),
            ColumnSpec(name="zip", dtype=DataType.STRING,
                       gen=PatternColumn(template="{digit:5}")),
        ],
    ),
)
```

### DSL form

```python
from dbldatagen.core.spec import dsl as datagendg

datagendg.struct(
    "address",
    [
        datagendg.pattern("street", "{digit:3} Main St"),
        datagendg.text("city", ["Austin", "NYC", "LA", "Chicago"]),
        datagendg.pattern("zip", "{digit:5}"),
    ],
)
```

`struct(name, fields)` builds a `ColumnSpec` with a `StructColumn`; the
dtype is derived from the field list. Access a field downstream with dot
notation (`df.select("address.city")`).

### Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["struct"]` | `"struct"` | Discriminator; set automatically. |
| `fields` | `list[ColumnSpec]` | — (required) | Ordered list of child columns. Must be non-empty; names unique within the struct. Fields may themselves be `StructColumn` or `ArrayColumn` (nesting is supported). |

### Validation & gotchas

- **At least one field.** An empty `fields` list builds `struct<>`,
  which downstream schema inference can't resolve — rejected at plan
  time.
- **Field names must be unique within the struct.** Duplicates would
  build an invalid `StructType` and make `select("addr.x")` ambiguous;
  the validator rejects them naming the dupes.
- **`FakerColumn` and `ForeignKeyColumn` can't be struct fields.** Faker
  needs a column-level `pandas_udf` and FK resolution keys on the
  top-level `(table, column)` pair — neither works inside a struct. Use
  a scalar strategy (`RangeColumn`, `ValuesColumn`, `PatternColumn`,
  `ConstantColumn`, `ExpressionColumn`) for struct fields. (To get
  Faker-like data nested, generate it as a top-level column instead.)
- A field can carry its own `null_fraction` — null injection is
  field-local and doesn't null the surrounding struct.

## ArrayColumn

Each element is produced from the `element` strategy with a different
seed offset; the per-row length is random in `[min_length, max_length]`.

### Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import ArrayColumn, ValuesColumn, RangeColumn

# Array of categorical values
ColumnSpec(
    name="tags",
    gen=ArrayColumn(
        element=ValuesColumn(values=["sale", "new", "popular", "clearance"]),
        min_length=1,
        max_length=4,
    ),
)

# Array of numbers from a range
ColumnSpec(
    name="scores",
    gen=ArrayColumn(element=RangeColumn(min=0, max=100), min_length=2, max_length=5),
)
```

### DSL form

```python
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import ValuesColumn, RangeColumn

datagendg.array(
    "tags",
    ValuesColumn(values=["sale", "new", "popular", "clearance"]),
    min_length=1,
    max_length=4,
)

datagendg.array("scores", RangeColumn(min=0, max=100), min_length=2, max_length=5)
```

`array(name, element, min_length=1, max_length=5)` builds a `ColumnSpec`
with an `ArrayColumn`. The `element` is a strategy model (not a
`ColumnSpec`). For a fixed-length array, set `min_length == max_length`.

### Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["array"]` | `"array"` | Discriminator; set automatically. |
| `element` | `ColumnStrategy` | — (required) | Element-generation strategy. Any strategy except `FakerColumn` and `ForeignKeyColumn`. May itself be `StructColumn` or `ArrayColumn`. |
| `min_length` | `int` | `1` | Inclusive minimum length per row. Must be `>= 0` (`0` allows sometimes-empty arrays). |
| `max_length` | `int` | `5` | Inclusive maximum length per row. Must be `>= 1`, `>= min_length`, and `<= 1000`. |

### Validation & gotchas

- **`min_length >= 0`, `max_length >= 1`, `min_length <= max_length`.**
  Violations are rejected at plan time.
- **`max_length == 0` is rejected.** It always produces empty arrays
  with an ambiguous element type. For sometimes-empty arrays, set
  `min_length=0, max_length>=1`; to omit the data entirely, drop the
  column.
- **`max_length` is capped at `1000`.** The engine materialises one
  Spark expression per array slot; larger fan-outs stall Catalyst. For
  wider fan-out, restructure as one row per element (a long/narrow
  table) rather than a single wide array. Core has no map-column
  strategy, so a `MapType` column is not something it can generate.
- **`FakerColumn` and `ForeignKeyColumn` can't be the element type** —
  same reasons as struct fields. Wrap them in a `StructColumn` if you
  need an array of records (with the same restriction applying inside
  that struct).
- An array *column* with `null_fraction=1.0` yields NULL arrays (not
  empty arrays — `null` and `[]` are distinct in Spark). There is no
  per-element `null_fraction`.

## A worked example

The two combine naturally — a struct and an array side by side on one
table (adapted from `tests/core/engine/test_nested.py`):

```python
from dbldatagen.core import DataGenPlan, TableSpec, PrimaryKey, generate
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import ValuesColumn

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="items",
            rows=100,
            primary_key=PrimaryKey(columns=["item_id"]),
            columns=[
                datagendg.pk_auto("item_id"),
                datagendg.struct(
                    "address",
                    [
                        datagendg.text("city", ["Austin", "NYC", "LA", "Chicago"]),
                        datagendg.text("state", ["TX", "NY", "CA", "IL"]),
                        datagendg.integer("zip", min=10000, max=99999),
                    ],
                ),
                datagendg.array(
                    "tags",
                    ValuesColumn(values=["sale", "new", "popular", "clearance"]),
                    min_length=1,
                    max_length=4,
                ),
                datagendg.integer("price", min=1, max=500),
            ],
        ),
    ],
)

dfs = generate(spark, plan)
dfs["items"].select("address.city", "tags").show(5, truncate=False)
```

## See also

- [text-and-patterns.md](text-and-patterns.md) — `PatternColumn` /
  `FakerColumn`, and why Faker can't be nested
- [categorical-values.md](categorical-values.md) — `ValuesColumn`, a
  common struct field and array element
- [numeric-ranges.md](numeric-ranges.md) — `RangeColumn`, likewise
- [../concepts/plans-and-tables.md](../concepts/plans-and-tables.md) —
  `ColumnSpec`, the shape of every struct field
- [index.md](index.md) — all strategies at a glance
