---
sidebar_label: faker_pool
title: dbldatagen.core.engine.columns.faker_pool
---

Pool-based Faker column generation for realistic text data.

Generates a fixed-size pool of Faker values on the driver, then selects from it
per row on the executors. The pool travels via closure rather than a broadcast
variable, for Spark Connect compatibility.

### build\_faker\_column

```python
def build_faker_column(id_column: Column,
                       column_seed: int,
                       provider: str,
                       kwargs: dict | None = None,
                       locale: str = "en_US",
                       pool_size: int = 10_000) -> Column
```

Builds a string column of realistic Faker values.

Generates a fixed-size pool of values on the driver using a seeded `Faker`
instance, then selects a pool entry per row from the column seed and row id.
Output is reproducible for a given seed.

**Arguments**:

- `id_column` - Row-id column.
- `column_seed` - Per-column seed. Seeds the driver-side pool and selects
  entries at execution time.
- `provider` - Faker provider method name (e.g. "first_name", "company").
- `kwargs` - Optional keyword arguments forwarded to the provider
  (default None, treated as no arguments).
- `locale` - Faker locale such as "de_DE" (default "en_US").
- `pool_size` - Optional number of values to pre-generate (default 10000).
  

**Returns**:

  A Spark string `Column` of per-row Faker values.
  

**Raises**:

- `ImportError` - If the `faker` package is not installed.
- `ValueError` - If `provider` is not a method on the Faker instance.
  

**Notes**:

  A column has at most `pool_size` distinct values regardless of row
  count, so large tables will repeat values.

### build\_faker\_expr

```python
def build_faker_expr(col_spec: ColumnSpec, id_column: Column,
                     column_seed: int) -> tuple[str, Column]
```

Builds a Faker column expression for the column-building loop.

Constructs the Faker pool expression via `build_faker_column` and wraps it
with the null mask when the null fraction is positive.

**Arguments**:

- `col_spec` - The column's spec; its `gen` must be a `FakerColumn`.
- `id_column` - Row-id column.
- `column_seed` - Per-column seed.
  

**Returns**:

  A tuple `(column_name, expr)` for the Faker column.
  

**Raises**:

- `TypeError` - If `col_spec.gen` is not a `FakerColumn`.

