---
sidebar_label: fk
title: dbldatagen.core.engine.fk
---

Foreign key generation with guaranteed referential integrity.

Generates child FK values by reconstructing the primary key the parent table
would have produced for a chosen row index. Every FK value matches a real parent
key, and the parent table never has to be materialized to look it up.

### build\_fk\_column

```python
def build_fk_column(id_column: Column, column_seed: int,
                    fk_resolution: FKResolution) -> Column
```

Builds a column of foreign-key values that are valid parent keys.

Derives a per-row seed, samples a parent row index using the configured
distribution, and reconstructs the parent's primary key for that index. When
the null fraction is positive, a fraction of rows are emitted as NULL.

**Arguments**:

- `id_column` - Row-id column.
- `column_seed` - Per-column seed for the FK column.
- `fk_resolution` - Resolved FK information carrying the parent key metadata,
  sampling distribution, and null fraction.
  

**Returns**:

  A Spark `Column` whose type matches the parent key (long for
  sequence/UUID keys, string for pattern keys).

### build\_fk\_column\_expr

```python
def build_fk_column_expr(
    col_spec: ColumnSpec, table_name: str, id_column: Column, column_seed: int,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None
) -> tuple[str, Column]
```

Builds an FK column expression, raising if its resolution is missing.

Used by the engine's column-building loop. A missing resolution is an error
rather than a silent all-NULL column, so calling `generate_table` directly
requires passing a `ResolvedPlan` that includes this column's FK.

**Arguments**:

- `col_spec` - The FK column's spec.
- `table_name` - Name of the table that owns the column.
- `id_column` - Row-id column.
- `column_seed` - Per-column seed.
- `fk_resolutions` - Map of `(table, column)` to its `FKResolution`, or None.
  

**Returns**:

  A tuple `(column_name, expr)` for the FK column.
  

**Raises**:

- `TypeError` - If no `FKResolution` exists for this column.

