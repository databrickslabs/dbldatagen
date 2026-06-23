---
sidebar_label: generator
title: dbldatagen.core.engine.generator
---

Core generation engine that turns a table spec into a Spark DataFrame.

Starts from `spark.range(rows)`, routes each column to its builder, and applies
the resulting expressions to produce the table.

### generate\_table

```python
def generate_table(spark: SparkSession,
                   table_spec: TableSpec,
                   resolved_plan: ResolvedPlan | None = None) -> DataFrame
```

Generates a single table as a deterministic Spark DataFrame.

`table_spec.seed` must be set -- typically by going through `DataGenPlan`,
which propagates the plan seed to each table. When `resolved_plan` is given,
foreign-key columns resolve against its parent metadata; without it, FK
columns raise.

**Arguments**:

- `spark` - Active `SparkSession` used to build the DataFrame.
- `table_spec` - The table's schema, row count, and per-table seed.
- `resolved_plan` - Optional resolved plan carrying FK resolution info; it
  must contain a table named `table_spec.name` (default None).
  

**Returns**:

  A DataFrame with `table_spec.rows` rows and one column per `ColumnSpec`.
  Output is deterministic for a given seed.
  

**Raises**:

- `ValueError` - If `table_spec.seed` is None, or `resolved_plan` does not
  contain a table named `table_spec.name`.

### build\_all\_column\_exprs

```python
def build_all_column_exprs(
    table_spec: TableSpec,
    id_column: Column,
    fk_resolutions: dict[tuple[str, str], FKResolution] | None = None,
    *,
    seed: int,
    row_count: int = 0
) -> tuple[list[Column], list[tuple[str, Column]], list[tuple[str, Column]]]
```

Builds the column expressions for every column in a table.

Returns the three groups consumed by `apply_column_phases`: plain Spark SQL
columns, UDF-based columns (FK and Faker), and `seed_from`-derived columns.

**Arguments**:

- `table_spec` - The table whose columns to build.
- `id_column` - Row-id column.
- `fk_resolutions` - Optional map of `(table, column)` to `FKResolution`
  (default None, meaning no FK columns).
- `seed` - Base seed for per-column seed derivation. Derive it from
  `table_spec.seed` so reruns stay reproducible.
- `row_count` - Optional number of rows, forwarded to builders (default 0).
  

**Returns**:

  A tuple `(col_exprs, udf_columns, seeded_columns)` for
  `apply_column_phases`.

### build\_column\_expr

```python
def build_column_expr(col_spec: ColumnSpec, id_column: Column,
                      column_seed: int, row_count: int,
                      global_seed: int) -> Column
```

Maps a column spec to its strategy's builder and returns the column.

**Arguments**:

- `col_spec` - The column to build.
- `id_column` - Row-id column.
- `column_seed` - Per-column seed.
- `row_count` - Number of rows, forwarded to builders that need it.
- `global_seed` - Plan-level seed, forwarded to nested struct/array seeds.
  

**Returns**:

  A Spark `Column` of generated values.
  

**Raises**:

- `TypeError` - If `col_spec.gen` is a `ForeignKeyColumn` (FK resolution
  must run earlier via `build_fk_column_expr`).
- `ValueError` - If `col_spec.gen` is an unsupported strategy (including
  `FakerColumn`, which is handled out-of-band).
  

**Notes**:

  `FakerColumn` and `ForeignKeyColumn` are not dispatched here. Faker
  goes through a column-level UDF, and foreign keys resolve against the
  FK metadata at the table level.

