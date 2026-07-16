---
sidebar_label: utils
title: dbldatagen.core.engine.utils
---

Shared helpers for building table DataFrames.

Provides the base range DataFrame, null-fraction wrapping, and the phased
application of column expressions.

### create\_range\_df

```python
def create_range_df(spark: SparkSession,
                    row_count: int) -> tuple[DataFrame, Column]
```

Creates the base row-ID DataFrame for a table.

Returns a one-column DataFrame backed by `spark.range`, with the column
named `_synth_row_id` to avoid clashing with a user column named `id`. The
column is internal and dropped before the final result is returned.

**Arguments**:

- `spark` - Active `SparkSession`.
- `row_count` - Number of rows to generate. Must be non-negative.
  

**Returns**:

  A tuple `(df, id_column)`: the DataFrame and a `Column` reference to its
  `_synth_row_id` column for use in downstream expressions.

### apply\_null\_fraction

```python
def apply_null_fraction(column: Column, column_seed: int, id_column: Column,
                        null_fraction: float) -> Column
```

Wraps a column expression so a fraction of its rows become NULL.

**Arguments**:

- `column` - The column expression to wrap.
- `column_seed` - Per-column seed.
- `id_column` - Row-id column.
- `null_fraction` - Fraction of rows to set to NULL, in [0.0, 1.0].
  

**Returns**:

  The original expression when `null_fraction` is 0 or less; otherwise an
  expression that yields NULL for the selected rows and `column` for the rest.

### apply\_column\_phases

```python
def apply_column_phases(df: DataFrame, id_column: Column,
                        col_exprs: list[Column],
                        udf_columns: list[tuple[str, Column]],
                        seeded_columns: list[tuple[str, Column]]) -> DataFrame
```

Applies column expressions to the base DataFrame and returns the table.

Builds the table in three passes: plain Spark SQL columns via a single
`select`, then UDF-based columns (foreign keys, Faker) via `withColumn`, then
`seed_from` columns, which depend on a column added in an earlier pass. The
internal `_synth_row_id` column is dropped at the end.

**Arguments**:

- `df` - Base DataFrame containing the `_synth_row_id` column.
- `id_column` - Column reference to `_synth_row_id`.
- `col_exprs` - Plain Spark SQL column expressions, applied first.
- `udf_columns` - `(name, column)` pairs for UDF-based columns.
- `seeded_columns` - `(name, column)` pairs for `seed_from`-derived columns.
  

**Returns**:

  The table DataFrame, without `_synth_row_id`. Columns follow phase order;
  `generate_table` re-selects them into the declared order.

