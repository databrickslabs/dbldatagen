---
sidebar_label: seed
title: dbldatagen.core.engine.seed
---

Deterministic seed derivation for partition-independent generation.

All randomness derives from a global seed, the table name, the column name, and
the row index. Seeds are derived in two phases: `derive_column_seed` computes a
per-column seed at planning time, and `cell_seed_expr` turns it into a per-row
seed at execution time.

### derive\_column\_seed

```python
def derive_column_seed(global_seed: int, table_name: str,
                       column_name: str) -> int
```

Derives a per-column seed from the global seed, table name, and column name.

The same inputs always produce the same seed.

**Arguments**:

- `global_seed` - The plan-level seed.
- `table_name` - Name of the table the column belongs to. Pass an empty
  string when chaining seeds for nested struct fields.
- `column_name` - Name of the column or struct field.
  

**Returns**:

  A signed 64-bit integer seed.

### cell\_seed\_expr

```python
def cell_seed_expr(column_seed: int, id_column: Column | str = "id") -> Column
```

Builds a Spark expression for a per-row seed.

Combines the column seed with the row ID. The result is independent of how
the data is partitioned.

**Arguments**:

- `column_seed` - Per-column seed.
- `id_column` - Optional row-id column, given as a `Column` or column name
  (default "id").
  

**Returns**:

  A Spark `Column` holding the per-row seed.

### uniform\_fraction

```python
def uniform_fraction(seed_column: Column) -> Column
```

Maps a hash column to a uniform double in [0.0, 1.0).

**Arguments**:

- `seed_column` - A long-typed seed or hash column.
  

**Returns**:

  A Spark double `Column` uniformly distributed in [0.0, 1.0).

### null\_mask\_expr

```python
def null_mask_expr(column_seed: int, id_column: Column | str,
                   null_fraction: float) -> Column
```

Builds a boolean column that is True for rows that should be NULL.

Uses a seed decorrelated from the column's value seed, so null placement is
independent of the generated values. A row is marked NULL when its uniform
draw in [0.0, 1.0) falls below `null_fraction`.

**Arguments**:

- `column_seed` - Per-column seed.
- `id_column` - Row-id column, given as a `Column` or column name.
- `null_fraction` - Fraction of rows to mark NULL, in [0.0, 1.0]. 0.0 yields
  all False; values >= 1.0 yield all True.
  

**Returns**:

  A Spark boolean `Column`, True for rows to emit as NULL.

### to\_signed64

```python
def to_signed64(n: int) -> int
```

Converts a Python int to the signed 64-bit range used by Spark.

Out-of-range values wrap using two's-complement, matching the Java long
semantics Spark relies on.

**Arguments**:

- `n` - Any Python integer.
  

**Returns**:

  `n` mapped into [-2**63, 2**63 - 1].

### seed\_xor

```python
def seed_xor(column_seed: int, constant: int) -> int
```

Derives a decorrelated sub-seed by XORing a column seed with a constant.

Used to draw several independent random values from the same base seed.
Clamps the value to Spark's signed 64-bit range.

**Arguments**:

- `column_seed` - Per-column seed.
- `constant` - A distinct constant per sub-draw.
  

**Returns**:

  The XOR of the inputs, mapped into the signed 64-bit range.

