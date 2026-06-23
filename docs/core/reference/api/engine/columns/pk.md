---
sidebar_label: pk
title: dbldatagen.core.engine.columns.pk
---

Sequential primary key generation.

Builds an integer key as `start + id * step` directly from each row's index.

### build\_sequential\_pk

```python
def build_sequential_pk(id_column: Column | str,
                        start: int = 1,
                        step: int = 1) -> Column
```

Builds a sequential primary key column.

The value at row `i` is `start + i * step`, cast to long.

**Arguments**:

- `id_column` - Row-id column, given as a `Column` or column name.
- `start` - Optional value at row 0 (default 1).
- `step` - Optional increment per row; may be negative (default 1).
  

**Returns**:

  A Spark long `Column` holding `start + id * step`.

