---
sidebar_label: string
title: dbldatagen.core.engine.columns.string
---

String column generation: values, patterns, constants, and expressions.

### build\_values\_column

```python
def build_values_column(id_column: Column | str,
                        column_seed: int,
                        values_list: list,
                        distribution: Distribution | None = None) -> Column
```

Builds a column that selects from a list of values.

`WeightedValues` selects using cumulative weights; other distributions index
into the value list, uniformly by default.

**Arguments**:

- `id_column` - Row-id column, given as a `Column` or column name.
- `column_seed` - Per-column seed.
- `values_list` - The allowed values. An empty list yields an all-NULL
  column; a single-element list yields that value on every row.
- `distribution` - Optional sampling distribution (default None, meaning
  uniform).
  

**Returns**:

  A Spark `Column` whose element type matches the values.

### build\_pattern\_column

```python
def build_pattern_column(id_column: Column | str, column_seed: int,
                         template: str) -> Column
```

Builds a string column from a template such as "ORD-{digit:4}-{alpha:3}".

Supported placeholders:
{seq}     - row sequence number
{uuid}    - deterministic UUID (36 chars, no width modifier)
{digit:N} - N random digits
{alpha:N} - N random uppercase letters
{hex:N}   - N random hex characters

**Arguments**:

- `id_column` - Row-id column, given as a `Column` or column name.
- `column_seed` - Per-column seed.
- `template` - Template string with literal text and placeholders.
  

**Returns**:

  A Spark string `Column` with values matching the template.
  

**Raises**:

- `ValueError` - If `{uuid}` carries a width modifier, or a `{kind:N}` width
  exceeds its limit ({digit} 18, {hex} 15, {alpha} 64, {seq} 24).

