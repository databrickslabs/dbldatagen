---
sidebar_label: numeric
title: dbldatagen.core.engine.columns.numeric
---

Numeric column generation.

Generates int, long, float, double, and decimal values within a range, with
optional distribution control and step-based (lattice) snapping.

### build\_range\_column

```python
def build_range_column(id_column: Column | str,
                       column_seed: int,
                       min_val: float | int,
                       max_val: float | int,
                       distribution: Distribution | None = None,
                       dtype: DataType | None = None,
                       precision: int | None = None,
                       scale: int | None = None,
                       step: float | int | None = None) -> Column
```

Builds a numeric column with values sampled from a range.

The bounds are inclusive. Integer types produce a discrete range; float,
double, and decimal types produce continuous values. When `step` is set,
output is snapped to the lattice `min_val, min_val + step, ...` within the
range. A `Normal` distribution with explicit `mean`/`stddev` is sampled in
value units; other distributions sample uniformly over the range.

**Arguments**:

- `id_column` - Row-id column, given as a `Column` or column name.
- `column_seed` - Per-column seed.
- `min_val` - Inclusive lower bound.
- `max_val` - Inclusive upper bound; must be >= min_val.
- `distribution` - Optional sampling distribution (default None, meaning
  uniform).
- `dtype` - Optional target type; when None, inferred as LONG for integer
  bounds and DOUBLE otherwise (default None).
- `precision` - Optional total number of digits for DECIMAL (default None,
  meaning 10).
- `scale` - Optional number of fractional digits for DECIMAL (default None,
  meaning 0).
- `step` - Optional step size; when None, output is continuous
  (default None).
  

**Returns**:

  A Spark `Column` of the resolved type holding the sampled values.

