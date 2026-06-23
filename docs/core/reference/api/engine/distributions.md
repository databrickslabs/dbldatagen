---
sidebar_label: distributions
title: dbldatagen.core.engine.distributions
---

Distribution sampling via Spark SQL expressions.

Each function maps a per-cell seed column (int64) to a sampled index or value
using only Spark's built-in expressions (no Python UDFs).

### uniform\_sample

```python
def uniform_sample(cell_seed_col: Column, n: int) -> Column
```

Maps a seed column to a uniform index in `[0, n)`.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `n` - Exclusive upper bound on the index; must be positive.
  

**Returns**:

  A Spark long `Column` of uniform indices in `[0, n)`.
  

**Raises**:

- `ValueError` - If `n <= 0`.

### weighted\_sample\_expr

```python
def weighted_sample_expr(cell_seed_col: Column, values: list,
                         weights: dict[str, float]) -> Column
```

Selects from `values` using cumulative weights.

Falls back to uniform selection when all weights are zero.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `values` - The list of allowed values.
- `weights` - Mapping of value (rendered as `str`) to relative weight. Values
  absent from the mapping get weight 0 and are never selected.
  

**Returns**:

  A Spark `Column` whose element type matches `values`.

### normal\_sample\_expr

```python
def normal_sample_expr(cell_seed_col: Column,
                       mean: float = 0.0,
                       stddev: float = 1.0) -> Column
```

Samples `N(mean, stddev)` using a Box-Muller transform.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `mean` - Optional distribution mean (default 0.0).
- `stddev` - Optional standard deviation (default 1.0).
  

**Returns**:

  A Spark double `Column` of `N(mean, stddev)` samples.
  

**Notes**:

  Output is unbounded. Callers that need a bounded result must clamp it.

### normal\_value\_expr

```python
def normal_value_expr(cell_seed_col: Column,
                      min_val: float,
                      max_val: float,
                      mean: float | None = None,
                      stddev: float | None = None) -> Column
```

Samples a normal distribution clamped to a specified value range.

`mean` and `stddev` are in the range's value units. When unset, the bell is
auto-centered: `mean` becomes the midpoint `(min_val + max_val) / 2` and
`stddev` becomes `(max_val - min_val) / 6`.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `min_val` - Inclusive lower bound, in value units.
- `max_val` - Inclusive upper bound, in value units.
- `mean` - Optional peak in value units (default None, meaning the midpoint).
- `stddev` - Optional spread in value units (default None, meaning
  (max_val - min_val) / 6).
  

**Returns**:

  A Spark double `Column` of values in `[min_val, max_val]`.

### zipf\_sample\_expr

```python
def zipf_sample_expr(cell_seed_col: Column,
                     n: int,
                     exponent: float = 1.5) -> Column
```

Samples a Zipf-like distribution over `[0, n)` using an inverse power-law CDF.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `n` - Exclusive upper bound on the index; `n <= 1` returns 0.
- `exponent` - Optional power-law exponent; must be > 1.0 (default 1.5).
  

**Returns**:

  A Spark long `Column` of indices in `[0, n)`.
  

**Raises**:

- `ValueError` - If `exponent <= 1.0`.

### exponential\_sample\_expr

```python
def exponential_sample_expr(cell_seed_col: Column,
                            n: int,
                            rate: float = 1.0) -> Column
```

Samples an exponential distribution over `[0, n)` using an inverse CDF.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `n` - Exclusive upper bound on the index; `n <= 1` returns 0.
- `rate` - Optional rate parameter (lambda); must be positive (default 1.0).
  

**Returns**:

  A Spark long `Column` of indices in `[0, n)`.

### lognormal\_sample\_expr

```python
def lognormal_sample_expr(cell_seed_col: Column,
                          n: int,
                          mean: float = 0.0,
                          stddev: float = 1.0) -> Column
```

Samples a log-normal distribution over `[0, n)`.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `n` - Exclusive upper bound on the index; `n <= 1` returns 0.
- `mean` - Optional mean of the underlying normal (default 0.0).
- `stddev` - Optional standard deviation of the underlying normal
  (default 1.0).
  

**Returns**:

  A Spark long `Column` of indices in `[0, n)`.

### apply\_distribution

```python
def apply_distribution(cell_seed_col: Column, n: int,
                       distribution: Distribution | None) -> Column
```

Routes a `Distribution` to its sampling function.

**Arguments**:

- `cell_seed_col` - Per-cell seed `Column` (long).
- `n` - Exclusive upper bound on the output index.
- `distribution` - The distribution to sample under (None means uniform).
  

**Returns**:

  A Spark long `Column` of indices in `[0, n)`.
  

**Raises**:

- `ValueError` - If `distribution` is `WeightedValues` (handled out-of-band
  by `weighted_sample_expr`).
- `TypeError` - If `distribution` is an unhandled `Distribution` subclass.

