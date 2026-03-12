---
sidebar_position: 1
title: "Architecture & Algorithms"
---

# Architecture Deep-Dive

> **TL;DR:** Technical internals for contributors and advanced users. Covers seed derivation, Feistel cipher for random unique PKs, FK reconstruction algorithm, flat select pattern, Faker pool strategy, streaming engine, and CDC state replay.

## Module Map

```
src/dbldatagen/v1/
├── __init__.py              # generate(), generate_stream(), generate_ingest() entry points
├── schema.py                # Pydantic v2 models (including StructColumn, ArrayColumn)
├── dsl.py                   # Convenience constructors (pk_auto, fk, integer, struct, array, etc.)
├── validation.py            # Post-generation referential integrity check
├── cdc.py                   # CDC public API: generate_cdc, generate_cdc_bulk, CDCStream
├── cdc_schema.py            # CDC Pydantic models: CDCPlan, CDCTableConfig, OperationWeights
├── cdc_dsl.py               # CDC convenience constructors: cdc_plan, cdc_config, ops, presets
├── ingest.py                # Ingest public API: generate_ingest, write_ingest_to_delta
├── ingest_schema.py         # Ingest Pydantic models: IngestPlan, IngestTableConfig
└── engine/
    ├── seed.py              # Seed derivation: derive_column_seed, cell_seed_expr, mix64
    ├── distributions.py     # Sampling: uniform, normal, zipf, exponential, weighted
    ├── generator.py         # Core pipeline: spark.range() -> select([cols]) -> DataFrame
    ├── planner.py           # FK resolution, topological sort, PK metadata extraction
    ├── fk.py                # FK generation via parent PK reconstruction
    ├── streaming.py         # Streaming generation via Spark rate source
    ├── utils.py             # Shared helpers: union_all, create_range_df, apply_null_fraction
    ├── cdc_state.py         # CDC batch size resolution (resolve_batch_size)
    ├── cdc_stateless.py     # Stateless "Three Clocks": birth_tick, death_tick, is_alive, update_due
    ├── cdc_generator.py     # CDC batch generation: inserts, updates (before/after), deletes
    ├── cdc_formats.py       # Output format application: raw, delta_cdf, sql_server, debezium
    ├── ingest_generator.py  # Ingest batch generation: snapshot, incremental, change detection
    └── columns/
        ├── numeric.py       # int/long/float/double/decimal via Spark SQL
        ├── temporal.py      # timestamp/date via epoch arithmetic
        ├── string.py        # values, patterns, constants, expressions
        ├── pk.py            # sequential, formatted, Feistel cipher (NumPy)
        ├── uuid.py          # deterministic UUID from double xxhash64
        └── faker_pool.py    # pool-based Faker via pandas_udf closure
```

---

## 1. Seed Derivation (`engine/seed.py`)

All randomness flows from a deterministic seed hierarchy.

### Phase 1: Column Seed (Planning Time)

`derive_column_seed(global_seed, table_name, column_name) -> int`

Computed once per column. Uses iterative character hashing:

```python
h = global_seed
for c in table_name:  h = (h * 31 + ord(c)) & MASK_64
for c in column_name: h = (h * 37 + ord(c)) & MASK_64
return to_signed64(h)
```

The result is a **signed** 64-bit integer (Java `long` range: -2^63 to 2^63-1) because PySpark's `F.lit(value).cast("long")` requires signed values. Unsigned 64-bit ints cause `NumberFormatException` in the JVM.

### Phase 2: Cell Seed (Execution Time)

`cell_seed_expr(column_seed, id_col) -> Column`

Returns the Spark SQL expression `xxhash64(lit(column_seed), id)`. Executed per row inside the Spark plan. `xxhash64` is a built-in Spark function — no UDF overhead.

### mix64 (Python-Side Hashing)

For Python-side seed mixing (e.g., Feistel round keys, Faker pool indices):

```python
def mix64(key, index) -> int:
    x = key ^ index
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9  # splitmix64 constants
    x = (x ^ (x >> 27)) * 0x94D049BB133111EB
    x = x ^ (x >> 31)
    return to_signed64(x)
```

Bijective for fixed `key`. Same algorithm as Java's `SplittableRandom`.

---

## 2. Column Generation Tiers

Columns are generated via one of three performance tiers:

### Tier 1: Pure Spark SQL Expressions

No Python UDFs. Compiled to JVM code by Catalyst. Fastest.

**Used for:** numeric ranges, timestamps/dates, value selection, sequential PKs, formatted PKs, UUID generation, constants, expressions, null injection.

Example (numeric range):
```python
cell_seed = F.xxhash64(F.lit(column_seed).cast("long"), id_col)
value = (F.abs(cell_seed) % F.lit(range_size)) + F.lit(min_val)
```

**Performance:** 50-200M rows/sec/core.

### Tier 2: pandas_udf (Arrow Batches)

Python UDFs that process Arrow batches via NumPy. Good performance.

**Used for:** Feistel cipher (random unique PKs), Faker pool selection.

Example (Faker pool):
```python
@pandas_udf(StringType())
def faker_udf(id_series: pd.Series) -> pd.Series:
    indices = id_series.apply(lambda i: mix64(seed, i) % pool_size)
    return pd.Series([pool[j] for j in indices])
```

**Performance:** 5-20M rows/sec/core.

### Tier 3: Scalar Python UDFs

Per-row Python function calls. Slowest. **Not used in dbldatagen.v1.**

---

## 3. Feistel Cipher for Random Unique PKs (`engine/columns/pk.py`)

### Problem

Generate N unique random-looking integers in `[0, N)` without shuffles, collects, or hash-based collision resolution.

### Solution: Feistel Network

A Feistel network is a cryptographic primitive that constructs a **bijective** (one-to-one) permutation from a non-bijective round function. If the input is `[0, N)`, the output is a permutation of `[0, N)` — guaranteed unique by mathematical construction.

### Algorithm

```
For each index i in [0, N):
  1. Split i into (left, right) halves of num_bits/2 each
  2. For 6 rounds:
     new_right = left XOR mix64(right, round_key)
     left = right
     right = new_right
  3. Combine: result = (left << half) | right
  4. If result >= N: re-apply (cycle walking)
```

### NumPy Vectorization

The key insight: Feistel rounds have **no data dependency between rows**. All operations (XOR, shift, multiply) are element-wise. The entire batch is processed as NumPy int64 arrays.

```python
_MIX_C1 = np.int64(-0x40A7B4924E0B5A67)  # 0xBF58476D1CE4E5B9 as signed int64

def _mix64_vec(values: np.ndarray, key: np.int64) -> np.ndarray:
    x = values ^ key
    x = (x ^ (x >> np.int64(30))) * _MIX_C1
    x = (x ^ (x >> np.int64(27))) * _MIX_C2
    x = x ^ (x >> np.int64(31))
    return x
```

NumPy int64 multiplication wraps on overflow, which is the correct behavior for hash mixing (same as C/Java).

### Cycle Walking for Non-Power-of-2

When N is not a power of 2, the Feistel domain `2^k` is larger than N. Some outputs land outside `[0, N)`. We re-apply the permutation **only to those elements** until they fall in range.

```python
for _ in range(64):  # safety bound
    out_of_range = result >= N
    if not out_of_range.any():
        break
    result[out_of_range] = feistel_round(result[out_of_range], ...)
```

Expected iterations: < 2 (since for `N > 2^(k-1)`, more than half the domain maps to `[0, N)`).

### Performance

~20-50M rows/sec/core via `pandas_udf`. Approaches Tier 1 for large batches.

---

## 4. FK Generation Algorithm (`engine/fk.py`)

### The Core Insight

If parent PK = `f(seed, row_index)` and parent has N rows, then a valid FK is:

```
fk_value = f(seed, random_index)    where random_index in [0, N)
```

We never materialize the parent table. We only need its **metadata**: PK function type, seed, row count, and parameters (start/step for sequential, template for pattern, etc.).

### Algorithm

```python
def build_fk_column(id_col, column_seed, fk_resolution):
    # 1. Per-cell seed
    seed_col = xxhash64(column_seed, id_col)

    # 2. Map seed to parent row index via distribution
    parent_index = apply_distribution(seed_col, N_parent, distribution)

    # 3. Reconstruct parent PK value
    fk_value = reconstruct_parent_pk(parent_index, pk_metadata)

    # 4. Null injection
    if null_fraction > 0:
        fk_value = when(null_mask, None).otherwise(fk_value)

    return fk_value
```

### Parent PK Reconstruction

`_reconstruct_parent_pk` applies the same function the parent table used:

| Parent PK Type | Reconstruction |
|----------------|----------------|
| Sequential | `parent_index * step + start` |
| Pattern | `build_pattern_column(parent_index, parent_seed, template)` |
| UUID | `build_uuid_column(parent_index, parent_seed)` |

This is why FK values are **guaranteed** to be valid parent PKs — they're computed by the same function.

### Why This Is Better Than Range Matching

dbldatagen v0's approach: generate FK values in the same numeric range as the parent PK and hope they match. This breaks when:
- Parent PK uses a non-trivial strategy (Feistel, UUID, pattern)
- Parent PK has gaps (filtered rows, non-sequential)
- You need exact referential integrity for join correctness

Our approach: reconstruct the exact PK value the parent would have produced for a given row index. **Integrity by construction, not by coincidence.**

---

## 5. Plan Resolution (`engine/planner.py`)

### What `resolve_plan` Does

1. **Validate FK references** — every `"table.column"` ref must point to an existing table and column.
2. **Validate PK constraints** — referenced columns must be part of the table's `PrimaryKey`.
3. **Extract PK metadata** — for each referenced parent, capture the PK function type, seed, row count, and parameters.
4. **Build dependency graph** — `{child_table: {parent_tables}}` from FK references.
5. **Topological sort** — Kahn's algorithm. Raises `ValueError` on circular dependencies.

### Output: `ResolvedPlan`

```python
@dataclass
class ResolvedPlan:
    generation_order: list[str]         # ["customers", "products", "orders", "order_items"]
    fk_resolutions: dict[               # (table, column) -> FK metadata
        tuple[str, str], FKResolution
    ]
    plan: DataGenPlan                   # original plan
```

### Topological Sort

Uses Kahn's algorithm (BFS-based). Tables with no parent dependencies are generated first.

```
Input:  order_items -> orders -> customers
                    -> products

Output: [customers, products, orders, order_items]
```

Cycle detection: if the sorted output has fewer tables than the input, a cycle exists.

---

## 6. Generator Pipeline (`engine/generator.py`)

### Per-Table Pipeline

```python
def generate_table(spark, table_spec, resolved_plan):
    # 1. spark.range(N) with renamed id to avoid user column collisions
    df = spark.range(row_count).withColumnRenamed("id", "_synth_row_id")
    id_col = F.col("_synth_row_id")

    # 2. Build Tier 1 column expressions
    col_exprs = [build_column_expr(col) for col in tier1_columns]

    # 3. Flat select (avoids O(n^2) planning)
    df = df.select(id_col, *col_exprs)

    # 4. Apply Tier 2 columns (FK, Faker) via withColumn
    for name, expr in udf_columns:
        df = df.withColumn(name, expr)

    # 5. Drop internal row-id (never part of output schema)
    df = df.drop("_synth_row_id")

    return df
```

### Why Flat select Instead of Chained withColumn

Spark's query planner has O(N^2) cost for N chained `withColumn` calls because each creates a new `Project` node that copies all previous columns. A single `select([all_columns])` creates one flat `Project` node.

For a table with 50 columns:
- Chained `withColumn`: 50 Project nodes, ~2500 column references
- Flat `select`: 1 Project node, 50 column references

### Column Dispatch

The generator dispatches each `ColumnSpec` to the appropriate builder based on the `gen` strategy type:

| Strategy | Builder | Tier |
|----------|---------|------|
| `RangeColumn` | `build_range_column` | 1 |
| `ValuesColumn` | `build_values_column` | 1 |
| `PatternColumn` | `build_pattern_column` | 1 |
| `SequenceColumn` | `build_sequential_pk` | 1 |
| `UUIDColumn` | `build_uuid_column` | 1 |
| `ExpressionColumn` | `build_expression_column` | 1 |
| `TimestampColumn` | `build_timestamp_column` | 1 |
| `ConstantColumn` | `build_constant_column` | 1 |
| `StructColumn` | `_build_struct_column` | 1 |
| `ArrayColumn` | `_build_array_column` | 1 |
| `FakerColumn` | `build_faker_column` | 2 |
| FK columns | `build_fk_column` | 1-2 |

---

## 7. Faker Pool Strategy (`engine/columns/faker_pool.py`)

### Problem

Faker generates values sequentially. `fake.name()` returns different results based on call order. This breaks partition-independence.

### Solution: Pre-generated Pool

1. **Driver-side**: Create a Faker instance with a deterministic seed derived from `column_seed`. Call the provider 10,000 times to build a pool.
2. **Executor-side**: A `pandas_udf` selects from the pool using `mix64(column_seed, row_id) % pool_size`.

### Spark Connect Compatibility

The pool is captured via Python closure, not `sc.broadcast()`. This works through Spark Connect because closures are serialized with the UDF.

```python
def build_faker_column(id_col, column_seed, provider, ...):
    pool = _generate_pool(column_seed, provider, pool_size)  # driver-side

    @pandas_udf(StringType())
    def _select_from_pool(id_series: pd.Series) -> pd.Series:
        # pool captured via closure
        indices = id_series.apply(lambda i: mix64(column_seed, i) % len(pool))
        return pd.Series([pool[j] for j in indices])

    return _select_from_pool(id_col)
```

### Pool Size and Cardinality

Default pool size: 10,000. For 1M rows, each pool entry is used ~100 times on average. For columns needing higher cardinality, increase `pool_size` or combine pools (e.g., 5K first names x 5K last names = 25M unique combinations).

---

## 8. Distribution Sampling (`engine/distributions.py`)

### Uniform

```python
abs(cell_seed) % n
```

Slight modular bias when `2^64 % n != 0`, but negligible for `n << 2^64`.

### Normal (Central Limit Theorem)

Sum 12 independent uniform `[0, 1)` values, subtract 6.0. This approximates `N(0, 1)` by the CLT. Then scale: `value = mean + stddev * z_score`.

```python
z = sum(xxhash64(seed + i, id) % 10000 / 10000 for i in range(12)) - 6.0
```

### Zipf (Power Law)

Approximate inverse CDF: `index = floor(N * u^(1/(1-s)))` where `u` is uniform `[0, 1)` and `s` is the exponent. Clamp to `[0, N)`.

### Weighted

CASE/WHEN chain with cumulative weight boundaries:

```python
CASE WHEN hash < 5500 THEN "page_view"
     WHEN hash < 8000 THEN "click"
     ...
END
```

---

## 9. Validation (`validation.py`)

Post-generation integrity check using left anti joins:

```python
orphans = child_fk.join(parent_pk, fk == pk, "left_anti")
if orphans.count() > 0:
    errors.append(f"{child}.{fk_col} has {count} orphan FK values")
```

This is optional and not called by default. It triggers a Spark job (the anti join).

---

## 10. Spark Connect Compatibility

dbldatagen.v1 is designed for Spark 4+ where Connect is the default. The entire library uses only:

| API | Status |
|-----|--------|
| `spark.range(n)` | Works |
| `df.select()` / `df.withColumn()` | Works |
| `F.xxhash64()`, `F.abs()`, `F.when()`, `F.concat()`, etc. | Works |
| `@pandas_udf` | Works (since Spark 3.5+) |
| `F.lit()`, `F.col()`, `F.expr()` | Works |

**Not used:** `SparkContext`, `RDD`, `sc.parallelize()`, `sc.broadcast()`, custom `Partitioner`, `Accumulator`.

Faker pools use closure capture instead of `sc.broadcast()` for Connect compatibility. The pool array is serialized with the UDF closure via Arrow.

---

## 11. Struct and Array Column Generation

### Struct Columns (`_build_struct_column`)

Struct columns recursively call `build_column_expr` for each child field, then wrap the results in `F.struct()`:

```python
def _build_struct_column(gen, id_col, parent_seed, row_count, global_seed):
    field_cols = []
    for field_spec in gen.fields:
        child_seed = derive_column_seed(parent_seed, "", field_spec.name)
        child_expr = build_column_expr(field_spec, id_col, child_seed, ...)
        field_cols.append(child_expr.alias(field_spec.name))
    return F.struct(*field_cols)
```

Child seeds are derived from the parent column seed + child field name, ensuring each nested field gets unique deterministic values. Null injection is applied per-field.

### Array Columns (`_build_array_column`)

Array columns generate `max_length` elements with unique seed offsets, combine into `F.array()`, then trim to a random length:

```python
for i in range(gen.max_length):
    elem_seed = column_seed ^ ((i + 1) * 0x9E3779B9)  # golden ratio mixing
    elem_expr = build_column_expr(dummy_spec, id_col, elem_seed, ...)
    element_cols.append(elem_expr)

full_array = F.array(*element_cols)
rand_len = (abs(cell_seed) % range_size) + min_length
return F.slice(full_array, 1, rand_len)
```

Element seeds use XOR with `(i+1) * 0x9E3779B9` (golden ratio constant) to ensure good distribution across elements. The random length per row is deterministic.

### CDC Compatibility

Both struct and array columns work through CDC automatically because `build_column_expr` is the shared dispatch point used by both `generate_table` and the CDC generator's `_generate_with_id`.

---

## 12. CDC Engine Architecture

The CDC engine generates deterministic change streams without materialising prior batches.

### Stateless "Three Clocks" Model (`engine/cdc_stateless.py`)

The primary CDC engine uses a stateless modular-recurrence model. Each row's entire lifecycle is determined by three clocks derived from its identity seed:

```python
birth_tick(k)   = k  # row k is born at batch ceil(k / inserts_per_batch)
death_tick(k)   = birth_tick(k) + min_life + (hash(k) % lifespan_range)
update_due(k,t) = (t - birth_tick(k)) % update_period == 0
is_alive(k, t)  = birth_tick(k) <= t < death_tick(k)
```

**Key properties:**
- **O(1) per row** — no iterative state replay needed
- **No state accumulation** — row liveness is a pure function of identity + batch
- **Batch-independent** — any batch N can be generated in isolation
- **`CDCPeriods`** dataclass precomputes all constants from `CDCTableConfig`

Range-scan functions (`insert_range`, `delete_indices_at_batch_fast`, `update_indices_at_batch`) compute exactly which rows are inserted/deleted/updated at any batch.

### Batch Size Resolution (`engine/cdc_state.py`)

The `resolve_batch_size` function converts `CDCTableConfig.batch_size` (fraction, absolute, or shorthand) into an integer row count for a given initial table size.

### Batch Generation (`engine/cdc_generator.py`)

For each batch, the generator:

1. **Computes row indices** for insert/update/delete using the Three Clocks model
2. **Generates row images** using `generate_for_indices` (reuses the core `build_column_expr` pipeline)
3. **Attaches operation metadata** (`_op` = I/U/UB/D)

```
CDCPeriods (precomputed from config)
  → compute insert_range, update_indices, delete_indices for batch N
  → generate_for_indices(spark, table_spec, indices)
  → tag with operation type
  → return RawBatchResult(inserts, updates_before, updates_after, deletes)
```

For fused multi-batch generation (`generate_cdc_bulk`), all batches are unioned into a single `spark.range()` call per operation type with a `_write_batch` column, using **map-literal seed lookup** (`element_at(map, _write_batch)`) to avoid Catalyst explosion from thousands of CASE WHEN branches.

### Output Formats (`engine/cdc_formats.py`)

The `apply_format` function transforms the raw `_op` column to the target format:

| Format | Column | Transformation |
|--------|--------|---------------|
| `raw` | `_op` | No transformation (I/U/UB/D) |
| `delta_cdf` | `_change_type` | I→insert, U→update_postimage, UB→update_preimage, D→delete |
| `sql_server` | `__$operation` | I→2, UB→3, U→4, D→1 |
| `debezium` | `op` | I→c, U→u, D→d (UB rows dropped) |

### FK Parent Delete Guard

When a table has FK dependents (e.g., `customers` referenced by `orders.customer_id`), the CDC engine automatically disables deletes for that table to maintain referential integrity:

```python
if _table_has_fk_dependents(plan, table_name):
    config = config.model_copy(update={
        "operations": OperationWeights(insert=..., update=..., delete=0),
    })
```

This is applied consistently in both batch generation and state replay.

### Batch Independence

Any batch can be generated independently:

```python
batch_5 = generate_cdc_batch(spark, plan, batch_id=5)
```

This works because `compute_table_state_at_batch` replays state from metadata (seed + weights + row counts) without any DataFrame materialisation. The state at batch 4 is computed, then batch 5's operations are applied to generate the change set.

---

## 13. Streaming Engine (`engine/streaming.py`)

### Why Streaming Works Without Changing Column Builders

The key architectural insight: **no leaf column builder uses `row_count`**. Every builder operates on `(column_seed, id_col)` — a per-column seed and a row identifier column. The `row_count` parameter exists in `build_column_expr` only for recursive dispatch to struct/array children.

This means the column generation pipeline is agnostic to whether `id_col` comes from `spark.range(N)` (batch) or `spark.readStream.format("rate")` (streaming).

### Rate Source as `spark.range()` Substitute

Spark's built-in `rate` source emits `(timestamp: TimestampType, value: LongType)` where `value` is monotonically increasing — the same role as `spark.range()`'s `id` column.

```
spark.readStream.format("rate")
        │
        │  produces: (timestamp, value)
        │            value: 0, 1, 2, 3, ...  (monotonically increasing)
        │
        ▼
rename "value" → "_synth_row_id"     ← same internal name as batch
        │
        ▼
apply column expressions               ← identical to batch pipeline
        │  cell_seed = xxhash64(lit(column_seed), _synth_row_id)
        │  build_range_column, build_values_column, etc.
        │
        ▼
apply UDF columns (Faker) via withColumn
        │
        ▼
drop "_synth_row_id"
        │
        ▼
streaming DataFrame
```

### Code Reuse

`generate_stream()` reuses the batch pipeline's core dispatch function `build_column_expr()` from `engine/generator.py`. No column builder was modified. The streaming module only adds:

1. **Rate source setup** — `spark.readStream.format("rate").option("rowsPerSecond", N)`
2. **Validation** — reject strategies incompatible with unbounded streams
3. **Same select/withColumn pattern** — Tier 1 columns via flat `select`, Faker via `withColumn`

### What Doesn't Work in Streaming (and Why)

Feistel cipher (random-unique PK) requires a **fixed row count N** that is unknown in an unbounded stream. The bijective permutation is defined on `[0, N)` — N must be known to compute `num_bits = ceil(log2(N))` and the cycle-walking domain.

This is detected at call time by `_validate_streaming_spec()` and raises `ValueError` — no silent data corruption.

### FK Columns in Streaming

FK columns work when `parent_specs` is provided. The helper `_resolve_streaming_fk()` builds `FKResolution` objects by:

1. Parsing the `foreign_key.ref` string → `(parent_table_name, parent_col_name)`
2. Looking up the parent in `parent_specs`
3. Calling the existing `_extract_pk_metadata()` from `planner.py`
4. Constructing an `FKResolution` identical to what `resolve_plan()` produces in batch mode

The resulting `FKResolution` is passed to `build_fk_column()` — the same function used in batch generation. No FK code was modified.

### Validation Rules

```python
def _validate_streaming_spec(table_spec, parent_specs=None):
    # 1. FK columns require parent_specs
    if col.foreign_key is not None:
        if parent_specs is None: raise ValueError(...)
        if parent_table not in parent_specs: raise ValueError(...)
        # Also validates: column exists, column is a PK

    # 2. PK columns must use streaming-compatible strategies
    #    Allowed: SequenceColumn, PatternColumn, UUIDColumn
    #    Rejected: RangeColumn (maps to Feistel in PK context)
    if col.name in pk_cols and not isinstance(gen, (SequenceColumn, PatternColumn, UUIDColumn)):
        raise ValueError(...)
```

### Streaming-Compatible PK Strategies

| PK Strategy | Works? | Reason |
|-------------|--------|--------|
| `SequenceColumn` | Yes | `id * step + start` — works with any id range |
| `PatternColumn` | Yes | Template filled from `cell_seed_expr(seed, id)` |
| `UUIDColumn` | Yes | `xxhash64(seed, id)` — deterministic per row |
| `RangeColumn` (Feistel) | No | Needs fixed N for bijective permutation domain |

### Determinism

For a given `seed` and row id, column values are always identical. Two runs of the same streaming query with the same seed produce the same data for the same row ids.

However, the rate source assigns ids based on wall-clock timing, so the *set of rows in a given time window* varies between runs. Determinism is **per-row** (same id → same data), not **per-run** (same duration → same total rows).

---

---

## 14. Ingest Pipeline Architecture (`ingest.py`, `ingest_schema.py`, `engine/ingest_generator.py`)

The ingest pipeline is a higher-level abstraction over CDC for landing data into Delta tables.

### Pipeline Shape

```
IngestPlan
  ├── base_plan: DataGenPlan      ← schema + row counts
  ├── mode: INCREMENTAL | SNAPSHOT
  ├── strategy: SYNTHETIC | STATELESS | DELTA
  ├── num_batches: int
  └── table_configs: {name: IngestTableConfig}
         ├── batch_size (fraction or absolute)
         ├── insert_fraction, update_fraction, delete_fraction
         └── min_life, update_window

generate_ingest(spark, plan)
  → IngestStream
        ├── initial: dict[str, DataFrame]    ← full snapshot at batch 0
        └── batches: LazyBatchList           ← on-demand per-batch generation
```

### Strategy Dispatch

Three strategies share the same `IngestStream` output interface but differ in how change rows are selected:

| Strategy | Engine | State | Best for |
|----------|--------|-------|----------|
| `SYNTHETIC` | Stateful CDC (`cdc_state.py`) | O(rows) driver-side array | Complete CDC fidelity, &lt;100M rows |
| `STATELESS` | Three Clocks (`cdc_stateless.py`) | O(1) — pure function | 100M+ rows, maximum throughput |
| `DELTA` | Delta read + anti-join | Reads Delta table | Maximum realism, live Delta required |

### STATELESS Strategy: Three Range Scans

For each batch N the engine executes three independent `spark.range()` scans:

```python
# 1. Inserts — rows born at batch N
inserts_df = spark.range(birth_start, birth_end)

# 2. Updates — rows due for update at batch N (modular stride)
updates_df = spark.range(target_mod, scan_end, update_period)

# 3. Deletes — rows whose death_tick == N
deletes_df = spark.range(...).filter(death_tick == batch_id)
```

The tight upper bound on the update scan (`scan_end = target_mod + updates_per_batch * update_period`) eliminates the need for `.limit()`, keeping the plan fully pipelined with no `CollectLimit` exchange.

### write_ingest_to_delta and chunk_size

`write_ingest_to_delta` writes in two phases:
1. **Initial snapshot** — `mode("overwrite")`, creates version 0
2. **Batches** — `mode("append")`, one Delta version per `chunk_size` batches

With `chunk_size=1` (default), each of N batches is a separate Spark job. With `chunk_size=N`, all batches are unioned into a single Spark job and one Delta commit. At large scale (100M+ rows per batch), `chunk_size=num_batches` significantly reduces driver overhead and the number of Delta transactions.

The ~2h driver gap at 30B scale between the snapshot job completing and the batch job starting is a known optimization target: the driver spends this time constructing N lazy batch DataFrames before submitting the union plan.

### Metadata Columns

`write_ingest_to_delta` adds optional metadata to each row:

| Column | Added when |
|--------|------------|
| `_batch_id` | `include_batch_id=True` (default) |
| `_op_type` | INCREMENTAL mode: `"I"`, `"U"`, `"D"` |
| `_ingest_ts` | `include_load_timestamp=True` (default) |

### detect_changes

`detect_changes(before_df, after_df, key_columns)` uses three Spark joins:
- **Inserts** — left anti-join: `after LEFT ANTI JOIN before ON key`
- **Deletes** — left anti-join: `before LEFT ANTI JOIN after ON key`
- **Updates** — inner join on key + filter where any data column differs

---

## See Also

- [API Reference](./api.md) — Public API functions and parameters
- [Schema Models](./schema-models.md) — Pydantic model field reference
- [Ingest Guide](../ingestion/overview.md) — User-facing ingest pipeline guide
- [Scaling Guide](../data-generation/scaling.md) — Configuration for 100B+ row workloads
- [Examples](../guides/basic.md) — Practical usage examples
