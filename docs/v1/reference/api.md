---
sidebar_position: 2
title: "API Reference"
---

# API Reference

> **TL;DR:** Complete reference for all public APIs. Entry point is `generate(spark, plan)`. DSL functions provide convenient constructors. CDC APIs support change stream generation.

## Core API

### `generate(spark, plan) -> dict[str, DataFrame]`

Generate all tables from a `DataGenPlan`.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan` | `DataGenPlan` | Plan defining tables, columns, and relationships |

**Returns:** `dict[str, DataFrame]` keyed by table name. Tables are generated in dependency order (parents first).

**Example:**

```python
from dbldatagen.v1 import generate, DataGenPlan

plan = DataGenPlan(tables=[customers, orders], seed=42)
dfs = generate(spark, plan)

customers_df = dfs["customers"]
orders_df = dfs["orders"]
```

**Notes:**
- Tables with foreign key relationships are automatically ordered (topological sort)
- FK values are guaranteed to reference valid parent PKs
- Generation is deterministic for a given seed

---

### `generate_stream(spark, table_spec, *, rows_per_second=1000, num_partitions=None, parent_specs=None) -> DataFrame`

Generate a single streaming DataFrame from a `TableSpec` using Spark's `rate` source.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark` | `SparkSession` | required | Active Spark session |
| `table_spec` | `TableSpec` | required | Single table definition (the `rows` field is ignored) |
| `rows_per_second` | `int` | `1000` | How many rows the rate source emits per second |
| `num_partitions` | `int \| None` | `None` | Number of streaming partitions (Spark default if omitted) |
| `parent_specs` | `dict[str, TableSpec] \| None` | `None` | Parent table definitions for FK resolution |

**Returns:** A streaming `DataFrame` (`df.isStreaming == True`) with all specified columns.

**Example — basic (no FK):**

```python
from dbldatagen.v1 import TableSpec, PrimaryKey, generate_stream
from dbldatagen.v1.dsl import pk_auto, faker, integer, text, timestamp

spec = TableSpec(
    name="events",
    rows=0,
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        faker("user", "user_name"),
        integer("score", min=0, max=100),
        text("level", values=["bronze", "silver", "gold"]),
        timestamp("event_time", start="2024-01-01", end="2025-12-31"),
    ],
    seed=42,
)

sdf = generate_stream(spark, spec, rows_per_second=500)
assert sdf.isStreaming

query = sdf.writeStream.format("console").start()
query.awaitTermination(10)
query.stop()
```

**Example — with FK columns:**

```python
from dbldatagen.v1 import TableSpec, PrimaryKey, generate_stream
from dbldatagen.v1.dsl import pk_auto, fk, integer

customers = TableSpec(
    name="customers", rows=10_000, seed=42,
    primary_key=PrimaryKey(columns=["id"]),
    columns=[pk_auto("id")],
)

orders = TableSpec(
    name="orders", rows=0, seed=42,
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        fk("customer_id", "customers.id"),
        integer("amount", min=10, max=1000),
    ],
)

sdf = generate_stream(
    spark, orders,
    rows_per_second=500,
    parent_specs={"customers": customers},
)
# Every customer_id is a valid PK from customers (1..10000)
```

**Supported column types:** All strategies work in streaming mode **except** Feistel (random-unique) PKs. FK columns require `parent_specs`. Use `SequenceColumn`, `PatternColumn`, or `UUIDColumn` for streaming PKs.

**Raises:** `ValueError` if the spec contains unsupported PK strategies, or FK columns without `parent_specs`.

**Notes:**
- Determinism is per-row: same seed + same row id → same values
- The rate source's `value` column serves the same role as `spark.range()`'s `id`
- `parent_specs` tables are not materialised — only PK metadata is used

---

## DSL Functions

Convenience constructors that return `ColumnSpec` instances. Import from `dbldatagen.v1` or `dbldatagen.v1.dsl`.

### Primary Key Helpers

#### `pk_auto(name="id") -> ColumnSpec`

Auto-incrementing integer primary key.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | `"id"` | Column name |

**Returns:** `ColumnSpec` with `dtype=LONG`, `gen=SequenceColumn(start=1, step=1)`

**Example:**

```python
from dbldatagen.v1 import pk_auto

pk_auto("user_id")
# Generates: 1, 2, 3, 4, ...
```

---

#### `pk_uuid(name="id") -> ColumnSpec`

Deterministic UUID primary key.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | `"id"` | Column name |

**Returns:** `ColumnSpec` with `dtype=STRING`, `gen=UUIDColumn()`

**Example:**

```python
from dbldatagen.v1 import pk_uuid

pk_uuid("order_id")
# Generates: "a3f1b2c4-d5e6-f7a8-b9c0-d1e2f3a4b5c6"
```

---

#### `pk_pattern(name, template) -> ColumnSpec`

Patterned string primary key.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Column name |
| `template` | `str` | Pattern template (see placeholders below) |

**Placeholders:**
- `{digit:N}` — N random digits
- `{alpha:N}` — N random uppercase letters
- `{hex:N}` — N random hex characters (lowercase)
- `{seq}` — Row sequence number
- `{uuid}` — Deterministic UUID

**Returns:** `ColumnSpec` with `dtype=STRING`, `gen=PatternColumn(template)`

**Examples:**

```python
from dbldatagen.v1 import pk_pattern

pk_pattern("customer_id", "CUST-{digit:8}")
# Generates: CUST-00000001, CUST-00000002, ...

pk_pattern("order_id", "ORD-{digit:4}-{alpha:3}")
# Generates: ORD-3847-KMX, ORD-9281-PLQ, ...

pk_pattern("transaction_id", "TXN-{hex:12}")
# Generates: TXN-a3f1b2c4d5e6, TXN-7f8e9d0c1b2a, ...
```

---

### Foreign Key Helper

#### `fk(name, ref, *, nullable=False, null_fraction=0.0, distribution=None) -> ColumnSpec`

Foreign key column referencing another table's primary key.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `ref` | `str` | required | Reference in format `"table.column"` |
| `nullable` | `bool` | `False` | Allow NULL FK values |
| `null_fraction` | `float` | `0.0` | Fraction of rows with NULL FK (auto-sets `nullable=True`) |
| `distribution` | `Distribution \| None` | `Zipf(exponent=1.2)` | Parent row selection distribution |

**Returns:** `ColumnSpec` with `foreign_key` set. The dtype and generation strategy are inferred from the referenced parent PK at plan resolution time.

**Examples:**

```python
from dbldatagen.v1 import fk
from dbldatagen.v1.schema import Zipf, Uniform

# Basic FK with default Zipf distribution
fk("customer_id", "customers.customer_id")

# Uniform FK distribution (every parent equally likely)
fk("product_id", "products.product_id", distribution=Uniform())

# Heavy skew FK (power users)
fk("user_id", "users.user_id", distribution=Zipf(exponent=2.0))

# Nullable FK (70% NULL)
fk("referrer_id", "users.user_id", null_fraction=0.7)
```

**FK Guarantees:**
- Every non-NULL FK value references a valid parent PK
- No parent table materialization (values are reconstructed)
- Works with all PK strategies (sequential, UUID, pattern)

---

### Column Shorthands

#### `integer(name, min=0, max=100, **kw) -> ColumnSpec`

Integer column with range.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `min` | `int` | `0` | Minimum value (inclusive) |
| `max` | `int` | `100` | Maximum value (inclusive) |
| `**kw` | | | Additional kwargs: `distribution`, `nullable`, `null_fraction` |

**Returns:** `ColumnSpec` with `dtype=INT`, `gen=RangeColumn(min, max, **kw)`

**Examples:**

```python
from dbldatagen.v1 import integer
from dbldatagen.v1.schema import Normal, Exponential

integer("age", min=18, max=90)
integer("quantity", min=1, max=100, distribution=Exponential(rate=0.5))
integer("score", min=0, max=100, distribution=Normal(mean=0.7, stddev=0.2))
```

---

#### `decimal(name, min=0.0, max=1000.0, **kw) -> ColumnSpec`

Floating-point column with range.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `min` | `float` | `0.0` | Minimum value |
| `max` | `float` | `1000.0` | Maximum value |
| `**kw` | | | Additional kwargs: `distribution`, `nullable`, `null_fraction` |

**Returns:** `ColumnSpec` with `dtype=DOUBLE`, `gen=RangeColumn(min, max, **kw)`

**Examples:**

```python
from dbldatagen.v1 import decimal
from dbldatagen.v1.schema import LogNormal

decimal("price", min=9.99, max=999.99)
decimal("latitude", min=-90.0, max=90.0)
decimal("salary", min=30000.0, max=500000.0, distribution=LogNormal(mean=11.0, stddev=0.5))
```

---

#### `text(name, values, **kw) -> ColumnSpec`

Categorical text column.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Column name |
| `values` | `list[str]` | List of allowed values |
| `**kw` | | Additional kwargs: `distribution`, `nullable`, `null_fraction` |

**Returns:** `ColumnSpec` with `dtype=STRING`, `gen=ValuesColumn(values, **kw)`

**Examples:**

```python
from dbldatagen.v1 import text
from dbldatagen.v1.schema import WeightedValues

text("status", values=["active", "inactive", "pending"])

text("tier", values=["free", "pro", "enterprise"],
     distribution=WeightedValues(weights={
         "free": 0.70, "pro": 0.25, "enterprise": 0.05
     }))
```

---

#### `faker(name, provider, *, dtype=STRING, locale=None, **kwargs) -> ColumnSpec`

Generate data using a Faker provider.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `provider` | `str` | required | Faker provider method name |
| `dtype` | `DataType` | `STRING` | Output data type |
| `locale` | `str \| None` | `None` | Faker locale (e.g., `"de_DE"`, `"ja_JP"`) |
| `**kwargs` | | | Arguments passed to the Faker provider |

**Returns:** `ColumnSpec` with `gen=FakerColumn(provider, kwargs, locale)`

**Examples:**

```python
from dbldatagen.v1 import faker

faker("name", "name")
faker("email", "email")
faker("phone", "phone_number")
faker("address", "street_address")

# With locale
faker("name", "name", locale="de_DE")
faker("city", "city", locale="ja_JP")

# With provider kwargs
faker("dob", "date_of_birth", minimum_age=18, maximum_age=80)
faker("company", "company", locale="en_US")
```

**Provider Reference:** See [Faker documentation](https://faker.readthedocs.io/en/master/providers.html) for full provider list.

---

#### `timestamp(name, start="2020-01-01", end="2025-12-31", **kw) -> ColumnSpec`

Timestamp column within a date range.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `start` | `str` | `"2020-01-01"` | Start date (ISO 8601) |
| `end` | `str` | `"2025-12-31"` | End date (ISO 8601) |
| `**kw` | | | Additional kwargs: `distribution`, `nullable`, `null_fraction` |

**Returns:** `ColumnSpec` with `dtype=TIMESTAMP`, `gen=TimestampColumn(start, end, **kw)`

**Examples:**

```python
from dbldatagen.v1 import timestamp
from dbldatagen.v1.schema import Normal

timestamp("created_at", start="2023-01-01", end="2025-12-31")

# Cluster timestamps in middle of range
timestamp("event_time", start="2025-01-01", end="2025-12-31",
          distribution=Normal(mean=0.5, stddev=0.15))
```

**Note:** Set `dtype=DataType.DATE` on the `ColumnSpec` to generate dates instead of timestamps.

---

#### `pattern(name, template) -> ColumnSpec`

Patterned string column (non-PK).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Column name |
| `template` | `str` | Pattern template (see `pk_pattern` for placeholders) |

**Returns:** `ColumnSpec` with `dtype=STRING`, `gen=PatternColumn(template)`

**Example:**

```python
from dbldatagen.v1 import pattern

pattern("tracking_code", "TRK-{digit:8}-{alpha:2}")
pattern("serial_number", "SN-{hex:12}")
```

---

#### `expression(name, expr, dtype=None) -> ColumnSpec`

Computed column using Spark SQL expression.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Column name |
| `expr` | `str` | Spark SQL expression referencing sibling columns |
| `dtype` | `DataType \| None` | Output data type (inferred if None) |

**Returns:** `ColumnSpec` with `gen=ExpressionColumn(expr)`

**Examples:**

```python
from dbldatagen.v1 import expression

expression("full_name", "concat(first_name, ' ', last_name)")
expression("line_total", "quantity * unit_price")
expression("tax", "subtotal * 0.08")
expression("is_adult", "CASE WHEN age >= 18 THEN true ELSE false END")
```

---

### Nested Type Helpers

#### `struct(name, fields) -> ColumnSpec`

Nested struct column (produces JSON object).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Column name |
| `fields` | `list[ColumnSpec]` | Child field definitions |

**Returns:** `ColumnSpec` with `gen=StructColumn(fields)`

**Example:**

```python
from dbldatagen.v1 import struct, faker, text
from dbldatagen.v1.schema import ColumnSpec, RangeColumn, ValuesColumn

struct("address", fields=[
    ColumnSpec(name="street", gen=FakerColumn(provider="street_address")),
    ColumnSpec(name="city", gen=FakerColumn(provider="city")),
    ColumnSpec(name="state", gen=FakerColumn(provider="state_abbr")),
    ColumnSpec(name="zip", gen=RangeColumn(min=10000, max=99999)),
])
```

**Accessing nested fields:**

```python
df.select("address.city", "address.zip")
df.filter(F.col("address.state") == "CA")
```

---

#### `array(name, element, min_length=1, max_length=5) -> ColumnSpec`

Variable-length array column.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `element` | `ColumnStrategy` | required | Strategy for generating each element |
| `min_length` | `int` | `1` | Minimum array length |
| `max_length` | `int` | `5` | Maximum array length |

**Returns:** `ColumnSpec` with `gen=ArrayColumn(element, min_length, max_length)`

**Examples:**

```python
from dbldatagen.v1 import array
from dbldatagen.v1.schema import ValuesColumn, RangeColumn

array("tags",
    element=ValuesColumn(values=["sale", "new", "premium", "clearance"]),
    min_length=1, max_length=4,
)

array("scores",
    element=RangeColumn(min=0, max=100),
    min_length=5, max_length=10,
)
```

---

## CDC API

### Entry Points

#### `generate_cdc(spark, plan_or_base, num_batches=None, format=None) -> CDCStream`

Generate a complete CDC stream (initial snapshot + all batches).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan_or_base` | `CDCPlan \| DataGenPlan` | Full CDC plan or base DataGenPlan |
| `num_batches` | `int \| None` | Override number of batches (required if passing DataGenPlan) |
| `format` | `str \| None` | Override output format: `"raw"`, `"delta_cdf"`, `"sql_server"`, `"debezium"` |

**Returns:** `CDCStream` with attributes:
- `.initial` — `dict[str, DataFrame]` initial snapshot
- `.batches` — `list[dict[str, DataFrame]]` change batches
- `.plan` — The resolved `CDCPlan`

**Example:**

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

plan = cdc_plan(
    base,
    num_batches=5,
    format="delta_cdf",
    users=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)),
)

stream = generate_cdc(spark, plan)
stream.initial["users"]       # DataFrame -- full initial snapshot
stream.batches[0]["users"]    # DataFrame -- batch 1 changes
```

---

#### `generate_cdc_bulk(spark, plan_or_base, num_batches=None, format=None, chunk_size=None) -> CDCStream`

Generate a CDC stream optimised for large-scale cluster execution. Batches are grouped into chunks so each Spark job processes millions of rows.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan_or_base` | `CDCPlan \| DataGenPlan` | Full CDC plan or base DataGenPlan |
| `num_batches` | `int \| None` | Override number of batches |
| `format` | `str \| None` | Override output format |
| `chunk_size` | `int \| None` | Batches per chunk (auto-calculated if None, targeting ~20M rows per chunk) |

**Returns:** `CDCStream` — usage identical to `generate_cdc()`, but each iteration of `stream.batches` yields a chunk containing multiple logical batches unioned together.

**Example:**

```python
from dbldatagen.v1.cdc import generate_cdc_bulk

stream = generate_cdc_bulk(spark, plan)
stream.initial["table"].write.format("delta").save(...)
for chunk in stream.batches:
    chunk["table"].write.format("delta").mode("append").save(...)
```

---

#### `precompute_cdc_plans(plan_or_base, num_batches=None, format=None) -> dict[str, list[BatchPlan]]`

Pre-compute all batch plans for a CDC stream (pure Python, no Spark needed).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `plan_or_base` | `CDCPlan \| DataGenPlan` | CDC plan or base DataGenPlan |
| `num_batches` | `int \| None` | Override for batch count |
| `format` | `str \| None` | Output format override |

**Returns:** `dict[str, list[BatchPlan]]` mapping table_name -> list of `BatchPlan` (one per batch), each containing insert/update/delete indices.

**Example:**

```python
from dbldatagen.v1.cdc import precompute_cdc_plans

plans = precompute_cdc_plans(cdc_plan)
for batch_plan in plans["orders"]:
    print(f"Batch {batch_plan.batch_id}: "
          f"{batch_plan.insert_count} inserts, "
          f"{len(batch_plan.update_indices)} updates, "
          f"{len(batch_plan.delete_indices)} deletes")
```

---

#### `generate_cdc_batch(spark, plan_or_base, batch_id, format=None) -> dict[str, DataFrame]`

Generate a single CDC batch independently.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan_or_base` | `CDCPlan \| DataGenPlan` | CDC plan or base DataGenPlan |
| `batch_id` | `int` | Batch number (1-indexed) |
| `format` | `str \| None` | Override output format |

**Returns:** `dict[str, DataFrame]` keyed by table name.

**Example:**

```python
from dbldatagen.v1.cdc import generate_cdc_batch

# Generate batch 5 without materializing batches 1-4
batch_5 = generate_cdc_batch(spark, plan, batch_id=5)
batch_5["orders"].show()
```

---

#### `generate_expected_state(spark, plan_or_stream, table_name, batch_id) -> DataFrame`

Generate the expected table state at a given batch.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan_or_stream` | `CDCPlan \| CDCStream` | CDC plan or stream |
| `table_name` | `str` | Table name |
| `batch_id` | `int` | Batch number (1-indexed) |

**Returns:** `DataFrame` with the expected table state after applying batch N.

**Example:**

```python
from dbldatagen.v1.cdc import generate_expected_state

expected = generate_expected_state(spark, plan, "orders", batch_id=5)

# Compare against pipeline output
pipeline_output = spark.table("my_pipeline.orders")
diff = expected.subtract(pipeline_output).count()
assert diff == 0, "Pipeline output differs"
```

---

#### `write_cdc_to_delta(spark, plan_or_base, *, catalog, schema, num_batches=None, format=None, chunk_size=None) -> dict[str, str]`

Generate a CDC stream and write it to Delta tables in one call.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark` | `SparkSession` | required | Active Spark session |
| `plan_or_base` | `CDCPlan \| DataGenPlan` | required | CDC plan or base DataGenPlan |
| `catalog` | `str` | required | Unity Catalog catalog name |
| `schema` | `str` | required | Unity Catalog schema name |
| `num_batches` | `int \| None` | `None` | Override batch count |
| `format` | `str \| CDCFormat \| None` | `None` | Override output format |
| `chunk_size` | `int \| None` | `None` | Batches per Delta version (auto-calculated if None) |

**Returns:** `dict[str, str]` mapping table_name -> fully qualified UC table path.

**Example:**

```python
from dbldatagen.v1.cdc import write_cdc_to_delta

paths = write_cdc_to_delta(
    spark, plan,
    catalog="my_catalog",
    schema="my_schema",
)
# paths = {"orders": "my_catalog.my_schema.orders", ...}
```

---

### CDC DSL Functions

#### `cdc_plan(base, num_batches=5, format="raw", **table_configs) -> CDCPlan`

Build a `CDCPlan` with per-table configs as keyword arguments.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `base` | `DataGenPlan` | required | Base schema for initial data |
| `num_batches` | `int` | `5` | Number of change batches |
| `format` | `str` | `"raw"` | Output format: `"raw"`, `"delta_cdf"`, `"sql_server"`, `"debezium"` |
| `**table_configs` | | | Per-table config: `table_name=cdc_config(...)` |

**Returns:** `CDCPlan`

**Example:**

```python
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

plan = cdc_plan(
    base,
    num_batches=10,
    format="delta_cdf",
    customers=cdc_config(batch_size=0.05, operations=ops(2, 7, 1)),
    orders=cdc_config(batch_size=0.10, operations=ops(5, 4, 1)),
)
```

---

#### `cdc_config(batch_size=0.1, operations=None, mutations_spec=None) -> CDCTableConfig`

Build a `CDCTableConfig` with convenient defaults.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | `int \| float \| str` | `0.1` | Rows per batch (fraction, absolute, or shorthand like `"10K"`) |
| `operations` | `OperationWeights \| None` | `OperationWeights()` | Insert/update/delete mix |
| `mutations_spec` | `MutationSpec \| None` | `MutationSpec()` | Which columns mutate on updates |

**Returns:** `CDCTableConfig`

---

#### `ops(insert=3.0, update=5.0, delete=2.0) -> OperationWeights`

Shorthand for `OperationWeights`.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `insert` | `float` | `3.0` | Relative insert weight |
| `update` | `float` | `5.0` | Relative update weight |
| `delete` | `float` | `2.0` | Relative delete weight |

**Returns:** `OperationWeights`

**Example:**

```python
ops(5, 3, 2)   # 50% inserts, 30% updates, 20% deletes
ops(10, 0, 0)  # append-only (inserts only)
```

---

#### `mutations(columns=None, fraction=0.5) -> MutationSpec`

Shorthand for `MutationSpec`.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `columns` | `list[str] \| None` | `None` | Columns eligible for mutation (None = all non-PK/FK columns) |
| `fraction` | `float` | `0.5` | Fraction of eligible columns that mutate per update |

**Returns:** `MutationSpec`

---

### CDC Presets

#### `append_only_config(batch_size=0.1) -> CDCTableConfig`

Inserts only (event logs, audit trails).

**Returns:** `CDCTableConfig` with `operations=OperationWeights(insert=1, update=0, delete=0)`

---

#### `high_churn_config(batch_size=0.2) -> CDCTableConfig`

Heavy updates + deletes (stress testing). 10% inserts, 60% updates, 30% deletes.

**Returns:** `CDCTableConfig` with `operations=OperationWeights(insert=1, update=6, delete=3)`

---

#### `scd2_config(batch_size=0.1) -> CDCTableConfig`

Moderate updates, few inserts, rare deletes (SCD2 testing). 10% inserts, 80% updates, 10% deletes.

**Returns:** `CDCTableConfig` with `operations=OperationWeights(insert=1, update=8, delete=1)`

---

#### `rename_cdc_columns(df, format="sql_server") -> DataFrame`

Rename CDC metadata columns to clean, SQL-friendly names. Useful for SQL Server format where column names contain `$` characters.

**Renames for `sql_server` format:**
- `__$operation` -> `cdc_operation`
- `__$start_lsn` -> `cdc_lsn`
- `__$seqval` -> `cdc_seqval`

**Example:**

```python
from dbldatagen.v1.cdc_dsl import rename_cdc_columns

stream = generate_cdc_bulk(spark, plan)
clean_df = rename_cdc_columns(stream.initial["my_table"])
clean_df.write.format("delta").saveAsTable("my_catalog.my_schema.my_table")
```

---

## Validation

### `validate_referential_integrity(dataframes, plan) -> list[str]`

Post-generation check that all FK values reference valid parent PKs.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataframes` | `dict[str, DataFrame]` | Generated DataFrames from `generate()` |
| `plan` | `DataGenPlan` | The plan used for generation |

**Returns:** `list[str]` — Empty list if all FK relationships are valid. Error messages listing orphan counts otherwise.

**Example:**

```python
from dbldatagen.v1.validation import validate_referential_integrity

errors = validate_referential_integrity(dfs, plan)
assert errors == [], f"Integrity errors: {errors}"
```

**Note:** This validator triggers a Spark job (left anti join per FK relationship). It's optional and not called by default.

---

## Ingest API

### `generate_ingest(spark, plan_or_base, *, num_batches=None, mode=None, strategy=None) -> IngestStream`

Generate a complete ingest stream (initial snapshot + batches).

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark` | `SparkSession` | required | Active Spark session |
| `plan_or_base` | `IngestPlan \| DataGenPlan` | required | Full ingest plan or base DataGenPlan |
| `num_batches` | `int \| None` | `None` | Override number of batches |
| `mode` | `str \| None` | `None` | Output mode: `"incremental"` or `"snapshot"` |
| `strategy` | `str \| None` | `None` | Generation strategy: `"synthetic"` or `"stateless"` |

**Returns:** `IngestStream` with `.initial` (snapshot), `.batches` (lazy list), `.plan`.

**Example:**

```python
from dbldatagen.v1 import generate_ingest

stream = generate_ingest(spark, base_plan, num_batches=10, mode="incremental")
stream.initial["orders"].show()
stream.batches[0]["orders"].show()
```

---

### `generate_ingest_batch(spark, plan_or_base, batch_id, *, mode=None) -> dict[str, DataFrame]`

Generate a single ingest batch independently (no prior batch materialization).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `plan_or_base` | `IngestPlan \| DataGenPlan` | Ingest plan or base DataGenPlan |
| `batch_id` | `int` | Batch number (1-indexed) |
| `mode` | `str \| None` | Output mode override |

**Returns:** `dict[str, DataFrame]` keyed by table name.

---

### `write_ingest_to_delta(spark, plan_or_base, *, catalog, schema, ...) -> dict[str, str]`

Generate an ingest stream and write it to Delta tables.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark` | `SparkSession` | required | Active Spark session |
| `plan_or_base` | `IngestPlan \| DataGenPlan` | required | Ingest plan or base DataGenPlan |
| `catalog` | `str` | required | Unity Catalog catalog name |
| `schema` | `str` | required | Unity Catalog schema name |
| `num_batches` | `int \| None` | `None` | Override batch count |
| `mode` | `str \| None` | `None` | Output mode override |
| `strategy` | `str \| None` | `None` | Strategy override |
| `chunk_size` | `int \| None` | `None` | Batches per Delta version (default 1) |

**Returns:** `dict[str, str]` mapping table_name -> fully qualified UC table path.

---

### `detect_changes(spark, before_df, after_df, key_columns, data_columns=None) -> dict[str, DataFrame]`

Identify inserts, updates, and deletes between two snapshots.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `before_df` | `DataFrame` | Snapshot at time T |
| `after_df` | `DataFrame` | Snapshot at time T+1 |
| `key_columns` | `list[str]` | Primary key column names |
| `data_columns` | `list[str] \| None` | Columns to compare for change detection (None = all non-key) |

**Returns:** `dict` with keys: `"inserts"`, `"updates"`, `"deletes"`, `"unchanged"`.

**Example:**

```python
from dbldatagen.v1 import detect_changes

changes = detect_changes(spark, snapshot_t0, snapshot_t1, ["txn_id"])
changes["inserts"].show()
changes["updates"].show()
changes["deletes"].show()
```

---

## Connector APIs

### JDBC Connector

#### `JDBCConnector(connection_string, *, tables=None, default_rows=1000, sample=True, schema=None)`

Extract schema from a SQL database via SQLAlchemy and produce a DataGenPlan.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection_string` | `str` | required | SQLAlchemy-compatible URL (e.g., `"sqlite:///my.db"`, `"postgresql://user:pass@host/db"`) |
| `tables` | `list[str] \| None` | `None` | Restrict to these tables (None = all) |
| `default_rows` | `int` | `1000` | Row count for each TableSpec |
| `sample` | `bool` | `True` | Sample existing data for smarter strategy selection |
| `schema` | `str \| None` | `None` | Database schema name |

**Methods:**
- `.extract() -> DataGenPlan` — Inspect and return a plan
- `.to_yaml(output_path) -> None` — Extract and write as YAML

#### `extract_from_jdbc(connection_string, *, tables=None, **kwargs) -> DataGenPlan`

Convenience function: create a `JDBCConnector` and extract.

**Example:**

```python
from dbldatagen.v1.connectors.jdbc import extract_from_jdbc

plan = extract_from_jdbc("postgresql://user:pass@host/db", tables=["users", "orders"])
dfs = generate(spark, plan)
```

---

### CSV Connector

#### `CSVConnector(paths, default_rows=1000, **pandas_kwargs)`

Extract schema from CSV files via pandas type inference.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `paths` | `str \| Path \| list` | required | One or more CSV file paths |
| `default_rows` | `int` | `1000` | Row count for each TableSpec |
| `**pandas_kwargs` | | | Extra kwargs forwarded to `pd.read_csv` |

**Methods:**
- `.extract() -> DataGenPlan` — Read and return a plan
- `.to_yaml(output_path) -> None` — Extract and write as YAML

#### `extract_from_csv(paths, default_rows=1000, **pandas_kwargs) -> DataGenPlan`

Convenience function: extract a DataGenPlan from CSV file(s).

**Example:**

```python
from dbldatagen.v1.connectors.csv import extract_from_csv

plan = extract_from_csv(["customers.csv", "orders.csv"], default_rows=10000)
dfs = generate(spark, plan)
```

---

## JSON/YAML Serialization

All Pydantic models support full round-trip serialization:

```python
# Serialize to JSON
plan_json = plan.model_dump_json(indent=2)
plan_dict = plan.model_dump()

# Deserialize from JSON
plan = DataGenPlan.model_validate_json(plan_json)
plan = DataGenPlan(**plan_dict)

# YAML (via PyYAML)
import yaml
plan_dict = yaml.safe_load(open("plan.yml"))
plan = DataGenPlan.model_validate(plan_dict)
```

---

## See Also

- [Schema Models](./schema-models.md) — Full Pydantic model reference
- [Architecture](./architecture.md) — Technical deep-dive into internals
- [Examples](../guides/basic.md) — Practical usage examples
