---
sidebar_position: 3
title: "Schema Models"
---

# Schema Models

> **TL;DR:** All dbldatagen.v1 models are [Pydantic v2](https://docs.pydantic.dev/) `BaseModel` subclasses with full JSON/YAML serialization support. This page documents every field, type, and default value.

## Core Models

### `DataGenPlan`

Top-level plan describing all tables to generate.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tables` | `list[TableSpec]` | required | Tables to generate |
| `seed` | `int` | `42` | Global random seed |
| `default_locale` | `str` | `"en_US"` | Default Faker locale |

**Behavior:**
- Tables without an explicit `seed` are automatically assigned `global_seed + table_index`
- Tables are generated in dependency order (FK relationships resolved via topological sort)

**Example:**

```python
from dbldatagen.v1.schema import DataGenPlan

plan = DataGenPlan(
    tables=[customers, orders],
    seed=42,
    default_locale="en_US",
)
```

---

### `TableSpec`

Defines one table.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Table name |
| `columns` | `list[ColumnSpec]` | required | Column definitions |
| `rows` | `int \| str` | required | Row count (accepts shorthand: `"1K"`, `"10M"`, `"1B"`) |
| `primary_key` | `PrimaryKey \| None` | `None` | Primary key definition |
| `seed` | `int \| None` | `None` | Per-table seed override |

**Row Count Shorthand:**
- `"1K"` = 1,000
- `"1M"` = 1,000,000
- `"1B"` = 1,000,000,000
- Fractional values work: `"2.5M"` = 2,500,000

**Example:**

```python
from dbldatagen.v1.schema import TableSpec, PrimaryKey

table = TableSpec(
    name="orders",
    rows="5M",
    columns=[...],
    primary_key=PrimaryKey(columns=["order_id"]),
    seed=42,
)
```

---

### `ColumnSpec`

Defines one column.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Column name |
| `dtype` | `DataType \| None` | `None` | Spark data type (inferred from strategy if omitted) |
| `gen` | `ColumnStrategy` | required | Generation strategy (discriminated union) |
| `nullable` | `bool` | `False` | Allow NULLs |
| `null_fraction` | `float` | `0.0` | Fraction of rows set to NULL (auto-sets `nullable=True`) |
| `foreign_key` | `ForeignKeyRef \| None` | `None` | FK relationship |
| `seed_from` | `str \| None` | `None` | Seed from another column's value instead of `_synth_row_id` (enables correlated columns) |

**Example:**

```python
from dbldatagen.v1.schema import ColumnSpec, DataType, RangeColumn

col = ColumnSpec(
    name="age",
    dtype=DataType.INT,
    gen=RangeColumn(min=18, max=90),
    nullable=False,
    null_fraction=0.0,
    foreign_key=None,
)
```

---

### `PrimaryKey`

Marks a column (or columns) as the primary key.

| Field | Type | Description |
|-------|------|-------------|
| `columns` | `list[str]` | Column names forming the primary key |

**Example:**

```python
from dbldatagen.v1.schema import PrimaryKey

# Single-column PK
PrimaryKey(columns=["order_id"])

# Composite PK
PrimaryKey(columns=["region", "order_id"])
```

---

### `ForeignKeyRef`

Defines a foreign key relationship to another table's primary key.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ref` | `str` | required | Reference in format `"table.column"` |
| `distribution` | `Distribution` | `Uniform()` | How to select parent rows |
| `cardinality` | `int \| tuple \| None` | `None` | Target children per parent (currently unused) |
| `nullable` | `bool` | `False` | Allow NULL FKs |
| `null_fraction` | `float` | `0.0` | Fraction of NULL FKs |

**Example:**

```python
from dbldatagen.v1.schema import ForeignKeyRef, Zipf

fk = ForeignKeyRef(
    ref="customers.customer_id",
    distribution=Zipf(exponent=1.2),
    nullable=False,
    null_fraction=0.0,
)
```

---

### `DataType`

Maps to Spark SQL types.

| Value | Spark Type |
|-------|-----------|
| `DataType.INT` | `IntegerType` |
| `DataType.LONG` | `LongType` |
| `DataType.FLOAT` | `FloatType` |
| `DataType.DOUBLE` | `DoubleType` |
| `DataType.STRING` | `StringType` |
| `DataType.BOOLEAN` | `BooleanType` |
| `DataType.DATE` | `DateType` |
| `DataType.TIMESTAMP` | `TimestampType` |
| `DataType.DECIMAL` | `DecimalType` |

**Alias:** `DataType.INTEGER` = `DataType.INT`

---

## Column Strategies

All strategies are Pydantic models with a `strategy` discriminator field forming a [tagged union](https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions).

### `RangeColumn`

Generate numeric values in `[min, max]`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["range"]` | `"range"` | Strategy discriminator |
| `min` | `float \| int` | `0` | Minimum value (inclusive) |
| `max` | `float \| int` | `100` | Maximum value (inclusive) |
| `step` | `float \| int \| None` | `None` | Step size (currently unused) |
| `distribution` | `Distribution` | `Uniform()` | Sampling distribution |

**Example:**

```python
from dbldatagen.v1.schema import RangeColumn, Normal

RangeColumn(min=0, max=100)
RangeColumn(min=1.0, max=1000.0, distribution=Normal(mean=500, stddev=100))
```

---

### `ValuesColumn`

Pick from an explicit list.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["values"]` | `"values"` | Strategy discriminator |
| `values` | `list[Any]` | required | List of allowed values |
| `distribution` | `Distribution` | `Uniform()` | Selection distribution |

**Example:**

```python
from dbldatagen.v1.schema import ValuesColumn, WeightedValues

ValuesColumn(values=["red", "green", "blue"])
ValuesColumn(
    values=["A", "B", "C"],
    distribution=WeightedValues(weights={"A": 0.7, "B": 0.2, "C": 0.1})
)
```

---

### `FakerColumn`

Generate data using a Faker provider.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["faker"]` | `"faker"` | Strategy discriminator |
| `provider` | `str` | required | Faker provider method name |
| `kwargs` | `dict[str, Any]` | `{}` | Arguments passed to provider |
| `locale` | `str \| None` | `None` | Faker locale (e.g., `"de_DE"`) |

**Example:**

```python
from dbldatagen.v1.schema import FakerColumn

FakerColumn(provider="name")
FakerColumn(provider="email", locale="de_DE")
FakerColumn(provider="date_of_birth", kwargs={"minimum_age": 18, "maximum_age": 80})
```

---

### `PatternColumn`

Generate strings from a template.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["pattern"]` | `"pattern"` | Strategy discriminator |
| `template` | `str` | required | Pattern template |

**Placeholders:**
- `{seq}` — row sequence number (monotonic)
- `{uuid}` — deterministic UUID
- `{alpha:N}` — N random uppercase letters
- `{digit:N}` — N random digits
- `{hex:N}` — N random hex characters (lowercase)

**Example:**

```python
from dbldatagen.v1.schema import PatternColumn

PatternColumn(template="ORD-{digit:4}-{alpha:3}")   # ORD-3847-KMX
PatternColumn(template="CUST-{digit:8}")             # CUST-00000001
PatternColumn(template="{hex:12}")                    # a3f1b2c4d5e6
```

---

### `SequenceColumn`

Auto-incrementing integers.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["sequence"]` | `"sequence"` | Strategy discriminator |
| `start` | `int` | `1` | Starting value |
| `step` | `int` | `1` | Step increment |

**Example:**

```python
from dbldatagen.v1.schema import SequenceColumn

SequenceColumn()                      # 1, 2, 3, ...
SequenceColumn(start=100, step=10)    # 100, 110, 120, ...
```

---

### `UUIDColumn`

Deterministic UUID strings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["uuid"]` | `"uuid"` | Strategy discriminator |

**Example:**

```python
from dbldatagen.v1.schema import UUIDColumn

UUIDColumn()   # "a3f1b2c4-d5e6-f7a8-b9c0-d1e2f3a4b5c6"
```

**Implementation:** UUID is derived from `xxhash64(seed, id)` + `xxhash64(seed+1, id)` for deterministic output.

---

### `ExpressionColumn`

Arbitrary Spark SQL expression referencing sibling columns.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["expression"]` | `"expression"` | Strategy discriminator |
| `expr` | `str` | required | Spark SQL expression |

**Example:**

```python
from dbldatagen.v1.schema import ExpressionColumn

ExpressionColumn(expr="quantity * unit_price")
ExpressionColumn(expr="concat(first_name, ' ', last_name)")
ExpressionColumn(expr="CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END")
```

---

### `TimestampColumn`

Timestamps or dates within a range.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["timestamp"]` | `"timestamp"` | Strategy discriminator |
| `start` | `str` | `"2020-01-01"` | Start date (ISO 8601) |
| `end` | `str` | `"2025-12-31"` | End date (ISO 8601) |
| `distribution` | `Distribution` | `Uniform()` | Temporal distribution |

**Example:**

```python
from dbldatagen.v1.schema import TimestampColumn, Normal

TimestampColumn(start="2023-01-01", end="2025-12-31")
TimestampColumn(
    start="2024-06-01", end="2024-06-30",
    distribution=Normal(mean=0.5, stddev=0.2)
)
```

**Note:** Set `dtype=DataType.DATE` on the `ColumnSpec` to get dates instead of timestamps.

---

### `ConstantColumn`

Every row gets the same value.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["constant"]` | `"constant"` | Strategy discriminator |
| `value` | `Any` | required | Constant value |

**Example:**

```python
from dbldatagen.v1.schema import ConstantColumn

ConstantColumn(value="active")
ConstantColumn(value=42)
```

---

### `StructColumn`

Group child fields into a Spark struct (nested object in JSON).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["struct"]` | `"struct"` | Strategy discriminator |
| `fields` | `list[ColumnSpec]` | required | Child field definitions |

**Example:**

```python
from dbldatagen.v1.schema import StructColumn, ColumnSpec, ValuesColumn, RangeColumn

StructColumn(fields=[
    ColumnSpec(name="city", gen=ValuesColumn(values=["NYC", "LA", "Chicago"])),
    ColumnSpec(name="zip", gen=RangeColumn(min=10000, max=99999)),
])
```

**JSON Output:**

```json
{"address": {"city": "NYC", "zip": 10023}}
```

---

### `ArrayColumn`

Generate a variable-length array of values.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `strategy` | `Literal["array"]` | `"array"` | Strategy discriminator |
| `element` | `ColumnStrategy` | required | Strategy for generating each element |
| `min_length` | `int` | `1` | Minimum array length |
| `max_length` | `int` | `5` | Maximum array length |

**Example:**

```python
from dbldatagen.v1.schema import ArrayColumn, ValuesColumn

ArrayColumn(
    element=ValuesColumn(values=["sale", "new", "premium"]),
    min_length=1,
    max_length=4,
)
```

**JSON Output:**

```json
{"tags": ["sale", "new"]}
```

---

## Distributions

### `Uniform`

Uniform distribution (default).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["uniform"]` | `"uniform"` | Distribution discriminator |

**Example:**

```python
from dbldatagen.v1.schema import Uniform

Uniform()   # No parameters
```

---

### `Normal`

Normal/Gaussian distribution.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["normal"]` | `"normal"` | Distribution discriminator |
| `mean` | `float` | `0.0` | Mean (center) of distribution |
| `stddev` | `float` | `1.0` | Standard deviation (spread) |

**Example:**

```python
from dbldatagen.v1.schema import Normal

Normal(mean=0.0, stddev=1.0)
Normal(mean=50.0, stddev=15.0)  # Age centered at 50, spread ±15
```

**Note:** Mean and stddev are applied **after** mapping to the [min, max] range. A mean of 0.5 centers values in the middle of the range.

---

### `LogNormal`

Log-normal distribution (right-skewed with long tail).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["lognormal"]` | `"lognormal"` | Distribution discriminator |
| `mean` | `float` | `0.0` | Mean of the underlying normal distribution |
| `stddev` | `float` | `1.0` | Standard deviation of underlying normal |

**Example:**

```python
from dbldatagen.v1.schema import LogNormal

LogNormal(mean=0.0, stddev=1.0)
LogNormal(mean=4.0, stddev=1.0)  # API latency: mostly fast, occasional outliers
```

---

### `Zipf`

Zipfian/power-law distribution (common for realistic cardinality skew).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["zipf"]` | `"zipf"` | Distribution discriminator |
| `exponent` | `float` | `1.5` | Power-law exponent (higher = more skew) |

**Example:**

```python
from dbldatagen.v1.schema import Zipf

Zipf(exponent=1.2)   # Mild skew (default for fk())
Zipf(exponent=2.0)   # Heavy skew (~1% of parents = 50% of children)
Zipf(exponent=3.0)   # Extreme skew (almost all traffic to top few parents)
```

---

### `Exponential`

Exponential distribution (heavily right-skewed, most values near min).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["exponential"]` | `"exponential"` | Distribution discriminator |
| `rate` | `float` | `1.0` | Rate parameter (higher = more concentration near min) |

**Example:**

```python
from dbldatagen.v1.schema import Exponential

Exponential(rate=1.0)
Exponential(rate=0.5)  # Order amounts: most small, few very large
```

---

### `WeightedValues`

Explicit weighted selection from a list of values.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `Literal["weighted"]` | `"weighted"` | Distribution discriminator |
| `weights` | `dict[str, float]` | required | Value → weight mapping |

**Example:**

```python
from dbldatagen.v1.schema import WeightedValues

WeightedValues(weights={
    "gold": 0.05,
    "silver": 0.15,
    "bronze": 0.80
})
```

**Note:** Weights are normalized internally (they don't need to sum to 1.0).

---

## CDC Models

### `CDCPlan`

Top-level plan for generating a CDC stream.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_plan` | `DataGenPlan` | required | Base schema for initial data |
| `num_batches` | `int` | `5` | Number of change batches |
| `table_configs` | `dict[str, CDCTableConfig]` | `{}` | Per-table CDC settings |
| `default_config` | `CDCTableConfig` | `CDCTableConfig()` | Default config for unconfigured tables |
| `format` | `CDCFormat` | `CDCFormat.RAW` | Output format |
| `batch_interval_seconds` | `int` | `3600` | Simulated time between batches |
| `start_timestamp` | `str` | `"2025-01-01T00:00:00Z"` | Simulated start time |
| `cdc_tables` | `list[str]` | `[]` | Tables to include (empty = all) |

**Example:**

```python
from dbldatagen.v1.cdc_schema import CDCPlan, CDCTableConfig, OperationWeights

plan = CDCPlan(
    base_plan=data_gen_plan,
    num_batches=10,
    format=CDCFormat.DELTA_CDF,
    table_configs={
        "orders": CDCTableConfig(
            batch_size=0.1,
            operations=OperationWeights(insert=3, update=5, delete=2),
        ),
    },
    batch_interval_seconds=86400,
)
```

---

### `CDCTableConfig`

Per-table CDC configuration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `operations` | `OperationWeights` | `OperationWeights()` | Insert/update/delete mix |
| `batch_size` | `int \| float \| str` | `0.1` | Rows per batch (fraction, absolute, or shorthand) |
| `mutations` | `MutationSpec` | `MutationSpec()` | Which columns mutate on updates |
| `min_life` | `int` | `3` | Minimum batches a row must exist before it can be deleted |
| `update_window` | `int \| None` | `None` | If set, only rows born within this many batches of the current batch are eligible for updates |

**batch_size Values:**
- **Float (0.0-1.0):** Fraction of initial rows (e.g., `0.1` = 10% per batch)
- **Int (>1):** Absolute row count (e.g., `1000` = 1000 rows per batch)
- **String:** Shorthand (e.g., `"10K"` = 10,000 rows per batch)

**Example:**

```python
from dbldatagen.v1.cdc_schema import CDCTableConfig, OperationWeights, MutationSpec

config = CDCTableConfig(
    batch_size=0.1,
    operations=OperationWeights(insert=3, update=5, delete=2),
    mutations=MutationSpec(columns=["status", "score"], fraction=0.5),
)
```

---

### `OperationWeights`

Controls the mix of insert/update/delete operations per batch.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `insert` | `float` | `3.0` | Relative insert weight |
| `update` | `float` | `5.0` | Relative update weight |
| `delete` | `float` | `2.0` | Relative delete weight |

**Example:**

```python
from dbldatagen.v1.cdc_schema import OperationWeights

OperationWeights(insert=3, update=5, delete=2)  # 30% insert, 50% update, 20% delete
OperationWeights(insert=10, update=0, delete=0) # Append-only (inserts only)
```

**Normalization:** Weights are relative and normalized internally to fractions.

---

### `MutationSpec`

Controls which columns mutate on updates.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `columns` | `list[str] \| None` | `None` | Columns eligible for mutation (None = all non-PK/FK columns) |
| `fraction` | `float` | `0.5` | Fraction of eligible columns that change per row |

**Example:**

```python
from dbldatagen.v1.cdc_schema import MutationSpec

MutationSpec()                                      # All non-PK/FK columns, 50% mutate per update
MutationSpec(columns=["status", "score"])           # Only these columns eligible
MutationSpec(columns=None, fraction=0.8)            # 80% of all non-PK/FK columns mutate
```

---

### `CDCFormat`

Supported CDC output formats.

| Value | Description |
|-------|-------------|
| `CDCFormat.RAW` | `_op` column with `I`, `U`, `UB`, `D` values |
| `CDCFormat.DELTA_CDF` | `_change_type` column matching Delta Lake CDF format |
| `CDCFormat.SQL_SERVER` | `__$operation` column with numeric codes (1-4) |
| `CDCFormat.DEBEZIUM` | `op` column with `c`, `u`, `d` values |

**Format Details:**

| Format | Column | Insert | Update Before | Update After | Delete |
|--------|--------|--------|---------------|--------------|--------|
| `RAW` | `_op` | `I` | `UB` | `U` | `D` |
| `DELTA_CDF` | `_change_type` | `insert` | `update_preimage` | `update_postimage` | `delete` |
| `SQL_SERVER` | `__$operation` | `2` | `3` | `4` | `1` |
| `DEBEZIUM` | `op` | `c` | (dropped) | `u` | `d` |

**Example:**

```python
from dbldatagen.v1.cdc_schema import CDCFormat

CDCFormat.RAW           # Raw format
CDCFormat.DELTA_CDF     # Delta Lake CDF
CDCFormat.SQL_SERVER    # SQL Server CDC
CDCFormat.DEBEZIUM      # Debezium format
```

---

## Ingest Models

### `IngestPlan`

Top-level plan for generating an ingestion stream.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_plan` | `DataGenPlan` | required | Base schema for initial data |
| `num_batches` | `int` | `5` | Number of change batches |
| `mode` | `IngestMode` | `INCREMENTAL` | Output mode: `incremental` or `snapshot` |
| `strategy` | `IngestStrategy` | `SYNTHETIC` | Generation strategy |
| `table_configs` | `dict[str, IngestTableConfig]` | `{}` | Per-table settings |
| `default_config` | `IngestTableConfig` | `IngestTableConfig()` | Default config |
| `batch_interval_seconds` | `int` | `86400` | Simulated time between batches (1 day) |
| `start_timestamp` | `str` | `"2025-01-01T00:00:00Z"` | Simulated start time |
| `ingest_tables` | `list[str]` | `[]` | Tables to include (empty = all) |
| `include_batch_id` | `bool` | `True` | Add `_batch_id` column |
| `include_load_timestamp` | `bool` | `True` | Add `_load_timestamp` column |

---

### `IngestTableConfig`

Per-table ingestion configuration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | `int \| float \| str` | `0.1` | Rows per batch (fraction, absolute, or shorthand) |
| `insert_fraction` | `float` | `0.6` | Fraction of batch = new rows |
| `update_fraction` | `float` | `0.3` | Fraction of batch = updated rows |
| `delete_fraction` | `float` | `0.1` | Fraction of rows removed from live set |
| `update_window` | `int \| None` | `None` | Update eligibility window |
| `min_life` | `int` | `1` | Minimum batches before row can be deleted |
| `mutations` | `MutationSpec` | `MutationSpec()` | Which columns change on update |

**Note:** `insert_fraction + update_fraction + delete_fraction` must sum to ~1.0.

---

### `IngestMode`

Output mode for ingestion batches.

| Value | Description |
|-------|-------------|
| `IngestMode.INCREMENTAL` | Each batch = new + changed rows only |
| `IngestMode.SNAPSHOT` | Each batch = all live rows |

---

### `IngestStrategy`

Generation strategy for ingestion data.

| Value | Description |
|-------|-------------|
| `IngestStrategy.SYNTHETIC` | Pure synthetic, no Delta reads (fast) |
| `IngestStrategy.DELTA` | Reads from Delta for update selection (realistic) |
| `IngestStrategy.STATELESS` | Stateless targeted scan, no Delta reads, O(batch_size) |

---

## YAML Serialization

All models support YAML serialization via Pydantic:

```python
import yaml
from dbldatagen.v1.schema import DataGenPlan
from dbldatagen.v1.cdc_schema import CDCPlan

# Load from YAML
raw = yaml.safe_load(open("plan.yml").read())

if "base_plan" in raw:
    plan = CDCPlan.model_validate(raw)
else:
    plan = DataGenPlan.model_validate(raw)

# Save to YAML
plan_dict = plan.model_dump()
with open("plan.yml", "w") as f:
    yaml.dump(plan_dict, f, default_flow_style=False)
```

---

## See Also

- [API Reference](./api.md) — Functions and parameters
- [Architecture](./architecture.md) — Technical implementation details
- [Examples](../guides/basic.md) — Practical usage examples
