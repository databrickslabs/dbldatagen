---
sidebar_position: 3
title: "Primary Keys"
---

# Primary Keys

> **TL;DR:** dbldatagen.v1 offers four primary key strategies, all guaranteed unique by construction: (1) sequential integers via `pk_auto()`, (2) deterministic UUIDs via `pk_uuid()`, (3) pattern-formatted strings via `pk_pattern()`, (4) random unique integers via Feistel cipher. Choose based on your use case: sequential for performance, UUID for distributed systems, pattern for human-readability, Feistel for random-looking IDs.

## Primary Key Model

The `PrimaryKey` model marks one or more columns as the table's primary key:

```python
from dbldatagen.v1.schema import PrimaryKey

# Single-column PK
PrimaryKey(columns=["order_id"])

# Composite PK (multiple columns)
PrimaryKey(columns=["region", "order_id"])
```

| Field | Type | Description |
|-------|------|-------------|
| `columns` | `list[str]` | Column names forming the primary key |

## Strategy 1: Sequential Integers (`pk_auto`)

The simplest and fastest strategy. Maps row `id` to `start + id * step`.

```python
from dbldatagen.v1 import pk_auto

# Default: 1, 2, 3, ...
pk_auto("order_id")

# Custom start and step: 1000, 1001, 1002, ...
pk_auto("order_id", start=1000, step=1)

# Larger steps: 100, 110, 120, ...
pk_auto("order_id", start=100, step=10)
```

**Underlying strategy:** `SequenceColumn`

**Data type:** `LONG` (64-bit integer)

**Performance:** Zero overhead. This is a single arithmetic expression on the row ID (Tier 1, pure SQL).

**When to use:**
- Default choice for most tables
- Maximum performance with no overhead
- Natural ascending order for time-series data
- Simplest to work with in SQL queries

**When NOT to use:**
- When you need globally unique IDs across distributed systems
- When sequential patterns reveal business information you want to hide

### Full Example

```python
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, integer, text

orders = TableSpec(
    name="orders",
    rows="1M",
    columns=[
        pk_auto("order_id"),
        integer("customer_id", min=1, max=100000),
        text("status", values=["pending", "shipped", "delivered"]),
    ],
    primary_key=PrimaryKey(columns=["order_id"]),
)

plan = DataGenPlan(tables=[orders])
```

## Strategy 2: Deterministic UUIDs (`pk_uuid`)

Generates standard UUID strings (128-bit) formatted as hex with dashes.

```python
from dbldatagen.v1 import pk_uuid

# Generates: "a3f1b2c4-d5e6-f7a8-b9c0-d1e2f3a4b5c6"
pk_uuid("event_id")
```

**Underlying strategy:** `UUIDColumn`

**Data type:** `STRING`

**Performance:** Fast. Uses two `xxhash64` calls per row (128 bits total), formatted into standard UUID hex (Tier 1, pure SQL).

**How it works:**
1. Compute `hash1 = xxhash64(column_seed, row_id)`
2. Compute `hash2 = xxhash64(column_seed + 1, row_id)`
3. Format as UUID string: `"HHHHHHHH-HHHH-HHHH-HHHH-HHHHHHHHHHHH"`

**Determinism:** Same row ID always produces the same UUID. This is NOT a time-based UUID (UUID v1) or random UUID (UUID v4), but a deterministic hash-based UUID.

**When to use:**
- Distributed systems where globally unique IDs are needed
- Event tracking (clickstream, logs)
- Interoperability with systems expecting UUID format
- When you want non-sequential IDs that don't reveal ordering

**When NOT to use:**
- When storage efficiency matters (36 chars vs 8 bytes for `LONG`)
- When you need to sort by creation time (UUIDs are random-looking)

### Full Example

```python
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_uuid, integer, timestamp

events = TableSpec(
    name="events",
    rows="10M",
    columns=[
        pk_uuid("event_id"),
        integer("user_id", min=1, max=1000000),
        text("event_type", values=["click", "view", "purchase"]),
        timestamp("occurred_at", start="2024-01-01", end="2024-12-31"),
    ],
    primary_key=PrimaryKey(columns=["event_id"]),
)

plan = DataGenPlan(tables=[events])
```

## Strategy 3: Pattern-Formatted Strings (`pk_pattern`)

Human-readable string PKs with deterministic formatting using placeholders.

```python
from dbldatagen.v1 import pk_pattern

# Customer IDs: CUST-00000001, CUST-00000002, ...
pk_pattern("customer_id", "CUST-{digit:8}")

# Order IDs: ORD-3847-KMX, ORD-9012-PLQ, ...
pk_pattern("order_id", "ORD-{digit:4}-{alpha:3}")

# Product SKUs: SKU-a3f1b2-001
pk_pattern("sku", "SKU-{hex:6}-{seq:03d}")
```

**Underlying strategy:** `PatternColumn`

**Data type:** `STRING`

**Performance:** Moderate. Pattern parsing + formatting (Tier 2, pandas UDF for complex patterns).

### Placeholder Reference

| Placeholder | Description | Example | Output |
|-------------|-------------|---------|--------|
| `{seq}` | Row sequence number (monotonic) | `"ID-{seq}"` | `ID-1`, `ID-2`, `ID-3` |
| `{seq:05d}` | Zero-padded sequence | `"{seq:08d}"` | `00000001`, `00000002` |
| `{digit:N}` | N random digits | `"{digit:4}"` | `3847`, `9012` |
| `{alpha:N}` | N random uppercase letters | `"{alpha:3}"` | `KMX`, `PLQ` |
| `{hex:N}` | N random hex characters | `"{hex:6}"` | `a3f1b2`, `c4d5e6` |
| `{uuid}` | Full UUID | `"{uuid}"` | `a3f1b2c4-d5e6-f7a8-...` |

**Uniqueness guarantee:**
- `{seq}` always produces unique values (it's just the row number)
- `{digit:N}` when used as sole placeholder derives digits from row ID for uniqueness
- `{alpha:N}` and `{hex:N}` depend on having enough entropy for the row count (e.g., `{alpha:3}` gives 26^3 = 17,576 unique values)

**When to use:**
- Human-readable IDs for customer support, invoices, order tracking
- Legacy system compatibility requiring specific ID formats
- Business rules requiring prefixes/suffixes (e.g., "ORD-" for orders)

**When NOT to use:**
- When maximum performance is needed (strings are slower than integers)
- Very high cardinality (billions of rows) with complex patterns

### Full Example

```python
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_pattern, integer, text, decimal

customers = TableSpec(
    name="customers",
    rows="500K",
    columns=[
        pk_pattern("customer_id", "CUST-{digit:8}"),
        text("name", values=["Alice", "Bob", "Carol"]),
        text("tier", values=["bronze", "silver", "gold"]),
        decimal("balance", min=0.0, max=10000.0),
    ],
    primary_key=PrimaryKey(columns=["customer_id"]),
)

plan = DataGenPlan(tables=[customers])
```

## Strategy 4: Random Unique Integers (Feistel Cipher)

For when you need random-looking but guaranteed-unique integer PKs. Uses a Feistel cipher to create a bijective (one-to-one) mapping from `[0, N)` to `[0, N)`.

```python
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey, ColumnSpec, DataType, RangeColumn

# Configure a column for random unique integers
random_pk = ColumnSpec(
    name="id",
    dtype=DataType.LONG,
    gen=RangeColumn(min=0, max=1000000),  # Range defines the permutation domain
)

orders = TableSpec(
    name="orders",
    rows="1M",
    columns=[random_pk, ...],
    primary_key=PrimaryKey(columns=["id"]),
)
```

**Note:** This strategy doesn't have a dedicated DSL helper. You configure it via `RangeColumn` with `min=0` and `max=N` where N is your row count.

**Data type:** `LONG`

**Performance:** Fast. NumPy-vectorized Feistel cipher inside a `pandas_udf`. Approximately 20-50M rows/sec/core (Tier 2).

### How Feistel Works

The Feistel cipher is a cryptographic primitive that creates a permutation:

1. Split each integer into left/right halves (32 bits each for 64-bit integers)
2. Apply 6 rounds of mixing:
   - XOR left half with `hash(right half + round_key)`
   - Swap halves
3. The result is a bijection: every input maps to a unique output

For non-power-of-2 domains (e.g., 1 million rows, not 2^20), a "cycle walking" technique is used:
- If the output is >= N, re-apply the permutation
- Repeat until the output falls in `[0, N)`

**Mathematical guarantee:** Every input in `[0, N)` maps to exactly one output in `[0, N)`, and vice versa.

**When to use:**
- When you want IDs that appear random but are guaranteed unique
- Load testing scenarios where you want to simulate random access patterns
- When you need to generate IDs out of order (e.g., in parallel workers)

**When NOT to use:**
- When sequential IDs are acceptable (use `pk_auto` instead, it's faster)
- When you need truly random IDs that might collide (this is collision-free)

### Full Example

```python
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey, ColumnSpec, DataType, RangeColumn
from dbldatagen.v1 import integer, text

# Random unique integer PK using Feistel permutation
random_id_col = ColumnSpec(
    name="session_id",
    dtype=DataType.LONG,
    gen=RangeColumn(min=0, max=10000000),  # 10M unique IDs
)

sessions = TableSpec(
    name="sessions",
    rows="10M",
    columns=[
        random_id_col,
        integer("user_id", min=1, max=1000000),
        text("device", values=["mobile", "desktop", "tablet"]),
    ],
    primary_key=PrimaryKey(columns=["session_id"]),
)

plan = DataGenPlan(tables=[sessions])
```

**Note:** The internal Feistel implementation is in `dbldatagen.v1.engine.columns.pk.build_random_unique_pk_udf` but it's not directly exposed as a public API. The recommended approach is to use `RangeColumn` as shown above.

## Composite Primary Keys

dbldatagen.v1 supports composite PKs (multiple columns):

```python
from dbldatagen.v1.schema import PrimaryKey

# Composite PK with two columns
PrimaryKey(columns=["region", "order_id"])
```

Example with both columns as PKs:

```python
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, text, integer

regional_orders = TableSpec(
    name="regional_orders",
    rows="1M",
    columns=[
        text("region", values=["US-EAST", "US-WEST", "EU", "APAC"]),
        pk_auto("order_id"),
        integer("quantity", min=1, max=100),
    ],
    primary_key=PrimaryKey(columns=["region", "order_id"]),
)

plan = DataGenPlan(tables=[regional_orders])
```

**Note:** Foreign keys can reference composite PKs using the same syntax: `fk("region_order_ref", "regional_orders.region")`.

## Choosing a Strategy

| Strategy | Best For | Performance | Storage | Readability |
|----------|----------|-------------|---------|-------------|
| **Sequential** (`pk_auto`) | General purpose, time-series | Fastest | Smallest | Low |
| **UUID** (`pk_uuid`) | Distributed systems, events | Fast | Moderate | Low |
| **Pattern** (`pk_pattern`) | Human-readable IDs | Moderate | Largest | High |
| **Feistel** (random unique) | Random-looking IDs | Fast | Smallest | Low |

**Default recommendation:** Start with `pk_auto()` unless you have a specific reason to use another strategy.

## Next Steps

- Configure [Foreign Key relationships](foreign-keys.md) that reference these PKs
- Explore [Column Strategies](column-strategies.md) for non-PK columns
- Learn about [Distributions](distributions.md) to control value selection
