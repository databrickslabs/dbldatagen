---
sidebar_position: 7
title: "Nullable Columns"
---

# Nullable Columns

> **TL;DR:** Any column can be made nullable by setting `nullable=True` and `null_fraction` (0.0-1.0). Null injection is deterministic per row and uses a separate hash seed to avoid correlation with value generation. Setting `null_fraction > 0` automatically enables `nullable=True`.

Real-world data often has missing values - optional phone numbers, sparse relationships, partial customer profiles. dbldatagen.v1 supports modeling these patterns with nullable columns.

## Basic Usage

Add `nullable=True` and `null_fraction` to any column spec:

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, faker, integer, text

users = TableSpec(
    name="users",
    rows=10000,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("email", "email"),

        # 40% of users don't have a phone number
        faker("phone", "phone_number", nullable=True, null_fraction=0.4),

        # 20% of users haven't set a bio
        faker("bio", "sentence", nullable=True, null_fraction=0.2),

        # 15% of users haven't set a profile picture
        text("avatar_url",
             values=["https://cdn.example.com/avatar1.jpg",
                     "https://cdn.example.com/avatar2.jpg"],
             nullable=True,
             null_fraction=0.15),
    ],
)

plan = DataGenPlan(tables=[users], seed=42)
dfs = generate(spark, plan)

# Check null rates
dfs["users"].selectExpr(
    "count(*) as total",
    "count(phone) as has_phone",
    "count(bio) as has_bio",
    "count(avatar_url) as has_avatar",
).show()
# +-----+---------+--------+----------+
# |total|has_phone| has_bio|has_avatar|
# +-----+---------+--------+----------+
# |10000|     6000|    8000|      8500|
# +-----+---------+--------+----------+
```

## Automatic `nullable` Flag

Setting `null_fraction > 0` automatically sets `nullable = True`, so you only need to specify the fraction:

```python
# These are equivalent:
faker("phone", "phone_number", nullable=True, null_fraction=0.3)
faker("phone", "phone_number", null_fraction=0.3)  # nullable=True implied
```

## Null Fraction Semantics

`null_fraction` is the **probability** that a given row will have NULL for that column:

- `0.0` - No nulls (default)
- `0.1` - ~10% of rows are NULL
- `0.5` - ~50% of rows are NULL
- `1.0` - All rows are NULL

The exact count is stochastic but deterministic - same seed always produces the same null pattern.

## How Null Injection Works

Null injection uses a deterministic mask based on the row ID and column seed:

1. **Separate seed:** Null decision uses `column_seed XOR 0x9E3779B9` (a constant) to avoid correlation with the value generation
2. **Deterministic per row:** Each row's null decision is `hash(null_seed, row_id) % 1.0 < null_fraction`
3. **Applied before value generation:** If the null mask is true, the value generator is skipped entirely

This ensures:
- **Determinism:** Same row always gets the same null decision
- **Independence:** Null pattern doesn't correlate with the generated values
- **Performance:** Null mask is computed once, value generator only runs for non-null rows

## Nullable Columns with Different Types

Nullable works with all column types:

### Nullable Numeric Columns

```python
columns=[
    integer("age", min=18, max=90, nullable=True, null_fraction=0.05),
    decimal("salary", min=30000.0, max=200000.0, nullable=True, null_fraction=0.1),
]
```

### Nullable Text Columns

```python
columns=[
    text("status",
         values=["active", "inactive", "pending"],
         nullable=True,
         null_fraction=0.2),
]
```

### Nullable Timestamps

```python
from dbldatagen.v1 import timestamp

columns=[
    timestamp("last_login",
              start="2024-01-01",
              end="2025-12-31",
              nullable=True,
              null_fraction=0.3),
]
```

### Nullable Faker Columns

```python
columns=[
    faker("middle_name", "first_name", nullable=True, null_fraction=0.6),
    faker("company", "company", nullable=True, null_fraction=0.4),
]
```

## Nullable Foreign Keys

Nullable foreign keys model optional relationships - a user who may or may not have a referrer, an order that may or may not have a discount code:

```python
from dbldatagen.v1 import fk

users = TableSpec(
    name="users",
    rows=100000,
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("email", "email"),

        # Optional referrer - 70% of users are organic signups (NULL referrer)
        fk("referrer_id",
           "users.user_id",  # Self-referencing FK
           nullable=True,
           null_fraction=0.7),
    ],
)
```

### Nullable FK with Distribution

Combine nullable FKs with distributions to model realistic patterns:

```python
orders = TableSpec(
    name="orders",
    rows=1000000,
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),

        # Optional discount code - only 15% of orders use one
        # Of those that do, some codes are more popular (Zipf distribution)
        fk("discount_code_id",
           "discount_codes.code_id",
           nullable=True,
           null_fraction=0.85,  # 85% of orders have no discount
           distribution=Zipf(exponent=1.5)),  # Popular codes used more
    ],
)
```

## Sparse Data Patterns

Model tables with many optional fields:

```python
from dbldatagen.v1 import TableSpec, PrimaryKey, pk_auto, faker, integer, text

customer_profiles = TableSpec(
    name="customer_profiles",
    rows=100000,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_auto("customer_id"),
        faker("email", "email"),  # Required field

        # Progressive profiling - optional fields filled over time
        faker("first_name", "first_name", nullable=True, null_fraction=0.2),
        faker("last_name", "last_name", nullable=True, null_fraction=0.2),
        faker("phone", "phone_number", nullable=True, null_fraction=0.5),
        faker("company", "company", nullable=True, null_fraction=0.6),
        faker("job_title", "job", nullable=True, null_fraction=0.7),

        # Preferences - rarely set
        text("email_opt_in",
             values=["yes", "no"],
             nullable=True,
             null_fraction=0.8),
        text("sms_opt_in",
             values=["yes", "no"],
             nullable=True,
             null_fraction=0.9),
    ],
)
```

## Nullable Nested Types

Struct and array columns can also be nullable:

### Nullable Struct

```python
from dbldatagen.v1 import struct
from dbldatagen.v1.schema import ColumnSpec, ValuesColumn

columns=[
    pk_auto("user_id"),

    # Optional shipping address
    struct("shipping_address", fields=[
        ColumnSpec(name="street", gen=faker("street", "street_address").gen),
        ColumnSpec(name="city", gen=faker("city", "city").gen),
        ColumnSpec(name="zip", gen=faker("zip", "zipcode").gen),
    ], nullable=True, null_fraction=0.3),  # 30% have no shipping address
]
```

### Nullable Array

```python
from dbldatagen.v1 import array
from dbldatagen.v1.schema import ValuesColumn

columns=[
    pk_auto("product_id"),

    # Optional tags array
    array("tags",
        element=ValuesColumn(values=["sale", "new", "premium"]),
        min_length=1,
        max_length=4,
        nullable=True,
        null_fraction=0.2,  # 20% of products have no tags
    ),
]
```

### Nullable Fields Within Structs

Individual struct fields can be nullable:

```python
struct("contact_info", fields=[
    ColumnSpec(name="email", gen=faker("email", "email").gen),
    ColumnSpec(name="phone", gen=faker("phone", "phone_number").gen,
               nullable=True, null_fraction=0.4),
    ColumnSpec(name="fax", gen=faker("phone", "phone_number").gen,
               nullable=True, null_fraction=0.9),  # Fax rarely provided
])
```

## YAML Configuration

Nullable columns in YAML:

```yaml
seed: 100
tables:
  - name: users
    rows: 10000
    primary_key: {columns: [user_id]}
    columns:
      - name: user_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: email
        gen: {strategy: faker, provider: email}

      # Nullable phone - 40% null
      - name: phone
        gen: {strategy: faker, provider: phone_number}
        nullable: true
        null_fraction: 0.4

      # Nullable FK - 70% null
      - name: referrer_id
        gen: {strategy: constant, value: null}
        nullable: true
        null_fraction: 0.7
        foreign_key:
          ref: users.user_id
```

## Validating Null Rates

Check actual null rates in generated data:

```python
dfs = generate(spark, plan)

# Count nulls per column
df = dfs["users"]
total = df.count()

null_counts = {}
for col in ["phone", "bio", "avatar_url"]:
    null_count = df.filter(F.col(col).isNull()).count()
    null_counts[col] = {
        "null_count": null_count,
        "null_rate": null_count / total,
    }

print(null_counts)
# {
#   "phone": {"null_count": 4002, "null_rate": 0.4002},
#   "bio": {"null_count": 1998, "null_rate": 0.1998},
#   "avatar_url": {"null_count": 1503, "null_rate": 0.1503}
# }
```

## CDC with Nullable Columns

Nullable columns work in CDC batches. Updated rows can change from NULL to non-NULL or vice versa:

```python
from dbldatagen.v1.cdc import generate_cdc
from dbldatagen.v1.cdc_dsl import cdc_plan, cdc_config, ops

base = DataGenPlan(tables=[users], seed=42)

plan = cdc_plan(
    base,
    num_batches=5,
    format="raw",
    users=cdc_config(batch_size=0.1, operations=ops(3, 5, 2)),
)

stream = generate_cdc(spark, plan)

# Initial snapshot has nulls
stream.initial["users"].filter(F.col("phone").isNull()).count()

# Batches can update NULL -> value or value -> NULL
stream.batches[0]["users"].filter(F.col("_op") == "U").select("phone").show()
```

## Common Patterns

### Progressive Profiling

Model users filling out profiles over time:

```python
# Initial state: sparse profiles
users_v1 = TableSpec(
    name="users",
    rows=100000,
    columns=[
        pk_auto("user_id"),
        faker("email", "email"),  # Only email required at signup
        faker("name", "name", nullable=True, null_fraction=0.8),
        faker("phone", "phone_number", nullable=True, null_fraction=0.9),
        faker("company", "company", nullable=True, null_fraction=0.95),
    ],
)

# Later batches: users fill in more fields (lower null_fraction in updates)
```

### Optional Relationships

Model many-to-one relationships where the "many" side may not have a parent:

```python
# Orders may not have a promo code
orders = TableSpec(
    name="orders",
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id"),
        fk("promo_code_id", "promo_codes.code_id",
           nullable=True, null_fraction=0.9),  # Most orders don't use promos
    ],
)
```

### Sparse Feature Flags

Model A/B test assignments where most users aren't in any test:

```python
columns=[
    pk_auto("user_id"),
    text("test_group_a",
         values=["control", "variant"],
         nullable=True,
         null_fraction=0.95),  # Only 5% in test A
    text("test_group_b",
         values=["control", "variant"],
         nullable=True,
         null_fraction=0.98),  # Only 2% in test B
]
```

## Related Documentation

- [Faker Guide](./faker.md) - Nullable Faker columns
- [Nested Types Guide](./nested-types.md) - Nullable struct/array columns
- [Architecture](../reference/architecture.md) - How null injection works internally
- [Examples](../guides/basic.md) - See nullable columns in complete examples
