---
sidebar_position: 4
title: "DSL Reference"
---

# V1 DSL Reference

The `dbldatagen.v1.dsl` module provides shorthand functions for defining columns. These are convenience wrappers around the full `ColumnSpec` pydantic models.

## Primary Key Helpers

```python
from dbldatagen.v1.dsl import pk_auto, pk_uuid, pk_pattern

pk_auto("id")                        # Sequential integer (1, 2, 3, ...)
pk_uuid("id")                        # Deterministic UUID
pk_pattern("order_id", "ORD-{digit:6}")  # Patterned string key
```

## Column Type Helpers

### `integer(name, min, max, **kwargs)`

Generate integer values in a range.

```python
from dbldatagen.v1.dsl import integer
integer("age", min=18, max=90)
```

### `decimal(name, min, max, **kwargs)`

Generate float/double values in a range.

```python
from dbldatagen.v1.dsl import decimal
decimal("amount", min=10.0, max=500.0)
```

### `text(name, values, **kwargs)`

Generate string values from a discrete set.

```python
from dbldatagen.v1.dsl import text
text("status", values=["active", "inactive", "pending"])
```

### `timestamp(name, start, end, **kwargs)`

Generate timestamps in a date range.

```python
from dbldatagen.v1.dsl import timestamp
timestamp("created_at", start="2020-01-01", end="2025-12-31")
```

### `pattern(name, template, **kwargs)`

Generate strings matching a pattern template.

```python
from dbldatagen.v1.dsl import pattern
pattern("sku", template="SKU-{digit:6}")
```

Template placeholders: `{digit:N}`, `{alpha:N}`, `{hex:N}`, `{seq}`, `{uuid}`

### `expression(name, expr, dtype, **kwargs)`

Generate values from a SQL expression referencing other columns.

```python
from dbldatagen.v1.dsl import expression
from dbldatagen.v1.schema import DataType
expression("total", "quantity * unit_price", dtype=DataType.DOUBLE)
```

### `faker(name, provider, **kwargs)`

Generate values using a Faker provider. Requires `pip install "dbldatagen[v1-faker]"`.

```python
from dbldatagen.v1.dsl import faker
faker("email", provider="email")
faker("full_name", provider="name")
```

### `struct(name, fields, **kwargs)`

Generate nested struct columns.

```python
from dbldatagen.v1.dsl import struct, text, integer
struct("address", [
    text("city", values=["Austin", "NYC", "LA"]),
    text("state", values=["TX", "NY", "CA"]),
    integer("zip", min=10000, max=99999),
])
```

### `array(name, element_gen, min_length, max_length, **kwargs)`

Generate array columns.

```python
from dbldatagen.v1.dsl import array
from dbldatagen.v1.schema import ValuesColumn
array("tags", ValuesColumn(values=["sale", "new", "popular"]), min_length=1, max_length=4)
```

### Foreign Keys

```python
from dbldatagen.v1.dsl import fk
fk("customer_id", ref="customers.id")
```

## Common Options

All column helpers accept these keyword arguments:

| Option | Description |
|--------|-------------|
| `nullable` | Whether the column can contain nulls (default: `False`) |
| `null_fraction` | Fraction of rows that should be null (0.0–1.0) |
| `seed_from` | Column name to seed deterministic values from (like V0 `baseColumn`) |
| `distribution` | Distribution object (`Normal`, `Exponential`, `LogNormal`, `Zipf`, `WeightedValues`) |
