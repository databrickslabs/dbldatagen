---
sidebar_position: 4
title: "Foreign Keys & Referential Integrity"
---

# Foreign Keys

> **TL;DR:** Use `fk("col", "parent_table.parent_col")` to create foreign key columns that reference parent primary keys. dbldatagen.v1 guarantees 100% referential integrity without materializing parent tables, using PK function reconstruction. Control parent selection via distributions (uniform, Zipf for hot parents, normal). Make FKs nullable for optional relationships.

## The Core Innovation

dbldatagen.v1's FK generation is **unique** in the test data generation space. Traditional approaches require either:
1. Materializing the parent table and sampling from it (doesn't scale to billions of rows)
2. Generating random values in the parent PK range and hoping they match (breaks with non-trivial PK strategies)
3. Using joins to filter valid child rows (wasteful, non-deterministic)

dbldatagen.v1 does none of these. Instead, it **reconstructs parent PKs on-the-fly** using PK function metadata.

### How It Works

Since parent PKs are deterministic functions of row index:

```
parent_pk(row_index) = pk_function(seed, row_index)
```

We generate FK values by:

1. **Pick a random parent row index** using the child row's seed and a distribution
2. **Reconstruct the parent PK** by applying the parent's PK function to that index

```python
fk_value = parent_pk_function(sample_row_index(cell_seed, N_parent))
```

This gives us:
- **Guaranteed referential integrity** — every FK value is a valid PK by construction
- **No parent materialization** — we only need the parent's metadata (PK function + row count + seed)
- **Distribution control** — the sampling step uses distributions (uniform, Zipf, etc.) to control how many children each parent gets
- **Deterministic** — same child row always picks the same parent
- **Scales to billions** — no memory overhead for parent data

## Basic Usage

The `fk()` DSL helper creates a foreign key column:

```python
from dbldatagen.v1 import fk

# Simple FK reference
fk("customer_id", "customers.customer_id")

# FK with nullable option
fk("referrer_id", "customers.customer_id", nullable=True, null_fraction=0.3)
```

The `"table.column"` reference is resolved at plan time. dbldatagen.v1:
1. Validates the referenced table and column exist
2. Verifies the referenced column is part of the table's primary key
3. Extracts the PK generation metadata (function type, seed, parameters)
4. Topologically sorts tables so parents are generated before children (for metadata propagation, not data dependency)

## ForeignKeyRef Model

The underlying model for FK configuration:

```python
from dbldatagen.v1.schema import ForeignKeyRef, Zipf

fk_ref = ForeignKeyRef(
    ref="customers.customer_id",
    distribution=Zipf(exponent=1.5),
    cardinality=None,
    nullable=False,
    null_fraction=0.0,
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ref` | `str` | required | `"table.column"` reference to parent PK |
| `distribution` | `Distribution` | `Uniform()` | How to select parent rows (see [Distributions](distributions.md)) |
| `cardinality` | `int \| tuple \| None` | `None` | Target children per parent (reserved for future use) |
| `nullable` | `bool` | `False` | Allow NULL FK values |
| `null_fraction` | `float` | `0.0` | Fraction of rows with NULL FK (0.0-1.0) |

## Distribution Control

By default, FK columns use **Zipf distribution** (exponent=1.2) via the DSL helper, which produces realistic skew where some parents have many children and most have few.

You can override the distribution:

### Uniform Distribution

Every parent has equal probability of being selected (even distribution of children).

```python
from dbldatagen.v1 import fk
from dbldatagen.v1.schema import Uniform

# Every customer gets roughly the same number of orders
fk("customer_id", "customers.customer_id", distribution=Uniform())
```

**When to use:**
- Testing scenarios where even distribution is desired
- When parent records should have equal load (e.g., round-robin assignment)

### Zipf Distribution (Power Law)

A few parents get many children, most get few. Models real-world phenomena like customer order patterns.

```python
from dbldatagen.v1 import fk
from dbldatagen.v1.schema import Zipf

# Hot customers: ~20% of customers get ~80% of orders
fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=1.5))

# Extreme skew: very few customers dominate
fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=2.0))

# Lighter skew: more evenly distributed
fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=1.0))
```

**Parameters:**
- `exponent`: Higher values = more skew (common range: 1.0-2.5)

**When to use:**
- Realistic e-commerce data (some customers order frequently, most rarely)
- Social networks (popular users have many followers)
- Product catalogs (bestsellers vs long-tail items)

**Default:** The `fk()` DSL helper uses `Zipf(exponent=1.2)` as a sensible default.

### Normal Distribution

Bell-curve distribution. Useful when you want clustering around a mean parent index.

```python
from dbldatagen.v1 import fk
from dbldatagen.v1.schema import Normal

# Children cluster around middle-index parents
fk("region_id", "regions.region_id", distribution=Normal(mean=0.5, stddev=0.2))
```

**When to use:**
- Geographic or temporal clustering
- When parent selection should follow a bell curve

## Nullable Foreign Keys

Use nullable FKs for optional relationships:

```python
from dbldatagen.v1 import fk

# ~30% of customers have no referrer (direct signups)
fk("referrer_id", "customers.customer_id", nullable=True, null_fraction=0.3)
```

**How it works:**
- `null_fraction` determines what fraction of rows get NULL
- The null decision is deterministic per row (uses a separate hash seed)
- Non-null rows pick a parent using the specified distribution

**When to use:**
- Optional relationships (e.g., nullable manager_id for top-level employees)
- Partial data scenarios (e.g., not all products have a supplier)

## Referential Integrity Guarantee

dbldatagen.v1 **guarantees 100% referential integrity** for non-null FK values:

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.validation import validate_referential_integrity

# Generate data
dfs = generate(spark, plan)

# Validate all FK relationships
errors = validate_referential_integrity(dfs, plan)
assert errors == [], f"Integrity errors: {errors}"
```

The validation uses left anti joins to find orphaned FK values. It will always return an empty list (no errors) unless:
- You manually modified the data after generation
- There's a bug in dbldatagen.v1 (please report it!)

## Complete Example: Customers and Orders

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey, Zipf
from dbldatagen.v1 import pk_auto, pk_pattern, fk, integer, decimal, text, timestamp
from pyspark.sql import SparkSession

# Parent table: customers
customers = TableSpec(
    name="customers",
    rows="100K",
    columns=[
        pk_pattern("customer_id", "CUST-{digit:8}"),
        text("name", values=["Alice", "Bob", "Carol", "Dave"]),
        text("tier", values=["bronze", "silver", "gold"]),
        integer("age", min=18, max=90),
    ],
    primary_key=PrimaryKey(columns=["customer_id"]),
)

# Child table: orders with FK to customers
orders = TableSpec(
    name="orders",
    rows="5M",
    columns=[
        pk_auto("order_id"),
        # Zipf distribution: some customers have many orders, most have few
        fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=1.5)),
        timestamp("order_date", start="2023-01-01", end="2024-12-31"),
        decimal("total", min=10.0, max=5000.0),
        text("status", values=["pending", "shipped", "delivered", "cancelled"]),
    ],
    primary_key=PrimaryKey(columns=["order_id"]),
)

# Create plan and generate
plan = DataGenPlan(tables=[customers, orders], seed=42)
spark = SparkSession.builder.appName("fk_example").getOrCreate()
dfs = generate(spark, plan)

# Validate referential integrity
from dbldatagen.v1.validation import validate_referential_integrity
errors = validate_referential_integrity(dfs, plan)
print(f"Integrity check: {errors}")  # Should print: []

# Analyze FK distribution
dfs["orders"].groupBy("customer_id").count().orderBy("count", ascending=False).show(20)
```

**Expected output:**
```
+------------+------+
|customer_id |count |
+------------+------+
|CUST-00001234|  382 |  # Hot customer
|CUST-00005678|  297 |  # Hot customer
|CUST-00009012|  243 |
|CUST-00003456|  198 |
...                     # Most customers have 1-50 orders
+------------+------+
```

The Zipf distribution creates realistic skew: a few customers have hundreds of orders, while most have just a handful.

## Multi-Level Foreign Keys

FK chains work naturally. Child tables can reference grandparent tables via parent tables:

```python
# Three-level hierarchy: products → categories → departments

departments = TableSpec(
    name="departments",
    rows="10",
    columns=[
        pk_auto("dept_id"),
        text("dept_name", values=["Electronics", "Clothing", "Home"]),
    ],
    primary_key=PrimaryKey(columns=["dept_id"]),
)

categories = TableSpec(
    name="categories",
    rows="100",
    columns=[
        pk_auto("category_id"),
        fk("dept_id", "departments.dept_id"),
        text("category_name", values=["Phones", "Laptops", "Shirts"]),
    ],
    primary_key=PrimaryKey(columns=["category_id"]),
)

products = TableSpec(
    name="products",
    rows="10K",
    columns=[
        pk_auto("product_id"),
        fk("category_id", "categories.category_id"),
        text("product_name", values=["iPhone", "MacBook", "T-Shirt"]),
        decimal("price", min=9.99, max=2999.99),
    ],
    primary_key=PrimaryKey(columns=["product_id"]),
)

plan = DataGenPlan(tables=[departments, categories, products])
```

dbldatagen.v1 automatically topologically sorts tables: `departments` → `categories` → `products`.

## FK with Different PK Strategies

Foreign keys work with **all** primary key strategies:

### FK to Sequential PK

```python
# Parent: sequential integer PK
customers = TableSpec(
    name="customers",
    rows="1M",
    columns=[pk_auto("customer_id"), ...],
    primary_key=PrimaryKey(columns=["customer_id"]),
)

# Child: FK reconstructs sequential PK function
orders = TableSpec(
    name="orders",
    rows="10M",
    columns=[pk_auto("order_id"), fk("customer_id", "customers.customer_id"), ...],
    primary_key=PrimaryKey(columns=["order_id"]),
)
```

### FK to UUID PK

```python
# Parent: UUID PK
events = TableSpec(
    name="events",
    rows="1B",
    columns=[pk_uuid("event_id"), ...],
    primary_key=PrimaryKey(columns=["event_id"]),
)

# Child: FK reconstructs UUID function
event_metadata = TableSpec(
    name="event_metadata",
    rows="500M",
    columns=[pk_auto("id"), fk("event_id", "events.event_id"), ...],
    primary_key=PrimaryKey(columns=["id"]),
)
```

### FK to Pattern PK

```python
# Parent: pattern-formatted PK
customers = TableSpec(
    name="customers",
    rows="500K",
    columns=[pk_pattern("customer_id", "CUST-{digit:8}"), ...],
    primary_key=PrimaryKey(columns=["customer_id"]),
)

# Child: FK reconstructs pattern function
orders = TableSpec(
    name="orders",
    rows="5M",
    columns=[pk_auto("order_id"), fk("customer_id", "customers.customer_id"), ...],
    primary_key=PrimaryKey(columns=["order_id"]),
)
```

**Note:** The FK column automatically inherits the parent's data type and generation strategy. You don't need to specify `dtype` or `gen` for FK columns.

## Composite Foreign Keys

dbldatagen.v1 supports FKs to composite PKs:

```python
# Parent with composite PK
regional_orders = TableSpec(
    name="regional_orders",
    rows="1M",
    columns=[
        text("region", values=["US-EAST", "US-WEST", "EU"]),
        pk_auto("order_id"),
        ...
    ],
    primary_key=PrimaryKey(columns=["region", "order_id"]),
)

# Child referencing composite PK (must create FK for EACH column)
shipments = TableSpec(
    name="shipments",
    rows="2M",
    columns=[
        pk_auto("shipment_id"),
        fk("order_region", "regional_orders.region"),
        fk("order_id", "regional_orders.order_id"),
        ...
    ],
)
```

**Important:** When referencing a composite PK, create a separate FK column for each component of the parent's PK.

## Performance Considerations

FK generation has minimal overhead:
- **No parent table materialization** — only metadata is used
- **No shuffles or joins** — FKs are generated independently per partition
- **Tier 1 for sequential/UUID PKs** — pure SQL expression
- **Tier 2 for pattern PKs** — pandas UDF with minimal overhead

**Benchmark:** Generating 1 billion child rows with FKs to 10 million parent rows takes roughly the same time as generating 1 billion independent rows.

## Common Patterns

### Self-Referential FK (Hierarchies)

```python
employees = TableSpec(
    name="employees",
    rows="10K",
    columns=[
        pk_auto("employee_id"),
        text("name", values=["Alice", "Bob", "Carol"]),
        # Nullable FK to same table (manager)
        fk("manager_id", "employees.employee_id", nullable=True, null_fraction=0.1),
    ],
    primary_key=PrimaryKey(columns=["employee_id"]),
)
```

**Note:** ~10% of employees have no manager (top-level executives).

### Many-to-Many via Junction Table

```python
# Students and courses with many-to-many relationship

students = TableSpec(
    name="students",
    rows="10K",
    columns=[pk_auto("student_id"), text("name", values=["Alice", "Bob"])],
    primary_key=PrimaryKey(columns=["student_id"]),
)

courses = TableSpec(
    name="courses",
    rows="500",
    columns=[pk_auto("course_id"), text("title", values=["Math", "Science"])],
    primary_key=PrimaryKey(columns=["course_id"]),
)

# Junction table with FKs to both
enrollments = TableSpec(
    name="enrollments",
    rows="50K",
    columns=[
        pk_auto("enrollment_id"),
        fk("student_id", "students.student_id"),
        fk("course_id", "courses.course_id"),
        text("grade", values=["A", "B", "C", "D", "F"]),
    ],
    primary_key=PrimaryKey(columns=["enrollment_id"]),
)

plan = DataGenPlan(tables=[students, courses, enrollments])
```

## Next Steps

- Explore all [Column Strategies](column-strategies.md) for non-FK columns
- Learn about [Distributions](distributions.md) in depth (normal, exponential, weighted)
- Understand [CDC (Change Data Capture)](../cdc/overview.md) for generating insert/update/delete streams
