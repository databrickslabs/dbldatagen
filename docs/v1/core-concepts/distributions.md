---
sidebar_position: 5
title: "Distributions"
---

# Distributions

> **TL;DR:** Distributions control the statistical shape of generated values. Use `Uniform()` for equal probability (default), `Normal()` for bell curves, `Zipf()` for power-law skew (hot items), `Exponential()` for rare events, `LogNormal()` for right-skewed positive values, and `WeightedValues()` for explicit probabilities. Apply to range columns, values columns, timestamp columns, and foreign key parent selection.

## What Are Distributions?

Distributions determine **how values are selected** from the available space. They apply to:
- **Range columns** — where in `[min, max]` to sample values
- **Values columns** — which items from the list to pick
- **Timestamp columns** — when in `[start, end]` to sample times
- **Foreign key columns** — which parent rows to reference

All distributions are:
- **Deterministic** — same seed + row ID = same value
- **Partition-independent** — work correctly across any cluster size
- **Composable** — can be used with any compatible column strategy

## Distribution 1: Uniform (Default)

Every value has equal probability. This is the default for all column types.

```python
from dbldatagen.v1 import integer, text, timestamp
from dbldatagen.v1.schema import Uniform

# Explicit uniform (though this is the default)
integer("age", min=18, max=90, distribution=Uniform())

# Implicit uniform (same as above)
integer("age", min=18, max=90)

# Uniform selection from list
text("status", values=["active", "inactive", "suspended"])

# Uniform timestamps
timestamp("created_at", start="2023-01-01", end="2025-12-31")
```

### Parameters

**None** — uniform distribution has no configuration parameters.

### When to Use

- **Default choice** for most columns
- When you have no reason to prefer certain values over others
- Testing with evenly-distributed data
- Baseline before adding realistic skew

### Example Output

For `integer("score", min=0, max=100)` with 1000 rows, you'll get roughly even distribution across the full range:

```
Score Range | Count
0-20        | ~200
21-40       | ~200
41-60       | ~200
61-80       | ~200
81-100      | ~200
```

## Distribution 2: Normal (Gaussian)

Bell-curve distribution with values clustered around a mean. Useful for realistic numeric data like human attributes, measurements, and scores.

```python
from dbldatagen.v1 import integer, decimal, timestamp
from dbldatagen.v1.schema import Normal

# Age clustered around 45 (mean=0.5 in [18,90] range)
integer("age", min=18, max=90, distribution=Normal(mean=0.5, stddev=0.2))

# Prices clustered around $50 (lighter tail)
decimal("price", min=1.0, max=1000.0, distribution=Normal(mean=50.0, stddev=30.0))

# Test scores clustered around 75
integer("test_score", min=0, max=100, distribution=Normal(mean=75, stddev=15))

# Event times clustered in middle of year
timestamp("event_time", start="2024-01-01", end="2024-12-31",
          distribution=Normal(mean=0.5, stddev=0.2))
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `mean` | `float` | `0.0` | Center of the bell curve (0.0-1.0 for normalized, or absolute value) |
| `stddev` | `float` | `1.0` | Standard deviation (spread of the curve) |

**Important:** For range columns, `mean` is interpreted as a **fraction** of the range (0.0 = min, 1.0 = max). For FK columns and internal uses, `mean` and `stddev` operate on normalized [0,1] space.

### When to Use

- Human attributes: age, height, weight, IQ
- Test scores and grades
- Response times (when not right-skewed)
- Any measurement that clusters around a typical value
- Time ranges where activity clusters (e.g., business hours)

### How It Works

Implementation uses the **Central Limit Theorem**: sum 12 uniform random variables to approximate a normal distribution. This is computationally efficient and deterministic.

### Example Output

For `integer("score", min=0, max=100, distribution=Normal(mean=75, stddev=15))`:

```
Score Range | Count | Visualization
0-40        | 50    | ▌
41-60       | 180   | ███▌
61-80       | 540   | ██████████▊
81-100      | 230   | ████▌
```

The bulk of values cluster around 75, with a gradual tail-off toward the extremes.

## Distribution 3: Zipf (Power Law)

Power-law distribution where a few values are very common and most are rare. Models real-world phenomena like word frequencies, city populations, and product popularity (the "80/20 rule").

```python
from dbldatagen.v1 import fk, integer
from dbldatagen.v1.schema import Zipf

# Hot customers: ~20% get ~80% of orders
fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=1.5))

# Extreme skew: very few customers dominate
fk("product_id", "products.product_id", distribution=Zipf(exponent=2.0))

# Lighter skew: more evenly distributed
fk("category_id", "categories.category_id", distribution=Zipf(exponent=1.0))

# Can also apply to range columns (though less common)
integer("popularity_rank", min=1, max=10000, distribution=Zipf(exponent=1.5))
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `exponent` | `float` | `1.5` | Skew intensity (higher = more concentrated) |

**Common values:**
- `exponent=1.0` — Mild skew (Zipf's law)
- `exponent=1.2` — Moderate skew (**default for `fk()` DSL helper**)
- `exponent=1.5` — Strong skew (80/20 rule)
- `exponent=2.0` — Extreme skew (90/10 or more)

### When to Use

- **Foreign keys** — realistic customer/product selection (default for FKs)
- E-commerce: some products are bestsellers, most are long-tail
- Social networks: popular users have many followers, most have few
- Web traffic: some pages get most views
- Natural language: common words appear frequently, rare words rarely

### Mathematical Background

Zipf distribution follows the formula:

```
P(k) ∝ 1 / k^exponent
```

Where `k` is the rank (1, 2, 3, ...). The first item is most common, second item is 1/2^exponent as common, third is 1/3^exponent, etc.

### Example Output

For FK to 100 parent rows with `Zipf(exponent=1.5)` and 10,000 child rows:

```
Parent ID | Child Count | Visualization
1         | 1850        | █████████████████████████████▌
2         | 654         | ██████████▌
3         | 384         | ██████▏
4         | 265         | ████▎
5         | 196         | ███▎
6-10      | ~800 total  | (decreasing)
11-100    | ~6851 total | (long tail)
```

The first few parents get the majority of children, creating realistic hot-item scenarios.

## Distribution 4: Exponential

Models time between events, failure rates, and rare occurrences. Values cluster near the minimum with a long right tail.

```python
from dbldatagen.v1 import decimal, integer
from dbldatagen.v1.schema import Exponential

# Wait times (most short, some very long)
decimal("wait_time_sec", min=0, max=300, distribution=Exponential(rate=0.1))

# Inter-arrival times
decimal("seconds_between_requests", min=0, max=3600, distribution=Exponential(rate=0.01))

# Failure counts (most items have 0-1 failures, rare outliers)
integer("failure_count", min=0, max=100, distribution=Exponential(rate=1.0))
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rate` | `float` | `1.0` | Rate parameter (higher = more concentrated near min) |

**Effect of rate:**
- `rate=0.1` — Gentle decay, spread across range
- `rate=1.0` — Moderate decay (**default**)
- `rate=5.0` — Steep decay, heavily concentrated near minimum

### When to Use

- Time between events (requests, failures, arrivals)
- Lifetimes and durations (until failure)
- Rare event counts
- Queue wait times
- Any phenomenon where "most are small, few are large"

### Example Output

For `decimal("wait_time", min=0, max=300, distribution=Exponential(rate=0.1))`:

```
Wait Time Range | Count | Visualization
0-30 sec        | 650   | █████████████▌
31-60 sec       | 180   | ███▌
61-120 sec      | 120   | ██▌
121-300 sec     | 50    | ▌
```

Most values cluster near zero with a long tail toward the maximum.

## Distribution 5: LogNormal

Right-skewed distribution for positive values. Models prices, incomes, file sizes, and other quantities that multiply rather than add.

```python
from dbldatagen.v1 import decimal, integer
from dbldatagen.v1.schema import LogNormal

# Salaries (most mid-range, some very high)
decimal("salary", min=30000, max=500000, distribution=LogNormal(mean=11.0, stddev=0.5))

# File sizes (most small, some huge)
integer("file_size_kb", min=1, max=1000000, distribution=LogNormal(mean=6.0, stddev=2.0))

# Product prices (realistic right skew)
decimal("price", min=0.99, max=9999.99, distribution=LogNormal(mean=3.5, stddev=1.2))
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `mean` | `float` | `0.0` | Mean of the underlying normal distribution (log space) |
| `stddev` | `float` | `1.0` | Standard deviation in log space |

**Note:** `mean` and `stddev` are parameters of the **logarithm** of the value, not the value itself. Adjusting these changes the skew and spread.

### When to Use

- Financial data: salaries, prices, account balances
- File sizes, memory usage, data volumes
- Any quantity that grows multiplicatively
- Right-skewed positive data (cannot be negative)

### Mathematical Background

LogNormal means `log(value)` follows a normal distribution. Values are:
1. Sample from normal distribution
2. Exponentiate: `value = exp(normal_sample)`
3. Scale to `[min, max]` range

### Example Output

For `decimal("salary", min=30000, max=500000, distribution=LogNormal(mean=11.0, stddev=0.5))`:

```
Salary Range    | Count | Visualization
$30K-$50K       | 450   | █████████▌
$50K-$80K       | 350   | ███████▌
$80K-$120K      | 140   | ███▌
$120K-$200K     | 50    | ▌
$200K-$500K     | 10    | ▎
```

Most salaries cluster in the mid-range with a long tail of high earners.

## Distribution 6: WeightedValues

Explicit probability weights for categorical columns. Gives fine-grained control over selection frequencies.

```python
from dbldatagen.v1 import text
from dbldatagen.v1.schema import WeightedValues

# Customer tiers (70% bronze, 25% silver, 5% gold)
text("tier",
     values=["bronze", "silver", "gold"],
     distribution=WeightedValues(weights={
         "bronze": 0.70,
         "silver": 0.25,
         "gold": 0.05,
     }))

# Event types with realistic mix
text("event_type",
     values=["page_view", "click", "add_to_cart", "purchase", "signup"],
     distribution=WeightedValues(weights={
         "page_view": 0.55,
         "click": 0.25,
         "add_to_cart": 0.10,
         "purchase": 0.08,
         "signup": 0.02,
     }))

# Order status (most delivered, few cancelled)
text("status",
     values=["pending", "shipped", "delivered", "cancelled"],
     distribution=WeightedValues(weights={
         "pending": 0.05,
         "shipped": 0.15,
         "delivered": 0.70,
         "cancelled": 0.10,
     }))
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `weights` | `dict[str, float]` | Map from value to relative weight |

**Important:**
- Weights are **relative** — they're automatically normalized to sum to 1.0
- Example: `{"A": 7, "B": 2, "C": 1}` = `{"A": 0.7, "B": 0.2, "C": 0.1}`
- Missing values get weight 0 (never selected)
- All values in `ValuesColumn.values` should have corresponding weights

### When to Use

- Categorical columns with known frequency distributions
- Realistic business data (e.g., most orders delivered, few cancelled)
- A/B testing scenarios (e.g., 90% control, 10% treatment)
- Priority levels (e.g., 60% low, 30% medium, 10% high)

### Example Output

For the customer tier example above with 10,000 customers:

```
Tier   | Count | Percentage | Visualization
bronze | 7,050 | 70.5%      | ██████████████▌
silver | 2,470 | 24.7%      | █████▌
gold   | 480   | 4.8%       | ▌
```

Values match the specified weights (with minor random variation).

## Where Distributions Apply

### Range Columns

All numeric distributions work:

```python
integer("age", min=18, max=90, distribution=Normal(mean=0.5, stddev=0.2))
decimal("price", min=1.0, max=1000.0, distribution=LogNormal(mean=4.0, stddev=1.5))
integer("count", min=0, max=1000, distribution=Zipf(exponent=1.5))
decimal("duration", min=0, max=3600, distribution=Exponential(rate=0.1))
```

**Not applicable:** `WeightedValues` (only for categorical data)

### Values Columns

Uniform and weighted distributions:

```python
# Uniform (default)
text("color", values=["red", "green", "blue"])

# Weighted
text("color", values=["red", "green", "blue"],
     distribution=WeightedValues(weights={"red": 0.5, "green": 0.3, "blue": 0.2}))
```

**Not applicable:** Normal, LogNormal, Zipf, Exponential (designed for continuous/ordinal data)

### Timestamp Columns

All numeric distributions work (applied to the Unix epoch range):

```python
timestamp("created_at", start="2024-01-01", end="2024-12-31",
          distribution=Normal(mean=0.5, stddev=0.2))  # Cluster in middle of year

timestamp("event_time", start="2024-01-01", end="2024-12-31",
          distribution=Exponential(rate=0.1))  # More events at start
```

### Foreign Key Columns

All distributions work:

```python
# Uniform: every parent gets ~equal children
fk("customer_id", "customers.customer_id", distribution=Uniform())

# Zipf: hot customers (default for fk() helper)
fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=1.2))

# Normal: cluster around middle parent indexes
fk("region_id", "regions.region_id", distribution=Normal(mean=0.5, stddev=0.2))
```

**Not applicable:** `WeightedValues` (FK selection operates on parent row indexes, not explicit values)

## Distribution Comparison

| Distribution | Shape | Common Use Cases | Parameters |
|--------------|-------|------------------|------------|
| **Uniform** | Flat | Default, testing, even distribution | None |
| **Normal** | Bell curve | Human attributes, scores, measurements | `mean`, `stddev` |
| **Zipf** | Power-law | Hot items, social networks, word frequencies | `exponent` |
| **Exponential** | Right-skewed | Wait times, lifetimes, rare events | `rate` |
| **LogNormal** | Right-skewed | Prices, salaries, file sizes | `mean`, `stddev` (log space) |
| **WeightedValues** | Custom | Explicit probabilities for categories | `weights` (dict) |

## Practical Examples

### Example 1: E-Commerce Realism

```python
from dbldatagen.v1 import generate
from dbldatagen.v1.schema import DataGenPlan, TableSpec, PrimaryKey, Zipf, LogNormal, WeightedValues
from dbldatagen.v1 import pk_auto, fk, integer, decimal, text, timestamp
from pyspark.sql import SparkSession

customers = TableSpec(
    name="customers",
    rows="100K",
    columns=[
        pk_auto("customer_id"),
        text("name", values=["Alice", "Bob", "Carol"]),
        text("tier", values=["bronze", "silver", "gold"],
             distribution=WeightedValues(weights={"bronze": 0.70, "silver": 0.25, "gold": 0.05})),
    ],
    primary_key=PrimaryKey(columns=["customer_id"]),
)

products = TableSpec(
    name="products",
    rows="10K",
    columns=[
        pk_auto("product_id"),
        text("name", values=["Widget", "Gadget", "Doohickey"]),
        # Prices with right-skew (most cheap, some expensive)
        decimal("price", min=0.99, max=9999.99, distribution=LogNormal(mean=3.0, stddev=1.5)),
    ],
    primary_key=PrimaryKey(columns=["product_id"]),
)

orders = TableSpec(
    name="orders",
    rows="5M",
    columns=[
        pk_auto("order_id"),
        # Hot customers (Zipf): some customers order frequently
        fk("customer_id", "customers.customer_id", distribution=Zipf(exponent=1.5)),
        # Product popularity follows Zipf (bestsellers)
        fk("product_id", "products.product_id", distribution=Zipf(exponent=1.8)),
        integer("quantity", min=1, max=10),
        # Realistic order status distribution
        text("status", values=["pending", "shipped", "delivered", "cancelled"],
             distribution=WeightedValues(weights={
                 "pending": 0.05,
                 "shipped": 0.15,
                 "delivered": 0.75,
                 "cancelled": 0.05,
             })),
        timestamp("order_date", start="2023-01-01", end="2024-12-31"),
    ],
    primary_key=PrimaryKey(columns=["order_id"]),
)

plan = DataGenPlan(tables=[customers, products, orders], seed=42)
spark = SparkSession.builder.appName("ecommerce").getOrCreate()
dfs = generate(spark, plan)

# Analyze distributions
dfs["orders"].groupBy("customer_id").count().orderBy("count", ascending=False).show(10)
dfs["orders"].groupBy("status").count().show()
```

### Example 2: Web Analytics

```python
sessions = TableSpec(
    name="sessions",
    rows="10M",
    columns=[
        pk_auto("session_id"),
        fk("user_id", "users.user_id", distribution=Zipf(exponent=1.3)),  # Power users
        # Event types with realistic mix (mostly page views)
        text("event_type", values=["page_view", "click", "purchase", "signup"],
             distribution=WeightedValues(weights={
                 "page_view": 0.70,
                 "click": 0.20,
                 "purchase": 0.08,
                 "signup": 0.02,
             })),
        # Session duration: most short, some very long
        integer("duration_sec", min=1, max=7200, distribution=Exponential(rate=0.001)),
        timestamp("started_at", start="2024-01-01", end="2024-12-31"),
    ],
    primary_key=PrimaryKey(columns=["session_id"]),
)
```

### Example 3: Human Attributes (Normal Distribution)

```python
people = TableSpec(
    name="people",
    rows="1M",
    columns=[
        pk_auto("person_id"),
        # Age: bell curve centered at 45, most between 25-65
        integer("age", min=18, max=90, distribution=Normal(mean=0.5, stddev=0.2)),
        # Height in cm: centered at 170cm
        integer("height_cm", min=140, max=210, distribution=Normal(mean=170, stddev=15)),
        # IQ: centered at 100
        integer("iq", min=70, max=160, distribution=Normal(mean=100, stddev=15)),
    ],
    primary_key=PrimaryKey(columns=["person_id"]),
)
```

## Tuning Distribution Parameters

### Normal Distribution

- `mean=0.5, stddev=0.1` — Tight cluster around center
- `mean=0.5, stddev=0.2` — **Recommended default** (covers ~95% within range)
- `mean=0.5, stddev=0.3` — Wide spread
- `mean=0.2, stddev=0.1` — Cluster near minimum
- `mean=0.8, stddev=0.1` — Cluster near maximum

### Zipf Distribution

- `exponent=1.0` — Mild skew (Zipf's law)
- `exponent=1.2` — **Recommended for FKs** (balanced realism)
- `exponent=1.5` — Strong skew (80/20 rule)
- `exponent=2.0` — Extreme skew (90/10 or more)
- `exponent=3.0` — Very extreme (99/1)

### Exponential Distribution

- `rate=0.01` — Gentle decay, spread across range
- `rate=0.1` — Moderate decay
- `rate=1.0` — **Default** (steep decay near min)
- `rate=10.0` — Very steep decay (almost all near min)

### LogNormal Distribution

- `mean=0.0, stddev=1.0` — **Default** (moderate right skew)
- `mean=1.0, stddev=0.5` — Shift toward higher values
- `mean=0.0, stddev=2.0` — Extreme right skew (very long tail)

**Tip:** For financial data, calibrate `mean` and `stddev` based on the log of typical values in your domain.

## Next Steps

- Apply distributions to [Foreign Keys](foreign-keys.md) for realistic parent selection
- Combine with [Column Strategies](column-strategies.md) for rich data models
- Generate realistic test data for [CDC streams](../cdc/overview.md)
