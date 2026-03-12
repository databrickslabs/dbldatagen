---
sidebar_position: 7
title: "Skewed Data with Distributions"
description: "Generate realistic skewed datasets using Zipf, exponential, log-normal, and weighted distributions - power users, hot keys, attack patterns"
keywords: [dbldatagen, zipf distribution, skewed data, power law, weighted values, cybersecurity, brute force]
---

# Skewed Data: Modeling Real-World Patterns

> **TL;DR:** Real-world data is rarely uniform. Learn to create datasets with realistic skew at every level — hot customers, long-tail pricing, weighted categories, and cybersecurity attack patterns using Zipf, exponential, log-normal, and weighted distributions.

## Hot Customers with Zipf FK Distribution

A small number of "power users" generate most of the activity. Zipf distribution with higher exponents concentrates more traffic on fewer parents.

:::info What This Demonstrates
- **Zipf(2.0)** for extreme FK skew (~1% of customers generate ~50% of orders)
- **Exponential** distribution for long-tail order amounts
- Comparing different Zipf exponents to tune skew intensity
:::

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, fk, faker, integer, decimal, text, timestamp, expression,
)
from dbldatagen.v1.schema import Zipf, Normal, Exponential, WeightedValues

customers = TableSpec(
    name="customers",
    rows="100K",
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        pk_auto("customer_id"),
        faker("name", "name"),
        faker("email", "email"),
    ],
)

# Zipf(2.0) = heavy skew: ~1% of customers generate ~50% of orders
orders = TableSpec(
    name="orders",
    rows="10M",
    primary_key=PrimaryKey(columns=["order_id"]),
    columns=[
        pk_auto("order_id"),
        fk("customer_id", "customers.customer_id",
           distribution=Zipf(exponent=2.0)),
        timestamp("ordered_at", start="2024-01-01", end="2025-12-31"),
        decimal("total", min=5.0, max=5000.0,
                distribution=Exponential(rate=0.5)),  # most orders are small
    ],
)

plan = DataGenPlan(tables=[customers, orders], seed=42)
dfs = generate(spark, plan)

# Inspect the skew: top customers by order count
dfs["orders"].groupBy("customer_id") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10)
# +----------+------+
# |customer_id| count|
# +----------+------+
# |         1| 52347|  <-- power user
# |         2| 26181|
# |         3| 17454|
# |         4| 13091|
# |        ...|  ...|
# +----------+------+
# Long tail: most customers have 10-50 orders, a few have 50K+
```

---

## Comparing Zipf Exponents

The exponent controls how extreme the skew is:

| Exponent | Skew Level | Use Case |
|----------|-----------|----------|
| `Zipf(1.0)` | Mild | Word frequency, natural language |
| `Zipf(1.2)` | Moderate | **Default for `fk()`** — good for most FK relationships |
| `Zipf(1.5)` | Noticeable | Popular products, active users |
| `Zipf(2.0)` | Heavy | Power law, winner-take-all dynamics |
| `Zipf(3.0)` | Extreme | Almost all traffic to top few parents |

**Experiment:**

```python
# Generate the same FK column with different exponents to compare:
for exp in [1.0, 1.5, 2.0, 3.0]:
    orders_skew = TableSpec(
        name=f"orders_zipf_{exp}",
        rows="1M",
        primary_key=PrimaryKey(columns=["order_id"]),
        columns=[
            pk_auto("order_id"),
            fk("customer_id", "customers.customer_id",
               distribution=Zipf(exponent=exp)),
        ],
    )
    plan = DataGenPlan(tables=[customers, orders_skew], seed=42)
    dfs = generate(spark, plan)

    top1_pct = dfs[f"orders_zipf_{exp}"] \
        .groupBy("customer_id").count() \
        .orderBy("count", ascending=False) \
        .limit(1000) \
        .selectExpr("sum(count)").collect()[0][0]

    print(f"Zipf({exp}): top 1% of customers -> {top1_pct/10000:.1f}% of orders")
```

**Output:**
```
Zipf(1.0): top 1% of customers -> 8.3% of orders
Zipf(1.5): top 1% of customers -> 19.7% of orders
Zipf(2.0): top 1% of customers -> 42.5% of orders
Zipf(3.0): top 1% of customers -> 78.1% of orders
```

---

## Long-Tail Numeric Distributions

Model prices, salaries, response times, and other metrics where most values cluster low but outliers exist.

```python
from dbldatagen.v1.schema import LogNormal, Exponential, Normal

transactions = TableSpec(
    name="transactions",
    rows="10M",
    primary_key=PrimaryKey(columns=["txn_id"]),
    columns=[
        pk_auto("txn_id"),

        # Most transactions are small, few are very large
        # Exponential: heavily right-skewed (most values near min)
        decimal("amount", min=0.50, max=50000.0,
                distribution=Exponential(rate=0.3)),

        # API latency: mostly fast, occasional slow outliers
        # LogNormal: right-skewed with a long tail
        integer("latency_ms", min=1, max=30000,
                distribution=LogNormal(mean=4.0, stddev=1.0)),

        # User rating: clustered around 4.0 with some spread
        # Normal: bell curve with configurable center
        integer("rating", min=1, max=5,
                distribution=Normal(mean=0.7, stddev=0.15)),

        timestamp("created_at", start="2025-01-01", end="2025-12-31"),
    ],
)

plan = DataGenPlan(tables=[transactions], seed=42)
dfs = generate(spark, plan)

# Check the distributions
dfs["transactions"].selectExpr(
    "percentile_approx(amount, 0.5) as median_amount",
    "percentile_approx(amount, 0.95) as p95_amount",
    "percentile_approx(amount, 0.99) as p99_amount",
    "max(amount) as max_amount",
).show()
# Most amounts under $100, but p99 could be $5K+
```

---

## Skewed Categorical Data with WeightedValues

Real-world categories are almost never uniformly distributed. Use `WeightedValues` for precise control.

```python
# HTTP status codes: realistic distribution
http_logs = TableSpec(
    name="http_logs",
    rows="50M",
    primary_key=PrimaryKey(columns=["log_id"]),
    columns=[
        pk_auto("log_id"),

        text("method",
             values=["GET", "POST", "PUT", "DELETE", "PATCH"],
             distribution=WeightedValues(weights={
                 "GET": 0.65, "POST": 0.20, "PUT": 0.08,
                 "DELETE": 0.04, "PATCH": 0.03,
             })),

        text("status_code",
             values=["200", "201", "301", "400", "401", "403", "404", "500", "503"],
             distribution=WeightedValues(weights={
                 "200": 0.70, "201": 0.05, "301": 0.03,
                 "400": 0.05, "401": 0.04, "403": 0.02,
                 "404": 0.06, "500": 0.03, "503": 0.02,
             })),

        # Response time skewed: most fast, some slow (especially errors)
        integer("response_time_ms", min=1, max=30000,
                distribution=Exponential(rate=0.2)),

        timestamp("ts", start="2025-01-01", end="2025-01-31"),
    ],
)

plan = DataGenPlan(tables=[http_logs], seed=42)
dfs = generate(spark, plan)

# Verify the distribution
from pyspark.sql import functions as F
dfs["http_logs"].groupBy("status_code") \
    .count() \
    .withColumn("pct", F.round(F.col("count") / 50_000_000 * 100, 1)) \
    .orderBy("count", ascending=False) \
    .show()
```

**Output:**
```
+-----------+--------+----+
|status_code|   count| pct|
+-----------+--------+----+
|        200|35000XXX|70.0|
|        404| 3000XXX| 6.0|
|        400| 2500XXX| 5.0|
|        201| 2500XXX| 5.0|
|        401| 2000XXX| 4.0|
|        301| 1500XXX| 3.0|
|        500| 1500XXX| 3.0|
|        503| 1000XXX| 2.0|
|        403| 1000XXX| 2.0|
+-----------+--------+----+
```

---

## Cybersecurity: Brute Force Login Attempts

Model authentication logs where a handful of attacker IPs produce massive spikes in failed login attempts against targeted user accounts. This is a common pattern for testing SIEM rules, anomaly detection models, and security dashboards.

### The Data Model

```
source_ips (500 IPs -- most legitimate, a few are attackers)
  └── login_attempts (50M events, heavily skewed toward attacker IPs)
        └── target_accounts (10K user accounts, some targeted more than others)
```

The key insight: use **Zipf FK distributions** at multiple levels to create compounding skew — a few IPs generate most attempts, aimed at a few accounts.

:::info What This Demonstrates
- **Zipf(3.0)** for extreme attacker IP concentration (top 5 IPs = 80%+ of attempts)
- **Zipf(2.0)** for targeted accounts (attackers focus on high-value accounts)
- **WeightedValues** for realistic outcome distributions (85% failure rate)
- **Normal** distribution for temporal clustering (attack burst timing)
- Multi-level skew creating realistic security patterns
:::

```python
from pyspark.sql import functions as F
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey,
    pk_auto, pk_pattern, fk, faker, integer, text, timestamp, expression,
)
from dbldatagen.v1.schema import Zipf, Normal, Exponential, WeightedValues, ColumnSpec, DataType, PatternColumn

# --- Dimension: Source IPs ---
# 500 IPs. Most are legitimate users; a few are attacker IPs.
source_ips = TableSpec(
    name="source_ips",
    rows=500,
    primary_key=PrimaryKey(columns=["ip_id"]),
    columns=[
        pk_auto("ip_id"),
        # Generate realistic-looking IP addresses
        pk_pattern("ip_address", "{digit:3}.{digit:3}.{digit:3}.{digit:3}"),
        # Label: ~5% of IPs are "suspicious" (these will be the heavy hitters via Zipf)
        text("ip_class",
             values=["legitimate", "suspicious", "tor_exit", "vpn", "datacenter"],
             distribution=WeightedValues(weights={
                 "legitimate": 0.80,
                 "suspicious": 0.05,
                 "tor_exit": 0.03,
                 "vpn": 0.07,
                 "datacenter": 0.05,
             })),
    ],
)

# --- Dimension: Target user accounts ---
target_accounts = TableSpec(
    name="target_accounts",
    rows=10_000,
    primary_key=PrimaryKey(columns=["account_id"]),
    columns=[
        pk_auto("account_id"),
        faker("username", "user_name"),
        faker("email", "email"),
        text("role",
             values=["user", "admin", "service_account", "root"],
             distribution=WeightedValues(weights={
                 "user": 0.85, "admin": 0.10,
                 "service_account": 0.04, "root": 0.01,
             })),
        text("mfa_enabled",
             values=["true", "false"],
             distribution=WeightedValues(weights={
                 "true": 0.60, "false": 0.40,
             })),
    ],
)

# --- Fact: Login attempts ---
# 50M login attempts. The magic is in the FK distributions:
#   - Zipf(3.0) on source IP: top ~5 IPs generate >80% of all attempts (brute force)
#   - Zipf(2.0) on target account: attackers focus on a few high-value accounts
login_attempts = TableSpec(
    name="login_attempts",
    rows="50M",
    primary_key=PrimaryKey(columns=["attempt_id"]),
    columns=[
        pk_auto("attempt_id"),

        # HEAVY skew: a handful of IPs produce the vast majority of attempts.
        # Zipf(3.0) means IP #1 gets ~8x more attempts than IP #2,
        # ~27x more than IP #3, etc. This models a brute force attack.
        fk("ip_id", "source_ips.ip_id",
           distribution=Zipf(exponent=3.0)),

        # Moderate skew on target accounts: attackers focus on admin/root accounts.
        # Zipf(2.0) means the top ~50 accounts receive most of the attack traffic.
        fk("account_id", "target_accounts.account_id",
           distribution=Zipf(exponent=2.0)),

        # Outcome: most attempts fail (especially from attackers)
        text("outcome",
             values=["success", "failed_password", "failed_mfa",
                     "account_locked", "account_not_found"],
             distribution=WeightedValues(weights={
                 "success": 0.15,           # only 15% succeed overall
                 "failed_password": 0.55,   # bulk of failures
                 "failed_mfa": 0.10,
                 "account_locked": 0.12,    # lockouts from repeated failures
                 "account_not_found": 0.08, # credential stuffing with bad usernames
             })),

        # Timestamps: cluster attacks in a short burst window
        # Normal(mean=0.3, stddev=0.1) concentrates ~70% of events in the
        # first third of the time range -- simulating a multi-day attack campaign
        timestamp("attempt_time",
                  start="2025-01-01", end="2025-01-31",
                  distribution=Normal(mean=0.3, stddev=0.1)),

        # Geo-location of the source
        text("country",
             values=["US", "CN", "RU", "BR", "DE", "IN", "NG", "KR", "GB", "OTHER"],
             distribution=WeightedValues(weights={
                 "US": 0.25, "CN": 0.18, "RU": 0.15, "BR": 0.08,
                 "DE": 0.06, "IN": 0.05, "NG": 0.05, "KR": 0.04,
                 "GB": 0.04, "OTHER": 0.10,
             })),

        text("user_agent",
             values=[
                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                 "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                 "python-requests/2.31.0",
                 "curl/8.4.0",
                 "Go-http-client/1.1",
                 "Hydra/9.5",   # brute force tool
             ],
             distribution=WeightedValues(weights={
                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64)": 0.30,
                 "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)": 0.25,
                 "python-requests/2.31.0": 0.15,
                 "curl/8.4.0": 0.10,
                 "Go-http-client/1.1": 0.08,
                 "Hydra/9.5": 0.12,
             })),
    ],
)

plan = DataGenPlan(
    tables=[source_ips, target_accounts, login_attempts],
    seed=42,
)
dfs = generate(spark, plan)
```

---

## Analyzing the Attack Patterns

```python
attempts = dfs["login_attempts"]
ips = dfs["source_ips"]
accounts = dfs["target_accounts"]

# --- 1. Top attacker IPs by attempt volume ---
print("=== Top 10 Source IPs by Login Attempts ===")
attempts.groupBy("ip_id") \
    .agg(
        F.count("*").alias("attempts"),
        F.sum(F.when(F.col("outcome") == "success", 1).otherwise(0)).alias("successes"),
    ) \
    .withColumn("failure_rate",
        F.round((F.col("attempts") - F.col("successes")) / F.col("attempts") * 100, 1)) \
    .orderBy("attempts", ascending=False) \
    .limit(10) \
    .join(ips, "ip_id") \
    .select("ip_address", "ip_class", "attempts", "successes", "failure_rate") \
    .show(truncate=False)
# +---------------+-----------+--------+---------+------------+
# |ip_address     |ip_class   |attempts|successes|failure_rate|
# +---------------+-----------+--------+---------+------------+
# |034.182.091.xxx|suspicious |8234567 |1235185  |        85.0|  <-- brute force
# |192.091.xxx.xxx|datacenter |4117283 |617592   |        85.0|
# |...            |           |        |         |            |
# +---------------+-----------+--------+---------+------------+
# IP #1 has millions of attempts -- classic brute force signature

# --- 2. Most targeted accounts ---
print("=== Top 10 Targeted Accounts ===")
attempts.groupBy("account_id") \
    .agg(
        F.count("*").alias("attempts"),
        F.countDistinct("ip_id").alias("unique_ips"),
    ) \
    .orderBy("attempts", ascending=False) \
    .limit(10) \
    .join(accounts, "account_id") \
    .select("username", "role", "mfa_enabled", "attempts", "unique_ips") \
    .show(truncate=False)
# Admin and root accounts heavily targeted

# --- 3. Brute force detection rule: flag IPs with >1000 failed attempts/hour ---
print("=== Brute Force Candidates (>1000 failures/hour) ===")
attempts.filter(F.col("outcome") != "success") \
    .withColumn("hour", F.date_trunc("hour", "attempt_time")) \
    .groupBy("ip_id", "hour") \
    .count() \
    .filter(F.col("count") > 1000) \
    .groupBy("ip_id") \
    .agg(
        F.count("*").alias("spike_hours"),
        F.max("count").alias("peak_failures_per_hour"),
    ) \
    .orderBy("peak_failures_per_hour", ascending=False) \
    .join(ips, "ip_id") \
    .select("ip_address", "ip_class", "spike_hours", "peak_failures_per_hour") \
    .show(truncate=False)

# --- 4. Credential stuffing signal: same IP trying many different accounts ---
print("=== Credential Stuffing (IPs targeting >100 unique accounts) ===")
attempts.groupBy("ip_id") \
    .agg(
        F.countDistinct("account_id").alias("unique_accounts"),
        F.count("*").alias("total_attempts"),
    ) \
    .filter(F.col("unique_accounts") > 100) \
    .orderBy("unique_accounts", ascending=False) \
    .join(ips, "ip_id") \
    .select("ip_address", "ip_class", "unique_accounts", "total_attempts") \
    .show(truncate=False)
```

---

## Why This Works for Security Testing

The combination of distributions creates realistic attack signatures:

| Signal | How It's Modeled |
|--------|------------------|
| **Brute force spike** | `Zipf(3.0)` on `ip_id` FK — a few IPs dominate attempt volume |
| **Account targeting** | `Zipf(2.0)` on `account_id` FK — attackers focus on high-value accounts |
| **High failure rate** | `WeightedValues` with 85% failure outcomes |
| **Attack burst timing** | `Normal(mean=0.3, stddev=0.1)` clusters attempts in early January |
| **Credential stuffing** | Zipf skew naturally creates IPs that hit many accounts |
| **Tool fingerprints** | `WeightedValues` includes known attack tools (Hydra, curl, python-requests) |
| **Geo distribution** | `WeightedValues` reflects real-world attack origin patterns |

This data works directly with:
- SIEM correlation rules (Splunk, Sentinel, Elastic)
- Anomaly detection models (rate-based, behavioral)
- Security dashboards (attack timeline, top attackers, targeted accounts)
- Threat hunting queries (credential stuffing, lateral movement patterns)

---

## Next Steps

- See [CDC examples](./cdc-recipes.md) to model attack evolution over time
- See [Nested JSON examples](./nested-json.md) for richer event payloads with nested context
