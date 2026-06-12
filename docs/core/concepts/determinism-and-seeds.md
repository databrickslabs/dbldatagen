# Determinism and seeds

Reproducibility is the core contract: **the same plan and seed always
produce byte-identical data**, on any cluster, at any scale. That's
what makes core output usable as test fixtures, regression baselines,
and customer-bug reproductions. This page explains the seed model and
how to vary it when you genuinely want different data.

## The seed model

- `DataGenPlan.seed` is the single global seed for the whole plan.
- It's **propagated to each table**: a `TableSpec` whose own `seed` is
  `None` (the usual case) gets `plan.seed + i`, where `i` is the
  table's index in the plan. So tables are independently seeded but
  still fully determined by the one global seed.
- Within a table, each cell's value is derived deterministically from
  the table seed and the row index, so generation is reproducible even
  though it runs distributed across Spark partitions.

```python
DataGenPlan(seed=42, tables=[a, b, c])
# a.seed -> 42, b.seed -> 43, c.seed -> 44  (when each table.seed is None)
```

Set a `TableSpec.seed` explicitly only when generating a single table
with `generate_table` outside a plan.

## The default seed and its warning

`DataGenPlan.seed` defaults to `42` for tutorial and demo convenience.
This is a deliberate reproducibility trap: two independent callers who
both omit `seed` produce identical data without realizing the match is
coincidental. To surface that, constructing a `DataGenPlan` **without
an explicit `seed` emits a `UserWarning`**:

```
DataGenPlan constructed without an explicit ``seed`` -- defaulting to 42. ...
Pass ``seed=<int>`` to silence this warning.
```

The default still fills in so tutorial code keeps working, but CI / log
scraping can catch the omission. **Production callers should always
pass `seed=<int>`.**

## Reproducible UUIDs and patterns

Strategies that look random are still seed-deterministic:

- `UUIDColumn` derives each UUID from `(table seed, row index)` — the
  same seed and row always yield the same UUID.
- `PatternColumn`, `ValuesColumn`, `RangeColumn`, etc. all draw from
  the same seeded stream.

## Correlated columns: `seed_from`

To make two columns agree — e.g. the same `device_id` always maps to
the same `country` — set `seed_from` on the dependent column to the
name of the column it should track. Equal values in the source column
produce equal values in the dependent one. See
[correlated-columns.md](../relationships/correlated-columns.md).

## Varying the data: randomness is an invocation-time decision

A common question: *"How do I get fresh, different data on every run?"*

Core deliberately does **not** offer a "randomize on omit" mode or a
magic sentinel seed. Auto-randomizing would break the determinism
contract — the same plan would silently produce different output across
runs, and a developer reproducing a customer issue couldn't get the
same data twice.

Instead, randomness enters at the **invocation boundary**, not in the
spec. The plan describes *what* to generate; the caller decides *this
run's* seed:

```python
import random

plan = DataGenPlan(seed=random.randint(0, 2**63 - 1), tables=[...])
```

For a plan loaded from a file, override the seed at load time rather
than baking randomness into the file:

```python
import random, yaml
from dbldatagen.core import DataGenPlan

raw = yaml.safe_load(open("plan.yml").read())
raw["seed"] = random.randint(0, 2**63 - 1)   # fresh each run
plan = DataGenPlan.model_validate(raw)
```

This keeps every spec deterministic by default and makes "different
data each run" an explicit, opt-in caller choice — see
[loading-plans.md](../loading-plans.md).

## Reproducing a specific dataset later

Because output is fully determined by the plan and seed, capturing the
seed is enough to regenerate the exact dataset. Log the seed you used
(especially if you randomized it), and you can reproduce that run
byte-for-byte by passing the same value back.
