---
sidebar_label: api
title: dbldatagen.core.api
---

Top-level generation entry point for `dbldatagen.core`.

Defines `generate`, which materializes every table in a plan and returns the
resulting DataFrames keyed by table name.

### generate

```python
def generate(
        spark: SparkSession,
        plan: DataGenPlan,
        resolved_plan: ResolvedPlan | None = None) -> dict[str, DataFrame]
```

Generates every table in a plan and returns them keyed by name.

Tables are generated in dependency order, so a foreign-key child is built
after the parent table it references.

**Arguments**:

- `spark` - Active `SparkSession` used to build the DataFrames.
- `plan` - The plan to generate. Its `seed` must be set, directly or
  propagated from `DataGenPlan` to each `TableSpec`.
- `resolved_plan` - Optional pre-resolved plan from `resolve_plan(plan)`,
  reused to avoid re-resolving when generating the same plan more than
  once (default None). It must come from the same plan object.
  

**Returns**:

  A dict mapping each table name to its generated `DataFrame`, ordered
  with parents before children.
  

**Raises**:

- `ValueError` - If `resolved_plan` was produced from a different plan than
  the one passed in.

