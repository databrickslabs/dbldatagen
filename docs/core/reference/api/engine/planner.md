---
sidebar_label: planner
title: dbldatagen.core.engine.planner
---

Plan resolution: validates foreign-key references, sorts tables into
dependency order, and extracts the primary-key metadata each foreign-key column
needs.

## PKMetadata Objects

```python
@dataclass
class PKMetadata()
```

Primary-key metadata a foreign-key column needs to reconstruct parent keys.

Produced during plan resolution and used by the engine to reconstruct a
parent key value at a given row index, so the parent table is never
generated twice.

**Attributes**:

- `table_name` - Name of the parent table.
- `pk_column` - Name of the primary-key column.
- `row_count` - Number of rows the parent table produces; the index range
  foreign-key children sample from.
- `pk_type` - Primary-key kind: "sequence", "pattern", or "uuid".
- `pk_seed` - Column seed used to generate the primary key, so the child's
  reconstruction matches the parent.
- `pk_start` - Sequence start value (sequence keys only).
- `pk_step` - Sequence step value (sequence keys only).
- `pk_template` - Pattern template (pattern keys only); None otherwise.

#### pk\_type

"sequence", "pattern", "uuid"

#### pk\_seed

The column seed used for PK generation

#### pk\_start

For sequence PKs: start value

#### pk\_step

For sequence PKs: step value

#### pk\_template

For pattern PKs

## FKResolution Objects

```python
@dataclass
class FKResolution()
```

Resolved foreign-key information for a single column.

Created per foreign-key column at resolution time and read by the engine to
sample parent rows and reconstruct the referenced key for each child row.

**Attributes**:

- `child_table` - Name of the table that owns the foreign-key column.
- `child_column` - Name of the foreign-key column.
- `parent_metadata` - Primary-key metadata of the referenced parent.
- `distribution` - Sampling distribution over the parent row range (None
  means uniform).
- `null_fraction` - Probability in [0.0, 1.0] that a child row is NULL
  instead of resolving the key.

## ResolvedPlan Objects

```python
@dataclass
class ResolvedPlan()
```

A fully resolved plan with foreign-key metadata and generation order.

Produced by `resolve_plan(plan)`. Pass it to `generate` or `generate_table`
to skip re-resolution when generating the same plan multiple times.

**Attributes**:

- `generation_order` - Table names ordered so each parent is built before its
  children.
- `fk_resolutions` - Map of `(table, column)` to its `FKResolution`.
- `plan` - `DataGenPlan` used to build this resolved plan. `generate`
  identity-checks it so a resolved plan can't be used against a
  different plan.

### resolve\_plan

```python
def resolve_plan(plan: DataGenPlan) -> ResolvedPlan
```

Resolves a `DataGenPlan` into a generation-ready plan.

Validates foreign-key references, sorts the tables so each parent comes
before its children, and extracts the primary-key metadata each foreign-key
column needs. The result can be reused across repeated generations of the
same plan so resolution is paid only once.

**Arguments**:

- `plan` - The plan to resolve. Foreign-key references must use the
  `"table.column"` form and point at a column in the referenced
  table's primary key.
  

**Returns**:

  A `ResolvedPlan` with the generation order, per-column `FKResolution`
  records, and a back-pointer to `plan`.
  

**Raises**:

- `ValueError` - If a foreign-key reference is malformed, points at a missing
  or non-primary-key column, or the foreign-key graph contains a cycle.
  Also propagated from the expression, seed_from, and primary-key
  validators.

