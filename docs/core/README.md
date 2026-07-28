# dbldatagen.core

`dbldatagen.core` is a deterministic, distributed synthetic data
generator for Spark. You describe the data you want as a **plan** — a
set of tables and columns — and the engine materializes it as Spark
`DataFrame`s. The same plan and seed always produce byte-identical
output, which is what makes the data usable for test fixtures,
regression baselines, and reproducing customer issues.

> **New to the library and coming from the classic API?** The core
> engine is a separate, newer surface from the v0 `DataGenerator` /
> `.withColumn(...)` API documented in the Sphinx site. This tree
> documents core on its own terms.

## Install

Core requires `pydantic>=2.0`, which ships in the `core` extra:

```bash
pip install 'dbldatagen[core]'
```

Some strategies need additional extras:

| Feature | Extra |
| --- | --- |
| `FakerColumn` / `datagendg.faker(...)` | `pip install 'dbldatagen[core-faker]'` |

## 30-second example

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, ColumnSpec
from dbldatagen.core.spec.schema import SequenceColumn, RangeColumn

plan = DataGenPlan(
    seed=42,
    tables=[
        TableSpec(
            name="orders",
            rows=1000,
            columns=[
                ColumnSpec(name="order_id", gen=SequenceColumn(start=1, step=1)),
                ColumnSpec(name="amount", gen=RangeColumn(min=5.0, max=500.0)),
            ],
        ),
    ],
)

dfs = generate(spark, plan)   # -> {"orders": DataFrame}
dfs["orders"].show()
```

`generate` returns a `dict` keyed by table name. See
[getting-started.md](getting-started.md) for the full walkthrough.

## Two ways to author a plan

Every plan can be written two equivalent ways — pick whichever reads
better for your team:

- **Schema form** — explicit Pydantic models (`ColumnSpec`,
  `SequenceColumn`, …). Verbose but self-documenting; this is also what
  JSON/YAML plans deserialize into.
- **DSL form** — lowercase factory helpers imported as
  `from dbldatagen.core.spec import dsl as datagendg`. Compact, mirrors
  the PySpark `import ... as F` convention.

See [concepts/authoring-styles.md](concepts/authoring-styles.md).

## Documentation map

### Start here
- [getting-started.md](getting-started.md) — build a plan, generate, inspect

### Concepts
- [concepts/plans-and-tables.md](concepts/plans-and-tables.md) — `DataGenPlan` / `TableSpec` / `ColumnSpec`
- [concepts/authoring-styles.md](concepts/authoring-styles.md) — schema vs DSL
- [concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md) — seeds, reproducibility

### Column strategies
- [column-strategies/index.md](column-strategies/index.md) — pick a strategy
- [numeric-ranges](column-strategies/numeric-ranges.md) ·
  [categorical-values](column-strategies/categorical-values.md) ·
  [sequences-and-ids](column-strategies/sequences-and-ids.md) ·
  [timestamps](column-strategies/timestamps.md) ·
  [text-and-patterns](column-strategies/text-and-patterns.md) ·
  [expressions](column-strategies/expressions.md) ·
  [constants](column-strategies/constants.md) ·
  [nested](column-strategies/nested.md)

### Shaping and relationships
- [distributions.md](distributions.md) — Uniform / Normal / LogNormal / Zipf / Exponential
- [nullable-and-dirty-data.md](nullable-and-dirty-data.md) — `null_fraction`, nulls, dirty data
- [relationships/foreign-keys.md](relationships/foreign-keys.md) — cross-table FKs
- [relationships/correlated-columns.md](relationships/correlated-columns.md) — `seed_from`

### Reference
- [loading-plans.md](loading-plans.md) — load plans from JSON / YAML
- [API reference](reference/api/api.md) — `generate` / `generate_table` /
  `resolve_plan`. Auto-generated from docstrings (`make docs-api-core`); see
  also [spec/schema](reference/api/spec/schema.md) and
  [spec/dsl](reference/api/spec/dsl.md).
- [limitations.md](limitations.md) — what core can't do today (and the workarounds)
- [troubleshooting.md](troubleshooting.md) — common errors and fixes
