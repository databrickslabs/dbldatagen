# dbldatagen.core

`dbldatagen.core` is a deterministic, distributed synthetic data
generator for Spark. You describe the data you want as a **plan** — a
set of tables and columns — and the engine materializes it as Spark
`DataFrame`s. The same plan and seed always produce byte-identical
output, which is what makes the data usable for test fixtures,
regression baselines, and reproducing customer issues.

> **New to the library and coming from the classic API?** The core
> engine is a separate, newer surface from the v0 `DataGenerator` /
> `.withColumn(...)` API documented in the Sphinx site. If you're
> porting an existing v0 generator, start with
> [`../MIGRATION_V0_TO_CORE.md`](../MIGRATION_V0_TO_CORE.md). This tree
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

### Output and reference
- [persisting-output.md](persisting-output.md) — write to Delta / Unity Catalog
- [loading-plans.md](loading-plans.md) — load plans from JSON / YAML
- [api-reference.md](api-reference.md) — `generate` / `generate_table` / `resolve_plan`
- [limitations.md](limitations.md) — what core can't do today (and the workarounds)
- [troubleshooting.md](troubleshooting.md) — common errors and fixes

### Recipes — [recipes/index.md](recipes/index.md)
Foundational building blocks:
- [recipes/multi-table.md](recipes/multi-table.md) — two tables joined by a foreign key
- [recipes/sequential-timestamps.md](recipes/sequential-timestamps.md) — evenly-spaced timestamps

Industry datasets:
- [recipes/retail-star-schema.md](recipes/retail-star-schema.md) — retail: `order_items` fact referencing orders + products
- [recipes/iot-telematics.md](recipes/iot-telematics.md) — IoT / GPS device telemetry (time series + device dimension)
- [recipes/stock-ticker.md](recipes/stock-ticker.md) — finance: daily OHLCV per symbol
- [recipes/process-historian.md](recipes/process-historian.md) — manufacturing: sensor / tag readings
- [recipes/realistic-customers.md](recipes/realistic-customers.md) — CRM: realistic PII with Faker
