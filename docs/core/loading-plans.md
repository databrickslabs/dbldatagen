# Loading plans from JSON and YAML

A `DataGenPlan` doesn't have to be built in Python. The same plan can
live in a JSON or YAML file and be deserialized into the identical
`DataGenPlan` you'd construct by hand. This keeps the *what* (the data
shape) in version-controllable config, separate from the *how* (the
Python that calls `generate`).

The whole mechanism is one Pydantic call: read the file into a plain
`dict`, then hand that dict to `DataGenPlan.model_validate(...)`.

## The pattern

```python
import json
from dbldatagen.core import DataGenPlan, generate

raw = json.loads(open("plan.json").read())
plan = DataGenPlan.model_validate(raw)

dfs = generate(spark, plan)
```

YAML is the same shape — only the parser changes:

```python
import yaml
from dbldatagen.core import DataGenPlan, generate

raw = yaml.safe_load(open("plan.yml").read())
plan = DataGenPlan.model_validate(raw)

dfs = generate(spark, plan)
```

> **YAML needs PyYAML.** `import yaml` comes from the `PyYAML`
> package, which isn't a hard dependency of core. Install it
> (`pip install pyyaml`) if you want YAML plans. JSON uses the
> standard-library `json` module, so it needs nothing extra.

`DataGenPlan.model_validate` runs the full set of model validators, so
a malformed plan fails at load time with a clear Pydantic error rather
than partway through generation.

## The canonical plan in YAML

This is the same `customers` / `orders` plan from
[getting-started.md](getting-started.md), written as a file:

```yaml
seed: 42

tables:
  - name: customers
    rows: 200
    primary_key:
      columns: [customer_id]
    columns:
      - name: customer_id
        gen:
          strategy: sequence
          start: 1
          step: 1
      - name: name
        dtype: string
        gen:
          strategy: values
          values: [Alice, Bob, Carol, Dave]
      - name: signup_date
        gen:
          strategy: timestamp
          start: "2022-01-01"
          end: "2024-12-31"

  - name: orders
    rows: 1000
    primary_key:
      columns: [order_id]
    columns:
      - name: order_id
        gen:
          strategy: sequence
          start: 1
          step: 1
      - name: customer_id
        gen:
          strategy: foreign_key
        foreign_key:
          ref: customers.customer_id
          distribution:
            type: zipf
            exponent: 1.2
      - name: amount
        dtype: double
        gen:
          strategy: range
          min: 5.0
          max: 500.0
      - name: status
        dtype: string
        gen:
          strategy: values
          values: [pending, shipped, delivered, cancelled]
          distribution:
            type: weighted
            weights:
              pending: 0.1
              shipped: 0.2
              delivered: 0.6
              cancelled: 0.1
```

The same plan in JSON is structurally identical — the field names and
nesting are the same, only the syntax differs.

## How the file maps to the models

The file format is not a separate schema; it's a direct serialization
of the Pydantic models. That means:

- **Field names in the file match the model fields.** `tables`,
  `seed`, `name`, `rows`, `primary_key`, `columns`, `dtype`, `gen`,
  `foreign_key` are the same names you'd pass to `DataGenPlan`,
  `TableSpec`, `ColumnSpec`, `PrimaryKey`, and `ForeignKeyRef` in
  Python.
- **The generation strategy goes under `gen` with a `strategy` tag.**
  Each `gen` object names its strategy (`sequence`, `values`,
  `timestamp`, `range`, `foreign_key`, …) plus that strategy's own
  fields (`start`/`step`, `values`, `min`/`max`, `template`, …). This
  is the discriminated union behind `ColumnSpec.gen`.
- **Distributions go under `distribution` with a `type` tag.**
  `{type: zipf, exponent: 1.2}`, `{type: weighted, weights: {...}}`,
  `{type: uniform}`, and so on. See
  [distributions.md](distributions.md).
- **Foreign keys live in a sibling `foreign_key` block**, alongside
  `gen: {strategy: foreign_key}` — exactly as in the Python form.

JSON and YAML deserialize to the *same* `DataGenPlan`: the two
fixtures used in the test suite (`plan.json` and `plan.yml`) compare
equal after loading.

### `DataType` alternate spellings

When you set `dtype`, the canonical spellings are the `DataType`
values (`int`, `long`, `float`, `double`, `string`, `boolean`, `date`,
`timestamp`, `decimal`). For convenience when loading from a file, a
few alternate spellings are accepted and normalized:

| You write | Normalizes to |
| --- | --- |
| `integer` | `int` |
| `bool` | `boolean` |
| `str` | `string` |

So `dtype: integer`, `dtype: bool`, and `dtype: str` all load
correctly. See [concepts/plans-and-tables.md](concepts/plans-and-tables.md#datatype)
for the full type list.

## Round-tripping a plan back to a file

The mapping works both ways. A plan built in Python can be written out
with Pydantic's dump methods and reloaded byte-for-byte:

```python
# Python plan -> JSON file
json_file.write_text(plan.model_dump_json(indent=2))
reloaded = DataGenPlan.model_validate_json(json_file.read_text())
assert reloaded == plan

# Python plan -> YAML file
import yaml
yaml_file.write_text(yaml.dump(plan.model_dump(mode="json"), default_flow_style=False))
reloaded = DataGenPlan.model_validate(yaml.safe_load(yaml_file.read_text()))
assert reloaded == plan
```

For YAML, dump the plan with `model_dump(mode="json")` first so values
are plain JSON-compatible scalars before `yaml.dump` sees them.

## Fresh-random data: override the seed at load time

A file is a fixed spec, and core is deterministic by design — loading
the same file and generating always produces the same data. That's the
point: it makes the file a reproducible fixture.

When you genuinely want *different* data on each run, don't bake
randomness into the file. Override the seed at load time, after parsing
but before `model_validate`:

```python
import random, yaml
from dbldatagen.core import DataGenPlan

raw = yaml.safe_load(open("plan.yml").read())
raw["seed"] = random.randint(0, 2**63 - 1)   # fresh each run
plan = DataGenPlan.model_validate(raw)
```

This keeps the spec deterministic by default and makes "different data
each run" an explicit, opt-in caller choice rather than a property of
the file. Log the seed you used so you can reproduce that run later.
See [concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md)
for the full reasoning.

## Next steps

- [api-reference.md](api-reference.md) — `generate`, `resolve_plan`,
  `generate_table`
- [concepts/plans-and-tables.md](concepts/plans-and-tables.md) — the
  model fields the file maps onto
- [concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md)
  — seeds and reproducibility
