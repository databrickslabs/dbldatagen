---
sidebar_position: 1
title: "YAML Plans"
---

# YAML Plans

> **TL;DR:** dbldatagen.v1 plans can be defined in YAML files instead of Python code. Use `DataGenPlan.model_validate()` to load them programmatically, or run them directly with `examples/run_from_yaml.py`. Supports all features: distributions, FKs, nested types, and CDC.

YAML plans let you define data generation schemas declaratively, separate from code. This is useful for:

- Version controlling test data definitions
- Sharing schemas across teams
- Runtime configuration without code changes
- Templating and generating plans programmatically

## Basic YAML Plan Structure

A basic `DataGenPlan` in YAML:

```yaml
# plan.yml
seed: 100

tables:
  - name: users
    rows: 1000
    primary_key: {columns: [user_id]}
    columns:
      - name: user_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: name
        gen: {strategy: faker, provider: name}

      - name: email
        gen: {strategy: faker, provider: email}

      - name: age
        dtype: int
        gen: {strategy: range, min: 18, max: 90}

      - name: status
        dtype: string
        gen:
          strategy: values
          values: [active, inactive, pending]

      - name: created_at
        dtype: timestamp
        gen:
          strategy: timestamp
          start: "2023-01-01"
          end: "2025-12-31"
```

## Loading YAML Plans

### From Python

```python
import yaml
from dbldatagen.v1.schema import DataGenPlan
from dbldatagen.v1 import generate

# Load YAML file
with open("plan.yml") as f:
    raw = yaml.safe_load(f)

# Validate and parse
plan = DataGenPlan.model_validate(raw)

# Generate data
dfs = generate(spark, plan)
```

### Using the CLI Runner

dbldatagen.v1 includes a generic runner script:

```bash
python examples/run_from_yaml.py plan.yml
```

This script:
1. Loads the YAML file
2. Detects whether it's a `DataGenPlan` or `CDCPlan` (checks for `base_plan` key)
3. Generates the data
4. Writes output to `examples/output/` as both Parquet and JSON

## Row Count Shorthand

For readability, row counts support K/M/B suffixes:

```yaml
tables:
  - name: small_table
    rows: 1000          # explicit integer

  - name: medium_table
    rows: "100K"        # 100,000

  - name: large_table
    rows: "10M"         # 10,000,000

  - name: huge_table
    rows: "5B"          # 5,000,000,000
```

Supported suffixes:
- `K` - thousands (multiply by 1,000)
- `M` - millions (multiply by 1,000,000)
- `B` - billions (multiply by 1,000,000,000)

## Column Strategies in YAML

### Range Columns

```yaml
- name: age
  dtype: int
  gen:
    strategy: range
    min: 18
    max: 90
```

With distribution:

```yaml
- name: price
  dtype: double
  gen:
    strategy: range
    min: 1.0
    max: 1000.0
    distribution:
      type: normal
      mean: 50.0
      stddev: 30.0
```

### Values Columns

```yaml
- name: status
  dtype: string
  gen:
    strategy: values
    values: [pending, approved, rejected]
```

With weighted distribution:

```yaml
- name: event_type
  dtype: string
  gen:
    strategy: values
    values: [page_view, click, purchase]
    distribution:
      type: weighted
      weights:
        page_view: 0.70
        click: 0.25
        purchase: 0.05
```

### Faker Columns

```yaml
- name: name
  gen:
    strategy: faker
    provider: name

- name: email
  gen:
    strategy: faker
    provider: email

- name: phone
  gen:
    strategy: faker
    provider: phone_number
    locale: en_US
    kwargs: {}
```

### Timestamp Columns

```yaml
- name: created_at
  dtype: timestamp
  gen:
    strategy: timestamp
    start: "2023-01-01"
    end: "2025-12-31"
```

With distribution:

```yaml
- name: event_time
  dtype: timestamp
  gen:
    strategy: timestamp
    start: "2025-01-01"
    end: "2025-12-31"
    distribution:
      type: normal
      mean: 0.5
      stddev: 0.15
```

### Pattern Columns

```yaml
- name: order_id
  dtype: string
  gen:
    strategy: pattern
    template: "ORD-{digit:8}-{alpha:3}"
```

### UUID Columns

```yaml
- name: event_id
  dtype: string
  gen:
    strategy: uuid
```

### Expression Columns

```yaml
- name: total
  dtype: double
  gen:
    strategy: expression
    expr: "quantity * unit_price"
```

### Constant Columns

```yaml
- name: version
  dtype: string
  gen:
    strategy: constant
    value: "v1.0"
```

## Distribution Types in YAML

### Uniform (default)

```yaml
gen:
  strategy: range
  min: 0
  max: 100
  distribution: {type: uniform}
```

### Normal

```yaml
distribution:
  type: normal
  mean: 0.5
  stddev: 0.2
```

### Log-Normal

```yaml
distribution:
  type: lognormal
  mean: 3.5
  stddev: 0.8
```

### Zipf

```yaml
distribution:
  type: zipf
  exponent: 1.5
```

### Exponential

```yaml
distribution:
  type: exponential
  rate: 0.5
```

### Weighted Values

```yaml
gen:
  strategy: values
  values: [red, green, blue]
  distribution:
    type: weighted
    weights:
      red: 0.5
      green: 0.3
      blue: 0.2
```

## Foreign Keys in YAML

```yaml
tables:
  - name: customers
    rows: 10000
    primary_key: {columns: [customer_id]}
    columns:
      - name: customer_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

  - name: orders
    rows: 100000
    primary_key: {columns: [order_id]}
    columns:
      - name: order_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      # Foreign key with default Zipf(1.2) distribution
      - name: customer_id
        gen: {strategy: constant, value: null}
        foreign_key:
          ref: customers.customer_id
```

### FK with Custom Distribution

```yaml
- name: product_id
  gen: {strategy: constant, value: null}
  foreign_key:
    ref: products.product_id
    distribution:
      type: zipf
      exponent: 2.0
```

### Nullable Foreign Key

```yaml
- name: referrer_id
  gen: {strategy: constant, value: null}
  nullable: true
  null_fraction: 0.7
  foreign_key:
    ref: users.user_id
```

## Nested Types in YAML

### Struct Column

```yaml
- name: address
  gen:
    strategy: struct
    fields:
      - name: street
        dtype: string
        gen:
          strategy: values
          values: ["123 Main St", "456 Oak Ave"]

      - name: city
        dtype: string
        gen:
          strategy: values
          values: [New York, Los Angeles, Chicago]

      - name: zip
        dtype: int
        gen: {strategy: range, min: 10000, max: 99999}
```

### Array Column

```yaml
- name: tags
  gen:
    strategy: array
    element:
      strategy: values
      values: [sale, new, premium, eco-friendly]
    min_length: 1
    max_length: 4
```

### Array of Structs

```yaml
- name: line_items
  gen:
    strategy: array
    element:
      strategy: struct
      fields:
        - name: product_name
          dtype: string
          gen:
            strategy: values
            values: [Laptop, Mouse, Keyboard]

        - name: quantity
          dtype: int
          gen: {strategy: range, min: 1, max: 5}

        - name: unit_price
          dtype: double
          gen: {strategy: range, min: 9.99, max: 999.99}
    min_length: 1
    max_length: 5
```

## CDC Plans in YAML

CDC plans have a `base_plan` and CDC configuration:

```yaml
# cdc_plan.yml
base_plan:
  seed: 500
  tables:
    - name: accounts
      rows: 100
      primary_key: {columns: [account_id]}
      columns:
        - name: account_id
          dtype: long
          gen: {strategy: sequence, start: 1, step: 1}

        - name: status
          dtype: string
          gen:
            strategy: values
            values: [active, suspended, closed]

    - name: transactions
      rows: 1000
      primary_key: {columns: [txn_id]}
      columns:
        - name: txn_id
          dtype: long
          gen: {strategy: sequence, start: 1, step: 1}

        - name: account_id
          gen: {strategy: constant, value: null}
          foreign_key: {ref: accounts.account_id}

        - name: amount
          dtype: double
          gen: {strategy: range, min: 1.0, max: 10000.0}

# CDC configuration
num_batches: 10
format: delta_cdf
batch_interval_seconds: 86400

table_configs:
  accounts:
    batch_size: 0.05
    operations:
      insert: 1
      update: 8
      delete: 1

  transactions:
    batch_size: 0.15
    operations:
      insert: 4
      update: 4
      delete: 2
```

### CDC Batch Size

Batch size can be:
- Float (0.0-1.0): fraction of initial rows
- Integer: absolute row count
- String: shorthand like "1K", "500K", "10M"

```yaml
table_configs:
  small_table:
    batch_size: 0.1        # 10% of initial rows

  medium_table:
    batch_size: 5000       # exactly 5000 rows per batch

  large_table:
    batch_size: "100K"     # 100,000 rows per batch
```

### CDC Output Formats

```yaml
format: raw           # _op column: I/U/UB/D
format: delta_cdf     # _change_type column: insert/update_preimage/update_postimage/delete
format: sql_server    # __$operation column: 1/2/3/4
format: debezium      # op column: c/u/d
```

## Complete YAML Examples

### Simple Table

`examples/yaml/01_simple.yml`:

```yaml
seed: 100

tables:
  - name: users
    rows: 1000
    primary_key: {columns: [user_id]}
    columns:
      - name: user_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: status
        dtype: string
        gen:
          strategy: values
          values: [active, inactive, pending]

      - name: age
        dtype: int
        gen: {strategy: range, min: 18, max: 85}

      - name: created_at
        dtype: timestamp
        gen:
          strategy: timestamp
          start: "2023-01-01"
          end: "2025-12-31"
```

### Two Tables with FK

`examples/yaml/02_medium.yml`:

```yaml
seed: 200

tables:
  - name: departments
    rows: 20
    primary_key: {columns: [dept_id]}
    columns:
      - name: dept_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: dept_name
        dtype: string
        gen:
          strategy: values
          values: [Engineering, Sales, Marketing, Finance, HR]

  - name: employees
    rows: 2000
    primary_key: {columns: [emp_id]}
    columns:
      - name: emp_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: dept_id
        gen: {strategy: constant, value: null}
        foreign_key:
          ref: departments.dept_id
          distribution: {type: zipf, exponent: 1.5}

      - name: first_name
        gen: {strategy: faker, provider: first_name}

      - name: last_name
        gen: {strategy: faker, provider: last_name}

      - name: email
        gen: {strategy: faker, provider: email}

      - name: salary
        dtype: double
        gen: {strategy: range, min: 35000.0, max: 250000.0}
```

### Nested JSON

`examples/yaml/07_nested_json.yml`:

```yaml
seed: 700

tables:
  - name: products
    rows: 500
    primary_key: {columns: [product_id]}
    columns:
      - name: product_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: name
        dtype: string
        gen:
          strategy: values
          values: [Laptop, Headphones, Keyboard, Monitor]

      - name: manufacturer
        gen:
          strategy: struct
          fields:
            - name: company
              dtype: string
              gen:
                strategy: values
                values: [Acme Corp, TechWorks, GadgetCo]

            - name: country
              dtype: string
              gen:
                strategy: values
                values: [US, CN, DE, JP]

      - name: tags
        gen:
          strategy: array
          element:
            strategy: values
            values: [bestseller, new, sale, premium]
          min_length: 1
          max_length: 4
```

## Loading Both Plan Types

Generic loader that handles both `DataGenPlan` and `CDCPlan`:

```python
import yaml
from dbldatagen.v1.schema import DataGenPlan
from dbldatagen.v1.cdc_schema import CDCPlan

def load_plan(yaml_path):
    """Load either a DataGenPlan or CDCPlan from YAML."""
    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    # CDCPlan has base_plan key
    if "base_plan" in raw:
        return CDCPlan.model_validate(raw)
    else:
        return DataGenPlan.model_validate(raw)

# Usage
plan = load_plan("plan.yml")

if isinstance(plan, CDCPlan):
    from dbldatagen.v1.cdc import generate_cdc
    stream = generate_cdc(spark, plan)
    print(f"Generated initial + {len(stream.batches)} batches")
else:
    from dbldatagen.v1 import generate
    dfs = generate(spark, plan)
    print(f"Generated {len(dfs)} tables")
```

## Running YAML Plans

### Using the Included Runner

```bash
# Data generation plan
python examples/run_from_yaml.py examples/yaml/01_simple.yml

# CDC plan
python examples/run_from_yaml.py examples/yaml/05_cdc_medium.yml

# Nested types
python examples/run_from_yaml.py examples/yaml/07_nested_json.yml
```

Output goes to `examples/output/{plan_name}/`:
- `{table_name}/` - Parquet files
- `{table_name}_json/` - JSON files
- For CDC: `{table_name}/initial/`, `{table_name}/batch_{N}/`

### Custom Runner

Write your own runner for specific output formats:

```python
import sys
import yaml
from pyspark.sql import SparkSession
from dbldatagen.v1.schema import DataGenPlan
from dbldatagen.v1 import generate

def main(yaml_path, output_path):
    spark = SparkSession.builder.getOrCreate()

    with open(yaml_path) as f:
        plan = DataGenPlan.model_validate(yaml.safe_load(f))

    dfs = generate(spark, plan)

    # Write to Unity Catalog
    for name, df in dfs.items():
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"catalog.schema.{name}"
        )

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
```

## YAML Schema Validation

YAML plans are validated by Pydantic models, so you'll get clear errors for:

- Unknown strategies
- Missing required fields
- Invalid distributions
- FK reference errors

Example error:

```python
pydantic_core._pydantic_core.ValidationError: 2 validation errors for DataGenPlan
tables.0.columns.1.gen.strategy
  Input should be 'range', 'values', 'faker', ... [type=literal_error]
tables.0.columns.2.gen.min
  Field required [type=missing]
```

## Best Practices

### Version Control

Store YAML plans in Git:

```
plans/
  dev/
    small_dataset.yml
  staging/
    medium_dataset.yml
  prod/
    large_dataset.yml
```

### Templating

Generate YAML plans programmatically using Jinja2 or Python string templates:

```python
import yaml
from jinja2 import Template

template = Template("""
seed: {{ seed }}
tables:
  - name: {{ table_name }}
    rows: {{ row_count }}
    primary_key: {columns: [id]}
    columns:
      - name: id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}
      {% for col in columns %}
      - name: {{ col.name }}
        dtype: {{ col.dtype }}
        gen: {{ col.gen | tojson }}
      {% endfor %}
""")

plan_yaml = template.render(
    seed=100,
    table_name="users",
    row_count=10000,
    columns=[
        {"name": "status", "dtype": "string",
         "gen": {"strategy": "values", "values": ["active", "inactive"]}},
    ]
)

plan = DataGenPlan.model_validate(yaml.safe_load(plan_yaml))
```

### Documentation

Add comments to YAML plans:

```yaml
seed: 100

tables:
  # User dimension table
  - name: users
    rows: 10000
    primary_key: {columns: [user_id]}
    columns:
      - name: user_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      # Status: ~70% active, rest inactive/pending
      - name: status
        dtype: string
        gen:
          strategy: values
          values: [active, inactive, pending]
          distribution:
            type: weighted
            weights: {active: 0.7, inactive: 0.2, pending: 0.1}
```

## Related Documentation

- [Examples](../guides/basic.md) - See YAML examples in action
- [Column Strategies](../core-concepts/column-strategies.md) - Understand generation strategies
- [Nested Types Guide](../core-concepts/nested-types.md) - YAML syntax for structs and arrays
- [Writing Output Guide](./writing-output.md) - Save generated data
