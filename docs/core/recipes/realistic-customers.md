# Recipe: a realistic customers / CRM table

This recipe builds a single `customers` table that looks like a real
CRM export — real-shaped names, emails, companies, phone numbers, and
postal addresses — instead of the regex gibberish a quick mock usually
produces. It's the `dbldatagen.core` version of v0's `basic/user`
dataset, upgraded from regex templates to [Faker](https://faker.readthedocs.io/)
providers for the PII columns and `PatternColumn` for the customer ID.

The shape:

```
customers
  customer_id   PK   "CUST-00481923"   (PatternColumn)
  first_name         "Maria"           (Faker)
  last_name          "Nguyen"          (Faker)
  email              "m.nguyen@...      (Faker)
  company            "Acme LLC"         (Faker)
  phone_number       "(415) 555-0182"  (Faker, ~5% NULL)
  street_address     "123 Main St"      (Faker)
  city               "Austin"           (Faker)
  state              "TX"               (Faker)
  postcode           "78701"            (Faker)
  signup_date        2023-06-14 ...     (TimestampColumn)
  segment            "smb"              (ValuesColumn + WeightedValues)
  account_balance    1820.55            (DECIMAL RangeColumn)
```

## What this builds

A 5,000-row `customers` table where every PII column carries
realistic-looking values, the `phone_number` column is ~5% NULL (so
your downstream code meets dirty data the way it will in production),
and `segment` follows a weighted distribution rather than a uniform
one. The whole thing is deterministic: the same plan and seed produce
byte-identical data every run.

## Prerequisites

Faker is an **optional** dependency. The Faker columns below raise
`ImportError` at generation time if it isn't installed, so install the
extra first:

```bash
pip install 'dbldatagen[core-faker]'
```

Everything else (`PatternColumn`, `TimestampColumn`, `ValuesColumn`,
`RangeColumn`) is part of the core engine and needs no extra.

## Build the plan

### Schema form

The explicit form spells out every strategy object. Note the address
is a set of **flat** columns — `FakerColumn` can't live inside a
`StructColumn` (it needs a column-level `pandas_udf`), so a nested
`address` struct of Faker fields is rejected at plan time. See the
note under [Notes / variations](#notes--variations).

```python
from dbldatagen.core import (
    generate, DataGenPlan, TableSpec, ColumnSpec, PrimaryKey,
)
from dbldatagen.core.spec.schema import (
    PatternColumn, FakerColumn, TimestampColumn, ValuesColumn,
    RangeColumn, WeightedValues, DataType,
)

customers = TableSpec(
    name="customers",
    rows=5000,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        ColumnSpec(
            name="customer_id",
            dtype=DataType.STRING,
            gen=PatternColumn(template="CUST-{digit:8}"),
        ),
        ColumnSpec(name="first_name", gen=FakerColumn(provider="first_name")),
        ColumnSpec(name="last_name", gen=FakerColumn(provider="last_name")),
        ColumnSpec(name="email", gen=FakerColumn(provider="email")),
        ColumnSpec(name="company", gen=FakerColumn(provider="company")),
        # ~5% of phone numbers are NULL, the way a real CRM has gaps.
        ColumnSpec(
            name="phone_number",
            gen=FakerColumn(provider="phone_number"),
            null_fraction=0.05,
        ),
        # Address as flat columns (Faker can't be a struct field).
        ColumnSpec(name="street_address", gen=FakerColumn(provider="street_address")),
        ColumnSpec(name="city", gen=FakerColumn(provider="city")),
        ColumnSpec(name="state", gen=FakerColumn(provider="state_abbr")),
        ColumnSpec(name="postcode", gen=FakerColumn(provider="postcode")),
        ColumnSpec(
            name="signup_date",
            dtype=DataType.TIMESTAMP,
            gen=TimestampColumn(start="2021-01-01", end="2024-12-31"),
        ),
        ColumnSpec(
            name="segment",
            dtype=DataType.STRING,
            gen=ValuesColumn(
                values=["smb", "mid_market", "enterprise"],
                distribution=WeightedValues(
                    weights={"smb": 70, "mid_market": 25, "enterprise": 5},
                ),
            ),
        ),
        ColumnSpec(
            name="account_balance",
            dtype=DataType.DECIMAL,
            precision=12,
            scale=2,
            gen=RangeColumn(min=0, max=250000),
        ),
    ],
)

plan = DataGenPlan(tables=[customers], seed=42)
```

A few things worth calling out:

- **`PatternColumn(template="CUST-{digit:8}")`** produces keys like
  `"CUST-00481923"`. The placeholder grammar is
  `{digit:N}` / `{alpha:N}` / `{hex:N}` / `{seq}` / `{uuid}`; see
  [text-and-patterns.md](../column-strategies/text-and-patterns.md).
- **`FakerColumn(provider=...)`** always emits `StringType`, so you
  never set `dtype` on a Faker column (any non-string dtype is rejected
  at plan time). `provider` is the name of any real
  [Faker provider method](https://faker.readthedocs.io/en/master/providers.html).
- **`null_fraction=0.05`** on `phone_number` nulls ~5% of rows. The
  value must be `0` or `>= 0.0001` (the engine's null-mask granularity);
  a smaller non-zero fraction is rejected at plan time. See
  [nullable-and-dirty-data.md](../nullable-and-dirty-data.md).
- **`WeightedValues`** biases `segment` so 70% are `smb`, 25%
  `mid_market`, 5% `enterprise`. Weights don't have to sum to 100 — the
  engine normalises them. See [distributions.md](../distributions.md).
- **`account_balance`** is a `DECIMAL(12, 2)` for clean money values;
  `precision`/`scale` are only valid on `DataType.DECIMAL`.

### DSL form

The same plan with the `datagendg` helpers reads much closer to a
column list. The `datagendg.faker()` helper is especially tidy here:

```python
from dbldatagen.core import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.core.spec import dsl as datagendg
from dbldatagen.core.spec.schema import WeightedValues

customers = TableSpec(
    name="customers",
    rows=5000,
    primary_key=PrimaryKey(columns=["customer_id"]),
    columns=[
        datagendg.pk_pattern("customer_id", "CUST-{digit:8}"),
        datagendg.faker("first_name", "first_name"),
        datagendg.faker("last_name", "last_name"),
        datagendg.faker("email", "email"),
        datagendg.faker("company", "company"),
        # null_fraction lives on the ColumnSpec; set it after building.
        datagendg.faker("phone_number", "phone_number"),
        datagendg.faker("street_address", "street_address"),
        datagendg.faker("city", "city"),
        datagendg.faker("state", "state_abbr"),
        datagendg.faker("postcode", "postcode"),
        datagendg.timestamp("signup_date", "2021-01-01", "2024-12-31"),
        datagendg.text(
            "segment",
            ["smb", "mid_market", "enterprise"],
            distribution=WeightedValues(
                weights={"smb": 70, "mid_market": 25, "enterprise": 5},
            ),
        ),
        datagendg.decimal("account_balance", min=0, max=250000, precision=12, scale=2),
    ],
)

# The DSL faker() helper doesn't take null_fraction, so set it on the
# returned ColumnSpec for the column you want partly NULL.
phone = next(c for c in customers.columns if c.name == "phone_number")
phone.null_fraction = 0.05

plan = DataGenPlan(tables=[customers], seed=42)
```

The DSL mapping, helper by helper:

- `datagendg.pk_pattern(name, template)` — the patterned string PK
  (still declare `PrimaryKey(columns=[name])` on the `TableSpec`).
- `datagendg.faker(name, provider)` — builds a `FakerColumn`
  (`DataType.STRING`). Signature:
  `faker(name, provider, *, dtype=DataType.STRING, locale=None, seed_from=None, **kwargs)`.
  Extra `**kwargs` are forwarded straight to the Faker provider method.
- `datagendg.timestamp(name, start, end)` — a `TimestampColumn`.
- `datagendg.text(name, values, **kw)` — a `ValuesColumn`; pass
  `distribution=WeightedValues(...)` for biased categorical selection.
- `datagendg.decimal(name, min, max, precision=, scale=)` — a
  `DECIMAL` `RangeColumn`.

`null_fraction` isn't a parameter on `datagendg.faker()`, so set it on
the returned `ColumnSpec` (as above) when you want a Faker column to be
partly NULL.

## Generate

```python
dfs = generate(spark, plan)
customers_df = dfs["customers"]
customers_df.show(5, truncate=False)
```

`generate` returns a `dict` keyed by table name. A few sample rows look
like real CRM data:

```
+-------------+----------+---------+--------------------------+------------------+--------------+
|customer_id  |first_name|last_name|email                     |company           |phone_number  |
+-------------+----------+---------+--------------------------+------------------+--------------+
|CUST-48193027|Maria     |Nguyen   |mnguyen@example.org       |Acme LLC          |(415) 555-0182|
|CUST-00917342|James     |Carter   |jcarter@example.com       |Globex Inc        |NULL          |
|CUST-73620194|Aisha     |Okafor   |aisha.o@example.net       |Initech Group     |(212) 555-7741|
+-------------+----------+---------+--------------------------+------------------+--------------+
```

(Exact values depend on your Faker version and the plan `seed`; the
NULL in row two is the ~5% `phone_number` gap.)

## Common Faker providers

`provider` is the name of any method on a Faker instance. Below are the
ones most useful for a customer / CRM table. The full list — hundreds
of providers, grouped by category — is in the
[Faker provider docs](https://faker.readthedocs.io/en/master/providers.html).

| Provider           | Example output            |
| ------------------ | ------------------------- |
| `name`             | `"Maria Nguyen"`          |
| `first_name`       | `"Maria"`                 |
| `last_name`        | `"Nguyen"`                |
| `email`            | `"mnguyen@example.org"`   |
| `company`          | `"Acme LLC"`              |
| `job`              | `"Data engineer"`         |
| `phone_number`     | `"(415) 555-0182"`        |
| `street_address`   | `"123 Main St"`           |
| `city`             | `"Austin"`                |
| `state_abbr`       | `"TX"`                    |
| `postcode`         | `"78701"`                 |
| `country`          | `"United States"`         |
| `ipv4`             | `"192.0.2.14"`            |

Provider methods that take arguments accept them as keyword args:
`datagendg.faker("ip", "ipv4", network=True)` forwards `network=True`
to `Faker().ipv4(...)`. An unknown provider name isn't caught until
generation time, where the engine raises `ValueError`.

## Notes / variations

- **Locale.** Faker localises its output. Set it per column —
  `datagendg.faker("city", "city", locale="de_DE")` — or set the
  plan-wide default once and let every Faker column inherit it:

  ```python
  plan = DataGenPlan(tables=[customers], seed=42, default_locale="de_DE")
  ```

  A per-column `locale=` overrides `DataGenPlan.default_locale`; when
  neither is set, Faker uses `"en_US"`.

- **Address can't be a nested struct.** It's tempting to group the
  address fields under a single `address` struct, but `FakerColumn`
  isn't allowed inside a `StructColumn` (or `ArrayColumn`) — Faker
  needs a column-level `pandas_udf`, which has no per-field equivalent.
  The plan validator rejects it. Keep the address as flat columns
  (`street_address`, `city`, `state`, `postcode`) as shown above. See
  [nested.md](../column-strategies/nested.md).

- **Correlating fields.** To make a derived column track a Faker
  column deterministically, use `seed_from=` so two rows with the same
  source value generate the same value here. (Faker columns are
  independent by default — `first_name` and `email` won't agree unless
  you build the email from the name yourself with an
  `ExpressionColumn`.)

- **Full name in one column.** Use `provider="name"` for a combined
  `"Maria Nguyen"` instead of separate `first_name` / `last_name`.

- **A real `customer_id` long.** v0's `basic/user` used a `long` ID.
  For a numeric PK use `datagendg.pk_auto("customer_id")` (a sequence)
  instead of the patterned string above — declare the same
  `PrimaryKey(columns=["customer_id"])` either way. See
  [sequences-and-ids.md](../column-strategies/sequences-and-ids.md).

- **Persisting.** Once generated, write the table to Delta / Unity
  Catalog like any DataFrame — see
  [persisting-output.md](../persisting-output.md).

## See also

- [text-and-patterns.md](../column-strategies/text-and-patterns.md) —
  `FakerColumn` and `PatternColumn` in full, with the placeholder
  grammar and width caps
- [nullable-and-dirty-data.md](../nullable-and-dirty-data.md) —
  `null_fraction`, nullable columns, and modelling dirty data
- [distributions.md](../distributions.md) — `WeightedValues` and the
  other sampling distributions
- [persisting-output.md](../persisting-output.md) — writing the
  generated tables to Delta / Unity Catalog
- [nested.md](../column-strategies/nested.md) — structs and arrays, and
  why Faker can't be nested
