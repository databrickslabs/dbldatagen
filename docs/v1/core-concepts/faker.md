---
sidebar_position: 6
title: "Faker Integration"
---

# Faker Integration

> **TL;DR:** dbldatagen.v1 integrates with Python's Faker library to generate realistic names, addresses, emails, and other text data. The pool strategy pre-generates 10K values on the driver and distributes them deterministically, achieving ~10M rows/sec while maintaining determinism across cluster sizes.

For realistic text data like names, addresses, emails, and phone numbers, dbldatagen.v1 integrates with Python's [Faker](https://faker.readthedocs.io/) library using a high-performance pool strategy that provides both determinism and speed.

## Installation

Faker is an optional dependency. Install it using the `v1-faker` extra:

```bash
pip install "dbldatagen[v1-faker]"
```

If you try to use Faker columns without installing this extra, you'll get a clear error message:

```
ImportError: faker is required for FakerColumn. Install with: pip install "dbldatagen[v1-faker]"
```

## Basic Usage

Use the `faker()` DSL function to create Faker columns:

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, faker

users = TableSpec(
    name="users",
    rows="100K",
    primary_key=PrimaryKey(columns=["user_id"]),
    columns=[
        pk_auto("user_id"),
        faker("name", "name"),                    # Full name
        faker("email", "email"),                  # Email address
        faker("phone", "phone_number"),           # Phone number
        faker("address", "street_address"),       # Street address
        faker("city", "city"),                    # City name
        faker("company", "company"),              # Company name
    ],
)

plan = DataGenPlan(tables=[users], seed=42)
dfs = generate(spark, plan)

dfs["users"].show(5, truncate=False)
# +-------+------------------+---------------------------+---------------+---------------------+-----------+------------------+
# |user_id|name              |email                      |phone          |address              |city       |company           |
# +-------+------------------+---------------------------+---------------+---------------------+-----------+------------------+
# |1      |Jennifer Martinez |jennifer.martinez@gmail.com|555-123-4567   |123 Oak Street       |New York   |Acme Corporation  |
# |2      |Michael Chen      |mchen@example.com          |555-987-6543   |456 Pine Avenue      |Los Angeles|TechWorks Inc     |
# ...
```

## How the Pool Strategy Works

The Faker integration uses a pool strategy that balances performance, determinism, and realistic variety:

1. **Driver-side generation:** When the plan is built, a Faker instance is created with a deterministic seed on the driver. The specified provider (e.g., `fake.name()`) is called 10,000 times to build a pool of values.

2. **Executor-side selection:** A `pandas_udf` distributes the pool to executors. Each row selects from the pool using `hash(column_seed, row_id) % pool_size`.

3. **Deterministic selection:** The same row ID always picks the same pool entry, ensuring determinism across runs and cluster sizes.

This approach provides:

- **Determinism** - same row always gets the same value
- **Performance** - ~10M rows/sec (200x faster than per-row Faker instantiation)
- **Realistic variety** - 10,000 unique values gives good diversity
- **Spark Connect compatibility** - pool is passed via closure, no `sc.broadcast` needed

## Common Faker Providers

Here are the most commonly used Faker providers:

### Personal Information

```python
faker("name", "name")                           # Full name
faker("first_name", "first_name")               # First name only
faker("last_name", "last_name")                 # Last name only
faker("email", "email")                         # Email address
faker("username", "user_name")                  # Username
faker("phone", "phone_number")                  # Phone number
faker("ssn", "ssn")                             # Social Security Number (US)
faker("dob", "date_of_birth")                   # Date of birth
```

### Location Data

```python
faker("address", "street_address")              # Street address
faker("city", "city")                           # City name
faker("state", "state")                         # State name
faker("state_abbr", "state_abbr")               # State abbreviation
faker("zip", "zipcode")                         # ZIP code
faker("country", "country")                     # Country name
```

### Business Data

```python
faker("company", "company")                     # Company name
faker("job", "job")                             # Job title
faker("credit_card", "credit_card_number")      # Credit card number
```

### Internet Data

```python
faker("url", "url")                             # URL
faker("domain", "domain_name")                  # Domain name
faker("ipv4", "ipv4")                           # IPv4 address
faker("ipv6", "ipv6")                           # IPv6 address
faker("user_agent", "user_agent")               # Browser user agent
```

### Text Content

```python
faker("text", "text")                           # Paragraph of text
faker("sentence", "sentence")                   # Single sentence
faker("paragraph", "paragraph")                 # Paragraph
faker("word", "word")                           # Single word
```

See the [Faker provider documentation](https://faker.readthedocs.io/en/master/providers.html) for the complete list.

## Locales

Faker supports locale-specific data for international datasets. Use the `locale` parameter to generate data for different countries and languages:

```python
from dbldatagen.v1 import generate, DataGenPlan, TableSpec, PrimaryKey
from dbldatagen.v1 import pk_auto, faker

# German customers
de_customers = TableSpec(
    name="de_customers",
    rows="50K",
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        faker("name", "name", locale="de_DE"),              # German name
        faker("address", "street_address", locale="de_DE"), # German address
        faker("city", "city", locale="de_DE"),              # German city
        faker("phone", "phone_number", locale="de_DE"),     # German phone format
    ],
)

# Japanese customers
jp_customers = TableSpec(
    name="jp_customers",
    rows="50K",
    primary_key=PrimaryKey(columns=["id"]),
    columns=[
        pk_auto("id"),
        faker("name", "name", locale="ja_JP"),              # Japanese name
        faker("address", "street_address", locale="ja_JP"), # Japanese address
        faker("city", "city", locale="ja_JP"),              # Japanese city
    ],
)

plan = DataGenPlan(tables=[de_customers, jp_customers], seed=42)
dfs = generate(spark, plan)
```

### Setting a Default Locale

You can set a default locale for all Faker columns in a plan:

```python
plan = DataGenPlan(
    tables=[users],
    seed=42,
    default_locale="fr_FR",  # French locale for all Faker columns
)
```

Individual columns can override the default:

```python
columns=[
    faker("name", "name"),                      # Uses default_locale
    faker("email", "email", locale="en_US"),    # Override: English
]
```

Common locales:
- `en_US` - English (United States)
- `en_GB` - English (United Kingdom)
- `de_DE` - German (Germany)
- `fr_FR` - French (France)
- `es_ES` - Spanish (Spain)
- `ja_JP` - Japanese (Japan)
- `zh_CN` - Chinese (China)
- `pt_BR` - Portuguese (Brazil)

See the [Faker locales documentation](https://faker.readthedocs.io/en/master/locales.html) for the full list.

## Custom Provider Arguments

Some Faker providers accept additional keyword arguments. Pass them using the `kwargs` parameter:

```python
from dbldatagen.v1 import faker

# Date of birth with age constraints
faker("dob", "date_of_birth",
      minimum_age=18,
      maximum_age=80)

# Latitude/longitude within bounds
faker("lat", "latitude",
      min=-90.0,
      max=90.0)

faker("lon", "longitude",
      min=-180.0,
      max=180.0)

# Text with specific length
faker("description", "text",
      max_nb_chars=200)

# Profile data (returns dict - use with struct columns)
faker("profile", "profile",
      fields=["username", "name", "mail"])
```

In Python DSL:

```python
faker("description", "text", max_nb_chars=200)
```

In YAML plans:

```yaml
- name: description
  gen:
    strategy: faker
    provider: text
    kwargs:
      max_nb_chars: 200
```

## Pool Size and Cardinality

The default pool size is 10,000 values per column. This provides good variety for most use cases:

- **Names:** 10K names gives realistic variety without repetition becoming obvious
- **Emails:** 10K emails is sufficient for most test datasets
- **Addresses:** 10K addresses provides good geographic diversity

For higher cardinality needs, combine multiple Faker columns:

```python
# Combine first_name (10K) + last_name (10K) = 100M unique combinations
columns=[
    faker("first_name", "first_name"),
    faker("last_name", "last_name"),
]
```

Or use Faker with other strategies:

```python
# Faker email (10K) + random numeric suffix = much higher cardinality
columns=[
    faker("email_base", "email"),
    integer("user_num", min=1, max=1_000_000),
    expression("email", "concat(email_base, '_', user_num)"),
]
```

## YAML Configuration

Faker columns can be defined in YAML plans:

```yaml
seed: 100
tables:
  - name: users
    rows: 10000
    primary_key: {columns: [user_id]}
    columns:
      - name: user_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}

      - name: name
        gen: {strategy: faker, provider: name}

      - name: email
        gen: {strategy: faker, provider: email}

      - name: phone
        gen:
          strategy: faker
          provider: phone_number
          locale: en_US

      - name: address
        gen:
          strategy: faker
          provider: street_address
          kwargs:
            locale: en_US
```

## Performance Characteristics

Faker columns are **Tier 2** in the dbldatagen.v1 performance model:

- **Tier 1** (pure Spark SQL expressions): range, values, timestamps - ~50M rows/sec/core
- **Tier 2** (pandas UDFs with precomputed data): Faker, random unique PKs - ~10M rows/sec/core
- **Tier 3** (pandas UDFs with per-row computation): expensive custom logic - varies

The pool strategy makes Faker columns nearly as fast as native Spark operations while maintaining full determinism.

## Related Documentation

- [Nullable Columns Guide](./nullable-columns.md) - Make Faker columns optional with null fractions
- [Nested Types Guide](./nested-types.md) - Use Faker in struct columns for nested JSON
- [Examples](../guides/basic.md) - See Faker in complete examples
