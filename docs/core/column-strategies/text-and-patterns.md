# Text and patterns

Two strategies generate string-shaped data without an explicit value
list: `PatternColumn` builds strings from a template of placeholders,
and `FakerColumn` produces realistic values (names, addresses, emails)
from a Faker provider.

## When to use

- **`PatternColumn`** — structured identifiers and codes you can
  describe as a template: `"ORD-{digit:4}-{alpha:3}"`,
  `"CUST-{digit:6}"`, a random hex token. Also the engine behind
  `pk_pattern` for human-readable string keys.
- **`FakerColumn`** — anything that looks like real-world text: people,
  companies, streets, IP addresses, phone numbers.

For a column that picks from a fixed list of labels (status, country,
plan tier), that's a categorical, not text — use `ValuesColumn`. See
[categorical-values.md](categorical-values.md).

## PatternColumn

Generates strings by filling placeholders in a template. Literal text
between placeholders is emitted verbatim.

### Placeholder grammar

Placeholders are matched by the shared regex
`\{(?P<kind>seq|uuid|digit|alpha|hex):?(?P<width>\d+)?\}`. Each
placeholder is `{kind}` or `{kind:N}`, where `N` is an optional width:

| Placeholder | Width modifier | What it emits |
| --- | --- | --- |
| `{seq}` | optional `{seq:N}` | The row sequence number (monotonic). `N` zero-pads the leading edge to width `N`. |
| `{uuid}` | **none allowed** | A deterministic UUID, always a fixed 36-character literal. A `{uuid:N}` form is rejected. |
| `{digit:N}` | required | `N` random digits (`0-9`). |
| `{hex:N}` | required | `N` random hex characters. |
| `{alpha:N}` | required | `N` random alphabetic characters. |

Example: `"ORD-{digit:4}-{alpha:3}"` produces strings like
`"ORD-3847-KMX"`.

Per-kind width caps (from `dbldatagen/core/spec/_constants.py`, shared
with the engine so the two can't drift):

| Kind | Max width | Why |
| --- | --- | --- |
| `digit` | `18` | `pmod(seed, 10**width)` must fit in signed int64. |
| `hex` | `15` | Same int64 constraint, base 16. |
| `alpha` | `64` | Bounds emitted string length and per-row Catalyst plan size. |
| `seq` | `24` | Bounds the leading-zero pad width. |

### Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import PatternColumn, DataType

ColumnSpec(
    name="order_ref",
    dtype=DataType.STRING,
    gen=PatternColumn(template="ORD-{digit:4}-{alpha:3}"),
)
```

For a patterned primary key, `pk_pattern` is the shorthand:

```python
from dbldatagen.core.spec import dsl as datagendg

datagendg.pk_pattern("customer_id", "CUST-{digit:6}")
```

### DSL form

```python
from dbldatagen.core.spec import dsl as datagendg

datagendg.pattern("order_ref", "ORD-{digit:4}-{alpha:3}")
```

`pattern(name, template)` builds a `ColumnSpec` with `DataType.STRING`
and a `PatternColumn`. It also accepts `seed_from=` to derive the cell
seed from another column (same `seed_from` value → same generated
string). `pk_pattern(name, template)` is the same wrapper, used for key
columns (still declare `PrimaryKey(columns=[name])` on the `TableSpec` —
see [sequences-and-ids.md](sequences-and-ids.md)).

### Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["pattern"]` | `"pattern"` | Discriminator; set automatically. |
| `template` | `str` | — (required) | Template string of literal text and placeholders. Min length 1. |

### Validation & gotchas

- **The template must contain at least one placeholder.** A template
  with only literal text is effectively a constant — the validator
  rejects it and points you to `ConstantColumn`.
- **Whitespace-only templates are rejected** (they would emit the same
  whitespace on every row, almost always a typo).
- **`{uuid}` takes no width modifier.** UUIDs are always 36 characters;
  `{uuid:N}` is rejected. For a short random token use `{hex:N}` or
  `{alpha:N}`.
- **Width must be `>= 1` and within the per-kind cap.** Width `0` and
  widths above the cap (see the table above) are rejected at plan time,
  with a single error message naming every violation.

## FakerColumn

Calls `Faker(...).<provider>(**kwargs)` once per row (via a pooled
`pandas_udf`) to produce realistic values.

> **Requires the `core-faker` extra:**
> `pip install 'dbldatagen[core-faker]'`. At materialisation the engine
> raises `ImportError` if Faker isn't installed and `ValueError` if
> `provider` isn't a known Faker method.

### Schema form

```python
from dbldatagen.core import ColumnSpec
from dbldatagen.core.spec.schema import FakerColumn, DataType

ColumnSpec(
    name="name",
    dtype=DataType.STRING,
    gen=FakerColumn(provider="name"),
)

ColumnSpec(
    name="email",
    dtype=DataType.STRING,
    gen=FakerColumn(provider="email", locale="de_DE"),
)
```

### DSL form

```python
from dbldatagen.core.spec import dsl as datagendg

datagendg.faker("name", "name")
datagendg.faker("email", "email", locale="de_DE")
```

`faker(name, provider)` builds a `ColumnSpec` with `DataType.STRING`
(override with `dtype=`) and a `FakerColumn`. Any extra keyword args are
forwarded to the provider method itself; `seed_from=` is also accepted.
For example, to pass a provider argument:

```python
datagendg.faker("ip", "ipv4", network=True)
```

### Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `strategy` | `Literal["faker"]` | `"faker"` | Discriminator; set automatically. |
| `provider` | `str` | — (required) | Faker provider method name (e.g. `"first_name"`, `"company"`, `"ipv4"`). Min length 1; must be a real Faker method (checked at materialisation). |
| `kwargs` | `dict[str, Any]` | `{}` | Keyword arguments forwarded to the provider. In the DSL, extra `**kwargs` to `faker(...)` land here. |
| `locale` | `str \| None` | `None` | Faker locale (e.g. `"en_US"`, `"de_DE"`). When `None`, falls back to `DataGenPlan.default_locale` (which itself defaults to `"en_US"`). |

### Validation & gotchas

- **`provider` must not be whitespace-only.** `Field(min_length=1)`
  alone would admit `"   "`; the validator strips and rejects it with a
  message suggesting real method names.
- **An unknown provider name fails at materialisation, not plan time.**
  The model only checks that `provider` is non-empty; whether it's a
  real Faker method is verified when the engine runs, raising
  `ValueError`.
- **Locale fallback:** leave `locale` unset to inherit
  `DataGenPlan.default_locale`. Set it per-column to override for that
  column only.
- **Not allowed inside `StructColumn` or `ArrayColumn`.** Faker needs a
  column-level `pandas_udf`, so it can't be a struct field or array
  element — see [nested.md](nested.md).

## See also

- [categorical-values.md](categorical-values.md) — `ValuesColumn` /
  `text` for fixed-list string columns
- [sequences-and-ids.md](sequences-and-ids.md) — `pk_pattern` for
  patterned primary keys, and the other key strategies
- [nested.md](nested.md) — why Faker can't be nested
- [../concepts/determinism-and-seeds.md](../concepts/determinism-and-seeds.md)
  — how `seed_from` correlates generated strings
- [index.md](index.md) — all strategies at a glance
