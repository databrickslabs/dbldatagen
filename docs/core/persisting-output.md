# Persisting output to Delta / Unity Catalog

`generate(spark, plan)` returns an in-memory `dict[str, DataFrame]` — it
does **not** write anything to storage. The library has no built-in
writer by design: the result is ordinary Spark `DataFrame`s, so you
persist them with the same `DataFrame.write` API you already use. This
page shows the common patterns.

## Write one table to a Unity Catalog table

```python
dfs = generate(spark, plan)

dfs["orders"].write.format("delta").mode("overwrite").saveAsTable(
    "my_catalog.my_schema.orders"
)
```

- Use the **three-level** name `catalog.schema.table` for Unity
  Catalog. A bare `table` name writes to the current catalog/schema.
- `format("delta")` is the default on Databricks; it's spelled out here
  for clarity.
- `mode("overwrite")` replaces the table; `"append"` adds rows;
  `"errorifexists"` (the default) fails if it already exists.

## Write an entire plan

`generate` returns the tables in **generation order** — parents before
children — and a Python `dict` preserves that insertion order. So
iterating the result writes FK parents before the children that
reference them, which is what you want if a downstream consumer enforces
referential integrity:

```python
dfs = generate(spark, plan)

for table_name, df in dfs.items():        # parents first, then children
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"my_catalog.my_schema.{table_name}"
    )
```

## Partitioning

Partition large tables on a low-cardinality column to speed up
downstream reads:

```python
dfs["orders"].write.format("delta").partitionBy("status").mode("overwrite").saveAsTable(
    "my_catalog.my_schema.orders"
)
```

Partition only on columns with modest cardinality (e.g. `status`,
a date bucket) — partitioning on a high-cardinality column such as a
primary key creates one tiny file per value and hurts performance.

## Writing to a path instead of a table

To write Delta files to a location (e.g. an external table or a raw
path) rather than a catalog table:

```python
dfs["orders"].write.format("delta").mode("overwrite").save(
    "/Volumes/my_catalog/my_schema/my_volume/orders"
)
```

## Generate once, write twice

`generate` builds lazy `DataFrame`s; each `.write` re-runs the plan.
That's fine and fully deterministic — the same plan and seed produce the
same rows every time (see
[concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md)),
so re-running for a second sink yields identical data. If you want to
write the same materialized rows to multiple sinks without recomputing,
`.cache()` (and `.count()` to force materialization) first:

```python
orders = dfs["orders"].cache()
orders.count()                            # materialize once
orders.write.format("delta").saveAsTable("my_catalog.my_schema.orders")
orders.write.format("parquet").save("/tmp/orders_parquet")
orders.unpersist()
```

## Other formats

The returned objects are plain `DataFrame`s, so any Spark sink works —
`parquet`, `csv`, `json`, JDBC, etc.:

```python
dfs["orders"].write.format("parquet").mode("overwrite").save("/tmp/orders")
```

## See also

- [api-reference.md](api-reference.md) — `generate` returns
  `dict[str, DataFrame]` keyed by table name, in generation order
- [recipes/multi-table.md](recipes/multi-table.md) — building and
  verifying a multi-table dataset before you write it
- [concepts/determinism-and-seeds.md](concepts/determinism-and-seeds.md)
  — why re-running a write reproduces the same rows
