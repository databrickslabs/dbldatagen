---
sidebar_label: uuid
title: dbldatagen.core.engine.columns.uuid
---

Deterministic UUID generation.

Each row gets a UUID-formatted string derived from two independent 64-bit hashes
of the column seed and row id. Output is reproducible for a given seed and row.

### build\_uuid\_column

```python
def build_uuid_column(id_column: Column | str, column_seed: int) -> Column
```

Builds a deterministic UUID-formatted string column.

Combines two independent 64-bit hashes into 128 bits and formats them as
`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. The same seed and row id always
produce the same value.

**Arguments**:

- `id_column` - Row-id column, given as a `Column` or column name.
- `column_seed` - Per-column seed.
  

**Returns**:

  A Spark string `Column` of UUID-formatted values.
  

**Notes**:

  The output is UUID-shaped but not an RFC 4122 UUID. It carries no
  version or variant bits. Parsers that validate for those bits will
  reject it.

