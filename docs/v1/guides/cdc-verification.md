---
sidebar_position: 5
title: "CDC Verification Queries"
description: "SQL queries to validate that CDC generation matches plan specifications — operation counts, mutation columns, PK uniqueness, and batch timing"
keywords: [dbldatagen, CDC, verification, validation, delta_cdf, change data feed]
---

# CDC Verification Queries

> **TL;DR:** After running a CDC plan, use these queries to confirm the generated data matches your plan spec — correct operation ratios, mutation columns, PK continuity, and batch timing.

These queries are written for the 4-Year Transaction CDC Plan with `delta_cdf` format, but adapt to any CDC plan by changing the table names and expected values.

**Plan spec being verified:**

| Setting | Value |
|---|---|
| Initial snapshot | 50,000,000 rows |
| Batch size | 500,000 changes/day |
| Operation weights | insert=9, update=1, delete=0 |
| Mutation columns | `status`, `is_flagged`, `amount` |
| Mutation fraction | 1.0 (all mutation columns change) |
| Format | `delta_cdf` |
| Batch interval | 86,400s (1 day) |
| Start timestamp | 2022-01-01T00:00:00Z |

---

## 1. Initial Snapshot Row Count

All initial rows should be `insert` operations.

```sql
SELECT
  _change_type,
  COUNT(*) AS row_count
FROM anup_kalburgi.datagen_demo.transactions
GROUP BY _change_type;
```

**Expected:** `insert` = 50,000,000

---

## 2. Total CDC Row Count

Each batch produces: 450K inserts + 50K `update_preimage` + 50K `update_postimage` = 550K rows per batch.

```sql
SELECT COUNT(*) AS total_cdc_rows
FROM anup_kalburgi.datagen_demo.transactions_cdc;
```

**Expected:** 550,000 × number of batches generated

---

## 3. Operation Mix Per Batch

Verify the insert/update/delete split matches the configured weights for each batch.

```sql
SELECT
  _commit_version AS batch_id,
  _change_type,
  COUNT(*) AS row_count
FROM anup_kalburgi.datagen_demo.transactions_cdc
GROUP BY _commit_version, _change_type
ORDER BY _commit_version, _change_type;
```

**Expected per batch:**

| `_change_type` | Count |
|---|---|
| `insert` | 450,000 |
| `update_postimage` | 50,000 |
| `update_preimage` | 50,000 |

---

## 4. Operation Ratios (Aggregated)

Cross-check the overall distribution across all batches.

```sql
SELECT
  _change_type,
  COUNT(*) AS total_rows,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM anup_kalburgi.datagen_demo.transactions_cdc
GROUP BY _change_type
ORDER BY _change_type;
```

**Expected:**

| `_change_type` | % of rows |
|---|---|
| `insert` | ~81.82% (450K / 550K) |
| `update_postimage` | ~9.09% (50K / 550K) |
| `update_preimage` | ~9.09% (50K / 550K) |
| `delete` | 0% |

---

## 5. Spot-Check a Single Batch

Pick batch 1 and verify exact counts.

```sql
SELECT
  _change_type,
  COUNT(*) AS row_count
FROM anup_kalburgi.datagen_demo.transactions_cdc
WHERE _commit_version = 1
GROUP BY _change_type
ORDER BY _change_type;
```

**Expected:** 450,000 insert + 50,000 `update_preimage` + 50,000 `update_postimage`

---

## 6. Zero Deletes

Confirm no delete rows exist (delete weight = 0).

```sql
SELECT COUNT(*) AS delete_count
FROM anup_kalburgi.datagen_demo.transactions_cdc
WHERE _change_type = 'delete';
```

**Expected:** 0

---

## 7. Mutation Column Validation

Only `status`, `is_flagged`, and `amount` should differ between preimage and postimage. All other columns should be identical.

```sql
WITH updates AS (
  SELECT
    txn_id,
    _change_type,
    _commit_version,
    account_id, merchant_id, currency, txn_type, channel, category, region,
    status, is_flagged, amount
  FROM anup_kalburgi.datagen_demo.transactions_cdc
  WHERE _change_type IN ('update_preimage', 'update_postimage')
    AND _commit_version = 1
)
SELECT
  pre.txn_id,
  -- These should NEVER change (not in mutations.columns)
  (pre.account_id != post.account_id) AS account_id_changed,
  (pre.merchant_id != post.merchant_id) AS merchant_id_changed,
  (pre.currency != post.currency) AS currency_changed,
  (pre.txn_type != post.txn_type) AS txn_type_changed,
  (pre.channel != post.channel) AS channel_changed,
  (pre.category != post.category) AS category_changed,
  (pre.region != post.region) AS region_changed,
  -- These SHOULD change (mutations.columns with fraction=1)
  (pre.status != post.status) AS status_changed,
  (pre.is_flagged != post.is_flagged) AS is_flagged_changed,
  (pre.amount != post.amount) AS amount_changed
FROM updates pre
JOIN updates post
  ON pre.txn_id = post.txn_id
  AND pre._commit_version = post._commit_version
WHERE pre._change_type = 'update_preimage'
  AND post._change_type = 'update_postimage'
LIMIT 20;
```

**Expected:**
- `account_id_changed` through `region_changed` → all `false`
- `status_changed`, `is_flagged_changed`, `amount_changed` → `true` (fraction = 1.0 means all three mutate)

---

## 8. PK Uniqueness Across Initial + CDC Inserts

New inserts should never reuse a `txn_id` from the initial snapshot.

```sql
SELECT
  COUNT(*) AS duplicate_pks
FROM (
  SELECT txn_id FROM anup_kalburgi.datagen_demo.transactions
  INTERSECT
  SELECT txn_id FROM anup_kalburgi.datagen_demo.transactions_cdc
  WHERE _change_type = 'insert'
);
```

**Expected:** 0 — new inserts get PKs starting beyond the initial 50M range

---

## 9. Batch Timestamps

Batches should be exactly 1 day apart (86,400 seconds), starting from 2022-01-01.

```sql
SELECT
  _commit_version AS batch_id,
  MIN(_commit_timestamp) AS batch_timestamp
FROM anup_kalburgi.datagen_demo.transactions_cdc
GROUP BY _commit_version
ORDER BY _commit_version
LIMIT 10;
```

**Expected:**

| batch_id | batch_timestamp |
|---|---|
| 1 | 2022-01-02 00:00:00 |
| 2 | 2022-01-03 00:00:00 |
| 3 | 2022-01-04 00:00:00 |
| ... | +1 day each |

---

## 10. Net Growth

With 0 deletes, total live rows = initial snapshot + all CDC inserts.

```sql
SELECT
  (SELECT COUNT(*) FROM anup_kalburgi.datagen_demo.transactions) AS initial_rows,
  (SELECT COUNT(*) FROM anup_kalburgi.datagen_demo.transactions_cdc
   WHERE _change_type = 'insert') AS total_new_inserts,
  (SELECT COUNT(*) FROM anup_kalburgi.datagen_demo.transactions) +
  (SELECT COUNT(*) FROM anup_kalburgi.datagen_demo.transactions_cdc
   WHERE _change_type = 'insert') AS expected_live_rows;
```

**Expected:** `initial_rows` = 50M, `total_new_inserts` = 450K × batches generated, `expected_live_rows` = sum of both

---

## Adapting for Other Plans

To use these queries with a different CDC plan:

1. **Change table names** — replace `anup_kalburgi.datagen_demo.transactions` with your catalog/schema/table
2. **Adjust expected counts** — recalculate based on your `batch_size` and `operations` weights
3. **Update mutation columns** — query 7 should reflect your `mutations.columns` list
4. **Different format** — if using `raw` instead of `delta_cdf`, replace `_change_type` with `_op` and `_commit_version` with `_batch_id`

| `delta_cdf` column | `raw` equivalent |
|---|---|
| `_change_type` = `insert` | `_op` = `I` |
| `_change_type` = `update_preimage` | `_op` = `UB` |
| `_change_type` = `update_postimage` | `_op` = `U` |
| `_change_type` = `delete` | `_op` = `D` |
| `_commit_version` | `_batch_id` |
| `_commit_timestamp` | `_ts` |
