---
sidebar_position: 2
title: "Scaling to Billions"
---

# Scaling to 100B+ Rows

> **TL;DR:** dbldatagen.v1 has reached **1.96M rows/sec** on a 20-node cluster, generating 40B rows in 5h 40m. The key settings are `spark.default.parallelism = workers x cores`, `optimizeWrite=false`, and `maxRecordsPerFile=10M`. Use the STATELESS strategy and DS5_v2 nodes for maximum throughput.

## Why the Architecture Scales

Three architectural decisions make arbitrary scale possible:

### 1. Partition-Independent Determinism

Every cell value is derived from `xxhash64(column_seed, row_id)` where `row_id` comes from `spark.range(N)`. The computation is purely local — no shuffle, no cross-partition communication, no random state that depends on partition boundaries.

This means adding workers linearly increases throughput. A 40-worker cluster generates data at twice the rate of a 20-worker cluster with no code changes.

### 2. No Parent Table Materialisation

Foreign key values are computed by reconstructing the parent PK function:
```
fk_value = parent_pk_function(distribution_sample(cell_seed, N_parent))
```

The parent DataFrame is never read. This eliminates the most common scaling bottleneck in synthetic data tools: the need to broadcast or join against parent data.

### 3. STATELESS: O(1) Per-Row Lifecycle

The STATELESS ingest strategy (Three Clocks engine) computes each row's insert/update/delete tick as a pure function of its identity seed. Generating batch 30 of 365 requires zero knowledge of batches 1–29. No driver-side state accumulates as scale grows.

For the stateful CDC engine, driver memory is `O(initial_rows)` — manageable up to ~100M rows, then memory-mapped files kick in. At 10B+ rows use STATELESS.

---

## Bottlenecks and Fixes

Three bottlenecks have been identified and addressed through benchmarking:

### Bottleneck 1: ADLS Write Saturation (iowait)

**Symptom:** Cluster CPU at 10–20%, iowait >50%, throughput below 500K rows/sec despite adequate compute.

**Root cause:** With high partition counts (default parallelism = `2 x total cores`), each partition writes a small file simultaneously. 1280 concurrent writers saturate Azure Data Lake Storage bandwidth.

**Fix:** Reduce parallelism to `workers x cores/worker`. For 20xDS5_v2 (16 vCPU each):
```python
spark.conf.set("spark.default.parallelism", "320")  # 20 x 16
spark.conf.set("spark.sql.shuffle.partitions", "320")
```

This reduces concurrent writers from 1280 to 320, creating larger files (3M+ rows/partition) that write sequentially with far less I/O contention. Initial snapshot time improved **2.6x** in benchmarks.

Also set:
```python
spark.conf.set("spark.sql.files.maxRecordsPerFile", "10000000")
```

This prevents any single partition from creating multiple small files, which occurs when partition count x rows-per-partition exceeds the 10M threshold.

### Bottleneck 2: optimizeWrite Shuffle Overhead

**Symptom:** Spark stage view shows an extra shuffle exchange before every Delta write. Batch writes take 2–3x longer than expected.

**Root cause:** `spark.databricks.delta.optimizeWrite.enabled=true` (Databricks default) adds a shuffle stage to bin-pack files into ~128MB. When partitions are already well-sized (200–500MB at p=320), this shuffle is pure overhead — it reads and rewrites the same data.

**Fix:**
```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
```

This eliminated **18 minutes** from a 1.5B-row job in benchmarks (Run 7 to Run 8, 50m to 32m). At 40B rows the savings are proportionally larger.

**When to keep optimizeWrite on:** Only when partition sizes are very small (under 32 MB) and you need compact files for downstream query performance. At scale with p=320, files are already well-sized.

### Bottleneck 3: Driver-Side Gap Between Jobs

**Symptom:** In Spark UI, the cluster sits at 0–30% CPU for several minutes (or hours at 30B+ rows) between the initial snapshot job completing and the first batch job starting.

**Root cause:** `write_ingest_to_delta` builds all N lazy batch DataFrames and unions them on the driver before submitting the batch Spark job. At 30B initial rows, this driver-side bookkeeping takes significant time.

**Current workaround:** Use `chunk_size=num_batches` to merge all batches into one Spark job submission, reducing the number of driver–executor round-trips:
```python
write_ingest_to_delta(spark, plan, catalog="cat", schema="sch", chunk_size=5)
```

**Future optimization:** Submit batch jobs incrementally rather than unioning all at once.

---

## Recommended Configuration

### For 1B–10B Row Jobs on Classic Compute

```python
# Set before generating data
spark.conf.set("spark.default.parallelism", str(num_workers * cores_per_worker))
spark.conf.set("spark.sql.shuffle.partitions", str(num_workers * cores_per_worker))
spark.conf.set("spark.sql.files.maxRecordsPerFile", "10000000")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
```

As a `spark_conf` dict for a job cluster definition:
```json
{
  "spark.default.parallelism": "320",
  "spark.sql.shuffle.partitions": "320",
  "spark.sql.files.maxRecordsPerFile": "10000000",
  "spark.databricks.delta.optimizeWrite.enabled": "false",
  "spark.databricks.delta.autoCompact.enabled": "false"
}
```

Adjust `320` to `workers x cores_per_worker` for your cluster.

### For Serverless Compute

Serverless manages parallelism automatically. The main levers are:
- `maxRecordsPerFile` to control file sizes
- `optimizeWrite=false` to skip the shuffle
- `chunk_size` on `write_ingest_to_delta`

Serverless is 4–5x faster for initial setup (no cluster startup) and shows better throughput at small-medium scale (up to ~3B rows). For 30B+ rows use classic compute where you can tune parallelism directly.

---

## Node Type Selection

Not all VM types perform equally for dbldatagen.v1 workloads. Key factors are **CPU count** (generation is CPU-bound) and **storage network throughput** (Delta writes are network-bound).

| Node Type | vCPU | RAM | Network | Best for |
|---|---|---|---|---|
| `Standard_DS5_v2` | 16 | 56GB | High | Best overall — highest storage throughput |
| `Standard_D16_v3` | 16 | 128GB | Medium | Good; more RAM for very large driver state |
| `Standard_D4ds_v5` | 4 | 16GB | Medium | Small clusters; Photon not recommended (see below) |
| `Standard_F16s_v2` | 16 | 32GB | Low | Avoid — network saturation at scale |

**DS5_v2 vs D16_v3:** At 40B rows, DS5_v2 was **42% faster** than D16_v3 on identical configs, despite less RAM (56GB vs 128GB). DS5_v2's premium storage network throughput is the dominant factor at scale.

**Photon:** Adds overhead for `pandas_udf` (Feistel cipher, Faker pool) and `xxhash64`. Standard runtime outperforms Photon for dbldatagen.v1 workloads. In benchmarks: 20xD4ds_v5 Photon achieved 408K rows/sec vs 1.38M rows/sec on 20xD16_v3 Standard — a 3.4x difference for the same config.

---

## Measured Benchmarks

### 40B Row Benchmark (Best Result)

**Schema:** 185-column Mastercard debit_dtl schema (stateless ingest, 75% insert / 25% update)
**Cluster:** 20xStandard_DS5_v2, DBR 17.3 Standard (no Photon)
**Config:** `parallelism=320`, `optimizeWrite=false`, `maxRecordsPerFile=10M`, `chunk_size=5`

| Metric | Value |
|--------|-------|
| Total rows | 40B (30B initial + 5x2B batches) |
| Wall time | 5h 40m 21s |
| **Throughput** | **1.96M rows/sec** |
| CPU profile | Near-100% user, no iowait |
| Peak memory | 37.25 GB / 56 GB (no swap) |
| Job ID | 479992488288399 |
| Run ID | 852560125330118 |

### 1B Row Sweep (8 Runs, Same Schema)

The following table shows how each optimization improved throughput on a 1.5B-row job (1B initial + 5x100M batches):

| Run | Cluster | parallelism | optimizeWrite | Wall Time | Throughput | vs baseline |
|-----|---------|-------------|---------------|-----------|------------|-------------|
| R1 | 20xD4ds_v5 Photon | 640 | off | 61.3 min | 408K/s | baseline |
| R5 | 20xD16_v3 | 1280 | on | 118 min | 211K/s | -48% |
| R6 | 20xD16_v3 | 1280 | on | 59.5 min | 420K/s | +3% |
| R7 | 20xD16_v3 | **320** | on | 50.8 min | ~625K/s | +53% |
| **R8** | **20xD16_v3** | **320** | **off** | **18.1 min** | **1,382K/s** | **+239%** |

Full experiment log with all job/run IDs is available in the design documentation.

### Serverless Benchmarks (store_orders schema, 10 columns)

| Scale | Wall time | Throughput |
|-------|-----------|------------|
| 1B | 3.5 min | ~4.8M rows/sec |
| 2B | 5.3 min | ~6.3M rows/sec |
| 3B | 7.6 min | ~6.6M rows/sec |

Serverless improves at scale (sub-linear). The 10-column schema is simpler than 185-column debit_dtl — real-world throughput depends heavily on column count and Faker usage.

---

## Scaling Limits and Future Work

### Single-Cluster Ceiling

At 20xDS5_v2 (320 cores), the measured ceiling is ~2M rows/sec for a wide (185-column) schema. The bottleneck is ADLS write bandwidth, not compute. Narrower schemas or write-optimised storage can push this higher.

Theoretical ceiling estimate for a single cluster:
- 320 cores x ~6M rows/sec/core (Tier 1 SQL generation) = ~1.9B rows/sec generation rate
- Write bottleneck: ~2M rows/sec to Delta at 185 cols x ~60 bytes/row = ~24 GB/s sustained write

### Multi-Cluster Patterns

For >100B rows, partition the problem by table:
```python
# Each cluster generates one table in parallel
# Tables without FKs are fully independent
generate_ingest(spark, plan_for_table_A, ...)  # Cluster 1
generate_ingest(spark, plan_for_table_B, ...)  # Cluster 2
```

FK-dependent tables must be generated in topological order (parents first), but sibling tables with no FK relationship can run fully in parallel.

### Driver-Side Gap at 30B Scale

The ~2h driver gap between initial snapshot and batch writes at 30B rows is the primary remaining optimization opportunity. Future work: batch submission without full union.

---

## See Also

- [Ingest Guide](../ingestion/overview.md) — `write_ingest_to_delta`, `IngestPlan`, strategy selection
- [Architecture](../reference/architecture.md) — Three Clocks engine and seed derivation
