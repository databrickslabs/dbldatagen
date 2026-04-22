# Future Work

Review-surfaced items that were deliberately deferred.  Each entry
should name the source (reviewer / commit / issue), the constraint that
makes it non-trivial, and a plausible path to resolution.

---

## SQL Server `__$seqval` — deterministic within-transaction ordering

**Source:** @ghanse review on `ak/synth-next`, April 2026.
**Code:** `dbldatagen/core/engine/cdc_formats.py::to_sql_server`
**Commit documenting the deviation:** `3f7b289`

### Problem

Real SQL Server CDC guarantees `__$seqval` is monotonic within a commit,
so consumers can `ORDER BY __$start_lsn, __$seqval` to reconstruct
within-transaction row order.  Our synthetic value is
`xxhash64(_batch_id, *data_cols)` — deterministic per row but unordered
inside a batch.  Consumers that rely on seqval for strict
within-transaction ordering silently get wrong order.

### Why the hash (current behaviour)

A rank-based seqval would use
`row_number() over (partition by _batch_id order by _synth_row_id)`.
The window forces a per-batch shuffle + sort, which is measurably costly
at the 500M–3B row scales this generator targets.  Hash is O(1) per row
with no shuffle.

### Paths forward

1. **Opt-in deterministic seqval.**  Add a `deterministic_seqval: bool`
   flag on the SQL Server format config.  Default `False` (current hash
   behaviour, fastest).  When `True`, emit `row_number() over (...)` at
   generation time.  Benchmark the overhead on the 500M row fixture
   before committing to it as an option — if it's > ~20% throughput
   loss, document the tradeoff in the flag's docstring.

2. **Post-hoc seqval.**  Ship a small helper
   (`dbldatagen.core.engine.cdc_postprocess.reseqval(df)`) that rewrites
   `__$seqval` to a rank at read time for consumers who need strict
   ordering.  Keeps the generator hot path unchanged; pushes cost to the
   consumer who actually cares.

Preference: option 2 first (zero blast radius), option 1 if enough
users request it.

### Out of scope / not a path forward

- Changing the default to rank-based — would regress throughput for every
  user whether they care about ordering or not.
- Partition by `__$start_lsn` instead of `_batch_id` — identical cost,
  just renames the partition key.
