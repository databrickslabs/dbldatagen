"""CDC output format transformers.

Each function takes a raw CDC DataFrame (with ``_op``, ``_batch_id``, ``_ts``
columns) and reshapes it to match a specific CDC format's schema.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def to_raw(df: DataFrame) -> DataFrame:
    """Identity — raw format already has _op, _batch_id, _ts."""
    return df


def to_delta_cdf(df: DataFrame) -> DataFrame:
    """Convert raw CDC to Delta Lake Change Data Feed format.

    Mapping:
        _op='I'  -> _change_type='insert'
        _op='U'  -> _change_type='update_postimage'
        _op='UB' -> _change_type='update_preimage'
        _op='D'  -> _change_type='delete'

    Adds ``_commit_version`` (= _batch_id) and ``_commit_timestamp``.
    """
    change_type = (
        F.when(F.col("_op") == "I", F.lit("insert"))
        .when(F.col("_op") == "U", F.lit("update_postimage"))
        .when(F.col("_op") == "UB", F.lit("update_preimage"))
        .when(F.col("_op") == "D", F.lit("delete"))
        .otherwise(F.lit("unknown"))
    )
    return (
        df.withColumn("_change_type", change_type)
        .withColumn("_commit_version", F.col("_batch_id"))
        .withColumn("_commit_timestamp", F.col("_ts"))
        .drop("_op", "_batch_id", "_ts")
    )


def to_sql_server(df: DataFrame) -> DataFrame:
    """Convert raw CDC to SQL Server CDC format.

    Mapping:
        _op='I'  -> __$operation=2  (insert)
        _op='U'  -> __$operation=4  (update after)
        _op='UB' -> __$operation=3  (update before)
        _op='D'  -> __$operation=1  (delete)

    Adds ``__$start_lsn`` and ``__$seqval`` (synthetic binary-like strings).
    """
    operation = (
        F.when(F.col("_op") == "I", F.lit(2))
        .when(F.col("_op") == "U", F.lit(4))
        .when(F.col("_op") == "UB", F.lit(3))
        .when(F.col("_op") == "D", F.lit(1))
        .otherwise(F.lit(0))
    )
    # Synthetic LSN: batch_id as zero-padded hex
    lsn = F.lpad(F.hex(F.col("_batch_id").cast("long")), 20, "0")
    # Deterministic seqval: hash batch_id with all data columns (no partition-dependent IDs)
    data_cols = [F.col(c) for c in df.columns if not c.startswith("_")]
    seqval = F.lpad(
        (
            F.hex(F.xxhash64(F.col("_batch_id").cast("long"), *data_cols))
            if data_cols
            else F.hex(F.col("_batch_id").cast("long"))
        ),
        20,
        "0",
    )
    return (
        df.withColumn("__$operation", operation)
        .withColumn("__$start_lsn", lsn)
        .withColumn("__$seqval", seqval)
        .drop("_op", "_batch_id", "_ts")
    )


def to_debezium(df: DataFrame) -> DataFrame:
    """Convert raw CDC to flattened Debezium format.

    Mapping:
        _op='I'  -> op='c' (create)
        _op='U'  -> op='u' (update) — after image, paired with UB before
        _op='UB' -> (dropped — folded into 'u' rows via before_* columns)
        _op='D'  -> op='d' (delete)

    For simplicity in v1, the Debezium format is flattened: ``before_*``
    and ``after_*`` columns rather than nested structs.  Update rows
    (op='u') carry ``after_*`` columns; delete rows (op='d') carry
    ``before_*`` columns.  Insert rows (op='c') carry ``after_*`` columns.

    .. warning:: Limitation — update before-images are dropped.

        Real Debezium events nest ``before`` and ``after`` fields in a
        single record.  This simplified format drops UB (update before)
        rows entirely.  Pipelines that rely on ``before`` fields for
        update events will see incomplete data.
    """
    op_col = (
        F.when(F.col("_op") == "I", F.lit("c"))
        .when(F.col("_op") == "U", F.lit("u"))
        .when(F.col("_op") == "UB", F.lit("ub_internal"))
        .when(F.col("_op") == "D", F.lit("d"))
        .otherwise(F.lit("r"))
    )
    ts_ms = F.col("_ts").cast("long") * 1000

    result = df.withColumn("op", op_col).withColumn("ts_ms", ts_ms).drop("_op", "_ts")

    # Filter out UB rows (they are internal; real Debezium would nest them)
    result = result.filter(F.col("op") != "ub_internal")
    return result.drop("_batch_id")


FORMAT_TRANSFORMERS = {
    "raw": to_raw,
    "delta_cdf": to_delta_cdf,
    "sql_server": to_sql_server,
    "debezium": to_debezium,
}


def apply_format(df: DataFrame, format_name: str) -> DataFrame:
    """Apply the appropriate format transformer."""
    transformer = FORMAT_TRANSFORMERS.get(format_name, to_raw)
    return transformer(df)
