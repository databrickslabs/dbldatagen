"""Convert v0 DataGenerator specs to v1 DataGenPlan.

Provides a migration path for users moving from dbldatagen v0 to v1.
Not all v0 features have a v1 equivalent — unsupported options emit warnings.

Usage::

    from dbldatagen import DataGenerator
    from dbldatagen.v1.compat import from_data_generator

    dg = (
        DataGenerator(spark, rows=10000, name="orders")
        .withColumn("id", LongType(), minValue=1, maxValue=10000)
        .withColumn("status", StringType(), values=["pending", "shipped"])
    )

    plan = from_data_generator(dg)
    # plan is a v1 DataGenPlan ready for generate(spark, plan)
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from dbldatagen.v1.schema import (
    ColumnSpec,
    ColumnStrategy,
    ConstantColumn,
    DataGenPlan,
    DataType,
    Exponential,
    ExpressionColumn,
    Normal,
    PatternColumn,
    PrimaryKey,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    TimestampColumn,
    ValuesColumn,
    WeightedValues,
)


if TYPE_CHECKING:
    from dbldatagen import DataGenerator
    from dbldatagen.column_generation_spec import ColumnGenerationSpec


# ---------------------------------------------------------------------------
# Spark type → v1 DataType mapping
# ---------------------------------------------------------------------------

_SPARK_TYPE_MAP: dict[str, DataType] = {
    "IntegerType": DataType.INT,
    "LongType": DataType.LONG,
    "FloatType": DataType.FLOAT,
    "DoubleType": DataType.DOUBLE,
    "StringType": DataType.STRING,
    "BooleanType": DataType.BOOLEAN,
    "DateType": DataType.DATE,
    "TimestampType": DataType.TIMESTAMP,
    "TimestampNTZType": DataType.TIMESTAMP,
    "DecimalType": DataType.DECIMAL,
    # Narrow integer types → closest v1 equivalent
    "ByteType": DataType.INT,
    "ShortType": DataType.INT,
}


def _map_spark_type(spark_type: object) -> DataType:
    """Map a PySpark DataType instance to a v1 DataType enum."""
    type_name = type(spark_type).__name__
    result = _SPARK_TYPE_MAP.get(type_name)
    if result is None:
        warnings.warn(
            f"Unsupported Spark type '{type_name}' — defaulting to STRING. "
            f"Review the converted column and adjust manually.",
            stacklevel=3,
        )
        return DataType.STRING
    return result


# ---------------------------------------------------------------------------
# Distribution mapping (v0 → v1)
# ---------------------------------------------------------------------------


def _map_distribution(v0_dist: object) -> Normal | Exponential | None:
    """Map a v0 distribution object to a v1 distribution, or None."""
    if v0_dist is None:
        return None
    cls_name = type(v0_dist).__name__
    if cls_name == "Normal":
        mean = getattr(v0_dist, "mean", 0.0) or 0.0
        stddev = getattr(v0_dist, "stddev", 1.0) or 1.0
        return Normal(mean=mean, stddev=stddev)
    if cls_name == "Exponential":
        rate = getattr(v0_dist, "rate", 1.0) or 1.0
        return Exponential(rate=rate)
    # Beta, Gamma have no v1 equivalent
    warnings.warn(
        f"v0 distribution '{cls_name}' has no v1 equivalent. Using Uniform (default).",
        stacklevel=4,
    )
    return None


# ---------------------------------------------------------------------------
# Column conversion
# ---------------------------------------------------------------------------


def _get_option(spec: ColumnGenerationSpec, key: str, default: Any = None) -> Any:
    """Safely read a v0 column option."""
    try:
        val = spec[key]
        return val if val is not None else default
    except (KeyError, TypeError):
        return default


def _resolve_base_column(base_column: object) -> str | None:
    """Extract a single base column name from v0's baseColumn value.

    v0 allows string, list of strings, or comma-separated string.
    seed_from only accepts a single column name, so we take the first.
    """
    if base_column is None:
        return None
    if isinstance(base_column, list):
        return str(base_column[0]) if base_column else None
    s = str(base_column)
    if "," in s:
        return s.split(",")[0].strip()
    return s


def _convert_column(spec: ColumnGenerationSpec) -> list[ColumnSpec]:
    """Convert a single v0 ColumnGenerationSpec to v1 ColumnSpec(s).

    Returns a list because numColumns/numFeatures can expand one v0 spec
    into multiple v1 columns. Returns empty list for omitted columns.
    """
    name = spec.name
    dtype = _map_spark_type(spec.datatype)

    # Skip internal seed column
    if spec.omit:
        return []

    # Nullable / null fraction
    nullable = spec.nullable
    percent_nulls = _get_option(spec, "percentNulls", 0.0)
    null_fraction = float(percent_nulls) if percent_nulls else 0.0
    if null_fraction > 0:
        nullable = True

    # Read all v0 options
    base_column = _get_option(spec, "baseColumn")
    base_column_type = _get_option(spec, "baseColumnType")
    unique_values = _get_option(spec, "uniqueValues")
    num_columns = _get_option(spec, "numColumns")
    num_features = _get_option(spec, "numFeatures")
    format_str = _get_option(spec, "format")
    expr_val = _get_option(spec, "expr")
    values = _get_option(spec, "values")
    weights = _get_option(spec, "weights")
    template = _get_option(spec, "template")
    text_gen = _get_option(spec, "text")
    prefix = _get_option(spec, "prefix", "")
    suffix = _get_option(spec, "suffix", "")
    min_val = _get_option(spec, "minValue")
    max_val = _get_option(spec, "maxValue")
    step = _get_option(spec, "step", 1)
    begin = _get_option(spec, "begin")
    end = _get_option(spec, "end")
    v0_distribution = _get_option(spec, "distribution")

    # --- Resolve seed_from from baseColumn ---
    seed_from = _resolve_base_column(base_column)
    if seed_from and base_column_type and base_column_type not in ("auto", "hash"):
        warnings.warn(
            f"Column '{name}': baseColumnType='{base_column_type}' maps imperfectly to v1 seed_from. "
            f"seed_from uses hash-based derivation (equivalent to baseColumnType='hash').",
            stacklevel=3,
        )

    # --- Map distribution ---
    v1_distribution = _map_distribution(v0_distribution)

    # --- Warn about truly unsupported features ---
    if format_str:
        warnings.warn(
            f"Column '{name}': format='{format_str}' has no v1 equivalent. "
            f"Consider PatternColumn or ExpressionColumn with format_string().",
            stacklevel=3,
        )
    if text_gen:
        warnings.warn(
            f"Column '{name}': TextGenerator objects cannot be directly converted to v1. "
            f"Consider using FakerColumn instead. Using placeholder pattern.",
            stacklevel=3,
        )

    # --- Adjust range for uniqueValues ---
    if unique_values and min_val is not None:
        uv = int(unique_values)
        s = int(step) if step else 1
        adjusted_max = float(min_val) + (uv - 1) * s
        if max_val is not None and adjusted_max != float(max_val):
            warnings.warn(
                f"Column '{name}': uniqueValues={uv} adjusted max from {max_val} to {adjusted_max}.",
                stacklevel=3,
            )
        max_val = adjusted_max

    # --- Determine generation strategy ---
    gen: ColumnStrategy | None = None

    # 1. Expression columns
    if expr_val:
        gen = ExpressionColumn(expr=str(expr_val))
        seed_from = None  # expressions don't use seed_from

    # 2. Template / text generator
    elif template or text_gen:
        if template:
            gen = PatternColumn(template=str(template))
        else:
            gen = PatternColumn(template=f"{name}_{{digit:6}}")

    # 3. Values-based columns
    elif values:
        kwargs_values: dict = {"values": list(values)}
        if weights and len(weights) == len(values):
            weight_dict = {str(v): w for v, w in zip(values, weights, strict=False)}
            kwargs_values["distribution"] = WeightedValues(weights=weight_dict)
        elif v1_distribution:
            kwargs_values["distribution"] = v1_distribution
        gen = ValuesColumn(**kwargs_values)

    # 4. Timestamp/Date range
    elif dtype in (DataType.TIMESTAMP, DataType.DATE) and (begin or end):
        start_str = str(begin) if begin else "2020-01-01"
        end_str = str(end) if end else "2025-12-31"
        ts_kwargs: dict = {"start": start_str, "end": end_str}
        if v1_distribution:
            ts_kwargs["distribution"] = v1_distribution
        gen = TimestampColumn(**ts_kwargs)

    # 5. Numeric range
    elif dtype in (DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.DECIMAL):
        if min_val is not None or max_val is not None:
            lo = float(min_val) if min_val is not None else 0
            hi = float(max_val) if max_val is not None else lo + 1000
            range_kwargs: dict = {"min": lo, "max": hi}
            if step != 1:
                range_kwargs["step"] = step
            if v1_distribution:
                range_kwargs["distribution"] = v1_distribution
            gen = RangeColumn(**range_kwargs)
        else:
            gen = RangeColumn(min=0, max=1000)

    # 6. Boolean → values
    elif dtype == DataType.BOOLEAN:
        gen = ValuesColumn(values=[True, False])

    # 7. String with prefix/suffix → pattern
    elif dtype == DataType.STRING and (prefix or suffix):
        gen = PatternColumn(template=f"{prefix}{{digit:6}}{suffix}")

    # 8. Fallback: string column
    elif dtype == DataType.STRING:
        gen = PatternColumn(template=f"{name}_{{digit:6}}")

    # 9. Catch-all
    if gen is None:
        gen = ConstantColumn(value=None)
        warnings.warn(
            f"Column '{name}': could not determine generation strategy. Using ConstantColumn(None).",
            stacklevel=3,
        )

    # --- Build the ColumnSpec ---
    col_kwargs: dict = {
        "name": name,
        "dtype": dtype,
        "gen": gen,
        "nullable": nullable,
        "null_fraction": null_fraction,
    }
    if seed_from:
        col_kwargs["seed_from"] = seed_from

    # --- Handle numColumns/numFeatures expansion ---
    n_cols = num_columns or num_features
    if n_cols:
        if isinstance(n_cols, (list, tuple)):
            n_cols = n_cols[1] if len(n_cols) > 1 else n_cols[0]
        n_cols = int(n_cols)
        expanded = []
        for i in range(n_cols):
            kw = col_kwargs.copy()
            kw["name"] = f"{name}_{i}"
            expanded.append(ColumnSpec(**kw))
        return expanded

    return [ColumnSpec(**col_kwargs)]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def from_data_generator(
    dg: DataGenerator,
    *,
    table_name: str | None = None,
    seed: int | None = None,
) -> DataGenPlan:
    """Convert a v0 ``DataGenerator`` to a v1 ``DataGenPlan``.

    Parameters
    ----------
    dg : DataGenerator
        A configured (but not necessarily built) v0 DataGenerator instance.
    table_name : str | None
        Name for the output table. Defaults to ``dg.name``.
    seed : int | None
        Global seed. Defaults to the v0 generator's random seed.

    Returns
    -------
    DataGenPlan
        A v1 plan with a single table, ready for ``generate(spark, plan)``.

    Supported Conversions
    ---------------------
    - ``baseColumn`` → v1 ``seed_from`` (same device_id → same derived value)
    - ``uniqueValues`` → adjusted ``RangeColumn`` range (max = min + (N-1) * step)
    - ``numColumns`` / ``numFeatures`` → expanded into N separate ``ColumnSpec``
    - ``distribution=Normal(...)`` → v1 ``Normal(...)``
    - ``distribution=Exponential(...)`` → v1 ``Exponential(...)``

    Unsupported (warnings emitted)
    ------------------------------
    - ``format`` (printf-style) — no v1 equivalent
    - ``text`` (TextGenerator/ILText objects) — use ``FakerColumn`` instead
    - Constraints — not in v1 schema
    - Beta/Gamma distributions — not in v1

    Examples
    --------
    >>> from dbldatagen import DataGenerator
    >>> from pyspark.sql.types import IntegerType, StringType
    >>> from dbldatagen.v1.compat import from_data_generator
    >>> from dbldatagen.v1 import generate
    >>>
    >>> dg = (
    ...     DataGenerator(spark, rows=1000, name="users")
    ...     .withColumn("user_id", IntegerType(), minValue=1, maxValue=1000)
    ...     .withColumn("status", StringType(), values=["active", "inactive"])
    ... )
    >>> plan = from_data_generator(dg)
    >>> result = generate(spark, plan)
    >>> result["users"].count()
    1000
    """
    name: str = table_name or str(getattr(dg, "name", "table"))
    rows = getattr(dg, "_rowCount", 1000)
    v0_seed = getattr(dg, "_randomSeed", 42)
    global_seed = seed if seed is not None else (v0_seed if isinstance(v0_seed, int) else 42)

    # Warn about constraints
    constraints = getattr(dg, "_constraints", [])
    if constraints:
        warnings.warn(
            f"DataGenerator has {len(constraints)} constraint(s) which are not supported in v1. "
            f"These will be ignored in the converted plan.",
            stacklevel=2,
        )

    # Convert columns
    all_specs: list[ColumnGenerationSpec] = getattr(dg, "_allColumnSpecs", [])
    columns: list[ColumnSpec] = []
    seed_col_name = getattr(dg, "_seedColumnName", "id")

    for spec in all_specs:
        if spec.name == seed_col_name and spec.omit:
            # The internal seed column — convert to a PK sequence
            columns.append(
                ColumnSpec(
                    name=seed_col_name,
                    dtype=DataType.LONG,
                    gen=SequenceColumn(start=getattr(dg, "starting_id", 0), step=1),
                )
            )
            continue

        converted = _convert_column(spec)
        columns.extend(converted)

    if not columns:
        warnings.warn(
            "No columns were converted. The DataGenerator may not have any column definitions.",
            stacklevel=2,
        )

    table = TableSpec(
        name=name,
        rows=rows,
        columns=columns,
        primary_key=PrimaryKey(columns=[seed_col_name]),
    )

    return DataGenPlan(tables=[table], seed=global_seed)
