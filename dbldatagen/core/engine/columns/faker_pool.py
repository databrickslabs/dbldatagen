"""Pool-based Faker column generation for realistic text data.

Generates a fixed-size pool of Faker values on the driver, then selects from it
per row on the executors. The pool travels via closure rather than a broadcast
variable, for Spark Connect compatibility.
"""

import numpy as np
import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbldatagen.core.engine.utils import apply_null_fraction
from dbldatagen.core.spec.schema import ColumnSpec, FakerColumn


def build_faker_column(
    id_column: Column,
    column_seed: int,
    provider: str,
    kwargs: dict | None = None,
    locale: str = "en_US",
    pool_size: int = 10_000,
) -> Column:
    """Builds a string column of realistic Faker values.

    Generates a fixed-size pool of values on the driver using a seeded `Faker`
    instance, then selects a pool entry per row from the column seed and row id.
    Output is reproducible for a given seed.

    Args:
        id_column: Row-id column.
        column_seed: Per-column seed. Seeds the driver-side pool and selects
            entries at execution time.
        provider: Faker provider method name (e.g. "first_name", "company").
        kwargs: Optional keyword arguments forwarded to the provider
            (default None, treated as no arguments).
        locale: Faker locale such as "de_DE" (default "en_US").
        pool_size: Optional number of values to pre-generate (default 10000).

    Returns:
        A Spark string `Column` of per-row Faker values.

    Raises:
        ImportError: If the `faker` package is not installed.
        ValueError: If `provider` is not a method on the Faker instance.

    Note:
        A column has at most `pool_size` distinct values regardless of row
        count, so large tables will repeat values.
    """
    try:
        from faker import Faker
    except ImportError:
        raise ImportError(
            "The 'faker' package is required for FakerColumn generation. "
            "Install it with: pip install dbldatagen[core-faker]"
        ) from None

    if kwargs is None:
        kwargs = {}

    # Faker wants a non-negative 32-bit seed. Mask to u64 BEFORE the right-shift:
    # Python ``>>`` on negative ints sign-extends and collapses half the signed-64
    # space onto the same mix value.
    faker_instance = Faker(locale)
    seed_u64 = column_seed & 0xFFFFFFFFFFFFFFFF
    seed_32bit = (seed_u64 ^ (seed_u64 >> 32)) & 0x7FFFFFFF
    faker_instance.seed_instance(seed_32bit)

    faker_method = getattr(faker_instance, provider, None)
    if faker_method is None:
        raise ValueError(f"Unknown Faker provider method: '{provider}'")

    pool: list[str] = []
    for _ in range(pool_size):
        value = faker_method(**kwargs)
        pool.append(str(value) if value is not None else "")

    # Capture pool as numpy array in closure for vectorized indexing
    pool_array = np.array(pool, dtype=object)
    _pool_size = pool_size
    _column_seed = column_seed

    @F.pandas_udf(T.StringType())  # type: ignore[call-overload]
    def _faker_pool_udf(id_series: pd.Series) -> pd.Series:
        ids = id_series.values.astype(np.int64)
        # ``np.mod`` (not ``np.abs() % N``): np.abs silently wraps on
        # Long.MIN_VALUE. ``errstate(over="ignore")`` silences the LCG overflow
        # warning -- overflow is the mixing mechanism here.
        with np.errstate(over="ignore"):
            mixed = ids ^ np.int64(_column_seed)
            mixed = mixed * np.int64(6364136223846793005) + np.int64(1442695040888963407)
        indices = np.mod(mixed, _pool_size)
        return pd.Series(pool_array[indices.astype(np.intp)])

    return _faker_pool_udf(id_column)  # type: ignore[no-any-return]


def build_faker_expr(
    col_spec: ColumnSpec,
    id_column: Column,
    column_seed: int,
) -> tuple[str, Column]:
    """Builds a Faker column expression for the column-building loop.

    Constructs the Faker pool expression via `build_faker_column` and wraps it
    with the null mask when the null fraction is positive.

    Args:
        col_spec: The column's spec; its `gen` must be a `FakerColumn`.
        id_column: Row-id column.
        column_seed: Per-column seed.

    Returns:
        A tuple `(column_name, expr)` for the Faker column.

    Raises:
        TypeError: If `col_spec.gen` is not a `FakerColumn`.
    """
    if not isinstance(col_spec.gen, FakerColumn):
        raise TypeError(
            f"build_faker_expr called for '{col_spec.name}' with non-FakerColumn gen "
            f"{type(col_spec.gen).__name__}; dispatcher invariant bypassed"
        )
    faker_expr = build_faker_column(
        id_column,
        column_seed,
        provider=col_spec.gen.provider,
        kwargs=col_spec.gen.kwargs or None,
        locale=col_spec.gen.locale or "en_US",
    )
    faker_expr = apply_null_fraction(faker_expr, column_seed, id_column, col_spec.null_fraction)
    return (col_spec.name, faker_expr)
