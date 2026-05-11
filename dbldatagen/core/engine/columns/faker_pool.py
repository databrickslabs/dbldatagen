"""Pool-based Faker column generation for realistic text data.

Strategy:
1. Driver-side: Create Faker with deterministic seed, generate pool of values
2. Executor-side: pandas_udf selects from pool via hash(column_seed, id) % pool_size

For Spark Connect compatibility: pool is passed via closure (not sc.broadcast).
"""

import numpy as np
import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_faker_column(
    id_col: Column,
    column_seed: int,
    provider: str,
    kwargs: dict | None = None,
    locale: str | None = None,
    pool_size: int = 10_000,
) -> Column:
    """Generates realistic text using a pre-computed Faker pool.

    Two-phase strategy chosen for Spark Connect compatibility (the
    pool travels via closure, not ``sc.broadcast``):

        1. Driver-side: create a ``Faker`` instance with a
           deterministic seed, generate a fixed-size pool of values.
        2. Executor-side: a ``pandas_udf`` selects from the pool via
           ``hash(column_seed, id) % pool_size``.

    Args:
        id_col: Row-id ``Column``.
        column_seed: Per-column seed.  Used both to seed the
          driver-side Faker (so the pool is reproducible) and to
          index into the pool at execution time.
        provider: Faker provider method name (e.g. ``"first_name"``,
          ``"company"``).
        kwargs: Keyword arguments forwarded to the provider.  ``None``
          (default) is treated as an empty mapping.
        locale: Faker locale (e.g. ``"en_US"``).  ``None`` (default)
          falls back to ``"en_US"``.
        pool_size: Number of pre-computed values to draw at the
          driver.  Larger pools reduce cross-row repetition at the
          cost of driver memory.  Defaults to ``10_000``.

    Returns:
        A Spark ``Column`` (string) holding the per-row Faker output.

    Raises:
        ImportError: ``faker`` is not installed; install with
          ``pip install 'dbldatagen[core-faker]'``.
        ValueError: ``provider`` is not a method on the ``Faker``
          instance for the chosen locale.
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

    # Faker wants a non-negative 32-bit seed.  Mask to u64 BEFORE the
    # right-shift -- Python ``>>`` on negative ints sign-extends and
    # collapses half the signed-64 space onto the same mix value.
    fake = Faker(locale or "en_US")
    seed_u64 = column_seed & 0xFFFFFFFFFFFFFFFF
    seed32 = (seed_u64 ^ (seed_u64 >> 32)) & 0x7FFFFFFF
    fake.seed_instance(seed32)

    faker_method = getattr(fake, provider, None)
    if faker_method is None:
        raise ValueError(f"Unknown Faker provider method: '{provider}'")

    pool: list[str] = []
    for _ in range(pool_size):
        val = faker_method(**kwargs)
        pool.append(str(val) if val is not None else "")

    # Capture pool as numpy array in closure for vectorized indexing
    pool_array = np.array(pool, dtype=object)
    _pool_size = pool_size
    _column_seed = column_seed

    @F.pandas_udf(T.StringType())  # type: ignore[call-overload]
    def _faker_pool_udf(id_series: pd.Series) -> pd.Series:
        ids = id_series.values.astype(np.int64)
        # ``np.mod`` (not ``np.abs() % N``): np.abs silently wraps on
        # Long.MIN_VALUE.  ``errstate(over="ignore")`` silences the LCG
        # overflow warning -- overflow is the mixing mechanism here.
        with np.errstate(over="ignore"):
            x = ids ^ np.int64(_column_seed)
            x = x * np.int64(6364136223846793005) + np.int64(1442695040888963407)
        indices = np.mod(x, _pool_size)
        return pd.Series(pool_array[indices.astype(np.intp)])

    return _faker_pool_udf(id_col)  # type: ignore[no-any-return]
