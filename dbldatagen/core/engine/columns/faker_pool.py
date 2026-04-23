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
    """Generate realistic text using a pre-computed Faker pool.

    1. Driver-side: Create Faker with deterministic seed, generate pool
    2. Executor-side: pandas_udf selects from pool via hash(column_seed, id) % pool_size

    Raises ImportError if faker is not installed.
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

    # Generate the pool on the driver (deterministic).  Faker expects a
    # non-negative 32-bit seed; mix the high 32 bits of ``column_seed``
    # into the low 32 bits before masking so two columns whose seeds
    # differ only in bits >= 31 get distinct pools.  The old
    # ``column_seed & 0x7FFFFFFF`` collided every 2**31 seeds, which —
    # combined with the polynomial hash in ``derive_column_seed`` —
    # produced correlated pools across columns with similar names.
    fake = Faker(locale or "en_US")
    seed32 = (column_seed ^ (column_seed >> 32)) & 0x7FFFFFFF
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
        # Deterministic index: non-negative pmod of mix(seed, id) over pool_size.
        # ``np.abs(np.iinfo(np.int64).min)`` silently wraps (NumPy does not
        # raise) so the old ``np.abs(x) % N`` produced negative indices on
        # the Long.MIN_VALUE row, which pandas then interpreted as Python
        # negative indexing.  ``np.mod`` uses Python-semantics modulo and
        # is always non-negative when the divisor is positive.
        x = ids ^ np.int64(_column_seed)
        x = x * np.int64(6364136223846793005) + np.int64(1442695040888963407)
        indices = np.mod(x, _pool_size)
        return pd.Series(pool_array[indices.astype(np.intp)])

    return _faker_pool_udf(id_col)  # type: ignore[no-any-return]
