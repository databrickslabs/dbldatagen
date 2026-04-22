"""dbldatagen.core.engine.cdc -- Public API for CDC data generation.

Simplest usage::

    from dbldatagen.core.engine.cdc import generate_cdc

    stream = generate_cdc(spark, base_plan, num_batches=5)
    initial = stream.initial["my_table"]       # DataFrame
    batch_1 = stream.batches[0]["my_table"]    # DataFrame

For large-scale generation on clusters, use ``generate_cdc_bulk``::

    from dbldatagen.core.engine.cdc import generate_cdc_bulk

    stream = generate_cdc_bulk(spark, plan)
    stream.initial["my_table"].write.format("delta").save(...)
    for chunk in stream.batches:
        chunk["my_table"].write.format("delta").mode("append").save(...)

For end-to-end generation + write in one call::

    from dbldatagen.core.engine.cdc import write_cdc_to_delta

    write_cdc_to_delta(spark, plan, catalog="my_catalog", schema="my_schema")

All tables in the base plan participate in CDC by default.  Per-table
customisation is available via ``CDCPlan``.
"""

from __future__ import annotations

from dbldatagen.core.engine.cdc._common import CDCStream, _auto_chunk_size
from dbldatagen.core.engine.cdc.api import (
    generate_cdc,
    generate_cdc_batch,
    generate_cdc_bulk,
    write_cdc_to_delta,
)
from dbldatagen.core.engine.cdc.oracle import generate_expected_state


__all__ = [
    "CDCStream",
    "_auto_chunk_size",
    "generate_cdc",
    "generate_cdc_batch",
    "generate_cdc_bulk",
    "generate_expected_state",
    "write_cdc_to_delta",
]
