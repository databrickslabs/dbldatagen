"""Schema↔engine width-cap parity for ``PatternColumn``.

Per the ``dbldatagen-engine`` A3 rule: any string that the schema's
``PatternColumn.validate_template`` accepts must also be accepted by
the engine's ``build_pattern_column``, and any string that the
schema rejects must also be rejected by the engine.  This is the
property test that pins schema and engine agreement after the
constant extraction to ``core/spec/_constants.py``.

If either side ever drifts (someone tightens engine without
updating the cap, or someone changes the cap without updating the
schema), this test fails immediately with the offending boundary.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from dbldatagen.core.engine.columns.string import build_pattern_column
from dbldatagen.core.spec._constants import (
    MAX_ALPHA_WIDTH,
    MAX_DIGIT_WIDTH,
    MAX_HEX_WIDTH,
    MAX_SEQ_WIDTH,
)
from dbldatagen.core.spec.schema import PatternColumn


CAPS = {
    "digit": MAX_DIGIT_WIDTH,
    "hex": MAX_HEX_WIDTH,
    "alpha": MAX_ALPHA_WIDTH,
    "seq": MAX_SEQ_WIDTH,
}


def _schema_accepts(template: str) -> bool:
    """Schema accepts iff ``PatternColumn`` validates without raising."""
    try:
        PatternColumn(template=template)
        return True
    except ValueError:
        return False


def _engine_accepts(spark, template: str) -> bool:
    """Engine accepts iff ``build_pattern_column`` constructs the
    expression without raising.  The ``spark`` fixture is required
    because column construction calls ``F.lit`` which needs a live
    SparkContext, even though we never materialise the column."""
    _ = spark  # fixture only consumed for its side effect of an active SparkSession
    try:
        build_pattern_column(F.lit(0), column_seed=42, template=template)
        return True
    except ValueError:
        return False


@pytest.mark.parametrize("kind, cap", list(CAPS.items()))
def test_at_cap_both_accept(spark, kind, cap):
    """Boundary: ``N == cap`` is admitted by both sides."""
    template = "X-{" + f"{kind}:{cap}" + "}-Y"
    assert _schema_accepts(template), f"schema rejected {template} at cap"
    assert _engine_accepts(spark, template), f"engine rejected {template} at cap"


@pytest.mark.parametrize("kind, cap", list(CAPS.items()))
def test_above_cap_both_reject(spark, kind, cap):
    """Boundary: ``N == cap + 1`` is rejected by both sides."""
    template = "X-{" + f"{kind}:{cap + 1}" + "}-Y"
    assert not _schema_accepts(template), f"schema accepted over-cap {template}"
    assert not _engine_accepts(spark, template), f"engine accepted over-cap {template}"


@pytest.mark.parametrize("kind", list(CAPS.keys()))
def test_typical_widths_both_accept(spark, kind):
    """Typical widths (1, 4, 8) are admitted on both sides for every kind."""
    for width in (1, 4, 8):
        template = "X-{" + f"{kind}:{width}" + "}-Y"
        assert _schema_accepts(template), f"schema rejected typical {template}"
        assert _engine_accepts(spark, template), f"engine rejected typical {template}"
