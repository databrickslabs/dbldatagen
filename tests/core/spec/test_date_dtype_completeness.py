"""Discriminated-union completeness test for ``validate_date_dtype_strategy``.

Pairs ``dtype=DATE`` with every ``ColumnStrategy`` member that can
exist on a ``ColumnSpec`` and asserts the validator's behaviour:
fixed-type / composite strategies raise; carriers of legitimate
date values (Constant, Values, Expression, Timestamp, ForeignKey)
pass.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.spec.schema import (
    ArrayColumn,
    ColumnSpec,
    ConstantColumn,
    DataType,
    ExpressionColumn,
    FakerColumn,
    PatternColumn,
    RangeColumn,
    SequenceColumn,
    StructColumn,
    TimestampColumn,
    UUIDColumn,
    ValuesColumn,
)


def _int_field(name: str) -> ColumnSpec:
    return ColumnSpec(name=name, gen=RangeColumn(min=0, max=10))


REJECTED_STRATEGIES = [
    ("RangeColumn", lambda: RangeColumn(min=0, max=10)),
    ("PatternColumn", lambda: PatternColumn(template="{digit:4}")),
    ("SequenceColumn", lambda: SequenceColumn(start=1, step=1)),
    ("UUIDColumn", lambda: UUIDColumn()),
    ("FakerColumn", lambda: FakerColumn(provider="date_of_birth")),
    ("StructColumn", lambda: StructColumn(fields=[_int_field("inner")])),
    ("ArrayColumn", lambda: ArrayColumn(element=RangeColumn(min=0, max=10))),
]


@pytest.mark.parametrize("descr, gen_factory", REJECTED_STRATEGIES)
def test_date_dtype_rejects_incompatible_strategy(descr, gen_factory):
    """Every fixed-type / composite-type strategy must be rejected
    when paired with ``dtype=DATE``."""
    with pytest.raises(ValueError, match="dtype=DATE is not compatible"):
        ColumnSpec(name=f"x_{descr}", dtype=DataType.DATE, gen=gen_factory())


ACCEPTED_STRATEGIES = [
    ("TimestampColumn", lambda: TimestampColumn(start="2024-01-01", end="2024-12-31")),
    ("ConstantColumn", lambda: ConstantColumn(value="2024-01-15")),
    ("ValuesColumn", lambda: ValuesColumn(values=["2024-01-01", "2024-06-30"])),
    ("ExpressionColumn", lambda: ExpressionColumn(expr="CAST('2024-01-15' AS DATE)")),
]


@pytest.mark.parametrize("descr, gen_factory", ACCEPTED_STRATEGIES)
def test_date_dtype_accepts_date_carrying_strategies(descr, gen_factory):
    """Strategies that can legitimately carry a date value must
    pass.  These exist as the exemption baseline -- any future
    rejection added to the validator must also update this list,
    otherwise the parametrize will fail and remind the author."""
    spec = ColumnSpec(name=f"x_{descr}", dtype=DataType.DATE, gen=gen_factory())
    assert spec.dtype == DataType.DATE
