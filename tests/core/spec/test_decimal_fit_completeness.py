"""Discriminated-union completeness test for ``validate_decimal_precision_scale``.

Per the ``dbldatagen-engine`` A2 rule: a validator that checks one
member of a discriminated union must check every member that can
produce the same class of violation, OR have a documented exemption.

This test parametrises over every ``ColumnStrategy`` member that
carries (or could produce) a numeric value and asserts the
DECIMAL-fit guard fires for the overflow case.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.spec.schema import (
    ColumnSpec,
    ConstantColumn,
    DataType,
    ExpressionColumn,
    RangeColumn,
    SequenceColumn,
    TableSpec,
    ValuesColumn,
)


@pytest.mark.parametrize(
    "gen_factory, descr",
    [
        # RangeColumn: pre-existing check.
        (lambda: RangeColumn(min=100_000, max=200_000), "RangeColumn upper bound"),
        # ValuesColumn: pre-existing check; pinned in completeness matrix.
        (lambda: ValuesColumn(values=[100_000]), "ValuesColumn entry"),
        # ConstantColumn: added in this round.
        (lambda: ConstantColumn(value=100_000), "ConstantColumn int"),
        (lambda: ConstantColumn(value=100_000.5), "ConstantColumn float"),
    ],
)
def test_decimal_fit_check_rejects_overflow_per_strategy(gen_factory, descr):
    """``decimal(5, 0)`` admits |value| <= 99_999.  Every numeric
    strategy with a value >= 100_000 must be rejected at plan time.
    """
    with pytest.raises(ValueError, match="does not fit in decimal"):
        ColumnSpec(
            name=f"x_{descr.replace(' ', '_')}",
            dtype=DataType.DECIMAL,
            gen=gen_factory(),
            precision=5,
            scale=0,
        )


def test_decimal_fit_check_rejects_sequence_overflow():
    """``SequenceColumn`` requires the owning ``TableSpec.rows`` to
    compute the final value; the check lives on
    ``TableSpec.validate_sequence_column_overflow`` (per the
    documented cross-cutting concern in ``validate_decimal_precision_scale``).
    Construct the failing TableSpec to confirm the row-aware path fires.
    """
    with pytest.raises(ValueError, match="does not fit"):
        TableSpec(
            name="t",
            rows=1_000_000,
            columns=[
                ColumnSpec(
                    name="id",
                    dtype=DataType.DECIMAL,
                    gen=SequenceColumn(start=1, step=1),
                    precision=5,
                    scale=0,
                )
            ],
        )


def test_decimal_fit_check_rejects_constant_nan_inf():
    """``abs(float('nan')) >= limit`` returns False, so a NaN/Inf
    ConstantColumn paired with DECIMAL silently bypassed the magnitude
    check.  ``_require_finite`` catches at the float boundary.
    """
    for bad_value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="is not finite"):
            ColumnSpec(
                name="x",
                dtype=DataType.DECIMAL,
                gen=ConstantColumn(value=bad_value),
                precision=5,
                scale=0,
            )


def test_decimal_fit_check_skips_constant_string_value():
    """``ConstantColumn(value="abc")`` paired with DECIMAL is a
    different misconfiguration (string-typed value into a numeric
    dtype) -- not a fit violation.  Document that the fit-check
    skips it silently by exercising the code path; if the skip
    breaks in a future refactor, this test fails.
    """
    # Plan-time validation must allow this construction (the
    # mismatch is left to engine cast, by design); fit-check must
    # not be the layer that catches a non-numeric value.
    spec = ColumnSpec(
        name="x",
        dtype=DataType.DECIMAL,
        gen=ConstantColumn(value="abc"),
        precision=5,
        scale=0,
    )
    assert spec.precision == 5


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dtype": DataType.DECIMAL, "precision": 11, "scale": 2},
        {"dtype": DataType.DECIMAL, "precision": 11},
        {"scale": 2},
        {"dtype": DataType.DOUBLE},
        {"dtype": DataType.DATE},
    ],
)
def test_declared_type_rejected_on_expression_column(kwargs):
    """An ``ExpressionColumn``'s type is always inferred from its SQL
    (the engine evaluates the expression as-is, no cast), so a declared
    ``dtype`` / ``precision`` / ``scale`` would be a silent no-op.
    ``validate_expression_column_type`` rejects all three -- the caller
    must cast inside the expression instead.
    """
    with pytest.raises(ValueError, match="cannot be set on an ExpressionColumn"):
        ColumnSpec(name="derived", gen=ExpressionColumn(expr="a * b"), **kwargs)


def test_bare_expression_column_is_allowed():
    """A bare ExpressionColumn (no declared type) is fine -- the
    rejection only fires when dtype/precision/scale are actually set."""
    spec = ColumnSpec(name="derived", gen=ExpressionColumn(expr="a * b"))
    assert spec.dtype is None and spec.precision is None and spec.scale is None
