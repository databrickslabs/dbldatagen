"""Adversarial-input tests for ``PatternColumn`` validators.

Width-cap tests double as a documentation of the schema/engine
contract: ``test_pattern_width_parity.py`` pins that the engine
rejects exactly the templates the schema rejects.
"""

from __future__ import annotations

import pytest

from dbldatagen.core.spec._constants import (
    MAX_ALPHA_WIDTH,
    MAX_DIGIT_WIDTH,
    MAX_HEX_WIDTH,
    MAX_SEQ_WIDTH,
)
from dbldatagen.core.spec.schema import PatternColumn


@pytest.mark.parametrize(
    "bad_template, err_substring",
    [
        # Field(min_length=1) catches empty before our validator runs.
        ("", "at least 1"),
        # Whitespace-only must be rejected too; min_length=1 admits "   ".
        ("   ", "whitespace-only"),
        ("\t\n  ", "whitespace-only"),
        # No placeholder at all == effectively a ConstantColumn.
        ("hello", "no placeholders"),
        ("ORD-1234-XYZ", "no placeholders"),
    ],
)
def test_pattern_template_rejects_malformed(bad_template, err_substring):
    with pytest.raises(ValueError, match=err_substring):
        PatternColumn(template=bad_template)


@pytest.mark.parametrize(
    "kind, cap",
    [
        ("digit", MAX_DIGIT_WIDTH),
        ("hex", MAX_HEX_WIDTH),
        ("alpha", MAX_ALPHA_WIDTH),
        ("seq", MAX_SEQ_WIDTH),
    ],
)
def test_pattern_template_rejects_width_cap_violations(kind, cap):
    """Any ``{kind:N}`` placeholder with N > cap must be rejected at
    plan time.  The cap value comes from
    ``dbldatagen/core/spec/_constants.py`` so this test stays in
    sync with the engine.
    """
    template = "X-{" + f"{kind}:{cap + 1}" + "}-Y"
    with pytest.raises(ValueError, match="width-cap"):
        PatternColumn(template=template)


@pytest.mark.parametrize(
    "kind, cap",
    [
        ("digit", MAX_DIGIT_WIDTH),
        ("hex", MAX_HEX_WIDTH),
        ("alpha", MAX_ALPHA_WIDTH),
        ("seq", MAX_SEQ_WIDTH),
    ],
)
def test_pattern_template_accepts_width_cap_boundary(kind, cap):
    """``N == cap`` must be accepted (inclusive cap)."""
    template = "X-{" + f"{kind}:{cap}" + "}-Y"
    pc = PatternColumn(template=template)
    assert pc.template == template


def test_pattern_template_rejects_uuid_width_modifier():
    """``{uuid:N}`` is rejected -- UUIDs are always 36 chars.  The
    engine has rejected this since day one; the schema validator now
    matches so the accept-sets agree (see test_pattern_width_parity).
    """
    with pytest.raises(ValueError, match="uuid takes no width modifier"):
        PatternColumn(template="X-{uuid:5}-Y")


@pytest.mark.parametrize("kind", ["digit", "hex", "alpha", "seq"])
def test_pattern_template_rejects_zero_width(kind):
    """Width-0 placeholders silently emit empty strings (``F.lpad("0",
    0, "0")`` -> "", ``pmod(seed, base**0) = 0``).  Reject at plan
    time -- almost certainly a user typo."""
    template = "X-{" + f"{kind}:0" + "}-Y"
    with pytest.raises(ValueError, match="width must be >= 1"):
        PatternColumn(template=template)


def test_pattern_template_accepts_placeholder_only():
    """A template that is *just* a placeholder is valid -- not common
    but not malformed either."""
    pc = PatternColumn(template="{uuid}")
    assert pc.template == "{uuid}"


def test_pattern_template_accepts_compound_template():
    """Realistic templates with multiple placeholders and literals."""
    pc = PatternColumn(template="ORD-{digit:4}-{alpha:3}")
    assert pc.template == "ORD-{digit:4}-{alpha:3}"
