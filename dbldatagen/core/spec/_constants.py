"""Constants shared between the spec layer and the engine.

Promoting these to a single source of truth prevents schema/engine
drift -- a width cap that's tight on one side and loose on the other
turns a plan-time validator into a load-bearing lie (schema accepts
a template the engine rejects at materialisation).  Both sides
import from this module; changing a cap is a one-line edit.

Width caps for ``PatternColumn`` placeholders:

* ``MAX_DIGIT_WIDTH`` -- the engine computes ``pmod(seed, 10**width)``
  and the result must fit in signed int64.  ``10**18`` fits;
  ``10**19`` overflows.
* ``MAX_HEX_WIDTH`` -- same int64 constraint with base 16.  ``16**15``
  fits; ``16**16`` overflows.
* ``MAX_ALPHA_WIDTH`` -- not int64-bound (alpha generation maps each
  position independently); the cap exists to bound emitted string
  length and per-row Catalyst plan size.  64 covers every realistic
  use; larger plans should use a UDF.
* ``MAX_SEQ_WIDTH`` -- bounds the leading-zero pad width of
  ``{seq:N}``.  24 already covers a 1-trillion-row table's sequence
  number without padding; larger is gratuitous.

``PLACEHOLDER_RE`` is the shared pattern parser.  Schema-side
validation walks it to enforce width caps; engine-side
``build_pattern_column`` walks the same regex to dispatch to per-kind
generators.  One regex, no drift.  Named groups (``kind``, ``width``)
keep call sites readable.
"""

import re


MAX_DIGIT_WIDTH = 18
MAX_HEX_WIDTH = 15
MAX_ALPHA_WIDTH = 64
MAX_SEQ_WIDTH = 24

# Default precision / scale for ``dtype=DECIMAL`` when the user
# omits them.  Matches Spark's ``DecimalType()`` default in the
# unset case.  Used by ``engine/columns/numeric.py`` at
# materialisation and referenced in the schema docstring so the
# value is documented in one place.
DEFAULT_DECIMAL_PRECISION = 10
DEFAULT_DECIMAL_SCALE = 0

PLACEHOLDER_RE = re.compile(r"\{(?P<kind>seq|uuid|digit|alpha|hex):?(?P<width>\d+)?\}")
