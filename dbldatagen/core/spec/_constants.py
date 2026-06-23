"""Constants shared by the spec layer and the engine.

Defines the per-placeholder width caps for `PatternColumn` (`MAX_DIGIT_WIDTH`,
`MAX_HEX_WIDTH`, `MAX_ALPHA_WIDTH`, `MAX_SEQ_WIDTH`), the default decimal
precision and scale, and `PLACEHOLDER_RE`, the regex used to parse pattern
templates.
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
