# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the `NRange` class used to specify numeric data ranges.
"""

import math

from pyspark.sql.types import (
    ByteType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
)

from .datarange import DataRange
from .serialization import SerializableToDict


_OLD_MIN_OPTION = "min"
_OLD_MAX_OPTION = "max"


class NRange(DataRange):
    """Represents a numeric interval for data generation.

    The numeric range represents the interval `minValue .. maxValue` inclusive and can be
    used as an alternative to the `minValue`, `maxValue`, and `step` parameters passed to
    the `DataGenerator.withColumn` method. Specify by passing an instance of `NRange`
    to the `dataRange` parameter.

    For a decreasing sequence, use a negative `step` value.

    :param minValue: Minimum value of range (integer / long / float).
    :param maxValue: Maximum value of range (integer / long / float).
    :param step: Step value for range (integer / long / float).
    :param until: Upper bound for the range (i.e. `maxValue + 1`).

    You may only specify either `maxValue` or `until`, but not both. For backwards
    compatibility, the legacy `min` and `max` keyword arguments are still supported
    but `minValue` and `maxValue` are preferred.

    .. note::
       The `until` parameter is used to specify the upper bound for the range. It is
       equivalent to `maxValue + 1`. You may  specify either `maxValue` or `until`, but not both.

    .. note::
       The `step` parameter is used to specify the step value for the range. It is
       used to generate the range of values.

    .. note::
        For backwards compatibility, the legacy `min` and `max` keyword arguments are still
        supported. Using `minValue` and `maxValue` is strongly preferred.
    """

    minValue: float | int | None
    maxValue: float | int | None
    step: float | int | None

    def __init__(
        self,
        minValue: int | float | None = None,
        maxValue: int | float | None = None,
        step: int | float | None = None,
        until: int | float | None = None,
        **kwargs: object,
    ) -> None:
        """Initializes a numeric range.

        :param minValue: Minimum value of range (integer / long / float).
        :param maxValue: Maximum value of range (integer / long / float).
        :param step: Step value for range (integer / long / float).
        :param until: Upper bound for the range (i.e. `maxValue + 1`).

        You may only specify either `maxValue` or `until`, but not both. For backwards
        compatibility, the legacy `min` and `max` keyword arguments are still supported
        but `minValue` and `maxValue` are preferred.
        """
        # Handle older form of `minValue` and `maxValue` arguments (`min`/`max`) if used.
        if _OLD_MIN_OPTION in kwargs:
            if minValue is not None:
                raise ValueError("Only one of 'minValue' and legacy 'min' may be specified")
            if not isinstance(kwargs[_OLD_MIN_OPTION], int | float):
                raise ValueError("Legacy 'min' argument must be an integer or float.")
            self.minValue = kwargs[_OLD_MIN_OPTION]  # type: ignore
        else:
            self.minValue = minValue

        if _OLD_MAX_OPTION in kwargs:
            if maxValue is not None:
                raise ValueError("Only one of 'maxValue' and legacy 'max' may be specified")
            if not isinstance(kwargs[_OLD_MAX_OPTION], int | float):
                raise ValueError("Legacy 'max' argument must be an integer or float.")
            self.maxValue = kwargs[_OLD_MAX_OPTION]  # type: ignore
        else:
            self.maxValue = maxValue

        unsupported_kwargs = kwargs.keys() - {_OLD_MIN_OPTION, _OLD_MAX_OPTION}
        if len(unsupported_kwargs) > 0:
            unexpected = ", ".join(sorted(unsupported_kwargs))
            raise ValueError(f"Unexpected keyword arguments for NRange: {unexpected}")

        if self.maxValue is not None and until is not None:
            raise ValueError("Only one of 'maxValue' or 'until' may be specified.")

        if until is not None:
            self.maxValue = until + 1

        self.step = step

    def _toInitializationDict(self) -> dict[str, object]:
        """Convert this `NRange` instance to a dictionary of constructor arguments.

        :return: Dictionary representation of the object.
        """
        _options: dict[str, object] = {
            "kind": self.__class__.__name__,
            "minValue": self.minValue,
            "maxValue": self.maxValue,
            "step": self.step,
        }
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    def __str__(self) -> str:
        """Return a string representation of the numeric range."""
        return f"NRange({self.minValue}, {self.maxValue}, {self.step})"

    def isEmpty(self) -> bool:
        """Check if the range is empty.

        An `NRange` is considered empty if all of `minValue`, `maxValue`, and `step` are `None`.

        :return: `True` if empty, `False` otherwise.
        """
        return self.minValue is None and self.maxValue is None and self.step is None

    def isFullyPopulated(self) -> bool:
        """Check if all range attributes are populated.

        :return: `True` if `minValue`, `maxValue`, and `step` are all not `None`,
            `False` otherwise.
        """
        return self.minValue is not None and self.maxValue is not None and self.step is not None

    def adjustForColumnDatatype(self, ctype: DataType) -> None:
        """Adjust default values for the specified Spark SQL column data type.

        This will:

        - Populate `minValue` and `maxValue` to the default range for the data type
          if they are not already set.
        - Validate that `maxValue` is within the allowed range for `ByteType` and
          `ShortType`.
        - Set a default `step` of 1.0 for floating point types and 1 for integral types
          if `step` is not already set.

        :param ctype: Spark SQL data type for the column.
        """
        numeric_types = (DecimalType, FloatType, DoubleType, ByteType, ShortType, IntegerType, LongType)

        if isinstance(ctype, numeric_types):
            numeric_range = NRange._getNumericDataTypeRange(ctype)
            if numeric_range is not None:
                if self.minValue is None:
                    self.minValue = numeric_range[0]
                if self.maxValue is None:
                    self.maxValue = numeric_range[1]

        if isinstance(ctype, ShortType) and self.maxValue is not None:
            if self.maxValue > 65536:
                raise ValueError("`maxValue` must be within the valid range for ShortType.")

        if isinstance(ctype, ByteType) and self.maxValue is not None:
            if self.maxValue > 256:
                raise ValueError("`maxValue` must be within the valid range (0 - 256) for ByteType.")

        if isinstance(ctype, (DoubleType, FloatType)) and self.step is None:
            self.step = 1.0

        if isinstance(ctype, (ByteType, ShortType, IntegerType, LongType)) and self.step is None:
            self.step = 1

    def getDiscreteRange(self) -> float:
        """Convert range to a discrete range size.

        This is the number of discrete values in the range. For example, `NRange(1, 5, 0.5)`
        has 8 discrete values.

        :return: Number of discrete values in the range.
        :raises ValueError: If the range is not fully specified or `step` is zero.

        .. note::
           A range of `NRange(0, 4, 0.5)` has 8 discrete values, not 9, as the `maxValue`
           value itself is not part of the range.
        """
        if self.minValue is None or self.maxValue is None or self.step is None:
            raise ValueError("Range must have 'minValue', 'maxValue', and 'step' defined.")

        if self.step == 0:
            raise ValueError("Parameter 'step' must be non-zero when computing discrete range.")

        if isinstance(self.minValue, int) and isinstance(self.maxValue, int) and self.step == 1:
            return float(self.maxValue - self.minValue)

        # when any component is a float, we will return a float for the discrete range
        # to simplify computations
        return float(math.floor((self.maxValue - self.minValue) * float(1.0 / self.step)))

    def getContinuousRange(self) -> float:
        """Convert range to continuous range.

        :return: Float value for the size of the interval from `minValue` to `maxValue`.
        :raises ValueError: If `minValue` or `maxValue` is not defined.
        """
        if self.minValue is None or self.maxValue is None:
            raise ValueError("Range must have 'minValue' and 'maxValue' defined.")

        return (self.maxValue - self.minValue) * 1.0

    def getScale(self) -> int:
        """Get the maximum scale (number of decimal places) of the range components.

        :return: Maximum scale across `minValue`, `maxValue`, and `step`.
        """
        smin = 0
        smax = 0
        sstep = 0

        if self.minValue is not None:
            smin = self._precision_and_scale(self.minValue)[1]
        if self.maxValue is not None:
            smax = self._precision_and_scale(self.maxValue)[1]
        if self.step is not None:
            sstep = self._precision_and_scale(self.step)[1]

        # return maximum scale of components
        return max(smin, smax, sstep)

    @staticmethod
    def _precision_and_scale(x: float | int) -> tuple[int, int]:
        """Compute precision and scale for a numeric value.

        :param x: Numeric value for which to compute precision and scale.
        :return: Tuple of `(precision, scale)`.
        """
        max_digits = 14
        int_part = int(abs(x))
        magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
        if magnitude >= max_digits:
            return magnitude, 0

        frac_part = abs(x) - int_part
        multiplier = 10 ** (max_digits - magnitude)
        frac_digits = multiplier + int(multiplier * frac_part + 0.5)
        while frac_digits % 10 == 0:
            frac_digits //= 10
        scale = int(math.log10(frac_digits))
        return magnitude + scale, scale

    @staticmethod
    def _getNumericDataTypeRange(ctype: DataType) -> tuple[float | int, float | int] | None:
        """Get the default numeric range for the specified Spark SQL data type.

        :param ctype: Spark SQL data type.
        :return: Tuple of `(minValue, maxValue)` for the type, or `None` if not supported.
        """
        value_ranges: dict[type, tuple[float | int, float | int]] = {
            ByteType: (0, (2**4 - 1)),
            ShortType: (0, (2**8 - 1)),
            IntegerType: (0, (2**16 - 1)),
            LongType: (0, (2**32 - 1)),
            FloatType: (0.0, 3.402e38),
            DoubleType: (0.0, 1.79769e308),
        }
        if isinstance(ctype, DecimalType):
            return 0.0, math.pow(10, ctype.precision - ctype.scale) - 1.0
        return value_ranges.get(type(ctype), None)
