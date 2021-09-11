# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the `NRange` class used to specify data ranges
"""

import math

from pyspark.sql.types import LongType, FloatType, IntegerType, DoubleType, ShortType, \
    ByteType

from .datarange import DataRange

_OLD_MIN_OPTION = 'min'
_OLD_MAX_OPTION = 'max'


class NRange(DataRange):
    """ Ranged numeric interval representing the interval minValue .. maxValue inclusive

    A ranged object can be uses as an alternative to the `minValue`, `maxValue`, `step` parameters
    to the DataGenerator `withColumn` and `withColumn` objects.
    Specify by passing an instance of `NRange` to the `dataRange` parameter.

    :param minValue: Minimum value of range. May be integer / long / float
    :param maxValue: Maximum value of range. May be integer / long / float
    :param step: Step value for range. May be integer / long / float
    :param until: Upper bound for range ( i.e maxValue+1)

    You may only specify a `maxValue` or `until` value not both.

    For a decreasing sequence, use a negative step value.
    """

    def __init__(self, minValue=None, maxValue=None, step=None, until=None, **kwArgs):
        # check if older form of `minValue` and `maxValue` are used, and if so
        if _OLD_MIN_OPTION in kwArgs:
            assert minValue is None, \
                "Only one of `minValue` and `minValue` can be specified. Use of `minValue` is preferred"
            self.minValue = kwArgs[_OLD_MIN_OPTION]
            kwArgs.pop(_OLD_MIN_OPTION, None)
        else:
            self.minValue = minValue

        if _OLD_MAX_OPTION in kwArgs:
            assert maxValue is None, \
                "Only one of `maxValue` and `maxValue` can be specified. Use of `maxValue` is preferred"
            self.maxValue = kwArgs[_OLD_MAX_OPTION]
            kwArgs.pop(_OLD_MAX_OPTION, None)
        else:
            self.maxValue = maxValue
        assert len(kwArgs.keys()) == 0, "no keyword options other than `min` and `max` allowed"

        assert until is None if self.maxValue is not None else True, "Only one of maxValue or until can be specified"
        assert self.maxValue is None if until is not None else True, "Only one of maxValue or until can be specified"

        if until is not None:
            self.maxValue = until + 1
        self.step = step

    def __str__(self):
        return f"NRange({self.minValue}, {self.maxValue}, {self.step})"

    def isEmpty(self):
        """Check if object is empty (i.e all instance vars of note are `None`

        :returns: `True` if empty, `False` otherwise
        """
        return self.minValue is None and self.maxValue is None and self.step is None

    def isFullyPopulated(self):
        """Check is all instance vars are populated

        :returns: `True` if fully populated, `False` otherwise
        """
        return self.minValue is not None and self.maxValue is not None and self.step is not None

    def adjustForColumnDatatype(self, ctype):
        """ Adjust default values for column output type

        :param ctype: Spark SQL type instance to adjust range for
        :returns: No return value - executes for effect only
        """
        if ctype.typeName() == 'decimal':
            if self.minValue is None:
                self.minValue = 0.0
            if self.maxValue is None:
                self.maxValue = math.pow(10, ctype.precision - ctype.scale) - 1.0
            if self.step is None:
                self.step = 1.0

        if type(ctype) is ShortType and self.maxValue is not None:
            assert self.maxValue <= 65536, "`maxValue` must be in range of short"

        if type(ctype) is ByteType and self.maxValue is not None:
            assert self.maxValue <= 256, "`maxValue` must be in range of byte (0 - 256)"

        if (type(ctype) is DoubleType or type(ctype) is FloatType) and self.step is None:
            self.step = 1.0

        if (type(ctype) is ByteType
              or type(ctype) is ShortType
              or type(ctype) is IntegerType
              or type(ctype) is LongType) and self.step is None:
            self.step = 1

    def getDiscreteRange(self):
        """Convert range to discrete range

        :returns: number of discrete values in range. For example `NRange(1, 5, 0.5)` has 8 discrete values

        .. note::
           A range of 0,4, 0.5 has 8 discrete values not 9 as the `maxValue` value is not part of the range

        TODO: check range of values

        """
        if type(self.minValue) is int and type(self.maxValue) is int and self.step == 1:
            return self.maxValue - self.minValue
        else:
            # when any component is a float, we will return a float for the discrete range
            # to simplify computations
            return float(math.floor((self.maxValue - self.minValue) * float(1.0 / self.step)))

    def getContinuousRange(self):
        """Convert range to continuous range

        :returns: float value for size of interval from `minValue` to `maxValue`
        """
        return (self.maxValue - self.minValue) * float(1.0)

    def getScale(self):
        """Get scale of range"""
        smin, smax, sstep = 0, 0, 0

        if self.minValue is not None:
            smin = self._precision_and_scale(self.minValue)[1]
        if self.maxValue is not None:
            smax = self._precision_and_scale(self.maxValue)[1]
        if self.step is not None:
            sstep = self._precision_and_scale(self.step)[1]

        # return maximum scale of components
        return max(smin, smax, sstep)

    def _precision_and_scale(self, x):
        max_digits = 14
        int_part = int(abs(x))
        magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
        if magnitude >= max_digits:
            return (magnitude, 0)
        frac_part = abs(x) - int_part
        multiplier = 10 ** (max_digits - magnitude)
        frac_digits = multiplier + int(multiplier * frac_part + 0.5)
        while frac_digits % 10 == 0:
            frac_digits /= 10
        scale = int(math.log10(frac_digits))
        return (magnitude + scale, scale)
