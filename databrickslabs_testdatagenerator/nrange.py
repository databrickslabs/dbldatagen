# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the `NRange` class used to specify data ranges
"""

import math
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType, ByteType
from .datarange import DataRange


class NRange(DataRange):
    """ Ranged numeric interval representing the interval min .. max inclusive

    A ranged object can be uses as an alternative to the `min`, `max`, `step` parameters
    to the DataGenerator `withColumn` and `withColumn` objects.
    Specify by passing an instance of `NRange` to the `data_range` parameter.

    :param min: Minimum value of range. May be integer / long / float
    :param max: Maximum value of range. May be integer / long / float
    :param step: Step value for range. May be integer / long / float
    :param until: Upper bound for range ( i.e max+1)

    You may only specify a `max` or `until` value not both.

    For a decreasing sequence, use a negative step value.
    """

    def __init__(self, min=None, max=None, step=None, until=None):
        assert until is None if max is not None else True, "Only one of max or until can be specified"
        assert max is None if until is not None else True, "Only one of max or until can be specified"
        self.min = min
        self.max = max if until is None else until + 1
        self.step = step

    def __str__(self):
        return "NRange({}, {}, {})".format(self.min, self.max, self.step)

    def isEmpty(self):
        """Check if object is empty (i.e all instance vars of note are `None`

        :returns: `True` if empty, `False` otherwise
        """
        return self.min is None and self.max is None and self.step is None

    def isFullyPopulated(self):
        """Check is all instance vars are populated

        :returns: `True` if fully populated, `False` otherwise
        """
        return self.min is not None and self.max is not None and self.step is not None

    def adjustForColumnDatatype(self, ctype):
        """ Adjust default values for column output type

        :param ctype: Spark SQL type instance to adjust range for
        :returns: No return value - executes for effect only
        """
        if ctype.typeName() == 'decimal':
            if self.min is None:
                self.min = 0.0
            if self.max is None:
                self.max = math.pow(10, ctype.precision - ctype.scale) - 1.0
            if self.step is None:
                self.step = 1.0

        if type(ctype) is ShortType and self.max is not None:
            assert self.max <= 65536, "`max` must be in range of short"

        if type(ctype) is ByteType and self.max is not None:
            assert self.max <= 256, "`max` must be in range of byte (0 - 256)"

        if (type(ctype) is DoubleType
            or type(ctype) is FloatType) and self.step is None:
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
           A range of 0,4, 0.5 has 8 discrete values not 9 as the `max` value is not part of the range

        TODO: check range of values

        """
        if type(self.min) is int and type(self.max) is int and self.step == 1:
            return self.max - self.min
        else:
            return (self.max - self.min) * float(1.0 / self.step)

    def getContinuousRange(self):
        """Convert range to continuous range

        :returns: float value for size of interval from `min` to `max`
        """
        return (self.max - self.min) * float(1.0)

    def getScale(self):
        """Get scale of range"""
        smin, smax, sstep = 0, 0, 0

        if self.min is not None:
            smin = self._precision_and_scale(self.min)[1]
        if self.max is not None:
            smax = self._precision_and_scale(self.max)[1]
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
