# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import date, datetime, timedelta, timezone
import math
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType, ByteType


class NRange(object):
    """ Ranged numeric interval representing the interval min .. max inclusive

    A ranged object can be uses as an alternative to the `min`, `max`, `step` parameters to the DataGenerator `withColumn` and `withColumn` objects.
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
        """Check if object is empty (i.e all instance vars of note are `None`"""
        return self.min is None and self.max is None and self.step is None

    def isFullyPopulated(self):
        """Check is all instance vars are populated"""
        return self.min is not None and self.max is not None and self.step is not None

    def adjustForColumnDatatype(self, ctype):
        """ Adjust default values for column output type"""
        if ctype.typeName() == 'decimal':
            if self.min is None:
                self.min = 0.0
            if self.max is None:
                self.max = math.pow(10, ctype.precision - ctype.scale) - 1.0
            if self.step is None:
                self.step = 1.0

        if type(ctype) is ShortType and self.max is not None:
            assert self.max <= 65536

        if type(ctype) is ByteType and self.max is not None:
            assert self.max <= 256

        if (type(ctype) is DoubleType
            or type(ctype) is FloatType) and self.step is None:
            self.step = 1.0

        if (type(ctype) is ByteType
            or type(ctype) is ShortType
            or type(ctype) is IntegerType
            or type(ctype) is LongType) and self.step is None:
            self.step = 1

    def getDiscreteRange(self):
        """Convert range to discrete range"""
        if type(self.min) is int and type(self.max) is int and self.step == 1:
            return self.max - self.min
        else:
            return (self.max - self.min) * float(1.0 / self.step)

    def getContinuousRange(self):
        """Convert range to continuous range"""
        return (self.max - self.min) * float(1.0)
