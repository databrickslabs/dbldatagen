# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import date, datetime, timedelta
import math
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType, ByteType

class NRange(object):
    """ Ranged numeric interval representing the interval min .. max inclusive"""

    def __init__(self, min=None, max=None, step=None, until=None):
        assert until is None if max is not None else True,"Only one of max or until can be specified"
        assert max is None if until is not None else True,"Only one of max or until can be specified"
        self.min=min
        self.max=max if until is None else until+1
        self.step=step

    def __str__(self):
        return "NRange({}, {}, {})".format(self.min, self.max, self.step)

    def isEmpty(self):
        """Check if object is empty (i.e all instance vars of note are `None`"""
        return self.min is None and self.max is None and self.step is None

    def isFullyPopulated(self):
        """Check is all instance vars are populated"""
        return self.min is not None and self.max is not None and self.step is not None

    def _adjustForColtype(self, ctype):
        """ Adjust default values for column output type"""
        if ctype.typeName() == 'decimal':
            if self.min is None:
                self.min = 0.0
            if self.max is None:
                self.max = math.pow(10, ctype.precision - ctype.scale) - 1.0

        if type(ctype) is ShortType and self.max is not None:
            assert self.max <= 65536

        if type(ctype) is ByteType and self.max is not None:
            assert self.max <= 256

    def getDiscreteRange(self):
        """Convert range to discrete range"""
        if type(self.min) is int and type(self.max) is int and self.step == 1:
            return (self.max - self.min)
        else:
            return (self.max - self.min) * float(1.0 / self.step)

    def getContinuousRange(self):
        """Convert range to continuous range"""
        return (self.max - self.min) * float(1.0)



class DateRange(object):
    """Class to represent Date range"""

    def __init__(self, begin, end, interval=None, datetime_format="%Y-%m-%d %H:%M:%S"):
        assert begin is not None
        assert end is not None

        self.begin = begin if not isinstance(begin, str) else datetime.strptime(begin, datetime_format)
        self.end = end if not isinstance(end, str) else datetime.strptime(end, datetime_format)
        self.interval = interval if not isinstance(interval, str) else timedelta(
            **self.parseInterval(interval))

        self.min = (self.begin - datetime(1970, 1, 1)).total_seconds()

        self.max = (self.min +self.interval.total_seconds()
               * self.computeTimestampIntervals(self.begin, self.end, self.interval))
        self.step = self.interval.total_seconds()

    @classmethod
    def parseInterval(cls, interval_str):
        """Parse interval from string"""
        assert interval_str is not None
        results = []
        for kv in interval_str.split(","):
            key, value = kv.split('=')
            results.append("'{}':{}".format(key, value))

        return eval("{{ {}  }} ".format(",".join(results)))

    def __str__(self):
        return "DateRange({},{},{} == {}, {}, {})".format(self.begin, self.end, self.interval,
                                                          self.min, self.max, self.step)


    def computeTimestampIntervals(self, start, end, interval):
        """ Compute number of intervals between start and end date """
        assert type(start) is datetime, "Expecting start as type datetime.datetime"
        assert type(end) is datetime, "Expecting end as type datetime.datetime"
        assert type(interval) is timedelta, "Expecting interval as type datetime.timedelta"
        i1 = end - start
        ni1 = i1 / interval
        return math.floor(ni1)

    def isFullyPopulated(self):
        """Check if min, max and step are specified """
        return self.min is not None and self.max is not None and self.step is not None

    def _adjustForColtype(self, ctype):
        """ adjust the range for the column output type"""
        pass

    def getDiscreteRange(self):
        """ Divide continuous range into discrete intervals"""
        return (self.max - self.min) * float(1.0 / self.step)

