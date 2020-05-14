# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import date, datetime, timedelta
import math
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType, ByteType

class NRange(object):

    def __init__(self, min=None, max=None, step=None):
        self.min=min
        self.max=max
        self.step=step

    def __str__(self):
        return "NRange({}, {}, {})".format(self.min, self.max, self.step)

    def is_empty(self):
        return self.min is None and self.max is None and self.step is None

    def is_fully_populated(self):
        return self.min is not None and self.max is not None and self.step is not None

    def _adjust_for_coltype(self, ctype):
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
        if type(self.min) is int and type(self.max) is int and self.step == 1:
            return (self.max - self.min)
        else:
            return (self.max - self.min) * float(1.0 / self.step)

    def getContinuousRange(self):
        return (self.max - self.min) * float(1.0)



class DateRange(object):

    @classmethod
    def parseInterval(cls, interval_str):
        assert interval_str is not None
        results = []
        for kv in interval_str.split(","):
            key, value = kv.split('=')
            results.append("'{}':{}".format(key, value))

        return eval("{{ {}  }} ".format(",".join(results)))

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

    def is_fully_populated(self):
        return self.min is not None and self.max is not None and self.step is not None

    def _adjust_for_coltype(self, ctype):
        pass

    def getDiscreteRange(self):
        return (self.max - self.min) * float(1.0 / self.step)

