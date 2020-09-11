# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import date, datetime, timedelta, timezone
import math
from .datarange import DataRange

from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType, ByteType


class DateRange(DataRange):
    """Class to represent Date range

    The date range will represented internally using `datetime` for `start` and `end`, and `timedelta` for `interval`

    When computing ranges for purposes of the sequences, the maximum value will be adjusted to the nearest whole multiple
    of the interval that is before the `end` value.

    When converting from a string, datetime is assumed to use local timezone unless specified as part of the format
    in keeping with the python `datetime` handling of datetime instances that do not specify a timezone

    :param begin: start of date range as python `datetime` object. If specified as string, converted to datetime
    :param end: end of date range as python `datetime` object. If specified as string, converted to datetime
    :param interval: interval of date range as python `timedelta` object.
    Note parsing format for interval uses standard timedelta parsing not the `datetime_format` string
    :param datetime_format: format for conversion of strings to datetime objects
    """
    DEFAULT_UTC_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
    DEFAULT_DATE_FORMAT = "%Y-%m-%d"

    # todo: deduce format from begin and end params

    def __init__(self, begin, end, interval=None, datetime_format=DEFAULT_UTC_TS_FORMAT):
        assert begin is not None
        assert end is not None

        self.begin = begin if not isinstance(begin, str) else self._datetime_from_string(begin, datetime_format)
        self.end = end if not isinstance(end, str) else self._datetime_from_string(end, datetime_format)
        self.interval = interval if not isinstance(interval, str) else self._timedelta_from_string(interval)

        self.min = self.begin.timestamp()

        self.max = (self.min + self.interval.total_seconds()
                    * self.computeTimestampIntervals(self.begin, self.end, self.interval))
        self.step = self.interval.total_seconds()

    @classmethod
    def _datetime_from_string(cls, date_str, date_format):
        """convert string to Python DateTime object using format"""
        result = datetime.strptime(date_str, date_format)
        return result

    @classmethod
    def _timedelta_from_string(cls, interval):
        return timedelta(**cls.parseInterval(interval))

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
        """ create string representation of date range"""
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

    def adjustForColumnDatatype(self, ctype):
        """ adjust the range for the column output type"""
        pass

    def getDiscreteRange(self):
        """ Divide continuous range into discrete intervals"""
        return (self.max - self.min) * float(1.0 / self.step)
