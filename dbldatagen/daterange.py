# See the License for the specific language governing permissions and
# limitations under the License.
#
import math
from datetime import datetime, timedelta

from .datarange import DataRange
from .utils import parse_time_interval


class DateRange(DataRange):
    """Class to represent Date range

    The date range will represented internally using `datetime` for `start` and `end`, and `timedelta` for `interval`

    When computing ranges for purposes of the sequences, the maximum value will be adjusted to the
    nearest whole multiple of the interval that is before the `end` value.

    When converting from a string, datetime is assumed to use local timezone unless specified as part of the format
    in keeping with the python `datetime` handling of datetime instances that do not specify a timezone

    :param begin: start of date range as python `datetime` object. If specified as string, converted to datetime
    :param end: end of date range as python `datetime` object. If specified as string, converted to datetime
    :param interval: interval of date range as python `timedelta` object.
                     Note parsing format for interval uses standard timedelta parsing not
                     the `datetime_format` string
    :param datetime_format: format for conversion of strings to datetime objects

    """

    DEFAULT_UTC_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
    DEFAULT_DATE_FORMAT = "%Y-%m-%d"

    DEFAULT_START_TIMESTAMP = datetime(year=datetime.now().year - 1, month=1, day=1)
    DEFAULT_END_TIMESTAMP = datetime(year=datetime.now().year - 1, month=12, day=31, hour=23, minute=59, second=59)
    DEFAULT_START_DATE = DEFAULT_START_TIMESTAMP.date()
    DEFAULT_END_DATE = DEFAULT_END_TIMESTAMP.date()
    DEFAULT_START_DATE_TIMESTAMP = DEFAULT_START_TIMESTAMP.replace(hour=0, minute=0, second=0)
    DEFAULT_END_DATE_TIMESTAMP = DEFAULT_END_TIMESTAMP.replace(hour=0, minute=0, second=0)

    # todo: deduce format from begin and end params

    def __init__(self, begin, end, interval=None, datetime_format=DEFAULT_UTC_TS_FORMAT):
        assert begin is not None, "`begin` must be specified"
        assert end is not None, "`end` must be specified"

        self.begin = begin if not isinstance(begin, str) else self._datetime_from_string(begin, datetime_format)
        self.end = end if not isinstance(end, str) else self._datetime_from_string(end, datetime_format)
        self.interval = interval if not isinstance(interval, str) else self._timedelta_from_string(interval)

        self.minValue = self.begin.timestamp()

        self.maxValue = (self.minValue + self.interval.total_seconds()
                         * self.computeTimestampIntervals(self.begin, self.end, self.interval))
        self.step = self.interval.total_seconds()

    @classmethod
    def _datetime_from_string(cls, date_str, date_format):
        """convert string to Python DateTime object using format"""
        result = datetime.strptime(date_str, date_format)
        return result

    @classmethod
    def _timedelta_from_string(cls, interval):
        return cls.parseInterval(interval)

    @classmethod
    def parseInterval(cls, interval_str):
        """Parse interval from string"""
        assert interval_str is not None, "`interval_str` must be specified"
        return parse_time_interval(interval_str)

    @classmethod
    def _getDateTime(cls, dt, datetime_format, default_value):
        if isinstance(dt, str):
            effective_dt = cls._datetime_from_string(dt, datetime_format)
        elif dt is None:
            effective_dt = default_value
        else:
            effective_dt = dt
        return effective_dt

    @classmethod
    def _getInterval(cls, interval, default_value):
        if isinstance(interval, str):
            effective_interval = parse_time_interval(interval)
        elif interval is None:
            effective_interval = default_value
        else:
            effective_interval = interval
        return effective_interval

    @classmethod
    def computeDateRange(cls, begin, end, interval, unique_values):
        effective_interval = cls._getInterval(interval, timedelta(days=1))
        effective_end = cls._getDateTime(end, DateRange.DEFAULT_DATE_FORMAT,
                                         cls.DEFAULT_END_DATE_TIMESTAMP)
        effective_begin = cls._getDateTime(begin, DateRange.DEFAULT_DATE_FORMAT, cls.DEFAULT_START_DATE_TIMESTAMP)

        if unique_values is not None:
            assert type(unique_values) is int, "unique_values must be integer"
            assert unique_values >= 1, "unique_values must be positive integer"

            effective_begin = effective_end - effective_interval * (unique_values - 1)

        result = DateRange(effective_begin, effective_end, effective_interval)
        return result

    @classmethod
    def computeTimestampRange(cls, begin, end, interval, unique_values):

        effective_interval = cls._getInterval(interval, timedelta(days=1))
        effective_end = cls._getDateTime(end, DateRange.DEFAULT_UTC_TS_FORMAT, cls.DEFAULT_END_TIMESTAMP)
        effective_begin = cls._getDateTime(begin, DateRange.DEFAULT_UTC_TS_FORMAT, cls.DEFAULT_START_TIMESTAMP)

        if unique_values is not None:
            assert type(unique_values) is int, "unique_values must be integer"
            assert unique_values >= 1, "unique_values must be positive integer"
            effective_begin = effective_end - effective_interval * (unique_values - 1)
        result = DateRange(effective_begin, effective_end, effective_interval)
        return result

    def __str__(self):
        """ create string representation of date range"""
        return f"DateRange({self.begin},{self.end},{self.interval} == {self.minValue}, {self.maxValue}, {self.step})"

    def computeTimestampIntervals(self, start, end, interval):
        """ Compute number of intervals between start and end date """
        assert type(start) is datetime, "Expecting start as type datetime.datetime"
        assert type(end) is datetime, "Expecting end as type datetime.datetime"
        assert type(interval) is timedelta, "Expecting interval as type datetime.timedelta"
        i1 = end - start
        ni1 = i1 / interval
        return math.floor(ni1)

    def isFullyPopulated(self):
        """Check if minValue, maxValue and step are specified """
        return self.minValue is not None and self.maxValue is not None and self.step is not None

    def adjustForColumnDatatype(self, ctype):
        """ adjust the range for the column output type

        :param ctype: Spark SQL data type for column

        """
        pass

    def getDiscreteRange(self):
        """ Divide continuous range into discrete intervals

            Note does not modify range object.

            :returns: range from minValue to maxValue

        """
        return (self.maxValue - self.minValue) * float(1.0 / self.step)

    def isEmpty(self):
        """Check if object is empty (i.e all instance vars of note are `None`)"""
        return self.begin is None and self.end is None and self.interval is None

    def getContinuousRange(self):
        """Convert range to continuous range"""
        return (self.maxValue - self.minValue) * float(1.0)

    def getScale(self):
        """Get scale of range"""
        return 0
