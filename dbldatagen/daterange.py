# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the DataRange abstract class.
"""

import datetime
import math

from pyspark.sql.types import DataType

from dbldatagen.datarange import DataRange
from dbldatagen.serialization import SerializableToDict
from dbldatagen.utils import parse_time_interval


class DateRange(DataRange):
    """Represents a date range. The date range will be represented internally using `datetime` for
    `begin` and `end`, and `timedelta` for `interval`.

    When computing ranges for purposes of the sequences, the maximum value will be adjusted to the
    nearest whole multiple of the interval that is before the `end` value.

    When converting from a string, datetime is assumed to use local timezone unless specified as part of the format
    in keeping with the python `datetime` handling of datetime instances that do not specify a timezone.

    :param begin: Start of the date range as a `datetime.datetime` or `str`
    :param end: End of the date range as a `datetime.datetime` or `str`
    :param interval: Date range increment as a `datetime.timedelta` or `str`; Parsing format for interval uses
        standard timedelta parsing not the `datetime_format` string. If not specified, defaults to 1 day.
    :param datetime_format: String format for converting strings to `datetime.datetime`
    """

    DEFAULT_UTC_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
    DEFAULT_DATE_FORMAT = "%Y-%m-%d"
    DEFAULT_START_TIMESTAMP = datetime.datetime(year=datetime.datetime.now().year - 1, month=1, day=1)
    DEFAULT_END_TIMESTAMP = datetime.datetime(
        year=datetime.datetime.now().year - 1, month=12, day=31, hour=23, minute=59, second=59
    )
    DEFAULT_START_DATE = DEFAULT_START_TIMESTAMP.date()
    DEFAULT_END_DATE = DEFAULT_END_TIMESTAMP.date()
    DEFAULT_START_DATE_TIMESTAMP = DEFAULT_START_TIMESTAMP.replace(hour=0, minute=0, second=0)
    DEFAULT_END_DATE_TIMESTAMP = DEFAULT_END_TIMESTAMP.replace(hour=0, minute=0, second=0)

    begin: datetime.datetime
    end: datetime.datetime
    interval: datetime.timedelta
    minValue: float
    maxValue: float
    step: float

    def __init__(
        self,
        begin: str | datetime.datetime,
        end: str | datetime.datetime,
        interval: str | datetime.timedelta,
        datetime_format: str = DEFAULT_UTC_TS_FORMAT,
    ) -> None:

        self.datetime_format = datetime_format
        self.begin = begin if not isinstance(begin, str) else self._datetime_from_string(begin, datetime_format)
        self.end = end if not isinstance(end, str) else self._datetime_from_string(end, datetime_format)
        self.interval = interval if not isinstance(interval, str) else self._timedelta_from_string(interval)

        self.minValue = self.begin.timestamp()
        self.maxValue = self.minValue + self.interval.total_seconds() * self.computeTimestampIntervals(
            self.begin, self.end, self.interval
        )
        self.step = self.interval.total_seconds()

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {
            "kind": self.__class__.__name__,
            "begin": datetime.datetime.strftime(self.begin, self.datetime_format),
            "end": datetime.datetime.strftime(self.end, self.datetime_format),
            "interval": f"INTERVAL {int(self.interval.total_seconds())} SECONDS",
            "datetime_format": self.datetime_format,
        }
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    @classmethod
    def _datetime_from_string(cls, date_str: str, date_format: str) -> datetime.datetime:
        """Converts a string to a `datetime.datetime` object using the specified format.

        :param date_str: String to convert to `datetime.datetime`
        :param date_format: Format string for conversion
        :return: `datetime.datetime` object
        """
        result = datetime.datetime.strptime(date_str, date_format)
        return result

    @classmethod
    def _timedelta_from_string(cls, interval: str | None) -> datetime.timedelta:
        """Converts a string to a `datetime.timedelta` object using the specified format.

        :param interval: String to convert to `datetime.timedelta`
        :return: `datetime.timedelta` object
        """
        return cls.parseInterval(interval)

    @classmethod
    def parseInterval(cls, interval_str: str | None) -> datetime.timedelta:
        """Parse interval from string"""
        if interval_str is None:
            raise ValueError("Parameter 'interval_str' must be specified")
        return parse_time_interval(interval_str)

    @classmethod
    def _getDateTime(
        cls, dt: str | datetime.datetime | None, datetime_format: str, default_value: datetime.datetime
    ) -> datetime.datetime:
        """Gets a `datetime.datetime` object from the specified string, datetime object, or default value.

        :param dt: String to convert to `datetime.datetime`
        :param datetime_format: Format string for conversion
        :param default_value: Default value to return if `dt` is None
        :return: `datetime.datetime` object
        """
        if isinstance(dt, str):
            effective_dt = cls._datetime_from_string(dt, datetime_format)
        elif dt is None:
            effective_dt = default_value
        else:
            effective_dt = dt
        return effective_dt

    @classmethod
    def _getInterval(
        cls, interval: str | datetime.timedelta | None, default_value: datetime.timedelta
    ) -> datetime.timedelta:
        """Gets a `datetime.timedelta` object from the specified string, timedelta object, or default value.

        :param interval: String to convert to `datetime.timedelta`
        :param default_value: Default value to return if `interval` is None
        :return: `datetime.timedelta` object
        """
        if isinstance(interval, str):
            effective_interval = parse_time_interval(interval)
        elif interval is None:
            effective_interval = default_value
        else:
            effective_interval = interval
        return effective_interval

    @classmethod
    def computeDateRange(
        cls,
        begin: datetime.datetime | None,
        end: datetime.datetime | None,
        interval: str | datetime.timedelta | None,
        unique_values: int | None,
    ) -> "DateRange":
        """Computes a date range from the specified begin, end, interval, and unique values.

        :param begin: Start of the date range as a `datetime.datetime` or `str`
        :param end: End of the date range as a `datetime.datetime` or `str`
        :param interval: Date range increment as a `datetime.timedelta` or `str`
        :param unique_values: Number of unique values to generate
        :return: `DateRange` object
        """
        effective_interval = cls._getInterval(interval, datetime.timedelta(days=1))
        effective_end = cls._getDateTime(end, DateRange.DEFAULT_DATE_FORMAT, cls.DEFAULT_END_DATE_TIMESTAMP)
        effective_begin = cls._getDateTime(begin, DateRange.DEFAULT_DATE_FORMAT, cls.DEFAULT_START_DATE_TIMESTAMP)

        if unique_values is not None:
            if unique_values < 1:
                raise ValueError("Parameter 'unique_values' must be a positive integer")

            effective_begin = effective_end - effective_interval * (unique_values - 1)

        result = DateRange(effective_begin, effective_end, effective_interval)
        return result

    @classmethod
    def computeTimestampRange(
        cls,
        begin: datetime.datetime | None,
        end: datetime.datetime | None,
        interval: str | datetime.timedelta | None,
        unique_values: int | None,
    ) -> "DateRange":
        """Computes a timestamp range from the specified begin, end, interval, and unique values.

        :param begin: Start of the timestamp range as a `datetime.datetime` or `str`
        :param end: End of the timestamp range as a `datetime.datetime` or `str`
        :param interval: Timestamp range increment as a `datetime.timedelta` or `str`
        :param unique_values: Number of unique values to generate
        :return: `DateRange` object
        """

        effective_interval = cls._getInterval(interval, datetime.timedelta(days=1))
        effective_end = cls._getDateTime(end, DateRange.DEFAULT_UTC_TS_FORMAT, cls.DEFAULT_END_TIMESTAMP)
        effective_begin = cls._getDateTime(begin, DateRange.DEFAULT_UTC_TS_FORMAT, cls.DEFAULT_START_TIMESTAMP)

        if unique_values is not None:
            if unique_values < 1:
                raise ValueError("Parameter 'unique_values' must be a positive integer")
            effective_begin = effective_end - effective_interval * (unique_values - 1)
        result = DateRange(effective_begin, effective_end, effective_interval)
        return result

    def __str__(self) -> str:
        """Creates a string representation of the date range.

        :return: String representation of the date range
        """
        return f"DateRange({self.begin},{self.end},{self.interval} == {self.minValue}, {self.maxValue}, {self.step})"

    def computeTimestampIntervals(
        self, start: datetime.datetime, end: datetime.datetime, interval: datetime.timedelta
    ) -> int:
        """Computes the number of intervals between the specified start and end dates.

        :param start: Start of the timestamp range as a `datetime.datetime`
        :param end: End of the timestamp range as a `datetime.datetime`
        :param interval: Timestamp range increment as a `datetime.timedelta`
        :return: Number of intervals between the start and end dates
        """
        i1 = end - start
        ni1 = i1 / interval
        return math.floor(ni1)

    def isFullyPopulated(self) -> bool:
        """Checks if the date range is fully populated.

        :return: True if the date range is fully populated, False otherwise
        """
        return self.minValue is not None and self.maxValue is not None and self.step is not None

    def adjustForColumnDatatype(self, ctype: DataType) -> None:
        """Adjusts the date range for the specified column output type.

        :param ctype: Spark SQL data type for column
        """
        pass

    def getDiscreteRange(self) -> float:
        """Gets the discrete range of the date range.

        :return: Discrete range object
        """
        return (self.maxValue - self.minValue) * float(1.0 / self.step)

    def isEmpty(self) -> bool:
        """Checks if the date range is empty.

        :return: True if the date range is empty, False otherwise
        """
        return False

    def getContinuousRange(self) -> float:
        """Gets the continuous range of the date range.

        :return: Continuous range as a float
        """
        return (self.maxValue - self.minValue) * 1.0

    def getScale(self) -> int:
        """Gets the scale of the date range.

        :return: Scale of the date range
        """
        return 0
