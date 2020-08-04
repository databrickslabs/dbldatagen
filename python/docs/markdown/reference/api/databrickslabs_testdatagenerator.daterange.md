# databrickslabs_testdatagenerator.daterange module

<!-- !! processed by numpydoc !! -->

### class DateRange(begin, end, interval=None, datetime_format='%Y-%m-%d %H:%M:%S')
Bases: `object`

Class to represent Date range

The date range will represented internally using datetime for start and end, and timedelta for interval

When computing ranges for purposes of the sequences, the maximum value will be adjusted to the nearest whole multiple
of the interval that is before the end value.

When converting from a string, datetime is assumed to use local timezone unless specified as part of the format
in keeping with the python datetime handling of datetime instances that do not specify a timezone


* **Parameters**

    
    * **begin** – start of date range as python datetime object. If specified as string, converted to datetime


    * **end** – end of date range as python datetime object. If specified as string, converted to datetime


    * **interval** – interval of date range as python timedelta object.


Note parsing format for interval uses standard timedelta parsing not the datetime_format string
:param datetime_format: format for conversion of strings to datetime objects

### Methods

| `computeTimestampIntervals`(start, end, interval)

 | Compute number of intervals between start and end date

 |
| `getDiscreteRange`()

                               | Divide continuous range into discrete intervals

                                                                                                                                                                             |
| `isFullyPopulated`()

                               | Check if min, max and step are specified

                                                                                                                                                                                    |
| `parseInterval`(interval_str)

                      | Parse interval from string

                                                                                                                                                                                                  |
<!-- !! processed by numpydoc !! -->

#### DEFAULT_UTC_FORMAT( = '%Y-%m-%d %H:%M:%S')

#### computeTimestampIntervals(start, end, interval)
Compute number of intervals between start and end date

<!-- !! processed by numpydoc !! -->

#### getDiscreteRange()
Divide continuous range into discrete intervals

<!-- !! processed by numpydoc !! -->

#### isFullyPopulated()
Check if min, max and step are specified

<!-- !! processed by numpydoc !! -->

#### classmethod parseInterval(interval_str)
Parse interval from string

<!-- !! processed by numpydoc !! -->
