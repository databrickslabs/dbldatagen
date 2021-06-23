# Understanding and using data distributions

When generating random data, the test data generator uses a uniform random number generator by default. 

The user can force generation of data to conform
By default, the data is only constrained to the range of the fields data type. 

Additionally, unless the `percent_nulls` option is used, the value `null` will not be generated for a field value.

The range of values for the generated data may be controlled in the following ways:
- Specifying the `unique_values` option to control the unique values for a column
- Specifying the `minValue`, `maxValue` and `step` options for a column
- Specifying the `begin`, `end` and `interval` options for data and timestamp valued columns
- Specifying an explicit set of values via the `values` option
- Using a specific range object for a column

> Each of the mechanisms for constraining the range of values determines the possible range of values
> for a given column. It does not guarantee that all values will be generated.

The set of actual values generated will be further constrained by the possible values of the underlying seed column. 
For example if a column is specified to have 10 possible values but the seed column only generates two values 
and random value generation is not used, then there will only be two values generated.

## Precedence rules for constraining the range of values

Some of the options can lead to conflicting information about the range of possible values. 
 
Our general philosophy is to try and generate data where possible - so if conflicting range constraints are specified, 
we may reduce the range

The following rules apply to any ranged data specifications:

- if the `unique_values` option is used, its combined with the any range options to produce the effective range. 
The number of unique_values is given highest priority, but the interval, start and end of the values tries to take 
into account any range options specified. If the range does not allow for sufficient unique values to be generated, 
the number of unique values is reduced. 
- if `values` are specified, the implied range is the set of values for column. Any other range options are ignored
- If both a range object is specified and any of the options for `begin`, `end` , `interval` are specified, 
the relevant value in the range object is used
- If both a range object is specified and any of the options for `minValue`, `maxValue` , `step` are specified, 
the relevant value in the range object is used
- if the range of values conflicts with the underlying data type, the data type values take precedence. For example 
a Boolean field with the range 1 .. 9 will be rescaled to the range 0 .. 1 and still produce both `True` and `False` 
values

### Handling of dates and timestamps

For dates and timestamps, if a number of unique values is specified, these will be generated starting from the start 
date time and incremented according to the interval. So if a date ranges is specified for a year with an interval of 7 
days but only 10 unique values, the max value will be the starting date or time + 10 weeks

If the date range is specified and no interval is specified, but a number of unique values is specified, then 
the interval will be computed to evenly space the values if possible.

If no interval is specified, the default interval of 1 day will be used for dates and 1 minute will be used for 
timestamps unless other criteria force a different interval criteria.

As part of the overall goals for the test data generator is to be able to generate repeatable data sets, 
if no starting datetime is specified for date time ranges, we will use the first day of the previous year as 
the starting date. At the time of writing, this will be 2020/1/1

If no start date and no end date is specified, then we will use a default end date of the last day of the previous year.
At the time of writing, this will be 2020/12/31

if starting and ending dates are specified, we will not produce dates or timestamps outside of these, but the number of 
unique values may be reduced, if there are insufficient values in the range.


### Recommendations

While we will try to generate data where possible, for dates and times, we recommend explicitly specifying a 
start and end date time. 

