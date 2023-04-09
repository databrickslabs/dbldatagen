# Understanding and Using Data Ranges

The data generator uses data ranges to constrain the values for generated data. 

By default, the data is only constrained to the range of the fields data type. 

Additionally, unless the `percentNulls` option is used, the value `null` will not be generated for a field value.

The range of values for the generated data may be controlled in the following ways:
- Specifying the `uniqueValues` option to control the unique values for a column
- Specifying the `minValue`, `maxValue` and `step` options for a column
- Specifying the `begin`, `end` and `interval` options for data and timestamp valued columns
- Specifying an explicit set of values via the `values` option
- Using a specific range object for a column

> Each of the mechanisms for constraining the range of values determines the possible range of values
> for a given column. It does not guarantee that all values will be generated.

The set of actual values generated will be further constrained by the possible values of the underlying seed column. 
For example if a column is specified to have 10 possible values but the seed column only generates two values 
and random value generation is not used, then there will only be two values generated.

Here is an example illustrating use of some of the range constraints 

```python 
import dbldatagen as dg
from pyspark.sql.types import IntegerType, StringType

row_count=1000 * 100
testDataSpec = (
   dg.DataGenerator(spark, name="test_data_set1", rows=row_count,
                    partitions=4, randomSeedMethod='hash_fieldname')
   .withIdOutput()
   .withColumn("purchase_id", IntegerType(), minValue=1000000, 
                maxValue=2000000, random=True)
   .withColumn("product_code", IntegerType(), uniqueValues=10000, random=True)
   .withColumn("in_stock", StringType(), values=['yes', 'no', 'unknown'])
   )

dfTestData = testDataSpec.build()
```

## Precedence rules for constraining the range of values

Some of the options can lead to conflicting information about the range of possible values. 
 
Our general philosophy is to try and generate data where possible - so if conflicting range constraints are specified, 
we may reduce the range

The following rules apply to any ranged data specifications:

- if the `uniqueValues` option is used, its combined with the any range options to produce the effective range. 
The number of unique values is given highest priority, but the interval, start and end of the values tries to take 
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

As part of the overall goals for the Databricks Labs data generator is to be able to generate repeatable data sets, 
if no starting datetime is specified for date time ranges, we will use the first day of the previous year as 
the starting date. At the time of writing, this will be 2020/1/1

If no start date and no end date is specified, then we will use a default end date of the last day of the previous year.
At the time of writing, this will be 2020/12/31

if starting and ending dates are specified, we will not produce dates or timestamps outside of these, but the number of 
unique values may be reduced, if there are insufficient values in the range.

#### Examples
Here is an example illustrating use of simple date range constraints. In this case, we are only specifying that
 we will have 300 unique dates. Note that we are not making `purchase_id` random to ensure unique values for every row

```python 
import dbldatagen as dg
from pyspark.sql.types import IntegerType

row_count=1000 * 100
testDataSpec = (
      dg.DataGenerator(spark, name="test_data_set1", rows=row_count,
                       partitions=4, randomSeedMethod='hash_fieldname', 
                       verbose=True)
      .withColumn("purchase_id", IntegerType(), minValue=1000000, 
                     maxValue=2000000)
      .withColumn("product_code", IntegerType(), uniqueValues=10000, 
                     random=True)
      .withColumn("purchase_date", "date", uniqueValues=300, 
                     random=True)
      )

dfTestData = testDataSpec.build()
```

In the following example, we will simulate returns and ensure the return date is after the purchase date.

Here we specify an explicit date range and add a random number of days for the return.

```python 
import dbldatagen as dg
from pyspark.sql.types import IntegerType

row_count = 1000 * 100
testDataSpec = (
    dg.DataGenerator( spark, name="test_data_set1", rows=row_count, partitions=4, 
                      randomSeedMethod="hash_fieldname", verbose=True, )
    .withColumn("purchase_id", IntegerType(), minValue=1000000, maxValue=2000000)
    .withColumn("product_code", IntegerType(), uniqueValues=10000, random=True)
    .withColumn(
        "purchase_date",
        "date",
        data_range=dg.DateRange("2017-10-01 00:00:00", "2018-10-06 11:55:00", "days=3"),
        random=True,
    )
    .withColumn(
        "return_date",
        "date",
        expr="date_add(purchase_date, cast(floor(rand() * 100 + 1) as int))",
        baseColumn="purchase_date",
    )
)
dfTestData = testDataSpec.build()
```


### Recommendations

While we will try to generate data where possible, for dates and times, we recommend explicitly specifying a 
start and end date time. 

