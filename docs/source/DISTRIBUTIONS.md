# Generating Data that Conforms to a Known Statistical Distribution

By the default, data that is being generated at random uses a uniform random number generator. 

Sometimes it is useful to generate data that conforms to a known distribution. 

While use of the `weights` option with discrete value lists can be used to introduce skew, 
this can be awkward to manage for large sets of values.

To enable this, we support use of known distributions for randomly generated data on any field.

When the field is not numeric, the underlying seed value will generated to conform to the 
known distribution before being converted 
to the appropriate type as per usual semantics. 

Note that the distribution will be scaled to the possible range of values

The following distributions are supported:
- normal or Gaussian distribution
- Beta distribution
- Gamma distribution
- Exponential distribution

> Note the `distribution` option will have no effect for values that are not randomly generated as
> per use of the `random` option.
> 
> For values generated randomly, continuous distributions can still be used with discrete values such as strings
> as the underlying random numbers used to select the appropriate discrete values will be drawn from the specified
> distribution. So, for discrete values, the frequency of occurrence of particular values should conform approximately
> to the underlying distribution.


### Examples 

In the following example (taken from the section on date ranges), we will simulate returns and 
ensure the return date is after the purchase date.

Here we specify an explicit date range and add a random number of days for the return.

However, unlike the example in the date range section, we will use a specific distribution to 
make returns more frequent in the period immediately following the purchase.

```python 
from pyspark.sql.types import IntegerType

import dbldatagen as dg
import dbldatagen.distributions as dist


row_count = 1000 * 100
testDataSpec = (
    dg.DataGenerator(spark, name="test_data_set1", rows=row_count)
    .withColumn("purchase_id", IntegerType(), minValue=1000000, maxValue=2000000)
    .withColumn("product_code", IntegerType(), uniqueValues=10000, random=True)
    .withColumn(
        "purchase_date",
        "date",
        data_range=dg.DateRange("2017-10-01 00:00:00", "2018-10-06 11:55:00", "days=3"),
        random=True,
    )
    # create return delay , favoring short delay times
    .withColumn(
        "return_delay",
        "int",
        minValue=1,
        maxValue=100,
        random=True,
        distribution=dist.Gamma(1.0, 2.0),
        omit=True,
    )
    .withColumn(
        "return_date",
        "date",
        expr="date_add(purchase_date, return_delay)",
        baseColumn=["purchase_date", "return_delay"],
    )
)

dfTestData = testDataSpec.build()
```

Here we use a computed column, `return_delay`, for effect only. By specifying `omit=True`, 
it is omitted from the final data set.

You can view the distribution of the return delays using the following code sample in the Databricks 
environment.

```python 
import pyspark.sql.functions as F
dfDelays = dfTestData.withColumn("delay", F.expr("datediff(return_date, purchase_date)"))

display(dfDelays)
```

Use the plot options to plot the delay as a bar chart.

Specify the key as `delay`, the values as `delay` and the aggregation as `COUNT` to see the data 
distribution.
