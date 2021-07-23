# Generating data that conforms to a known statistical distribution

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
> per use of the `random` option

### Examples 

In the following example (taken from the section on date ranges), we will simulate returns and 
ensure the return date is after the purchase date.

Here we specify an explicit date range and add a random number of days for the return.

However, unlike the example in the date range section, we will use a specific distribution to 
make returns more frequent in the period immediately following the purchase.

```python 
from pyspark.sql.types import IntegerType

import dbldatagen as dg

row_count = 1000 * 100
testDataSpec = (dg.DataGenerator(spark, name="test_data_set1", rows=row_count,
                                 partitions=4, randomSeedMethod='hash_fieldname',
                                 verbose=True)
                .withColumn("purchase_id", IntegerType(), minValue=1000000, maxValue=2000000)
                .withColumn("product_code", IntegerType(), uniqueValues=10000, random=True)
                .withColumn("purchase_date", "date",
                            data_range=dg.DateRange("2017-10-01 00:00:00",
                                                    "2018-10-06 11:55:00",
                                                    "days=3"),
                            random=True)
                .withColumn("return_date", "date",
                    expr="date_add('purchase_date', cast(floor(rand() * 100 + 1) as int))")

                )

dfTestData = testDataSpec.build()
```

Here we use a computed column, `return_delay`, for effect only. By specifying `omit=True`, 
it is omitted from the final data set.
