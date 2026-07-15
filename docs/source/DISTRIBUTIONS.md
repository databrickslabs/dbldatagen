# Generating Data that Conforms to a Known Statistical Distribution

By the default, data that is being generated at random uses a uniform random number generator. 

Sometimes it is useful to generate data that conforms to a known distribution. 

While use of the `weights` option with discrete value lists can be used to introduce skew, 
this can be awkward to manage for large sets of values.

To enable this, we support use of known distributions for randomly generated data on any field.

When the field is not numeric, the underlying seed value will generated to conform to the 
known distribution before being converted 
to the appropriate type as per usual semantics. 

Note that the distribution will be scaled to the possible range of values. If all samples from a
distribution are equal (e.g. if the requested data contains only 1 row), the minimum value is returned.

The following distributions are supported:
- Normal (Gaussian) distribution
- Beta distribution
- Gamma distribution
- Exponential distribution
- Pareto distribution

> Note the `distribution` option will have no effect for values that are not randomly generated as
> per use of the `random` option.
> 
> For values generated randomly, continuous distributions can still be used with discrete values such as strings
> as the underlying random numbers used to select the appropriate discrete values will be drawn from the specified
> distribution. So, for discrete values, the frequency of occurrence of particular values should conform approximately
> to the underlying distribution.


## Examples 

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

## Specifying distributions as strings

As an alternative to passing a distribution object (e.g. `dist.Normal()` or `dist.Gamma(1.0, 2.0)`), 
you can pass the distribution as a string. This is convenient for quick exploration and when specs 
are built dynamically from configuration.

Matching is case-insensitive — `"Normal"` and `"NORMAL"` are equivalent.

```python
.withColumn("value", "float", minValue=0, maxValue=100, 
            random=True, distribution="normal")
```

### Specifying keyword arguments

To specify keyword arguments, use the `name(key=value, ...)` syntax. Values should be numeric 
literals (int or float; negatives are allowed). Argument names must match the distribution's 
constructor parameters.

```python
.withColumn("x", "float", minValue=0, maxValue=100, random=True,
            distribution="normal(mean=5.0, stddev=2.0)")

.withColumn("y", "float", minValue=0, maxValue=100, random=True,
            distribution="beta(alpha=3.0, beta=7.0)")

.withColumn("z", "float", minValue=0, maxValue=100, random=True,
            distribution="gamma(shape=2.0, scale=0.5)")

.withColumn("w", "float", minValue=0, maxValue=100, random=True,
            distribution="exponential(rate=1.5)")

.withColumn("v", "float", minValue=0, maxValue=100, random=True,
            distribution="pareto(shape=1.16)")
```

Note that partial overrides are supported. Any arguments you omit fall back to their default 
values. For example, `"gamma(shape=2.0)"` will create a distribution with the default value 
`scale=1.0`.

## Normal distribution

The Normal (Gaussian) distribution models symmetric variation around a central value. This is useful
for physical measurements such as adult heights and weights, exam scores clustering around a class 
mean, or sensor readings fluctuating around a calibrated baseline. The bell-curve shape means most 
generated values fall close to the mean and become exponentially rarer farther away.

The `mean` parameter sets the centre of the distribution; `stddev` controls the spread.
With `mean=170.0` and `stddev=10.0`, roughly 68% of generated values fall between 160 and 180 —
a realistic model for adult height in centimetres where extreme values below 140 or above 210 are
effectively impossible.

```python
import dbldatagen as dg
import dbldatagen.distributions as dist

testDataSpec = (
    dg.DataGenerator(spark, name="normal_example", rows=100_000)
    .withColumn("person_id", "integer", minValue=1, maxValue=100_000)
    # heights cluster around 170 cm; ~68% of values fall within one stddev (160–180)
    .withColumn(
        "height_cm",
        "float",
        minValue=140.0,
        maxValue=210.0,
        random=True,
        distribution=dist.Normal(mean=170.0, stddev=10.0),
    )
)

dfTestData = testDataSpec.build()
```

## Beta distribution

The Beta distribution is the standard choice for modelling bounded proportions and rates that live
naturally in the interval [0, 1]. This includes click-through rates, A/B test conversion probabilities, 
the fraction of budget consumed, or defect rates. Because its output is natively in [0, 1], a column
with `minValue=0.0` and `maxValue=1.0` maps the distribution directly without rescaling artefacts.

The two shape parameters `alpha` and `beta` jointly control location and skew. When `alpha == beta`
the distribution is symmetric around 0.5. Increasing `alpha` relative to `beta` shifts probability
mass toward higher values; a larger `beta` relative to `alpha` concentrates mass near zero.
`alpha=2.0, beta=18.0` yields a mean of 0.10 — a realistic e-commerce page conversion rate where
most pages convert below 20% but a handful reach 30% or more.

```python
import dbldatagen as dg
import dbldatagen.distributions as dist

testDataSpec = (
    dg.DataGenerator(spark, name="beta_example", rows=100_000)
    .withColumn("page_id", "integer", minValue=1, maxValue=100_000)
    # most pages convert at low rates; a few outliers reach 30%+
    .withColumn(
        "conversion_rate",
        "float",
        minValue=0.0,
        maxValue=1.0,
        random=True,
        distribution=dist.Beta(alpha=2.0, beta=18.0),
    )
)

dfTestData = testDataSpec.build()
```

## Gamma distribution

The Gamma distribution generates positive-valued durations, waiting times, and magnitudes —
session lengths, time-to-resolution for support tickets, or insurance claim amounts. Unlike the
Exponential distribution, Gamma can produce a mode away from zero when `shape > 1`, making it
more realistic for quantities that are accumulated over time or durations that are not instantaneous.

The `shape` parameter controls how many effective "stages" contribute to the total duration:
`shape=1` reduces to an Exponential, while larger values shift the peak rightward and concentrate
the distribution. The `scale` parameter stretches the time axis — the mean equals `shape × scale`.
With `shape=2.0` and `scale=5.0`, the mean session length is 10 minutes and values are concentrated
in the 2–30 minute range, which models typical web session behaviour.

```python
import dbldatagen as dg
import dbldatagen.distributions as dist

testDataSpec = (
    dg.DataGenerator(spark, name="gamma_example", rows=100_000)
    .withColumn("session_id", "integer", minValue=1, maxValue=100_000)
    # session durations peak around 10 min; shape=2 avoids a spike at zero
    .withColumn(
        "session_duration_minutes",
        "float",
        minValue=0.0,
        maxValue=120.0,
        random=True,
        distribution=dist.Gamma(shape=2.0, scale=5.0),
    )
)

dfTestData = testDataSpec.build()
```

## Exponential distribution

The Exponential distribution models the time between successive events in a Poisson process. This
could be the inter-arrival times between API requests, time-to-failure for hardware components, or 
gaps between customer purchases. Its defining property is memorylessness: the probability of the next 
event occurring in the next instant is independent of how long you have already waited.

The single `rate` parameter (λ) is the reciprocal of the mean: `mean = 1 / rate`. The
Exponential is a special case of the Gamma distribution with `shape=1`. With `rate=2.0`, the
average gap between requests is 0.5 seconds — appropriate for a moderately busy API endpoint where
most inter-arrival times are well under a second but occasional pauses of several seconds still
occur.

```python
import dbldatagen as dg
import dbldatagen.distributions as dist

testDataSpec = (
    dg.DataGenerator(spark, name="exponential_example", rows=100_000)
    .withColumn("request_id", "integer", minValue=1, maxValue=100_000)
    # inter-arrival times at 2 req/s on average; memoryless — each gap is independent
    .withColumn(
        "request_gap_seconds",
        "float",
        minValue=0.0,
        maxValue=30.0,
        random=True,
        distribution=dist.Exponential(rate=2.0),
    )
)

dfTestData = testDataSpec.build()
```

## Pareto distribution

The Pareto (power-law) distribution is useful for modelling naturally skewed quantities where
most values are small but a small number of values are very large — for example, the number of
orders per customer, file sizes, or city populations.

The `shape` parameter (tail index `alpha`) controls how heavy the tail is. A smaller value
produces more skew; `shape ≈ 1.16` corresponds to the classic 80/20 Pareto principle.

```python
import dbldatagen as dg
import dbldatagen.distributions as dist

testDataSpec = (
    dg.DataGenerator(spark, name="pareto_example", rows=100_000)
    .withColumn("customer_id", "integer", minValue=1, maxValue=100_000)
    # most customers place few orders; a handful place very many
    .withColumn(
        "order_count",
        "integer",
        minValue=1,
        maxValue=500,
        random=True,
        distribution=dist.Pareto(shape=1.16),
    )
)

dfTestData = testDataSpec.build()
```
