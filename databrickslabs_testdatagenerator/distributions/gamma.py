# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the statistical distributions related classes

The general pattern will be as follows:

Distribibution will be defined by class such as NormalDistribution

Will have to handle the following cases:

- columns with a set of discrete values
- columns with a real valued boundaries
- columns with a minValue and maxValue value (and  optional step)

For all cases, the distribution may be defined with:

minValue-value, maxValue-value, median / mean and some other parameter

Here are the parameterisations for each of the distributions:

exponential: unbounded range is 0 - inf (but effective range is 0 - 5?)
   minValue, maxValue , rate or mean

normal: main range is mean +/- 3.5 x std (values can occur up to mean +/- 6 x std )

gamma: main range is mean +/- 3.5 x std (values can occur up to mean +/- 6 x std )

beta: range is zero - 1

There are multiple parameterizations
shape k, and scale (phi)
shape alpha and rate beta (1/scale)
shape k and mean miu= (k x scale)

Key aspects are the following

- how to map mean from mean value of column range
- how to map resulting distribution back to data set

- Key decisions
- any parameters mean,median, mode refer to absolute values in data set
- any parameters mean_value, median_value, mode_value refer to value in terms of range
- so if a column has the values [ online, offline, outage, inactive ] and mean_value is offline
- this may be translated behind the scenes to a normal distribution (minValue = 0, maxValue = 3, mean=1, std=2/6)
- this will essentially make it a truncated distribution

- ways to map range of values to distribution
- a: scale range to values, if bounds are predictable
- b: truncate (making values < minValue= minValue , > maxValue= maxValue) - which may cause output to have different distribution than expected
- c: discard values outside of range
   - requires generation of more values than required to allow for discarded values
   - can sample correct values to fill in missing data
- d: modulo - will change distribution

- high priority distributions are normal, exponential, gamma, beta




"""

import math
from datetime import date, datetime, timedelta
import random

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round as sql_round, array, expr, udf
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType
import numpy as np
import pandas as pd


class Gamma(object):
    def __init__(self, mean=None, std=None, minValue=None, maxValue=None, rectify=True, std_range=3.5, rounding=False):
        self.mean = mean if mean is not None else 0.0
        self.stddev, self.minValue, self.maxValue  = std if std is not None else 1.0, minValue, maxValue
        self.std_range, self.rectify = std_range, rectify
        self.round = rounding

        if minValue is None and rectify:
            self.minValue = 0.0

        assert type(std_range) is int or type(std_range) is float

        if maxValue is not None:
            if mean is None:
                self.mean = (self.minValue + self.maxValue) / 2.0
            if std is None:
                self.std = (self.mean - self.minValue) / self.std_range

    def __str__(self):
        return ("NormalDistribution(minValue={}, maxValue={}, mean={}, std={})"
                .format(self.minValue, self.maxValue, self.mean, self.std))

    def generate(self, size):
        retval = np.random.normal(self.mean, self.std, size=size)

        if self.rectify:
            retval = np.maximum(self.minValue, retval)

            if self.maxValue is not None:
                retval = np.minimum(self.maxValue, retval)

        if self.round:
            retval = np.round(retval)
        return retval

    def test_bounds(self, size):
        retval = self.generate(size)
        return (min(retval), max(retval), np.mean(retval), np.std(retval))


class ExponentialDistribution(object):
    def __init__(self, mean=None, median=None, min=None, max=None, rate=None, rectify=True, rounding=False):
        self.mean, self.median, self.min, self.max = mean, median, min, max
        self.rectify = rectify
        self.round = rounding
        self.rate = rate

        if min is None:
            self.min = 0.0

        assert (self.max is not None or
                self.rate is not None or
                self.median is not None or
                self.mean is not None), "Must have an explicit mean, maxValue, median or rate"

        if rate is not None:
            assert (self.mean is None) or (self.mean == 1.0 / self.rate), "Cant specify rate and mean"
            self.mean = (1.0 / rate) - self.min
            self.median = (math.log(2.0) / self.rate) - self.min
        elif mean is not None:
            self.mean = self.mean - self.min
            self.rate = 1.0 / self.mean
            self.median = math.log(2.0) / self.rate
        elif median is not None:
            self.median = self.median - self.min
            self.rate = 1.0 / (self.median / math.log(2.0))
            self.mean = 1.0 / self.rate
        else:
            # compute the rate if not specified
            if max is not None:
                if self.median is None:
                    self.median = ((self.max + self.min) / 2.0 - self.min)
                    self.rate = 1.0 / (self.median / math.log(2.0))
                    self.mean = 1.0 / self.rate

    def __str__(self):
        return ("{}(minValue={}, maxValue={}, adjusted_mean={}, adjusted_median={}, rate={},  std={})"
                .format("ExponentialDistribution", self.min, self.max,
                        self.mean + self.min, self.median + self.min, self.rate, 1.0 / self.rate))

    def generate(self, size):
        retval = np.random.exponential(self.mean, size=size)

        if self.min != 0.0 and self.min != 0:
            retval = retval + self.min

        if self.rectify:
            retval = np.maximum(self.min, retval)

            if self.max is not None:
                retval = np.minimum(self.max, retval)

        if self.round:
            retval = np.round(retval)
        return retval

    def test_bounds(self, size):
        retval = self.generate(size)
        return (min(retval), max(retval), np.mean(retval), np.std(retval), np.median(retval))