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
- columns with a min and max value (and  optional step)

For all cases, the distribution may be defined with:

min-value, max-value, median / mean and some other parameter

Here are the parameterisations for each of the distributions:

exponential: unbounded range is 0 - inf (but effective range is 0 - 5?)
   min, max , rate or mean

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
- this may be translated behind the scenes to a normal distribution (min = 0, max = 3, mean=1, std=2/6)
- this will essentially make it a truncated distribution

- ways to map range of values to distribution
- a: scale range to values, if bounds are predictable
- b: truncate (making values < min= min , > max= max) - which may cause output to have different distribution than expected
- c: discard values outside of range
   - requires generation of more values than required to allow for discarded values
   - can sample correct values to fill in missing data
- d: modulo - will change distribution

- high priority distributions are normal, exponential, gamma, beta




"""

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round, array, expr, udf
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType
import math
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

import random


class Beta(object):
    def __init__(self, mean=None, std=None, min=None, max=None, rectify=True, std_range=3.5, round=False):
        self.mean, self.stddev, self.min, self.max = mean if mean is not None else 0.0, std if std is not None else 1.0, min, max
        self.std_range, self.rectify = std_range, rectify
        self.round = round

        if min is None and rectify:
            self.min = 0.0

        assert type(std_range) is int or type(std_range) is float

        if max is not None:
            if mean is None:
                self.mean = (self.min + self.max) / 2.0
            if std is None:
                self.std = (self.mean - self.min) / self.std_range

    def __str__(self):
        return "NormalDistribution(min={}, max={}, mean={}, std={})".format(self.min, self.max, self.mean, self.std)

    def generate(self, size):
        retval = np.random.normal(self.mean, self.std, size=size)

        if self.rectify:
            retval = np.maximum(self.min, retval)

            if self.max is not None:
                retval = np.minimum(self.max, retval)

        if self.round:
            retval = np.round(retval)
        return retval

    def test_bounds(self, size):
        retval = self.generate(size)
        return (min(retval), max(retval), np.mean(retval), np.std(retval))

