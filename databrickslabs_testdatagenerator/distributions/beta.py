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
- b: truncate (making values < minValue= minValue , > maxValue= maxValue) -
    which may cause output to have different distribution than expected
- c: discard values outside of range
   - requires generation of more values than required to allow for discarded values
   - can sample correct values to fill in missing data
- d: modulo - will change distribution

- high priority distributions are normal, exponential, gamma, beta


"""

import math
from datetime import date, datetime, timedelta
import random

import numpy as np
import pandas as pd
from . import DataDistribution


class Beta(DataDistribution):
    def __init__(self, alpha=None, beta=None):
        DataDistribution.__init__(self)

        assert type(alpha) in [float, int, np.float64, np.int32, np.int64], "alpha must be int-like or float-like"
        assert type(beta) in [float, int, np.float64, np.int32, np.int64], "beta must be int-like or float-like"
        self._alpha = alpha
        self._beta = beta

    def __str__(self):
        return ("BetaDistribution(alpha={}, beta={}, randomSeed={})"
                .format(self._alpha, self._beta, self.randomSeed))

    def generate(self, size):
        retval = np.random.normal(self.mean, self.std, size=size)

        if self.rectify:
            retval = np.maximum(self.minValue, retval)

            if self.maxValue is not None:
                retval = np.minimum(self.maxValue, retval)

        if self.round:
            retval = np.round(retval)
        return retval
