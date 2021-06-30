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
- b: truncate (making values < minValue= minValue , > maxValue= maxValue)
   - which may cause output to have different distribution than expected
- c: discard values outside of range
   - requires generation of more values than required to allow for discarded values
   - can sample correct values to fill in missing data
- d: modulo - will change distribution

- high priority distributions are normal, exponential, gamma, beta




"""

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, FloatType

from .data_distribution import DataDistribution


class Gamma(DataDistribution):
    """ Specify Gamma distribution with specific shape and scale

    :param shape: shape parameter (k)
    :param scale: scale parameter (theta)

    See https://en.wikipedia.org/wiki/Gamma_distribution

    Scaling is performed to normalize values between 0 and 1

    """

    def __init__(self, shape, scale):
        DataDistribution.__init__(self)
        assert type(shape) in [float, int, np.float64, np.int32, np.int64], "alpha must be int-like or float-like"
        assert type(scale) in [float, int, np.float64, np.int32, np.int64], "beta must be int-like or float-like"
        self._shape = shape
        self._scale = scale

    def __str__(self):
        return ("GammaDistribution(shape(`k`)={}, scale(`theta`)={}, randomSeed={})"
                .format(self._shape, self._scale, self.randomSeed))

    @staticmethod
    def gamma_func(shape_series: pd.Series, scale_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """ Pandas / Numpy based function to generate gamma samples

        :param shape_series: pandas series of shape (k) values
        :param scale_series: pandas series of scale (theta) values
        :param random_seed:  pandas series of random seed values
        :return: Samples scaled from 0 .. 1
        """
        shape = shape_series.to_numpy()
        scale = scale_series.to_numpy()
        random_seed = random_seed.to_numpy()[0]

        rng = DataDistribution.get_np_random_generator(random_seed)

        results = rng.gamma(shape, scale)

        # scale results to range [0, 1]
        amin = np.amin(results) * 1.0
        amax = np.amax(results) * 1.0

        adjusted_results = results - amin

        scaling_factor = amax - amin

        results2 = adjusted_results / scaling_factor
        return pd.Series(results2)

    def generateNormalizedDistributionSample(self):
        """ Generate sample of data for distribution

        :return: random samples from distribution scaled to values between 0 and 1
        """
        gamma_sample = F.pandas_udf(self.gamma_func, returnType=FloatType()).asNondeterministic()

        newDef = gamma_sample(F.lit(self._shape),
                             F.lit(self._scale),
                             F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1.0))
        return newDef

