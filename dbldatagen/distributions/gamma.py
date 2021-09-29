# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Gamma statistical distributions related classes

"""

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

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

    @property
    def shape(self):
        """ Return shape parameter."""
        return self._shape

    @property
    def scale(self):
        """ Return scale parameter."""
        return self._scale

    def __str__(self):
        """ Return string representation of object """
        return f"GammaDistribution(shape(`k`)={self._shape}, scale(`theta`)={self._scale}, seed={self.randomSeed})"

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
