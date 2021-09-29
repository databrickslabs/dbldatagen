# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Beta statistical distributions related classes

"""

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

import numpy as np
import pandas as pd

from .data_distribution import DataDistribution


class Beta(DataDistribution):
    """ Specify that random samples should be drawn from the Beta distribution parameterized by alpha and beta

    :param alpha: value for alpha parameter - float, int or other numeric value, greater than 0
    :param beta: value for beta parameter - float, int or other numeric value, greater than 0

    See https://en.wikipedia.org/wiki/Beta_distribution

    By default the Beta distribution produces values between 0 and 1 so no scaling is needed
    """

    def __init__(self, alpha=None, beta=None):
        DataDistribution.__init__(self)

        assert type(alpha) in [float, int, np.float64, np.int32, np.int64], "alpha must be int-like or float-like"
        assert type(beta) in [float, int, np.float64, np.int32, np.int64], "beta must be int-like or float-like"
        self._alpha = alpha
        self._beta = beta

    @property
    def alpha(self):
        """ Return alpha parameter."""
        return self._alpha

    @property
    def beta(self):
        """ Return beta parameter."""
        return self._beta

    def __str__(self):
        """ Return string representation of object"""
        return f"BetaDistribution(alpha={self._alpha}, beta={self._beta}, randomSeed={self.randomSeed})"

    @staticmethod
    def beta_func(alpha_series: pd.Series, beta_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """ Generate sample of beta distribution using pandas / numpy

        :param alpha_series: value for alpha parameter as Pandas Series
        :param beta_series: value for beta parameter as Pandas Series
        :param random_seed: value for randomSeed parameter as Pandas Series

        :return: random samples from distribution scaled to values between 0 and 1

        """
        alpha = alpha_series.to_numpy()
        beta = beta_series.to_numpy()
        random_seed = random_seed.to_numpy()[0]

        rng = DataDistribution.get_np_random_generator(random_seed)

        results = rng.beta(alpha, beta)
        return pd.Series(results)

    def generateNormalizedDistributionSample(self):
        """ Generate sample of data for distribution

        :return: random samples from distribution scaled to values between 0 and 1
        """
        beta_sample = F.pandas_udf(self.beta_func, returnType=FloatType()).asNondeterministic()

        newDef = beta_sample(F.lit(self._alpha),
                             F.lit(self._beta),
                             F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1))
        return newDef
