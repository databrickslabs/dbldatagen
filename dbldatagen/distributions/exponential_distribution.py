# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Exponential statistical distributions related classes

"""

import numpy as np
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

from .data_distribution import DataDistribution


class Exponential(DataDistribution):
    """ Specify that random samples should be drawn from the exponential distribution parameterized by rate

    :param rate: value for rate parameter - float, int or other numeric value, greater than 0

    See https://en.wikipedia.org/wiki/Exponential_distribution

    Scaling is performed to normalize values between 0 and 1
    """

    def __init__(self, rate=None):
        DataDistribution.__init__(self)
        self._rate = rate

    def __str__(self):
        """ Return string representation"""
        return f"ExponentialDistribution(rate={self.rate}, randomSeed={self.randomSeed})"

    @staticmethod
    def exponential_func(scale_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """ Generate sample of exponential distribution using pandas / numpy

        :param scale_series: value for scale parameter as Pandas Series
        :param random_seed: value for randomSeed parameter as Pandas Series
        :return: random samples from distribution scaled to values between 0 and 1

        """
        scale_param = scale_series.to_numpy()
        random_seed = random_seed.to_numpy()[0]

        rng = DataDistribution.get_np_random_generator(random_seed)

        results = rng.exponential(scale_param)

        # scale the results
        amin = np.amin(results) * 1.0
        amax = np.amax(results) * 1.0

        adjusted_results = results - amin

        scaling_factor = amax - amin

        results2 = adjusted_results / scaling_factor

        return pd.Series(results2)

    @property
    def rate(self):
        """ Return rate parameter"""
        return self._rate

    @property
    def scale(self):
        """ Return scale implicit parameter. Scale is 1/rate"""
        return 1.0 / self._rate

    def generateNormalizedDistributionSample(self):
        """ Generate sample of data for distribution

        :return: random samples from distribution scaled to values between 0 and 1
        """
        exponential_sample = F.pandas_udf(self.exponential_func, returnType=FloatType()).asNondeterministic()

        # scala formulation uses scale = 1/rate
        newDef = exponential_sample(F.lit(1.0 / self._rate),
                             F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1.0))
        return newDef
