# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Normal / Gaussian statistical distributions related classes

"""

import numpy as np
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

from .data_distribution import DataDistribution


class Normal(DataDistribution):
    def __init__(self, mean, stddev):
        ''' Specify that data should follow normal distribution

        :param mean: mean of distribution
        :param stddev: standard deviation of distribution
        '''
        DataDistribution.__init__(self)
        self.mean = mean if mean is not None else 0.0
        self.stddev = stddev if stddev is not None else 1.0

    @staticmethod
    def normal_func(mean_series: pd.Series, std_dev_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """ Pandas / Numpy based function to generate normal / gaussian samples

        :param mean_series: pandas series of mean values
        :param std_dev_series: pandas series of standard deviation values
        :param random_seed:  pandas series of random seed values

        :return: Samples scaled from 0 .. 1
        """

        mean = mean_series.to_numpy()
        std_dev = std_dev_series.to_numpy()
        random_seed = random_seed.to_numpy()[0]

        rng = DataDistribution.get_np_random_generator(random_seed)

        results = rng.normal(mean, std_dev)

        # scale the results
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
        normal_sample = F.pandas_udf(self.normal_func, returnType=FloatType()).asNondeterministic()

        # scala formulation uses scale = 1/rate
        newDef = normal_sample(F.lit(self.mean),
                               F.lit(self.stddev),
                             F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1.0))
        return newDef

    def __str__(self):
        """ Return string representation of object """
        return f"NormalDistribution( mean={self.mean}, stddev={self.stddev}, randomSeed={self.randomSeed})"

    @classmethod
    def standardNormal(cls):
        """ return instance of standard normal distribution """
        return Normal(mean=0.0, stddev=1.0)
