# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the base class for statistical distributions

Each inherited version of the DataDistribution object is used to generate random numbers drawn from
a specific distribution.

As the test data generator needs to scale the set of values generated across different data ranges,
the generate function is intended to generate values scaled to values between 0 and 1.

AS some distributions don't have easily predicted bounds, we scale the random data sets
by taking the minimum and maximum value of each generated data set and using that as the range for the generated data.

For some distributions, there may be alternative more efficient mechanisms for scaling the data to the [0, 1] interval.

Some data distributions are scaled to the [0,1] interval as part of their data generation
and no further scaling is needed.
"""
import copy
import pyspark.sql.functions as F
import numpy as np


class DataDistribution(object):
    """ Base class for all distributions"""
    def __init__(self):
        self._rounding = False
        self._randomSeed = None

    @staticmethod
    def get_np_random_generator(random_seed):
        """ Get numpy random number generator

        :param random_seed: Numeric random seed to use. If < 0, then no random
        :return:
        """
        assert random_seed is None or type(random_seed) in [ np.int32, np.int64, int],\
               f"`randomSeed` must be int or int-like not {type(random_seed)}"
        from numpy.random import default_rng
        if random_seed not in (-1, -1.0):
            rng = default_rng(random_seed)
        else:
            rng = default_rng()

        return rng

    def generateNormalizedDistributionSample(self):
        """ Generate sample of data for distribution

        :return: random samples from distribution scaled to values between 0 and 1
        """
        if self.randomSeed == -1 or self.randomSeed is None:
            newDef = F.expr("rand()")
        else:
            assert type(self.randomSeed) in [int, float],  "random seed should be numeric"
            newDef = F.expr(f"rand({self.randomSeed})")
        return newDef

    def withRounding(self, rounding):
        """ Create copy of object and set the rounding attribute

        :param rounding: rounding value to set. Should be True or False
        :return: new instance of data distribution object with rounding set
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._rounding = rounding
        return new_distribution_instance

    @property
    def rounding(self):
        """get the `rounding` attribute """
        return self._rounding

    def withRandomSeed(self, seed):
        """ Create copy of object and set the random seed attribute

        :param seed: random generator seed value to set. Should be integer,  float or None
        :return: new instance of data distribution object with rounding set
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._randomSeed = seed
        return new_distribution_instance

    @property
    def randomSeed(self):
        """get the `randomSeed` attribute """
        return self._randomSeed
