# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the base class for statistical distributions

"""
import copy
import pyspark.sql.functions as F
import numpy as np
import pandas as pd


class DataDistribution(object):
    """ Base class for all distributions"""
    def __init__(self):
        self._rounding = False
        self._randomSeed = None

    def generateNormalizedDistributionSample(self, seed=-1):
        """ Generate sample of data for distribution

        :param seed: seed to random number generator. -1 means dont use any seed
        :return:
        """
        if seed == -1 or seed is None:
            newDef = F.expr("rand()")
        else:
            assert type(seed) in [int, float],  "random seed should be numeric"
            newDef = F.expr(f"rand({seed})")
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


