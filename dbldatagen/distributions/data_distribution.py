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
from abc import ABC, abstractmethod

import numpy as np
from pyspark.sql import Column

from dbldatagen.serialization import SerializableToDict


class DataDistribution(SerializableToDict, ABC):
    """Base class for all distributions"""

    _randomSeed: int | np.int32 | np.int64 | None = None
    _rounding: bool = False

    @staticmethod
    def get_np_random_generator(random_seed: int | np.int32 | np.int64 | None) -> np.random.Generator:
        """Gets a numpy random number generator.

        :param random_seed: Numeric random seed to use; If < 0, then no random
        :return: Numpy random number generator
        """
        if random_seed not in (-1, -1.0):
            rng = np.random.default_rng(random_seed)
        else:
            rng = np.random.default_rng()
        return rng

    @abstractmethod
    def generateNormalizedDistributionSample(self) -> Column:
        """Generates a sample of data for the distribution. Implementors must provide an implementation for this method.

        :return: Pyspark SQL column expression for the sample
        """
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not implement 'generateNormalizedDistributionSample'"
        )

    def withRounding(self, rounding: bool) -> "DataDistribution":
        """Creates a copy of the object and sets the rounding attribute.

        :param rounding: Rounding value to set
        :return: New instance of data distribution object with rounding set
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._rounding = rounding
        return new_distribution_instance

    @property
    def rounding(self) -> bool:
        """Returns the rounding attribute.

        :return: Rounding attribute
        """
        return self._rounding

    def withRandomSeed(self, seed: int | np.int32 | np.int64 | None) -> "DataDistribution":
        """Creates a copy of the object and with a new random seed value.

        :param seed: Random generator seed value to set; Should be integer,  float or None
        :return: New instance of data distribution object with random seed set
        """
        new_distribution_instance = copy.copy(self)
        new_distribution_instance._randomSeed = seed
        return new_distribution_instance

    @property
    def randomSeed(self) -> int | np.int32 | np.int64 | None:
        """Returns the random seed attribute.

        :return: Random seed attribute
        """
        return self._randomSeed
