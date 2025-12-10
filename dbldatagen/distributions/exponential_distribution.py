# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Exponential statistical distributions related classes

"""

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import FloatType

from dbldatagen.datagen_types import NumericLike
from dbldatagen.distributions.data_distribution import DataDistribution
from dbldatagen.serialization import SerializableToDict


class Exponential(DataDistribution):
    """Specifies that random samples should be drawn from the exponential distribution parameterized
    by rate. See https://en.wikipedia.org/wiki/Exponential_distribution

    :param rate: Value for rate parameter; Should be a float, int or other numeric value greater than 0
    """

    def __init__(self, rate: NumericLike | None = None) -> None:
        DataDistribution.__init__(self)
        self._rate = rate

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "rate": self._rate}
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    def __str__(self) -> str:
        """Returns a string representation of the object.

        :return: String representation of the object
        """
        return f"ExponentialDistribution(rate={self.rate}, randomSeed={self.randomSeed})"

    @staticmethod
    def exponential_func(scale_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """Generates samples from the exponential distribution using pandas / numpy.

        :param scale_series: Value for scale parameter as Pandas Series
        :param random_seed: Value for randomSeed parameter as Pandas Series
        :return: Random samples from distribution scaled to values between 0 and 1
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
    def rate(self) -> NumericLike | None:
        """Returns the rate parameter.

        :return: Rate parameter
        """
        return self._rate

    @property
    def scale(self) -> float | np.floating | None:
        """Returns the scale implicit parameter. Scale is 1/rate.

        :return: Scale implicit parameter
        """
        if not self._rate:
            raise ValueError("Cannot compute value for 'scale'; Missing value for 'rate'")
        return 1.0 / self._rate

    def generateNormalizedDistributionSample(self) -> Column:
        """Generates a sample of data for the distribution.

        :return: Pyspark SQL column expression for the sample values
        """
        if not self._rate:
            raise ValueError("Cannot compute value for 'scale'; Missing value for 'rate'")

        exponential_sample = F.pandas_udf(  # type: ignore
            self.exponential_func, returnType=FloatType()
        ).asNondeterministic()

        # scala formulation uses scale = 1/rate
        newDef: Column = exponential_sample(
            F.lit(1.0 / self._rate), F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1.0)
        )
        return newDef
