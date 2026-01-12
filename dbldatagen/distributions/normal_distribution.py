# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Normal / Gaussian statistical distributions related classes

"""

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import FloatType

from dbldatagen.datagen_types import NumericLike
from dbldatagen.distributions.data_distribution import DataDistribution
from dbldatagen.serialization import SerializableToDict


class Normal(DataDistribution):
    def __init__(self, mean: NumericLike | None = None, stddev: NumericLike | None = None) -> None:
        """Specifies that random samples should be drawn from the normal distribution parameterized by mean and standard deviation.

        :param mean: Value for mean parameter; Should be a float, int or other numeric value
        :param stddev: Value for standard deviation parameter; Should be a float, int or other numeric value
        """
        DataDistribution.__init__(self)
        self.mean = mean if mean is not None else 0.0
        self.stddev = stddev if stddev is not None else 1.0

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "mean": self.mean, "stddev": self.stddev}
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    @staticmethod
    def normal_func(mean_series: pd.Series, std_dev_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """Generates samples from the normal distribution using pandas / numpy.

        :param mean_series: Value for mean parameter as Pandas Series
        :param std_dev_series: Value for standard deviation parameter as Pandas Series
        :param random_seed: Value for randomSeed parameter as Pandas Series

        :return: Random samples from distribution scaled to values between 0 and 1
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

    def generateNormalizedDistributionSample(self) -> Column:
        """Generates a sample of data for the distribution.

        :return: Pyspark SQL column expression for the sample values
        """
        normal_sample = F.pandas_udf(self.normal_func, returnType=FloatType()).asNondeterministic()  # type: ignore

        # scala formulation uses scale = 1/rate
        newDef: Column = normal_sample(
            F.lit(self.mean), F.lit(self.stddev), F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1.0)
        )
        return newDef

    def __str__(self) -> str:
        """Returns a string representation of the object.

        :return: String representation of the object
        """
        return f"NormalDistribution( mean={self.mean}, stddev={self.stddev}, randomSeed={self.randomSeed})"

    @classmethod
    def standardNormal(cls) -> "Normal":
        """Returns an instance of the standard normal distribution with mean 0.0 and standard deviation 1.0.

        :return: Instance of the standard normal distribution
        """
        return Normal(mean=0.0, stddev=1.0)
