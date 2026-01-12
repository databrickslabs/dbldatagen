# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Gamma statistical distributions related classes

"""

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import FloatType

from dbldatagen.datagen_types import NumericLike
from dbldatagen.distributions.data_distribution import DataDistribution
from dbldatagen.serialization import SerializableToDict


class Gamma(DataDistribution):
    """Specifies that random samples should be drawn from the gamma distribution parameterized by shape
    and scale. See https://en.wikipedia.org/wiki/Gamma_distribution.

    :param shape: Shape parameter; Should be a float, int or other numeric value greater than 0
    :param scale: Scale parameter; Should be a float, int or other numeric value greater than 0
    """

    def __init__(self, shape: NumericLike | None = None, scale: NumericLike | None = None) -> None:
        DataDistribution.__init__(self)
        self._shape = shape
        self._scale = scale

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "shape": self._shape, "scale": self._scale}
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    @property
    def shape(self) -> NumericLike | None:
        """Returns the shape parameter.

        :return: Shape parameter
        """
        return self._shape

    @property
    def scale(self) -> NumericLike | None:
        """Returns the scale parameter.

        :return: Scale parameter
        """
        return self._scale

    def __str__(self) -> str:
        """Returns a string representation of the object.

        :return: String representation of the object
        """
        return f"GammaDistribution(shape(`k`)={self._shape}, scale(`theta`)={self._scale}, seed={self.randomSeed})"

    @staticmethod
    def gamma_func(shape_series: pd.Series, scale_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """Generates samples from the gamma distribution using pandas / numpy.

        :param shape_series: Value for shape parameter as Pandas Series
        :param scale_series: Value for scale parameter as Pandas Series
        :param random_seed: Value for randomSeed parameter as Pandas Series

        :return: Random samples from distribution scaled to values between 0 and 1
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

    def generateNormalizedDistributionSample(self) -> Column:
        """Generates a sample of data for the distribution.

        :return: Pyspark SQL column expression for the sample values
        """
        gamma_sample = F.pandas_udf(self.gamma_func, returnType=FloatType()).asNondeterministic()  # type: ignore

        newDef: Column = gamma_sample(
            F.lit(self._shape),
            F.lit(self._scale),
            F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1.0),
        )
        return newDef
