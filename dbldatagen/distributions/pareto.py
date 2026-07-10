# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the Pareto statistical distributions related classes

"""

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import FloatType

from dbldatagen.datagen_types import NumericLike
from dbldatagen.distributions.data_distribution import DataDistribution, register_distribution
from dbldatagen.serialization import SerializableToDict


@register_distribution("pareto", shape=1.0)
class Pareto(DataDistribution):
    """Specifies that random samples should be drawn from the Pareto (power-law) distribution parameterized
    by shape. See https://en.wikipedia.org/wiki/Pareto_distribution.

    The Pareto distribution produces a heavy right tail: most generated values are small while a small
    number of values are very large. This makes it suitable for modelling naturally skewed quantities such
    as the number of orders per customer, file sizes, or city populations.

    The shape parameter (also called the tail index, `alpha`) controls how skewed the distribution is.
    A smaller shape produces a heavier tail (more skew); a larger shape produces a lighter tail (less skew).
    A shape of approximately 1.16 corresponds to the classic 80/20 Pareto principle.

    :param shape: Shape parameter (tail index `alpha`); Should be a float, int or other numeric value greater than 0
    """

    def __init__(self, shape: NumericLike | None = None) -> None:
        DataDistribution.__init__(self)
        self._shape = shape if shape is not None else 1.0

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "shape": self._shape}
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

    def __str__(self) -> str:
        """Returns a string representation of the object.

        :return: String representation of the object
        """
        return f"ParetoDistribution(shape(`alpha`)={self._shape}, seed={self.randomSeed})"

    @staticmethod
    def pareto_func(shape_series: pd.Series, random_seed: pd.Series) -> pd.Series:
        """Generates samples from the Pareto distribution using pandas / numpy.

        :param shape_series: Value for shape parameter as Pandas Series
        :param random_seed: Value for randomSeed parameter as Pandas Series

        :return: Random samples from distribution scaled to values between 0 and 1
        """
        shape = shape_series.to_numpy()
        random_seed = random_seed.to_numpy()[0]

        rng = DataDistribution.get_np_random_generator(random_seed)

        results = rng.pareto(shape)

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
        pareto_sample = F.pandas_udf(self.pareto_func, returnType=FloatType()).asNondeterministic()  # type: ignore

        newDef: Column = pareto_sample(
            F.lit(self._shape),
            F.lit(self.randomSeed) if self.randomSeed is not None else F.lit(-1.0),
        )
        return newDef
