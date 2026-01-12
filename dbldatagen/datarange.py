# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the DataRange abstract class.
"""

from pyspark.sql.types import DataType

from dbldatagen.serialization import SerializableToDict


class DataRange(SerializableToDict):
    """Abstract class used as base class for NRange and DateRange"""

    minValue: object | None
    maxValue: object | None

    def isEmpty(self) -> bool:
        """Checks if object is empty (i.e all instance vars of note are `None`).

        :return: True if the object is empty
        """
        raise NotImplementedError(f"'{self.__class__.__name__}' does not implement method 'isEmpty'")

    def isFullyPopulated(self) -> bool:
        """Checks if all instance vars are populated.

        :return: True if all instance vars are populated, False otherwise
        """
        raise NotImplementedError(f"'{self.__class__.__name__}' does not implement method 'isFullyPopulated'")

    def adjustForColumnDatatype(self, ctype: DataType) -> None:
        """Adjust default values for column output type.

        :param ctype: Spark SQL data type for column
        """
        raise NotImplementedError(f"'{self.__class__.__name__}' does not implement method 'adjustForColumnDatatype'")

    def getDiscreteRange(self) -> float:
        """Convert range to discrete range.

        :return: Discrete range object
        """
        raise NotImplementedError(f"'{self.__class__.__name__}' does not implement method 'getDiscreteRange'")

    @property
    def min(self) -> object:
        """get the `min` attribute"""
        return self.minValue

    @property
    def max(self) -> object:
        """get the `max` attribute"""
        return self.maxValue
