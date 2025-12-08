# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the UniqueCombinations class
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from dbldatagen.constraints.constraint import Constraint, NoFilterMixin
from dbldatagen.serialization import SerializableToDict


if TYPE_CHECKING:
    # Imported only for type checking to avoid circular dependency at runtime
    from dbldatagen.data_generator import DataGenerator


class UniqueCombinations(NoFilterMixin, Constraint):
    """Unique Combinations Constraint object - ensures that columns have unique combinations of values - i.e
    the set of columns supplied only have one combination of each set of values.

    If the columns are not specified, or the column name of '*' is used, all columns that would be present
    in the final output are considered.

    The uniqueness constraint may apply to columns that are omitted - i.e not part of the final output. If no
    column or column list is supplied, all columns that would be present in the final output are considered.

    This is useful to enforce unique ids, unique keys, etc.

    :param columns: String column name or list of column names as strings. If no columns are specified, all output
                    columns will be considered when dropping duplicate combinations.

    ..Note: When applied to streaming dataframe, it will perform any deduplication only within a batch.

            If stateful operation is needed, where duplicates are eliminated across the entire stream,
            it is recommended to use a watermark and apply deduplication logic to the dataframe
            produced by the `build()` method.

            For high volume streaming dataframes, this may consume substantial resources when maintaining state - hence
            deduplication will only be performed within a batch.

    """

    def __init__(self, columns: str | list[str] | None = None) -> None:
        super().__init__(supportsStreaming=False)
        if columns and columns != "*":
            self._columns = self._columnsFromListOrString(columns)
        else:
            self._columns = []

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "columns": self._columns or []}
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    def prepareDataGenerator(self, dataGenerator: DataGenerator) -> DataGenerator:
        """Prepare the data generator to generate data that matches the constraint.

        This method may modify the data generation rules to meet the constraint.

        :param dataGenerator: Data generation object that will generate the dataframe
        :return: Modified or unmodified data generator
        """
        return dataGenerator

    def transformDataframe(self, dataGenerator: DataGenerator, dataFrame: DataFrame) -> DataFrame:
        """Transform the dataframe to make data conform to constraint if possible.

        This method should not modify the dataGenerator - but may modify the dataframe.

        :param dataGenerator: Data generation object that generated the dataframe
        :param dataFrame: Generated dataframe
        :return: Modified or unmodified Spark dataframe

        The default transformation returns the dataframe unmodified

        """
        if not self._columns:
            # if no columns are specified, then all columns that would appear in the final output are used
            # when determining duplicates
            columnsToEvaluate = dataGenerator.getOutputColumnNames()
        else:
            columnsToEvaluate = self._columns

        # for batch processing, duplicate rows will be removed via drop duplicates
        return dataFrame.dropDuplicates(columnsToEvaluate)
