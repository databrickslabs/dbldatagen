# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the RangedValues class
"""
import pyspark.sql.functions as F
from pyspark.sql import Column

from dbldatagen.constraints.constraint import Constraint, NoPrepareTransformMixin
from dbldatagen.serialization import SerializableToDict


class RangedValues(NoPrepareTransformMixin, Constraint):
    """RangedValues Constraint object - validates that column values are in the range defined by values in
    `lowValue` and `highValue` columns. `lowValue` and `highValue` must be names of columns that contain
    the low and high values respectively.

    :param columns: Name of column or list of column names
    :param lowValue: Name of column containing the low value
    :param highValue: Name of column containing the high value
    :param strict: If True, excludes low and high values from range. Defaults to False
    """

    def __init__(self, columns: str | list[str], lowValue: str, highValue: str, strict: bool = False) -> None:
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._lowValue = lowValue
        self._highValue = highValue
        self._strict = strict

    def _toInitializationDict(self) -> dict[str, object]:
        """Returns an internal mapping dictionary for the object. Keys represent the
        class constructor arguments and values representing the object's internal data.

        :return: Dictionary mapping constructor options to the object properties
        """
        _options = {
            "kind": self.__class__.__name__,
            "columns": self._columns,
            "lowValue": self._lowValue,
            "highValue": self._highValue,
            "strict": self._strict,
        }
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    def _generateFilterExpression(self) -> Column | None:
        """Generate a SQL filter expression that may be used for filtering.

        :return: SQL filter expression as Pyspark SQL Column object
        """
        expressions = [F.col(colname) for colname in self._columns]
        minValue = F.col(self._lowValue)
        maxValue = F.col(self._highValue)

        # build ranged comparison expressions
        if self._strict:
            filters = [(column_expr > minValue) & (column_expr < maxValue) for column_expr in expressions]
        else:
            filters = [column_expr.between(minValue, maxValue) for column_expr in expressions]

        # ... and combine them using logical `and` operation
        return self.mkCombinedConstraintExpression(filters)
