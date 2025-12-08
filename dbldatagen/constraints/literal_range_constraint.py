# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the LiteralRange class
"""
import pyspark.sql.functions as F
from pyspark.sql import Column

from dbldatagen.constraints.constraint import Constraint, NoPrepareTransformMixin
from dbldatagen.serialization import SerializableToDict


class LiteralRange(NoPrepareTransformMixin, Constraint):
    """LiteralRange Constraint object - validates that column value(s) are between 2 literal values.

    :param columns: Column name or list of column names as string or list of strings
    :param lowValue: Tests that columns have values greater than low value (greater or equal if `strict` is False)
    :param highValue: Tests that columns have values less than high value (less or equal if `strict` is False)
    :param strict: If True, excludes low and high values from range. Defaults to False

    Note `lowValue` and `highValue` must be values that can be converted to a literal expression using the
    `pyspark.sql.functions.lit` function
    """

    def __init__(self, columns: str | list[str], lowValue: object, highValue: object, strict: bool = False) -> None:
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._lowValue = lowValue
        self._highValue = highValue
        self._strict = strict

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
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
        minValue = F.lit(self._lowValue)
        maxValue = F.lit(self._highValue)

        # build ranged comparison expressions
        if self._strict:
            filters = [(column_expr > minValue) & (column_expr < maxValue) for column_expr in expressions]
        else:
            filters = [column_expr.between(minValue, maxValue) for column_expr in expressions]

        # ... and combine them using logical `and` operation
        return self.mkCombinedConstraintExpression(filters)
