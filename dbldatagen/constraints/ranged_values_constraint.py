# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ScalarRange class
"""
import pyspark.sql.functions as F

from .constraint import Constraint, NoPrepareTransformMixin


class RangedValues(NoPrepareTransformMixin, Constraint):
    """ RangedValues Constraint object - validates that column value(s) are between 2 column values

    :param columns: Name of column or list of column names
    :param lowValue: Tests that columns have values greater than low value (greater or equal if `strict` is False)
    :param highValue: Tests that columns have values less than high value (less or equal if `strict` is False)
    :param strict: If True, excludes low and high values from range. Defaults to False

    Note `lowValue` and `highValue` must be names of columns that contain the low and high values
    """

    def __init__(self, columns, lowValue, highValue, strict=False):
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._lowValue = lowValue
        self._highValue = highValue
        self._strict = strict

    def _generateFilterExpression(self):
        """ Generate a SQL filter expression that may be used for filtering"""
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

    def toDict(self):
        """ Returns a Python dictionary representation of a Constraint.
            :return: Python dictionary representing the constraint
        """
        return {
            "type": "RangedValues",
            "columns": self._columns,
            "lowValue": self._lowValue,
            "highValue": self._highValue,
            "strict": self._strict
        }
