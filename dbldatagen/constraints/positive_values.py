# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the PositiveValues class
"""
import pyspark.sql.functions as F
from pyspark.sql import Column

from dbldatagen.constraints.constraint import Constraint, NoPrepareTransformMixin
from dbldatagen.serialization import SerializableToDict


class PositiveValues(NoPrepareTransformMixin, Constraint):
    """Positive Value constraints.

    Applies constraint to ensure columns have positive values. Constrains values in named
    columns to be greater than equal zero or greater than zero if strict has the value `True`.

    :param columns: Column name or list of column names as string or list of strings
    :param strict: If True, the zero value is not considered positive
    """

    def __init__(self, columns: str | list[str], strict: bool = False) -> None:
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._strict = strict

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "columns": self._columns, "strict": self._strict}
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
        if self._strict:
            filters = [col.isNotNull() & (col > 0) for col in expressions]
        else:
            filters = [col.isNotNull() & (col >= 0) for col in expressions]

        return self.mkCombinedConstraintExpression(filters)
