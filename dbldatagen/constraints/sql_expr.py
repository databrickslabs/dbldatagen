# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the SqlExpr class
"""
import pyspark.sql.functions as F
from pyspark.sql import Column

from dbldatagen.constraints.constraint import Constraint, NoPrepareTransformMixin
from dbldatagen.serialization import SerializableToDict


class SqlExpr(NoPrepareTransformMixin, Constraint):
    """SQL Expression Constraint object - represents a constraint that is modelled using a SQL expression.

    :param expr: A SQL expression as a string
    """

    def __init__(self, expr: str) -> None:
        super().__init__(supportsStreaming=True)
        if not expr:
            raise ValueError("Expression must be a valid non-empty SQL string")
        self._expr = expr

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "expr": self._expr}
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    def _generateFilterExpression(self) -> Column:
        """Generate a SQL filter expression that may be used for filtering.

        :return: SQL filter expression as Pyspark SQL Column object
        """
        return F.expr(self._expr)
