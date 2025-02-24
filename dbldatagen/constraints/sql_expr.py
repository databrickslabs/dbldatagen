# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the SqlExpr class
"""
import pyspark.sql.functions as F

from .constraint import Constraint, NoPrepareTransformMixin


class SqlExpr(NoPrepareTransformMixin, Constraint):
    """ SQL Expression Constraint object

    This class represents a constraint that is modelled using a SQL expression

    :param expr: A SQL expression as a string

    """

    def __init__(self, expr: str):
        super().__init__(supportsStreaming=True)
        assert expr is not None, "Expression must be a valid SQL string"
        assert isinstance(expr, str) and len(expr.strip()) > 0, "Expression must be a valid SQL string"
        self._expr = expr

    def _getConstructorOptions(self):
        """ Returns an internal mapping dictionary for the object. Keys represent the
            class constructor arguments and values representing the object's internal data.
            :return: Python dictionary mapping constructor options to the object properties
        """
        return {"expr": self._expr}

    def _generateFilterExpression(self):
        """ Generate a SQL filter expression that may be used for filtering"""
        return F.expr(self._expr)
