# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the SqlExpr class
"""
import pyspark.sql.functions as F

from .constraint import Constraint, NoPrepareTransformMixin
from ..serialization import SerializableToDict


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

    def _toInitializationDict(self):
        """ Converts an object to a Python dictionary. Keys represent the object's
            constructor arguments.
            :return: Python dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "expr": self._expr}
        return {
            k: v._toInitializationDict()
            if isinstance(v, SerializableToDict) else v
            for k, v in _options.items() if v is not None
        }

    def _generateFilterExpression(self):
        """ Generate a SQL filter expression that may be used for filtering"""
        return F.expr(self._expr)
