# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the SqlExpr class
"""
from .constraint import Constraint
import pyspark.sql.functions as F


class SqlExpr(Constraint):
    """ SQL Expression Constraint object

    This class represents a constraint that is modelled using a SQL expression

    :param expr: A SQL expression as a string

    """
    def __init__(self, expr: str):
        Constraint.__init__(self)
        assert expr is not None, "Expression must be a valid SQL string"
        assert isinstance(expr, str) and len(expr.strip()) > 0, "Expression must be a valid SQL string"
        self._expr = expr

    def _generate_filter_expression(self):
        """ Generate a SQL filter expression that may be used for filtering"""
        return F.expr(self._expr)

