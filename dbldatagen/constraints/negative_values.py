# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Negative class
"""
import pyspark.sql.functions as F
from .constraint import Constraint, NoPrepareTransformMixin


class NegativeValues(NoPrepareTransformMixin, Constraint):
    """ Negative Value constraints

    Applies constraint to ensure columns have negative values

    :param columns: string column name or list of column names as strings
    :param strict: if strict is True, the zero value is not considered negative

    Essentially applies the constraint that the named columns must be less than equal zero
    or less than zero if strict has the value `True`

    """

    def __init__(self, columns, strict=False):
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._strict = strict

    def _generateFilterExpression(self):
        expressions = [F.col(colname) for colname in self._columns]
        if self._strict:
            filters = [col.isNotNull() & (col < 0) for col in expressions]
        else:
            filters = [col.isNotNull() & (col <= 0) for col in expressions]

        return self.mkCombinedConstraintExpression(filters)
