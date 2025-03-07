# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Positive class
"""
import pyspark.sql.functions as F
from .constraint import Constraint, NoPrepareTransformMixin
from ..serialization import SerializableToDict


class PositiveValues(NoPrepareTransformMixin, Constraint):
    """ Positive Value constraints

    Applies constraint to ensure columns have positive values

    :param columns: string column name or list of column names as strings
    :param strict: if strict is True, the zero value is not considered positive

    Essentially applies the constraint that the named columns must be greater than equal zero
    or greater than zero if strict has the value `True`

    """

    def __init__(self, columns, strict=False):
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._strict = strict

    def _toInitializationDict(self):
        """ Converts an object to a Python dictionary. Keys represent the object's
            constructor arguments.
            :return: Python dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "columns": self._columns, "strict": self._strict}
        return {
            k: v._toInitializationDict()
            if isinstance(v, SerializableToDict) else v
            for k, v in _options.items() if v is not None
        }

    def _generateFilterExpression(self):
        """ Generate a filter expression that may be used for filtering"""
        expressions = [F.col(colname) for colname in self._columns]
        if self._strict:
            filters = [col.isNotNull() & (col > 0) for col in expressions]
        else:
            filters = [col.isNotNull() & (col >= 0) for col in expressions]

        return self.mkCombinedConstraintExpression(filters)
