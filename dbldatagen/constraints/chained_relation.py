# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ChainedInequality class
"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .constraint import Constraint, NoPrepareTransformMixin
from ..serialization import SerializableToDict


class ChainedRelation(NoPrepareTransformMixin, Constraint):
    """ChainedRelation constraint

    Constrains one or more columns so that each column has a relationship to the next.

    For example if the constraint is defined as `ChainedRelation(['a', 'b','c'], "<")` then only rows that
    satisfy the condition `a < b < c` will be included in the output
    (where `a`, `b` and `c` represent the data values for the rows).

    This can be used to model time related transactions (for example in retail where the purchaseDate, shippingDate
    and returnDate all have a specific relationship) etc.

    Relations supported include <, <=, >=, >, !=, ==

    :param columns: column name or list of column names as string or list of strings
    :param relation: operator to check - should be one of <,> , =,>=,<=, ==, !=
    """
    def __init__(self, columns, relation):
        super().__init__(supportsStreaming=True)
        self._relation = relation
        self._columns = self._columnsFromListOrString(columns)

        if relation not in self.SUPPORTED_OPERATORS:
            raise ValueError(f"Parameter `relation` should be one of the operators :{self.SUPPORTED_OPERATORS}")

        if not isinstance(self._columns, list) or len(self._columns) <= 1:
            raise ValueError("ChainedRelation constraints must be defined across more than one column")

    def _toInitializationDict(self):
        """ Converts an object to a Python dictionary. Keys represent the object's
            constructor arguments.
            :return: Python dictionary representation of the object
        """
        _options = {"kind": self.__class__.__name__, "relation": self._relation, "columns": self._columns}
        return {
            k: v._toInitializationDict()
            if isinstance(v, SerializableToDict) else v
            for k, v in _options.items() if v is not None
        }

    def _generateFilterExpression(self):
        """ Generated composite filter expression for chained set of filter expressions

        I.e if columns is ['a', 'b', 'c'] and relation is '<'

        create set of filters [ col('a') < col('b'), col('b') < col('c')]
        and combine them as single expression using logical and operation

        :return: filter expression for chained expressions
        """
        expressions = [F.col(colname) for colname in self._columns]

        filters = []
        # build set of filters for chained expressions
        for ix in range(1, len(expressions)):
            filters.append(self._generate_relation_expression(expressions[ix - 1], self._relation, expressions[ix]))

        # ... and combine them using logical `and` operation
        return self.mkCombinedConstraintExpression(filters)
