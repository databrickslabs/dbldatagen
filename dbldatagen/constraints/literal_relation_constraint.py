# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ScalarInequality class
"""
import pyspark.sql.functions as F

from .constraint import Constraint, NoPrepareTransformMixin


class LiteralRelation(NoPrepareTransformMixin, Constraint):
    """LiteralRelation constraint

    Constrains one or more columns so that the columns have an a relationship to a constant value

    :param columns: column name or list of column names
    :param relation: operator to check - should be one of <,> , =,>=,<=, ==, !=
    :param value: A literal value to to compare against
    """

    def __init__(self, columns, relation, value):
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._relation = relation
        self._value = value

        if relation not in self.SUPPORTED_OPERATORS:
            raise ValueError(f"Parameter `relation` should be one of the operators :{self.SUPPORTED_OPERATORS}")

    @classmethod
    def getMapping(cls):
        return {"columns": "_columns", "relation": "_relation", "value": "_value"}

    def _generateFilterExpression(self):
        expressions = [F.col(colname) for colname in self._columns]
        literalValue = F.lit(self._value)
        filters = [self._generate_relation_expression(col, self._relation, literalValue) for col in expressions]

        return self.mkCombinedConstraintExpression(filters)
