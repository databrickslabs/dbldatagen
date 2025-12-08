# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the LiteralRelation class
"""
import pyspark.sql.functions as F
from pyspark.sql import Column

from dbldatagen.constraints.constraint import Constraint, NoPrepareTransformMixin
from dbldatagen.serialization import SerializableToDict


class LiteralRelation(NoPrepareTransformMixin, Constraint):
    """Literal Relation Constraint object - constrains one or more columns so that the columns have an a relationship to a constant value.

    :param columns: Column name or list of column names as string or list of strings
    :param relation: Operator to check - should be one of <,> , =,>=,<=, ==, !=
    :param value: Literal value to to compare against
    """

    def __init__(self, columns: str | list[str], relation: str, value: object) -> None:
        super().__init__(supportsStreaming=True)
        self._columns = self._columnsFromListOrString(columns)
        self._relation = relation
        self._value = value

        if relation not in self.SUPPORTED_OPERATORS:
            raise ValueError(f"Parameter `relation` should be one of the operators :{self.SUPPORTED_OPERATORS}")

    def _toInitializationDict(self) -> dict[str, object]:
        """Converts an object to a Python dictionary. Keys represent the object's
        constructor arguments.

        :return: Dictionary representation of the object
        """
        _options = {
            "kind": self.__class__.__name__,
            "columns": self._columns,
            "relation": self._relation,
            "value": self._value,
        }
        return {
            k: v._toInitializationDict() if isinstance(v, SerializableToDict) else v
            for k, v in _options.items()
            if v is not None
        }

    def _generateFilterExpression(self) -> Column | None:
        expressions = [F.col(colname) for colname in self._columns]
        literalValue = F.lit(self._value)
        filters = [self._generate_relation_expression(col, self._relation, literalValue) for col in expressions]

        return self.mkCombinedConstraintExpression(filters)
