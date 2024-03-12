# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Constraint class
"""
import types


class Constraint(object):
    SUPPORTED_OPERATORS = ["<", ">", ">=", "!=", "==", "=", "<=", "<>"]

    """ Constraint object - base class for predefined and custom constraints

    This class is meant for internal use only.

    """

    def __init__(self):
        """

        """
        self._filterExpression = None
        self._calculatedFilterExpression = False

    def _columnsFromListOrString(self, columns):
        """ Get columns as  list of columns from string of list-like

        :param columns: string or list of strings representing column names
        """
        if isinstance(columns, str):
            return [columns]
        elif isinstance(columns, (list, set, tuple, types.GeneratorType)):
            return list(columns)
        else:
            raise ValueError("Columns must be a string or list of strings")

    def _generate_relation_expression(self, column, relation, valueExpression):
        """ Generate comparison expression

        :param column: Column to generate comparison against
        :param relation: relation to implement
        :param valueExpression: expression to compare to
        :return: relation expression as variation of Pyspark SQL columns
        """
        if relation == ">":
            return column > valueExpression
        elif relation == ">=":
            return column >= valueExpression
        elif relation == "<":
            return column < valueExpression
        elif relation == "<=":
            return column <= valueExpression
        elif relation in ["!=", "<>"]:
            return column != valueExpression
        elif relation in ["=", "=="]:
            return column == valueExpression
        else:
            raise ValueError(f"Unsupported relation type '{relation}")

    @classmethod
    def combineConstraintExpressions(cls, constraintExpressions):
        """ Combine constraint expressions

        :param constraintExpressions: list of constraint expressions
        :return: combined constraint expression
        """
        assert constraintExpressions is not None and isinstance(constraintExpressions, list), \
            "Constraint expressions must be a list of constraint expressions"

        if len(constraintExpressions) > 0:
            constraint_expression = constraintExpressions[0]

            for additional_constraint in constraintExpressions[1:]:
                constraint_expression = constraint_expression & additional_constraint

            return constraint_expression
        else:
            raise ValueError("Invalid list of constraint expressions")

    def _prepare_data_generator(self, dataGenerator):
        """ Prepare the data generator to generate data that matches the constraint

           This method may modify the data generation rules to meet the constraint
        """
        return dataGenerator

    def _generate_filter_expression(self):
        """ Generate a Pyspark expression that may be used for filtering"""
        return None

    @property
    def filterExpression(self):
        """ Return the filter expression (as instance of type Column that evaluates to True or non-True)"""
        if not self._calculatedFilterExpression:
            self._filterExpression = self._generate_filter_expression()
            self._calculatedFilterExpression = True
        return self._filterExpression




