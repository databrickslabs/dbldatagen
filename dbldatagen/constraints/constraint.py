# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Constraint class
"""
import types
from abc import ABC, abstractmethod

# import optional_abstractmethod from type_utils
# import check_optional_abstract_methods from type_utils
from dbldatagen.type_utils import optional_abstractmethod, abstract_with_optional_methods


@abstract_with_optional_methods
class Constraint(ABC):
    """ Constraint object - base class for predefined and custom constraints

    This class is meant for internal use only.

    """
    SUPPORTED_OPERATORS = ["<", ">", ">=", "!=", "==", "=", "<=", "<>"]

    def __init__(self):
        """
        Initialize the constraint object
        """
        self._filterExpression = None
        self._calculatedFilterExpression = False

    @staticmethod
    def _columnsFromListOrString(columns):
        """ Get columns as  list of columns from string of list-like

        :param columns: string or list of strings representing column names
        """
        if isinstance(columns, str):
            return [columns]
        elif isinstance(columns, (list, set, tuple, types.GeneratorType)):
            return list(columns)
        else:
            raise ValueError("Columns must be a string or list of strings")

    @staticmethod
    def _generate_relation_expression(column, relation, valueExpression):
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

    @staticmethod
    def combineConstraintExpressions(constraintExpressions):
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

    @optional_abstractmethod
    def prepareDataGenerator(self, dataGenerator):
        """ Prepare the data generator to generate data that matches the constraint

           This method may modify the data generation rules to meet the constraint

           :param dataGenerator: Data generation object that will generate the dataframe
           :return: modified or unmodified data generator
        """
        return dataGenerator

    @optional_abstractmethod
    def transformDataframe(self, dataGenerator, dataFrame):
        """ Transform the dataframe to make data conform to constraint if possible

           This method should not modify the dataGenerator - but may modify the dataframe

           :param dataGenerator: Data generation object that generated the dataframe
           :param dataFrame: generated dataframe
           :return: modified or unmodified Spark dataframe

           The default transformation returns the dataframe unmodified

        """
        return dataFrame

    @abstractmethod
    def _generateFilterExpression(self):
        """ Generate a Pyspark expression that may be used for filtering"""
        return None

    @property
    def filterExpression(self):
        """ Return the filter expression (as instance of type Column that evaluates to True or non-True)"""
        if not self._calculatedFilterExpression:
            self._filterExpression = self._generateFilterExpression()
            self._calculatedFilterExpression = True
        return self._filterExpression
