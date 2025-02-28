# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Constraint class
"""
import types
from abc import ABC, abstractmethod
from pyspark.sql import Column
from ..serialization import SerializableToDict


class Constraint(SerializableToDict, ABC):
    """ Constraint object - base class for predefined and custom constraints

    This class is meant for internal use only.

    """
    SUPPORTED_OPERATORS = ["<", ">", ">=", "!=", "==", "=", "<=", "<>"]

    def __init__(self, supportsStreaming=False):
        """
        Initialize the constraint object
        """
        self._filterExpression = None
        self._calculatedFilterExpression = False
        self._supportsStreaming = supportsStreaming

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
    def mkCombinedConstraintExpression(constraintExpressions):
        """ Generate a SQL expression that combines multiple constraints using AND

        :param constraintExpressions: list of Pyspark SQL Column constraint expression objects
        :return: combined constraint expression as Pyspark SQL Column object (or None if no valid expressions)

        """
        assert constraintExpressions is not None and isinstance(constraintExpressions, list), \
            "Constraints must be a list of Pyspark SQL Column instances"

        assert all(expr is None or isinstance(expr, Column) for expr in constraintExpressions), \
            "Constraint expressions must be Pyspark SQL columns or None"

        valid_constraint_expressions = [expr for expr in constraintExpressions if expr is not None]

        if len(valid_constraint_expressions) > 0:
            combined_constraint_expression = valid_constraint_expressions[0]

            for additional_constraint in valid_constraint_expressions[1:]:
                combined_constraint_expression = combined_constraint_expression & additional_constraint

            return combined_constraint_expression
        else:
            return None

    @abstractmethod
    def prepareDataGenerator(self, dataGenerator):
        """ Prepare the data generator to generate data that matches the constraint

           This method may modify the data generation rules to meet the constraint

           :param dataGenerator: Data generation object that will generate the dataframe
           :return: modified or unmodified data generator
        """
        raise NotImplementedError("Method prepareDataGenerator must be implemented in derived class")

    @abstractmethod
    def transformDataframe(self, dataGenerator, dataFrame):
        """ Transform the dataframe to make data conform to constraint if possible

           This method should not modify the dataGenerator - but may modify the dataframe

           :param dataGenerator: Data generation object that generated the dataframe
           :param dataFrame: generated dataframe
           :return: modified or unmodified Spark dataframe

           The default transformation returns the dataframe unmodified

        """
        raise NotImplementedError("Method transformDataframe must be implemented in derived class")

    @abstractmethod
    def _generateFilterExpression(self):
        """ Generate a Pyspark SQL expression that may be used for filtering"""
        raise NotImplementedError("Method _generateFilterExpression must be implemented in derived class")

    @property
    def supportsStreaming(self):
        """ Return True if the constraint supports streaming dataframes"""
        return self._supportsStreaming

    @property
    def filterExpression(self):
        """ Return the filter expression (as instance of type Column that evaluates to True or non-True)"""
        if not self._calculatedFilterExpression:
            self._filterExpression = self._generateFilterExpression()
            self._calculatedFilterExpression = True
        return self._filterExpression


class NoFilterMixin:
    """ Mixin class to indicate that constraint has no filter expression

    Intended to be used in implementation of the concrete constraint classes.

    Use of the mixin class is optional but when used with the Constraint class and multiple inheritance,
    it will provide a default implementation of the _generateFilterExpression method that satisfies
    the abstract method requirement of the Constraint class.

    When using mixins, place the mixin class first in the list of base classes.
    """
    def _generateFilterExpression(self):
        """ Generate a Pyspark SQL expression that may be used for filtering"""
        return None


class NoPrepareTransformMixin:
    """ Mixin class to indicate that constraint has no filter expression

    Intended to be used in implementation of the concrete constraint classes.

    Use of the mixin class is optional but when used with the Constraint class and multiple inheritance,
    it will provide a default implementation of the `prepareDataGenerator` and `transformeDataFrame` methods
    that satisfies the abstract method requirements of the Constraint class.

    When using mixins, place the mixin class first in the list of base classes.
    """
    def prepareDataGenerator(self, dataGenerator):
        """ Prepare the data generator to generate data that matches the constraint

           This method may modify the data generation rules to meet the constraint

           :param dataGenerator: Data generation object that will generate the dataframe
           :return: modified or unmodified data generator
        """
        return dataGenerator

    def transformDataframe(self, dataGenerator, dataFrame):
        """ Transform the dataframe to make data conform to constraint if possible

           This method should not modify the dataGenerator - but may modify the dataframe

           :param dataGenerator: Data generation object that generated the dataframe
           :param dataFrame: generated dataframe
           :return: modified or unmodified Spark dataframe

           The default transformation returns the dataframe unmodified

        """
        return dataFrame
