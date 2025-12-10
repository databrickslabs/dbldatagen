# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Constraint class
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from types import GeneratorType
from typing import TYPE_CHECKING, ClassVar

from pyspark.sql import Column, DataFrame

from dbldatagen.serialization import SerializableToDict


if TYPE_CHECKING:
    # Imported only for type checking to avoid circular dependency at runtime
    from dbldatagen.data_generator import DataGenerator


class Constraint(SerializableToDict, ABC):
    """Constraint object - base class for predefined and custom constraints.

    This class is meant for internal use only.
    """

    SUPPORTED_OPERATORS: ClassVar[list[str]] = ["<", ">", ">=", "!=", "==", "=", "<=", "<>"]

    def __init__(self, supportsStreaming: bool = False) -> None:
        self._supportsStreaming = supportsStreaming
        self._filterExpression: Column | None = None
        self._calculatedFilterExpression: bool = False

    @staticmethod
    def _columnsFromListOrString(
        columns: str | list[str] | set[str] | tuple[str] | GeneratorType | None,
    ) -> list[str]:
        """Get a list of columns from string or list-like object

        :param columns: String or list-like object representing column names
        :return: List of column names
        """
        if columns is None:
            raise ValueError("Columns must be a non-empty string or list-like of column names")

        if isinstance(columns, str):
            return [columns]

        return list(columns)

    @staticmethod
    def _generate_relation_expression(column: Column, relation: str, valueExpression: Column) -> Column:
        """Generate comparison expression

        :param column: Column to generate comparison against
        :param relation: Relation to implement
        :param valueExpression: Expression to compare to
        :return: Relation expression as variation of Pyspark SQL columns
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
    def mkCombinedConstraintExpression(constraintExpressions: list[Column] | None) -> Column | None:
        """Generate a SQL expression that combines multiple constraints using AND.

        :param constraintExpressions: List of Pyspark SQL Column constraint expression objects
        :return: Combined constraint expression as Pyspark SQL Column object (or None if no valid expressions)
        """
        if constraintExpressions is None or not isinstance(constraintExpressions, list):
            raise ValueError("Constraints must be a list of Pyspark SQL Column instances")

        if not all(expr is None or isinstance(expr, Column) for expr in constraintExpressions):
            raise ValueError("Constraint expressions must be Pyspark SQL columns or None")

        valid_constraint_expressions = [expr for expr in constraintExpressions if expr is not None]

        if len(valid_constraint_expressions) > 0:
            combined_constraint_expression = valid_constraint_expressions[0]

            for additional_constraint in valid_constraint_expressions[1:]:
                combined_constraint_expression = combined_constraint_expression & additional_constraint

            return combined_constraint_expression

        return None

    @abstractmethod
    def prepareDataGenerator(self, dataGenerator: DataGenerator) -> DataGenerator:
        """Prepare the data generator to generate data that matches the constraint. This method may modify the data
        generation rules to meet the constraint.

        :param dataGenerator: Data generation object that will generate the dataframe
        :return: Modified or unmodified data generator
        """
        raise NotImplementedError("Method prepareDataGenerator must be implemented in derived class")

    @abstractmethod
    def transformDataframe(self, dataGenerator: DataGenerator, dataFrame: DataFrame) -> DataFrame:
        """Transform the dataframe to make data conform to constraint if possible

        This method should not modify the dataGenerator - but may modify the dataframe. The default
        transformation returns the dataframe unmodified

        :param dataGenerator: Data generation object that generated the dataframe
        :param dataFrame: Generated dataframe
        :return: Modified or unmodified Spark dataframe
        """
        raise NotImplementedError("Method transformDataframe must be implemented in derived class")

    @abstractmethod
    def _generateFilterExpression(self) -> Column | None:
        """Generate a Pyspark SQL expression that may be used for filtering.

        :return: Pyspark SQL expression that may be used for filtering
        """
        raise NotImplementedError("Method _generateFilterExpression must be implemented in derived class")

    @property
    def supportsStreaming(self) -> bool:
        """Return True if the constraint supports streaming dataframes.

        :return: True if the constraint supports streaming dataframes
        """
        return self._supportsStreaming

    @property
    def filterExpression(self) -> Column | None:
        """Return the filter expression (as instance of type Column that evaluates to True or non-True).

        :return: Filter expression as Pyspark SQL Column object
        """
        if self._calculatedFilterExpression:
            return self._filterExpression

        self._calculatedFilterExpression = True
        self._filterExpression = self._generateFilterExpression()
        return self._filterExpression


class NoFilterMixin:
    """Mixin class to indicate that constraint has no filter expression.

    Intended to be used in implementation of the concrete constraint classes.

    Use of the mixin class is optional but when used with the Constraint class and multiple inheritance,
    it will provide a default implementation of the _generateFilterExpression method that satisfies
    the abstract method requirement of the Constraint class.

    When using mixins, place the mixin class first in the list of base classes.
    """

    def _generateFilterExpression(self) -> None:
        """Generate a Pyspark SQL expression that may be used for filtering.

        :return: Pyspark SQL expression that may be used for filtering
        """
        return None


class NoPrepareTransformMixin:
    """Mixin class to indicate that constraint has no filter expression

    Intended to be used in implementation of the concrete constraint classes.

    Use of the mixin class is optional but when used with the Constraint class and multiple inheritance,
    it will provide a default implementation of the `prepareDataGenerator` and `transformeDataFrame` methods
    that satisfies the abstract method requirements of the Constraint class.

    When using mixins, place the mixin class first in the list of base classes.
    """

    def prepareDataGenerator(self, dataGenerator: DataGenerator) -> DataGenerator:
        """Prepare the data generator to generate data that matches the constraint

        This method may modify the data generation rules to meet the constraint

        :param dataGenerator: Data generation object that will generate the dataframe
        :return: Modified or unmodified data generator
        """
        return dataGenerator

    def transformDataframe(self, dataGenerator: DataGenerator, dataFrame: DataFrame) -> DataFrame:
        """Transform the dataframe to make data conform to constraint if possible

        This method should not modify the dataGenerator - but may modify the dataframe. The default
        transformation returns the dataframe unmodified

        :param dataGenerator: Data generation object that generated the dataframe
        :param dataFrame: Generated dataframe
        :return: Modified or unmodified Spark dataframe
        """
        return dataFrame
