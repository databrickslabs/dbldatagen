# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `ColumnGeneratorBuilder` class and utility functions
"""

import itertools
from typing import Any

from pyspark.sql.types import DataType, DateType, StringType, TimestampType


class ColumnGeneratorBuilder:
    """
    Helper class to build functional column generators of specific forms
    """

    @classmethod
    def _mkList(cls, x: object) -> list:
        """
        Makes a list of the supplied object instance if it is not already a list.

        :param x: Input object to process
        :returns: List containing the supplied object if it is not already a list; otherwise returns the object
        """
        return [x] if type(x) is not list else x

    @classmethod
    def _lastElement(cls, x: object) -> object:
        """
        Gets the last element from the supplied object if it is a list.

        :param x: Input object
        :returns: Last element of the input object if it is a list; otherwise returns the object
        """
        return x[-1] if isinstance(x, list) else x

    @classmethod
    def _mkCdfProbabilities(cls, weights: list[float]) -> list[float]:
        """
        Makes cumulative distribution function probabilities for each value in values list.

        a cumulative distribution function for discrete values can uses
        a  table of cumulative probabilities to evaluate different expressions
        based on the cumulative  probability of each value occurring

        The probabilities can be computed from the weights using the code::

           weight_interval = 1.0 / total_weights
           probs = [x * weight_interval for x in weights]
           return reduce((lambda x, y: cls._mk_list(x) + [cls._last_element(x) + y]), probs)

        but Python provides built-in methods (itertools.accumulate) to compute the accumulated weights
        and dividing each resulting accumulated weight by the sum of the total weights will give the cumulative
        probabilities.

        For large data sets (relative to number of values), tests verify that the resulting distribution is
        within 10% of the expected distribution when using Spark SQL Rand() as a uniform random number generator.
        In testing, test data sets of size 3000 * number_of_values rows produce a distribution within 10% of expected ,
        while datasets of size 10,000 x `number of values` gives a repeated
        distribution within 5% of expected distribution.

        :param weights: List of weights to compute CDF probabilities for
        :returns: List of CDF probabilities

        Example code to be generated (pseudo code)::

           # given values value1 .. valueN, prob1 to probN
           # cdf_probabilities = [ prob1, prob1+prob2, ... prob1+prob2+prob3 .. +probN]
           # this will then be used as follows

           prob_occurrence = uniform_rand(0.0, 1.0)
           if prob_occurrence <= cdf_prob_value1 then value1
           elif prob_occurrence <= cdf_prob_value2 then value2
           ...
           prob_occurrence <= cdf_prob_valueN then valueN
           else valueN

        """
        total_weights = sum(weights)
        return [x / total_weights for x in itertools.accumulate(weights)]

    @classmethod
    def mkExprChoicesFn(cls, values: list[Any], weights: list[float], seed_column: str, datatype: DataType) -> str:
        """
        Creates a SQL expression to compute a weighted values expression. Builds an expression of the form::

           case
              when rnd_column <= weight1 then value1
              when rnd_column <= weight2 then value2
              ...
              when rnd_column <= weightN then  valueN
              else valueN
           end

        The output expression is based on the computed probability distribution for the specified values.

        In Python 3.6 onwards, we could use the choices function but this python version is not guaranteed on all
        Databricks distributions.

        :param values: List of values
        :param weights: List of weights
        :param seed_column: Base column name for expression
        :param datatype: Spark `DataType` of the output expression
        :returns: SQL expression representing the weighted values
        """
        cdf_probs = cls._mkCdfProbabilities(weights)

        output = [" CASE "]

        conditions = zip(values, cdf_probs, strict=False)

        for v, cdf in conditions:
            if isinstance(datatype, (StringType, DateType, TimestampType)):
                # Escape single quotes with backslash for Spark SQL string literals
                v_escaped = str(v).replace("'", "\\'")
                output.append(f" when {seed_column} <= {cdf} then '{v_escaped}' ")
            else:
                output.append(f" when {seed_column} <= {cdf} then {v} ")

        if isinstance(datatype, (StringType, DateType, TimestampType)):
            # Escape single quotes with backslash for Spark SQL string literals
            last_value_escaped = str(values[-1]).replace("'", "\\'")
            output.append(f"else '{last_value_escaped}'")
        else:
            output.append(f"else {values[-1]}")
        output.append("end")

        retval = "\n".join(output)

        return retval
