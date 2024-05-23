# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Positive class
"""
from .constraint import Constraint, NoFilterMixin


class UniqueCombinations(NoFilterMixin, Constraint):
    """ Unique Combinations constraints

    Applies constraint to ensure columns have unique combinations - i.e the set of columns supplied only have
    one combination of each set of values

    :param columns: string column name or list of column names as strings.If no columns are specified, all output
                    columns will be considered when dropping duplicate combinations.

    Essentially applies the constraint that the named columns have unique values for each combination of columns.

    The uniqueness constraint may apply to columns that are omitted - i.e not part of the final output.
    If no column or column list is supplied, all columns that would be present in the final output are considered.

    This is useful to enforce unique ids, unique keys etc.

    ..Note: When applied to streaming dataframe, it will perform any deduplication only within a batch.

            If stateful operation is needed, where duplicates are eliminated across the entire stream,
            it is recommended to use a watermark and apply deduplication logic to the dataframe
            produced by the `build()` method.

            For high volume streaming dataframes, this may consume substantial resources when maintaining state - hence
            deduplication will only be performed within a batch.

    """

    def __init__(self, columns=None):
        super().__init__(supportsStreaming=False)
        if columns is not None and columns != "*":
            self._columns = self._columnsFromListOrString(columns)
        else:
            self._columns = None

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
        if self._columns is None:
            # if no columns are specified, then all columns that would appear in the final output are used
            # when determining duplicates
            columnsToEvaluate = dataGenerator.getOutputColumnNames()
        else:
            columnsToEvaluate = self._columns

        # for batch processing, duplicate rows will be removed via drop duplicates

        results = dataFrame.dropDuplicates(columnsToEvaluate)

        return results
