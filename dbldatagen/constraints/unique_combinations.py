# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the Positive class
"""
import pyspark.sql.functions as F
from .constraint import Constraint


class UniqueCombinations(Constraint):
    """ Unique Combinations constraints

    Applies constraint to ensure columns have unique combinations - i.e the set of columns supplied only have
    one combination of each set of values

    :param columns: string column name or list of column names as strings.If no columns are specified, all columns
                    will be considered when dropping duplicate combinations

    Essentially applies the constraint that the named columns have unique values for each combination of columns

    This is useful to enforce unique ids, unique keys etc.

    ..Note: When applied to streaming dataframe, it will perform any deduplication only within a batch.

            If stateful operation is needed, where duplicates are eliminated across the entire stream,
            it is recommended to use a watermark and apply deduplication logic to the dataframe
            produced by the `build()` method.

            For high volume streaming dataframes, this may consume substantial resources when maintaining state.

    """

    def __init__(self, columns=None):
        Constraint.__init__(self)
        if columns is not None:
            self._columns = self._columnsFromListOrString(columns)
        else:
            self._columns = None

    def transformDataframe(self, dataGenerator, dataFrame):
        """ Generate a filter expression that may be used for filtering"""
        if self._columns is None:
            dataFrame.dropDuplicates()
        else:
            dataFrame.dropDuplicates(self._columns)

        return dataFrame