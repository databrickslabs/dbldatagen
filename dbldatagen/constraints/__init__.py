# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This package defines the constraints classes for the `dbldatagen` library.

The constraints classes are used to define predefined constraints that may be used to constrain the generated data.

Constraining the generated data is implemented in several ways:

- Rejection of rows that do not meet the criteria
- Modifying the generated data to meet the constraint (including modifying the data generation parameters)

Some constraints may be implemented using a combination of the above approaches.

For implementations using the rejection approach, the data generation process will possibly generate less than the
requested number of rows.

For the current implementation, most of the constraint strategies will be implemented using rejection based criteria.
"""

from .constraint import Constraint
from .sql_expr import SqlExpr
from .negative_values import NegativeValues
from .positive_values import PositiveValues
from .literal_relation_constraint import LiteralRelation
from .chained_relation import ChainedRelation
from .literal_range_constraint import LiteralRange
from .ranged_values_constraint import RangedValues

__all__ = ["chained_relation",
           "constraint",
           "negative_values",
           "literal_range_constraint",
           "literal_relation_constraint",
           "positive_values",
           "ranged_values_constraint"]
