# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines type aliases for common types used throughout the library.
"""
import numpy as np
from pyspark.sql import Column


NumericLike = float | int | np.float64 | np.int32 | np.int64
ColumnLike = Column | str
