# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `ColumnSpecOptions` class
"""

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round, array, expr, when, udf, format_string
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType
import math
from datetime import date, datetime, timedelta
from .utils import ensure
from .text_generators import TemplateGenerator
from .dataranges import DateRange, NRange

from pyspark.sql.functions import col, pandas_udf

class ColumnSpecOptions:
    """ Column spec options object - manages options for column specs.

    This class has limited functionality - mainly used to document the options


    This class is meant for internal use only
    """

    #: the set of attributes that must be present for any columns
    _required_props = {'name', 'type'}

    #: the set of attributes , we know about
    _allowed_props = {'name', 'type', 'min', 'max', 'step',
                     'prefix', 'random', 'distribution',
                     'range', 'base_column', 'base_column_type', 'values',
                     'numColumns', 'numFeatures', 'structType',
                     'begin', 'end', 'interval', 'expr', 'omit',
                     'weights', 'description', 'continuous',
                     'percent_nulls', 'template', 'format',
                     'unique_values', 'data_range', 'text',
                     'precision', 'scale'

                      }

    #: the set of disallowed column attributes
    _forbidden_props = {
        'range'
    }


