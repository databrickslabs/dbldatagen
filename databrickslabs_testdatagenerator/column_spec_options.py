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
                     'precision', 'scale',
                      'random_seed_method', 'random_seed',
                      'nullable', 'implicit'

                      }

    #: the set of disallowed column attributes
    _forbidden_props = {
        'range'
    }

    #: max values for each column type, only if where value is intentionally restricted
    _max_type_range = {
        'byte': 256,
        'short': 65536
    }


    def __init__(self, props):
        self._column_spec_options = props

    def _getOrElse(self, key, default=None):
        """ Get val for key if it exists or else return default"""
        return self._column_spec_options.get(key, default)

    def __getitem__(self, key):
        """ implement the built in dereference by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self._column_spec_options.get(key, None)

    def _checkBoolOption(self, v, name=None, optional=True ):
        """ Check that option is either not specified or of type boolean"""
        assert name is not None
        if optional:
            ensure(v is None or type(v) is bool,
                   "Option `{}` must be boolean if specified - value: {}, type:".format(name, v, type(v)))
        else:
            ensure( type(v) is bool,
                   "Option `{}` must be boolean  - value: {}, type:".format(name, v, type(v)))



    def _checkExclusiveOptions(self, options):
        """check if the options are exclusive - i.e only one is not None

        :param options: list of options that will be mutually exclusive
        """
        assert options is not None, "options must be non empty"
        assert type(options) is list
        assert len([self[x] for x in options if self[x] is not None]) <= 1, \
            f" only one of of the options: {options} may be specified "

    def _checkOptionValues(self, option, option_values):
        """check if option value is in list of values

        :param option: list of options that will be mutually exclusive
        :param option_values: list of possible option values that will be mutually exclusive
        """
        assert option is not None and len(option.strip()) > 0, "option must be non empty"
        assert type(option_values) is list
        assert self[option] in option_values, "option: `{}` must have one of the values {}".format(option, option_values)

    def _checkProps(self, column_props):
        """
            check that column definition properties are recognized
            and that the column definition has required properties
        """
        ensure(column_props is not None, "coldef should be non-empty")

        colType = self['type']
        if colType.typeName() in self._max_type_range:
            min = self['min']
            max  = self['max']

            if min is not None and max is not None:
                effective_range = max - min
                if effective_range > self._max_type_range[colType.typeName()]:
                    raise ValueError("Effective range greater than range of type")

        for k in column_props.keys():
            ensure(k in ColumnSpecOptions._allowed_props, 'invalid column option {0}'.format(k))

        for arg in self._required_props:
            ensure(arg in column_props.keys() and column_props[arg] is not None,
                   'missing column option {0}'.format(arg))

        for arg in self._forbidden_props:
            ensure(arg not in column_props.keys(),
                   'forbidden column option {0}'.format(arg))

        # check weights and values
        if 'weights' in column_props.keys():
            ensure('values' in column_props.keys(),
                   "weights are only allowed for columns with values - column '{}' ".format(column_props['name']))
            ensure(column_props['values'] is not None and len(column_props['values']) > 0,
                   "weights must be associated with non-empty list of values - column '{}' ".format(
                       column_props['name']))
            ensure(len(column_props['values']) == len(column_props['weights']),
                   "length of list of weights must be  equal to length of list of values - column '{}' ".format(
                       column_props['name']))



