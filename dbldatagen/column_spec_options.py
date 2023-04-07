# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
.. title::Column Spec Options

This file defines the `ColumnSpecOptions` class
"""

import copy

from .utils import ensure


class ColumnSpecOptions(object):
    """ Column spec options object - manages options for column specs.

    This class has limited functionality - mainly used to validate and document the options,
    and the class is meant for internal use only.

    :param props: Used to pass list of properties for column generation spec property checking.

    The following options are permitted on data generator `withColumn`, `withColumnSpec` and `withColumnSpecs` methods:

    :param name: Column name

    :param type: Data type of column. Can be either instance of Spark SQL Datatype such as `IntegerType()` \
                 or string containing SQL name of type

    :param minValue: Minimum value for range of generated value. \
                     As an alternative, you may use the `dataRange` parameter

    :param maxValue: Maximum value for range of generated value. \
                     As an alternative, you may use the `dataRange` parameter

    :param step: Step to use for range of generated value. As an alternative, you may use the `dataRange` parameter

    :param numColumns: generate `n` columns numbered from 1 .. n-1 with same definition

    :param numFeatures: generate `n` columns numbered from 0 .. n-1 with same definition. Alias for `numColumns`

    :param structType: If specified as "array" and used with numColumns / numFeatures, will combine columns as array

    :param random: If True, will generate random values for column value. Defaults to `False`

    :param baseColumn: Either the string name of the base column, or a list of columns to use to
                        control data generation. The option ``baseColumns`` is an alias for ``baseColumn``.

    :param values: List of discrete values for the colummn. Discrete values for the column can be strings, numbers
                   or constants conforming to type of column

    :param weights: List of discrete weights for the colummn. Should be integer values.
                    For example, you might declare a column for status values with a weighted distribution with
                    the following statement:
                    `withColumn("status", StringType(), values=['online', 'offline', 'unknown'], weights=[3,2,1])`

    :param percentNulls: Specifies numeric percentage of generated values to be populated with SQL `null`.
                          Value is fraction representing percentage between 0.0 and 1.0
                          For example: `percentNulls=0.12` will give approximately 12% nulls for this field in the
                          output.

    :param uniqueValues: Number of unique values for column.
                          If the unique values are specified for a timestamp or date field, the values will be chosen
                          working back from the end of the previous month,
                          unless `begin`, `end` and `interval` parameters are specified

    :param begin: Beginning of range for date and timestamp fields.
                   For dates and timestamp fields, use the `begin`, `end` and `interval`
                   or `dataRange` parameters instead of `minValue`, `maxValue` and `step`

    :param end: End of range for date and timestamp fields.
                   For dates and timestamp fields, use the `begin`, `end` and `interval`
                   or `dataRange` parameters instead of `minValue`, `maxValue` and `step`

    :param interval: Interval of range for date and timestamp fields.
                   For dates and timestamp fields, use the `begin`, `end` and `interval`
                   or `dataRange` parameters instead of `minValue`, `maxValue` and `step`

    :param dataRange: An instance of an `NRange` or `DateRange` object. This can be used in place of `minValue`,
                       `maxValue`, `step` or `begin`, `end`, `interval`.

    :param template: template controlling how text should be generated

    :param textSeparator: string specifying separator to be used when constructing strings with prefix and suffix

    :param prefix: string specifying prefix text to construct field from prefix and numeric value. Both `prefix` and
                   `suffix` can be used together

    :param suffix: string specifying suffix text to construct field from suffix and numeric value. Both `prefix` and
                   `suffix` can be used together

    :param omit: if True, column is omitted from the output. Used to use column for interim effect only.

    :param expr: SQL expression to control data generation. Ignores column base value if present.

    :param implicit: Used by system to mark that column has been inferred from a schema.
                     Allows definition to be explicitly overridden.

    :param precision: Used for rounding to specific decimal layout.

    :param scale: Used for rounding to specific decimal layout.

    :param distribution: Distribution for random number. Ignored if column is not random.

    :param escapeSpecialChars: if True, require escape for all special chars in template

    .. note::
        If the `dataRange` parameter is specified as well as the `minValue`, `maxValue` or `step`,
        the results are undetermined.

        For more information, see :doc:`/reference/api/dbldatagen.daterange`
        or :doc:`/reference/api/dbldatagen.nrange`.

    """

    #: the set of attributes that must be present for any columns
    _REQUIRED_PROPERTIES = {'name', 'type'}

    _PROPERTY_ALIASES = {
        'data_range': 'dataRange',
        'base_column': 'baseColumn',
        'base_column_type': 'baseColumnType',
        'base_columns': 'baseColumn',
        'baseColumns': 'baseColumn',
        'percent_nulls': 'percentNulls',
        'unique_values': 'uniqueValues',
        'random_seed_method': 'randomSeedMethod',
        'random_seed': 'randomSeed',
        'text_separator': 'textSeparator',

    }
    #: the set of attributes that are permitted for any call to data generator `withColumn` or `withColumnSpec`
    _ALLOWED_PROPERTIES = {'name', 'type', 'minValue', 'maxValue', 'step',
                           'prefix', 'random', 'distribution',
                           'range', 'baseColumn', 'baseColumnType', 'values',
                           'numColumns', 'numFeatures', 'structType',
                           'begin', 'end', 'interval', 'expr', 'omit',
                           'weights', 'description', 'continuous',
                           'percentNulls', 'template', 'format',
                           'uniqueValues', 'dataRange', 'text',
                           'precision', 'scale',
                           'randomSeedMethod', 'randomSeed',
                           'nullable', 'implicit', 'escapeSpecialChars',
                           'suffix', 'textSeparator'
                           }

    #: the set of disallowed column attributes for any call to data generator `withColumn` or `withColumnSpec`
    _FORBIDDEN_PROPERTIES = {
        'range'
    }

    #: maxValue values for each column type, only if where value is intentionally restricted
    _MAX_TYPE_RANGE = {
        'byte': 256,
        'short': 65536,
        'int': 4294967296
    }

    def __init__(self, props, aliases=None):  # TODO: check if additional options are needed here as `**kwArgs`
        self._options = props

        self._aliases = aliases if aliases is not None else self._PROPERTY_ALIASES

        # translate aliases
        # need to copy options dictionary as you cant directly change a
        # dictionary that you are iterating over
        updated_options = copy.copy(props)
        for k in props.keys():
            if k in self._aliases:
                v = props[k]
                alias_name = self._aliases[k]
                updated_options[alias_name] = v
                del updated_options[k]

        self._options = updated_options

    @property
    def options(self):
        """ Get options dictionary for object

            :return: options dictionary for object

        """
        return self._options

    def getOrElse(self, key, default=None):
        """ Get val for key if it exists or else return default"""
        assert key is not None, "key must be valid key string"

        if key in self._options:
            return self._options.get(key, default)
        if key in self._aliases:
            return self._options.get(self._aliases[key], default)
        return default

    def __getitem__(self, key):
        """ implement the built in dereference by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self._options.get(key, None)

    def checkBoolOption(self, v, name=None, optional=True):
        """ Check that option is either not specified or of type boolean

        :param v: value to test
        :param name: name of value to use in any reported errors or exceptions
        :param optional: If True (default), indicates that value is optional and
                         that `None` is a valid value for the option
        """
        assert name is not None, "`name` must be specified"
        if optional:
            ensure(v is None or type(v) is bool,
                   f"Option `{name}` must be boolean if specified - value: {v}, type: {type(v)}")
        else:
            ensure(type(v) is bool,
                   f"Option `{name}` must be boolean  - value: {v}, type: {type(v)}")

    def checkExclusiveOptions(self, options):
        """check if the options are exclusive - i.e only one is not None

        :param options: list of options that will be mutually exclusive
        """
        assert options is not None, "options must be non empty"
        assert type(options) is list, "`options` must be list"
        assert len([self[x] for x in options if self[x] is not None]) <= 1, \
            f" only one of of the options: {options} may be specified "

    def checkOptionValues(self, option, option_values):
        """check if option value is in list of values

        :param option: list of options that will be mutually exclusive
        :param option_values: list of possible option values that will be mutually exclusive
        """
        assert option is not None and len(option.strip()) > 0, "option must be non empty"
        assert type(option_values) is list, "`option_values` must be list"
        assert self[option] in option_values, f"option: `{option}` must have one of the values {option_values}"

    def checkValidColumnProperties(self, columnProps):
        """
            check that column definition properties are recognized
            and that the column definition has required properties

            :param columnProps:
        """
        ensure(columnProps is not None, "columnProps should be non-empty")

        col_type = self['type']
        if col_type.typeName() in self._MAX_TYPE_RANGE:
            minValue = self['minValue']
            maxValue = self['maxValue']

            if minValue is not None and maxValue is not None:
                effective_range = maxValue - minValue
                if effective_range > self._MAX_TYPE_RANGE[col_type.typeName()]:
                    raise ValueError("Effective range greater than range of type")

        for k in columnProps.keys():
            ensure(k in ColumnSpecOptions._ALLOWED_PROPERTIES or k in ColumnSpecOptions._PROPERTY_ALIASES,
                   f"invalid column option {k}")

        for arg in self._REQUIRED_PROPERTIES:
            ensure(columnProps.get(arg) is not None, f"missing column option {arg}")

        for arg in self._FORBIDDEN_PROPERTIES:
            ensure(arg not in columnProps, f"forbidden column option {arg}")

        # check weights and values
        if 'weights' in columnProps:
            ensure('values' in columnProps,
                   f"weights are only allowed for columns with values - column '{columnProps['name']}' ")
            ensure(columnProps['values'] is not None and len(columnProps['values']) > 0,
                   f"weights must be associated with non-empty list of values - column '{columnProps['name']}' ")
            ensure(len(columnProps['values']) == len(columnProps['weights']),
                   f"length(list of weights) must equal length(list of values) - column '{columnProps['name']}' ")
