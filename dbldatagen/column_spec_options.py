# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
.. title::Column Spec Options
This file defines the `ColumnSpecOptions` class
"""

from .utils import ensure


class ColumnSpecOptions(object):
    """ Column spec suppliedOptions object - manages suppliedOptions for column specs.

    This class has limited functionality - mainly used to validate and document the suppliedOptions,
    and the class is meant for internal use only.

    :param props: Used to pass list of properties for column generation spec property checking.

    The following suppliedOptions are permitted on data generator `withColumn`, `withColumnSpec` and `withColumnSpecs` methods:

    :param name: Column name

    :param type: Data type of column. Can be either instance of Spark SQL Datatype such as `IntegerType()`
                 or string containing SQL name of type

    :param minValue: Minimum value for range of generated value.
                     As an alternative, you may use the `dataRange` parameter

    :param maxValue: Maximum value for range of generated value.
                     As an alternative, you may use the `dataRange` parameter

    :param step: Step to use for range of generated value. As an alternative, you may use the `dataRange` parameter

    :param random: If True, will generate random values for column value. Defaults to `False`

    :param baseColumn: Either the string name of the base column, or a list of columns to use to
                        control data generation.

    :param values: List of discrete values for the colummn. Discrete values for the column can be strings, numbers
                   or constants conforming to type of column

    :param weights: List of discrete weights for the colummn. Should be integer values.
                    For example, you might declare a column for status values with a weighted distribution with
                    the following statement:
                    `withColumn("status", StringType(), values=['online', 'offline', 'unknown'], weights=[3,2,1])`

    :param percentNulls: Specifies numeric percentage of generated values to be populated with SQL `null`.
                          For example: `percentNulls=12`

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

    .. note::
        If the `dataRange` parameter is specified as well as the `minValue`, `maxValue` or `step`,
        the results are undetermined.

        For more information, see :doc:`/reference/api/dbldatagen.daterange`
        or :doc:`/reference/api/dbldatagen.nrange`.

    """

    #: the set of attributes that must be present for any columns
    _required_properties = {'name', 'type'}

    # alternate names for properties
    _property_aliases = {
        'max': 'maxValue',
        'min': 'minValue',
        'base_column': 'baseColumn',
        'base_columns': 'baseColumns',
        'base_column_type': 'baseColumnType',
        'unique_values': 'uniqueValues',
        'data_range': 'dataRange',
        'percent_nulls': 'percentNulls',
        'random_seed_method': 'randomSeedMethod',
        'random_seed': 'randomSeed',
        'text_separator': 'textSeparator'
    }

    #: the set of attributes that are permitted for any call to data generator `withColumn` or `withColumnSpec`
    _allowed_properties = {'name', 'type', 'minValue', 'maxValue', 'minValue', 'maxValue', 'step',
                          'prefix', 'random', 'distribution',
                          'range', 'baseColumn', 'baseColumnType', 'values', 'baseColumns',
                          'numColumns', 'numFeatures', 'structType',
                          'begin', 'end', 'interval', 'expr', 'omit',
                          'weights', 'description', 'continuous',
                          'percentNulls', 'template', 'format',
                          'uniqueValues', 'dataRange', 'text',
                          'precision', 'scale',
                          'randomSeedMethod', 'randomSeed',
                          'nullable', 'implicit',
                           'suffix', 'textSeparator'

                           }

    #: the set of disallowed column attributes for any call to data generator `withColumn` or `withColumnSpec`
    _forbidden_properties = {
        'range'
    }

    #: maxValue values for each column type, only if where value is intentionally restricted
    _max_type_range = {
        'byte': 256,
        'short': 65536
    }

    def __init__(self, props):  # TODO: check if additional suppliedOptions are needed here as `**kwArgs`
        self._column_spec_options = props

        self._replaceAliasedOptions()

    def _replaceAliasedOptions(self):
        ''' Replace each of the property entries with the old key names
            using the aliased key names

            Used to migrate suppliedOptions to consistent use of camelCase names
        '''
        # for each of the aliases
        for key in self._property_aliases:
            # if key is in the existing properties
            if key in self._column_spec_options:
                # add an entry for the alias with the existing value ..
                newKey = self._property_aliases[key]
                existingValue = self._column_spec_options[key]
                self._column_spec_options[newKey] = existingValue
                # and remove the old key entry
                del self._column_spec_options[key]



    def _getOrElse(self, key, default=None):
        """ Get val for key if it exists or else return default"""
        return self._column_spec_options.get(key, default)

    def __getitem__(self, key):
        """ implement the built in dereference by key behavior """
        ensure(key is not None, "key should be non-empty")

        result = None
        # if key in the suppliedOptions, get it
        if key in self._column_spec_options:
            result = self._column_spec_options.get(key, None)
        # otherwise if the key is one of the aliased suppliedOptions, process it
        elif key in self._property_aliases:
            newKey = self._property_aliases[key]
            if newKey in self._column_spec_options:
                result = self._column_spec_options.get(newKey, None)
        return result

    def __getattr__(self, name):
        """ get property dereference for options"""
        assert name is not None and len(name.strip()) > 0, "attribute must be valid non empty option name"
        assert name in self._allowed_properties, f"attribute `{name}` must be valid non empty option name"
        return self._column_spec_options.get(name, None)

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
                   "Option `{}` must be boolean if specified - value: {}, type: {}".format(name, v, type(v)))
        else:
            ensure(type(v) is bool,
                   "Option `{}` must be boolean  - value: {}, type: {}".format(name, v, type(v)))

    def checkExclusiveOptions(self, options):
        """check if the suppliedOptions are exclusive - i.e only one is not None

        :param options: list of suppliedOptions that will be mutually exclusive
        """
        assert options is not None, "suppliedOptions must be non empty"
        assert type(options) is list, "`suppliedOptions` must be list"
        assert len([self[x] for x in options if self[x] is not None]) <= 1, \
            f" only one of of the suppliedOptions: {options} may be specified "

    def checkOptionValues(self, option, option_values):
        """check if option value is in list of values

        :param option: list of suppliedOptions that will be mutually exclusive
        :param option_values: list of possible option values that will be mutually exclusive
        """
        assert option is not None and len(option.strip()) > 0, "option must be non empty"
        assert type(option_values) is list, "`option_values` must be list"
        assert self[option] in option_values, "option: `{}` must have one of the values {}".format(option,
                                                                                                   option_values)

    def checkValidColumnProperties(self, column_props):
        """
            check that column definition properties are recognized
            and that the column definition has required properties
        """
        ensure(column_props is not None, "columnProps should be non-empty")

        col_type = self['type']
        if col_type.typeName() in self._max_type_range:
            minValue = self['minValue']
            maxValue = self['maxValue']

            if minValue is not None and maxValue is not None:
                effective_range = maxValue - minValue
                if effective_range > self._max_type_range[col_type.typeName()]:
                    raise ValueError("Effective range greater than range of type")

        for k in column_props.keys():
            self.checkIfOptionAllowed(k)

        self.checkForRequiredProperties(column_props)

        self.checkForForbiddenProperties(column_props)

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

    def checkIfOptionAllowed(self, optionName):
        """
        check if option is allowed
        :param optionName: name of option
        :raises: DataGenError exception if option is not allowed or supported
        :return: nothing
        """
        assert optionName is not None, "option name must be non null"
        ensure(optionName in self._allowed_properties
               or optionName in self._property_aliases.keys(), 'invalid column option {0}'.format(optionName))

    def checkForRequiredProperties(self, suppliedOptions):
        """ Check list of options for required properties expected to have value that is not None

        :param suppliedOptions: dictionary of options (key is option name, value is option value)
        :return: nothing
        :raises: DataGenError exception if required option is not present
        """
        assert suppliedOptions is not None, "suppliedOptions must be non null"
        for arg in self._required_properties:
            ensure(arg in suppliedOptions.keys() and suppliedOptions[arg] is not None,
                   'missing column option {0}'.format(arg))

    def checkForForbiddenProperties(self, suppliedOptions):
        """ Check list of options for forbidden properties

        :param suppliedOptions: dictionary of options (key is option name, value is option value)
        :return: nothing
        :raises: DataGenError exception if forbidden option is  present
        """
        assert suppliedOptions is not None, "suppliedOptions must be non null"
        for arg in self._forbidden_properties:
            ensure(arg not in suppliedOptions.keys(),
                   'forbidden column option {0}'.format(arg))
