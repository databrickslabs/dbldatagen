# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
.. title::Column Spec Options
This file defines the `ColumnSpecOptions` class
"""

from .utils import ensure


class ColumnSpecOptions(object):
    """ Column spec options object - manages options for column specs.

    This class has limited functionality - mainly used to validate and document the options,
    and the class is meant for internal use only.

    :param props: Used to pass list of properties for column generation spec property checking.

    The following options are permitted on data generator `withColumn`, `withColumnSpec` and `withColumnSpecs` methods:

    :param name: Column name

    :param type: Data type of column. Can be either instance of Spark SQL Datatype such as `IntegerType()`
                 or string containing SQL name of type

    :param min: Minimum value for range of generated value. As an alternative, you may use the `data_range` parameter

    :param max: Maximum value for range of generated value. As an alternative, you may use the `data_range` parameter

    :param step: Step to use for range of generated value. As an alternative, you may use the `data_range` parameter

    :param random: If True, will generate random values for column value. Defaults to `False`

    :param base_column: Either the string name of the base column, or a list of columns to use to
                        control data generation.

    :param values: List of discrete values for the colummn. Discrete values for the column can be strings, numbers
                   or constants conforming to type of column

    :param weights: List of discrete weights for the colummn. Should be integer values.
                    For example, you might declare a column for status values with a weighted distribution with
                    the following statement:
                    `withColumn("status", StringType(), values=['online', 'offline', 'unknown'], weights=[3,2,1])`

    :param percent_nulls: Specifies numeric percentage of generated values to be populated with SQL `null`.
                          For example: `percent_nulls=12`

    :param unique_values: Number of unique values for column.
                          If the unique values are specified for a timestamp or date field, the values will be chosen
                          working back from the end of the previous month,
                          unless `begin`, `end` and `interval` parameters are specified

    :param begin: Beginning of range for date and timestamp fields.
                   For dates and timestamp fields, use the `begin`, `end` and `interval`
                   or `data_range` parameters instead of `min`, `max` and `step`

    :param end: End of range for date and timestamp fields.
                   For dates and timestamp fields, use the `begin`, `end` and `interval`
                   or `data_range` parameters instead of `min`, `max` and `step`

    :param interval: Interval of range for date and timestamp fields.
                   For dates and timestamp fields, use the `begin`, `end` and `interval`
                   or `data_range` parameters instead of `min`, `max` and `step`

    :param data_range: An instance of an `NRange` or `DateRange` object. This can be used in place of `min`, `max`,
                       `step` or `begin`, `end`, `interval`.

    :param template: template controlling how text should be generated

    :param text_separator: string specifying separator to be used when constructing strings with prefix and suffix

    :param prefix: string specifying prefix text to construct field from prefix and numeric value. Both `prefix` and
    `suffix` can be used together

    :param suffix: string specifying suffix text to construct field from suffix and numeric value. Both `prefix` and
    `suffix` can be used together

    .. note::
        If the `data_range` parameter is specified as well as the `min`, `max` or `step`, the results are undetermined.
        For more information, see :doc:`/reference/api/databrickslabs_testdatagenerator.daterange`
        or :doc:`/reference/api/databrickslabs_testdatagenerator.nrange`.

    """

    #: the set of attributes that must be present for any columns
    required_properties = {'name', 'type'}

    #: the set of attributes that are permitted for any call to data generator `withColumn` or `withColumnSpec`
    allowed_properties = {'name', 'type', 'min', 'max', 'step',
                          'prefix', 'random', 'distribution',
                          'range', 'base_column', 'base_column_type', 'values', 'base_columns',
                          'numColumns', 'numFeatures', 'structType',
                          'begin', 'end', 'interval', 'expr', 'omit',
                          'weights', 'description', 'continuous',
                          'percent_nulls', 'template', 'format',
                          'unique_values', 'data_range', 'text',
                          'precision', 'scale',
                          'random_seed_method', 'random_seed',
                          'nullable', 'implicit',
                          'suffix', 'text_separator'

                          }

    #: the set of disallowed column attributes for any call to data generator `withColumn` or `withColumnSpec`
    forbidden_properties = {
        'range'
    }

    #: max values for each column type, only if where value is intentionally restricted
    _max_type_range = {
        'byte': 256,
        'short': 65536
    }

    def __init__(self, props, **kwargs):
        self._column_spec_options = props

    def _getOrElse(self, key, default=None):
        """ Get val for key if it exists or else return default"""
        return self._column_spec_options.get(key, default)

    def __getitem__(self, key):
        """ implement the built in dereference by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self._column_spec_options.get(key, None)

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
                   "Option `{}` must be boolean if specified - value: {}, type:".format(name, v, type(v)))
        else:
            ensure(type(v) is bool,
                   "Option `{}` must be boolean  - value: {}, type:".format(name, v, type(v)))

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
        assert self[option] in option_values, "option: `{}` must have one of the values {}".format(option,
                                                                                                   option_values)

    def checkValidColumnProperties(self, column_props):
        """
            check that column definition properties are recognized
            and that the column definition has required properties
        """
        ensure(column_props is not None, "column_props should be non-empty")

        col_type = self['type']
        if col_type.typeName() in self._max_type_range:
            min = self['min']
            max = self['max']

            if min is not None and max is not None:
                effective_range = max - min
                if effective_range > self._max_type_range[col_type.typeName()]:
                    raise ValueError("Effective range greater than range of type")

        for k in column_props.keys():
            ensure(k in ColumnSpecOptions.allowed_properties, 'invalid column option {0}'.format(k))

        for arg in self.required_properties:
            ensure(arg in column_props.keys() and column_props[arg] is not None,
                   'missing column option {0}'.format(arg))

        for arg in self.forbidden_properties:
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
