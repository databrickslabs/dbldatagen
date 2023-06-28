# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `ColumnGenerationSpec` class
"""

import copy
import logging

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.functions import lit, concat, rand, round as sql_round, array, expr, when, udf, \
    format_string

import pyspark.sql.functions as F

from pyspark.sql.types import FloatType, IntegerType, StringType, DoubleType, BooleanType, \
    TimestampType, DataType, DateType, ArrayType, MapType, StructType

from .column_spec_options import ColumnSpecOptions
from .datagen_constants import RANDOM_SEED_FIXED, RANDOM_SEED_HASH_FIELD_NAME, RANDOM_SEED_RANDOM, \
    DEFAULT_SEED_COLUMN, OPTION_RANDOM, OPTION_RANDOM_SEED, OPTION_RANDOM_SEED_METHOD, INFER_DATATYPE

from .daterange import DateRange
from .distributions import Normal, DataDistribution
from .nrange import NRange
from .text_generators import TemplateGenerator
from .utils import ensure, coalesce_values
from .schema_parser import SchemaParser

HASH_COMPUTE_METHOD = "hash"
VALUES_COMPUTE_METHOD = "values"
RAW_VALUES_COMPUTE_METHOD = "raw_values"
AUTO_COMPUTE_METHOD = "auto"
EXPR_OPTION = "expr"
COMPUTE_METHOD_VALID_VALUES = [HASH_COMPUTE_METHOD,
                               AUTO_COMPUTE_METHOD,
                               VALUES_COMPUTE_METHOD,
                               RAW_VALUES_COMPUTE_METHOD]


class ColumnGenerationSpec(object):
    """ Column generation spec object - specifies how column is to be generated

    Each column to be output will have a corresponding ColumnGenerationSpec object.
    This is added explicitly using the DataGenerators `withColumnSpec` or `withColumn` methods

    If none is explicitly added, a default one will be generated.

    The full set of arguments to the class is more than the explicitly called out parameters as any
    arguments that are not explicitly called out can still be passed due to the `**kwargs` expression.

    This class is meant for internal use only.

    :param name: Name of column (string).
    :param colType: Spark SQL datatype instance, representing the type of the column.
    :param min: minimum value of column
    :param max: maximum value of the column
    :param step: numeric step used in column data generation
    :param prefix: string used as prefix to the column underlying value to produce a string value
    :param random: Boolean, if True, will generate random values
    :param distribution: Instance of distribution, that will control the distribution of the generated values
    :param baseColumn: String or list of strings representing columns used as basis for generating the column data
    :param randomSeed: random seed value used to generate the random value, if column data is random
    :param randomSeedMethod: method for computing random values from the random seed. It may take on the
           values `fixed`, `hash_fieldname` or None

    :param implicit: If True, the specification for the column can be replaced by a later definition.
           If not, a later attempt to replace the definition will flag an error.
           Typically used when generating definitions automatically from a schema, or when using wildcards
           in the specification

    :param omit: if True, omit from the final output.
    :param nullable: If True, column may be null - defaults to True.
    :param debug: If True, output debugging log statements. Defaults to False.
    :param verbose: If True, output logging statements at the info level. If False (the default),
                    only output warning and error logging statements.
    :param seedColumnName: if supplied, specifies seed column name

    For full list of options, see :doc:`/reference/api/dbldatagen.column_spec_options`.
    """

    #: maxValue values for each column type, only if where value is intentionally restricted
    _max_type_range = {
        'byte': 256,
        'short': 65536
    }

    _ARRAY_STRUCT_TYPE = "array"

    # set up logging

    # restrict spurious messages from java gateway
    logging.getLogger("py4j").setLevel(logging.WARNING)

    def __init__(self, name, colType=None, minValue=0, maxValue=None, step=1, prefix='', random=False,
                 distribution=None, baseColumn=None, randomSeed=None, randomSeedMethod=None,
                 implicit=False, omit=False, nullable=True, debug=False, verbose=False,
                 seedColumnName=DEFAULT_SEED_COLUMN,
                 **kwargs):

        # set up logging
        self.verbose = verbose
        self.debug = debug

        self._setup_logger()

        # set up default range and type for column
        self._dataRange = NRange(None, None, None)  # by default range of values for  column is unconstrained

        self._inferDataType = False
        if colType is None:  # default to integer field if none specified
            colType = IntegerType()
        elif colType == INFER_DATATYPE:
            colType = StringType()  # default inferred data type to string until exact type is known
            self._inferDataType = True

            if EXPR_OPTION not in kwargs:
                raise ValueError("Column generation spec must have `expr` attribute specified if datatype is inferred")

        elif type(colType) == str:
            colType = SchemaParser.columnTypeFromString(colType)

        assert isinstance(colType, DataType), f"colType `{colType}` is not instance of DataType"

        self._initialBuildPlan = []  # the build plan for the column - descriptive only
        self.executionHistory = []  # the execution history for the column

        self._seedColumnName = seedColumnName

        # If no base column is specified, assume its dependent on the seed column
        if baseColumn is None:
            baseColumn = self._seedColumnName

        # to allow for open ended extension of many column attributes, we use a few specific
        # parameters and pass the rest as keyword arguments
        supplied_options = {'name': name, 'minValue': minValue, 'type': colType,
                            'maxValue': maxValue, 'step': step,
                            'prefix': prefix, 'baseColumn': baseColumn,
                            OPTION_RANDOM: random, 'distribution': distribution,
                            OPTION_RANDOM_SEED_METHOD: randomSeedMethod, OPTION_RANDOM_SEED: randomSeed,
                            'omit': omit, 'nullable': nullable, 'implicit': implicit
                            }

        supplied_options.update(kwargs)

        self._csOptions = ColumnSpecOptions(supplied_options)

        self._csOptions.checkValidColumnProperties(supplied_options)

        # only allow `template` or `text`
        self._csOptions.checkExclusiveOptions(["template", "text"])

        # only allow `weights` or `distribution`
        self._csOptions.checkExclusiveOptions(["distribution", "weights"])

        # check for alternative forms of specifying range
        # column_spec_options._checkExclusiveOptions(["minValue", "minValue", "begin", "dataRange"])
        # column_spec_options._checkExclusiveOptions(["maxValue", "maxValue", "end", "dataRange"])
        # column_spec_options._checkExclusiveOptions(["step", "interval", "dataRange"])

        # we want to assign each of the properties to the appropriate instance variables
        # but compute sensible defaults in the process as needed
        # in particular, we want to ensure that things like values and weights match
        # and that minValue and maxValue are not inconsistent with distributions, ranges etc

        # if a column spec is implicit, it can be overwritten
        # by default column specs added by wild cards or inferred from schemas are implicit
        self._csOptions.checkBoolOption(implicit, name="implicit")
        self.implicit = implicit

        # if true, omit the column from the final output

        self._csOptions.checkBoolOption(omit, name="omit")
        self.omit = omit

        # the column name
        self.name = name

        # the base column data types
        self._baseColumnDatatypes = []

        # not used for much other than to validate against option to generate nulls
        self._csOptions.checkBoolOption(nullable, name="nullable")
        self.nullable = nullable

        # should be either a literal or None
        # use of a random seed method will ensure that we have repeatability of data generation
        assert randomSeed is None or type(randomSeed) in [int, float], "seed should be None or numeric"

        assert randomSeedMethod is None or randomSeedMethod in [RANDOM_SEED_FIXED, RANDOM_SEED_HASH_FIELD_NAME], \
            f"`randomSeedMethod` should be none or `{RANDOM_SEED_FIXED}` or `{RANDOM_SEED_HASH_FIELD_NAME}`"

        self._randomSeedMethod = self[OPTION_RANDOM_SEED_METHOD]
        self.random = self[OPTION_RANDOM]

        if self._randomSeedMethod == RANDOM_SEED_HASH_FIELD_NAME:
            assert self.name is not None, "field name cannot be None"
            self._randomSeed = abs(hash(self.name))
        else:
            self._randomSeed = self[OPTION_RANDOM_SEED]

        # random seed method should be "fixed" or "hash_fieldname"
        if self._randomSeed is not None and self._randomSeedMethod is None:
            self._randomSeedMethod = RANDOM_SEED_FIXED

        # compute dependencies
        self.dependencies = self._computeBasicDependencies()

        # value of `base_column_type` must be `None`,"values", "raw_values", "auto",  or "hash"
        # this is the method of computing current column value from base column, not the data type of the base column
        allowed_compute_methods = [AUTO_COMPUTE_METHOD, VALUES_COMPUTE_METHOD, HASH_COMPUTE_METHOD,
                                   RAW_VALUES_COMPUTE_METHOD, None]
        self._csOptions.checkOptionValues("baseColumnType", allowed_compute_methods)
        self._baseColumnComputeMethod = self['baseColumnType']

        # handle text generation templates
        if self['template'] is not None:
            assert isinstance(self['template'], str), "template must be a string "
            escapeSpecialChars = self['escapeSpecialChars'] if self['escapeSpecialChars'] is not None else False
            self._textGenerator = TemplateGenerator(self['template'], escapeSpecialChars)
        elif self['text'] is not None:
            self._textGenerator = copy.deepcopy(self['text'])
        else:
            self._textGenerator = None

        # specify random seed for text generator if one is in effect
        if self._textGenerator is not None and self._randomSeed is not None:
            self._textGenerator = self._textGenerator.withRandomSeed(self._randomSeed)

        # compute required temporary values
        self.temporaryColumns = []

        data_range = self["dataRange"]

        unique_values = self["uniqueValues"]

        c_min, c_max, c_step = (self["minValue"], self["maxValue"], self["step"])
        c_begin, c_end, c_interval = self['begin'], self['end'], self['interval']

        # handle weights / values and distributions
        self.weights, self.values = (self["weights"], self["values"])

        self.distribution = self["distribution"]

        # if distribution is just specified as `normal` use standard normal distribution
        if self.distribution == "normal":
            self.distribution = Normal.standardNormal()

        # specify random seed for distribution if one is in effect
        if self.distribution is not None and self._randomSeed is not None:
            self.distribution = self.distribution.withRandomSeed(self._randomSeed)

        # force weights and values to list
        if self.weights is not None:
            # coerce to list - this will allow for pandas series, numpy arrays and tuples to be used
            self.weights = list(self.weights)

        if self.values is not None:
            # coerce to list - this will allow for pandas series, numpy arrays and tuples to be used
            self.values = list(self.values)

        # handle default method of computing the base column value
        # if we have text manipulation, use 'values' as default for format but 'hash' as default if
        # its a column with multiple values
        if self._baseColumnComputeMethod in [None, AUTO_COMPUTE_METHOD] \
                and (self.textGenerator is not None or self['format'] is not None
                     or self['prefix'] is not None or self['suffix'] is not None):
            if self.values is not None:
                self.logger.info("""Column [%s] has no `base_column_type` attribute and uses discrete values
                                       => Assuming `hash` for attribute `base_column_type`. 
                                       => Use explicit value for `base_column_type` if alternate interpretation needed

                                    """, self.name)
                self._baseColumnComputeMethod = HASH_COMPUTE_METHOD
            else:
                self.logger.info("""Column [%s] has no `base_column_type` attribute specified for formatted text
                                       => Assuming `values` for attribute `base_column_type`. 
                                       => Use explicit value for `base_column_type` if alternate interpretation  needed

                                    """, self.name)
                self._baseColumnComputeMethod = VALUES_COMPUTE_METHOD

        # adjust the range by merging type and range information

        self._dataRange = self._computeAdjustedRangeForColumn(colType=colType,
                                                              c_min=c_min, c_max=c_max, c_step=c_step,
                                                              c_begin=c_begin, c_end=c_end,
                                                              c_interval=c_interval,
                                                              c_unique=unique_values, c_range=data_range)

        if self.distribution is not None:
            ensure((self._dataRange is not None and self._dataRange.isFullyPopulated())
                   or
                   self.values is not None,
                   """When using an explicit distribution, provide a fully populated range or a set of values""")

        # set up the temporary columns needed for data generation
        self._setupTemporaryColumns()

    def _temporaryRename(self, tmpName):
        """ Create enter / exit object to support temporary renaming of column spec

        This is to support the functionality:

        ```
           with columSpec._temporaryRename("test") as modifiedColumn:
             modifiedColumn.doSomthing()
        ```

        When building array or multi-column valued columns, we rename the column definition temporarily
        to ensure that logging, saving messages to the execution plan and random number generation based on
        the field name work correctly

        :param tmpName: temporary name for the column.
        :return: object supporting `with columSpec._temporaryRename("test") as modifiedColumn:` semantics

        .. note::
           This does not create a copy of the column spec object

        """
        # create class for temporary rename enter / exit
        assert tmpName is not None and len(tmpName) > 0, "method expects valid temporary name"

        class RenameEnterExit:
            def __init__(self, columnSpec, newName):
                """ Save column spec and old name to support enter / exit semantics """
                self._cs = columnSpec
                self._oldName = columnSpec.name
                self._newName = newName
                self._randomSeed = columnSpec._randomSeed

            def __enter__(self):
                """ Return the inner column spec object """
                self._cs.name = self._newName

                if self._cs._randomSeedMethod == RANDOM_SEED_HASH_FIELD_NAME:
                    self._cs._randomSeed = abs(hash(self._cs.name))

                    if self._cs._textGenerator is not None and self._cs._randomSeed is not None:
                        self._cs._textGenerator = self._cs._textGenerator.withRandomSeed(self._cs._randomSeed)

                return self._cs

            def __exit__(self, exc_type, exc_value, tb):
                # restore old name
                self._cs.name = self._oldName

                # restore old random seed
                self._cs._randomSeed = self._randomSeed

                if self._cs._randomSeedMethod == RANDOM_SEED_HASH_FIELD_NAME:
                    if self._cs._textGenerator is not None and self._cs._randomSeed is not None:
                        self._cs._textGenerator = self._cs._textGenerator.withRandomSeed(self._cs._randomSeed)

                if exc_type is not None:
                    # uncomment for traceback
                    # import traceback
                    # traceback.print_exception(exc_type, exc_value, tb)
                    return False

                return True

        return RenameEnterExit(self, tmpName)

    @property
    def specOptions(self):
        """ get column spec options for spec

        .. note::
            This is intended for testing use only.
            Option values set directly through the options dict are not supported.

        :return: underlying options object
        """
        return self._csOptions.options

    def __deepcopy__(self, memo):
        """Custom deep copy method that resets the logger to avoid trying to copy the logger

        :see https://docs.python.org/3/library/copy.html
        """
        self.logger = None  # pylint: disable=attribute-defined-outside-init
        result = None

        try:
            cls = self.__class__
            result = cls.__new__(cls)
            memo[id(self)] = result
            for k, v in self.__dict__.items():
                setattr(result, k, copy.deepcopy(v, memo))
        finally:
            self._setup_logger()
            if result is not None:
                result._setup_logger()
        return result

    @property
    def randomSeed(self):
        """ get random seed for column spec"""
        return self._randomSeed

    @property
    def isRandom(self):
        """ returns True if column will be randomly generated"""
        return self[OPTION_RANDOM]

    @property
    def textGenerator(self):
        """ Get the text generator for the column spec"""
        return self._textGenerator

    @property
    def inferDatatype(self):
        """ If True indicates that datatype should be inferred to be result of computing SQL expression
        """
        return self._inferDataType

    @property
    def baseColumns(self):
        """ Return base columns as list of strings"""

        # if base column  is string and contains multiple columns, split them
        # other build list of columns if needed
        if type(self.baseColumn) is str and "," in self.baseColumn:
            return [x.strip() for x in self.baseColumn.split(",")]
        elif type(self.baseColumn) is list:
            return self.baseColumn
        else:
            return [self.baseColumn]

    def _computeBasicDependencies(self):
        """ get set of basic column dependencies.

        These are used to compute the order of field evaluation

        :return: base columns as list with dependency on seed column added
        """
        if self.baseColumn != self._seedColumnName:
            return list(set(self.baseColumns + [self._seedColumnName]))
        else:
            return [self._seedColumnName]

    def setBaseColumnDatatypes(self, columnDatatypes):
        """ Set the data types for the base columns

        :param column_datatypes: = list of data types for the base columns

        """
        assert type(columnDatatypes) is list, " `column_datatypes` parameter must be list"
        ensure(len(columnDatatypes) == len(self.baseColumns),
               "number of base column datatypes must match number of  base columns")
        self._baseColumnDatatypes = [].append(columnDatatypes)

    def _setupTemporaryColumns(self):
        """ Set up any temporary columns needed for test data generation.

        For some types of test data, intermediate columns are used in the data generation process
        but dropped from the final output
        """
        if self.isWeightedValuesColumn:
            # if its a weighted values column, then create temporary for it
            # not supported for feature / array columns for now
            min_num_columns, max_num_columns, struct_type = self._getMultiColumnDetails(validate=False)
            ensure(max_num_columns is None or max_num_columns <= 1,
                   "weighted columns not supported for multi-column or multi-feature values")
            if self.random:
                temp_name = f"_rnd_{self.name}"
                self.dependencies.append(temp_name)
                desc = f"adding temporary column {temp_name} required by {self.name}"
                self._initialBuildPlan.append(desc)
                sql_random_generator = self._getUniformRandomSQLExpression(self.name)
                self.temporaryColumns.append((temp_name, DoubleType(), {'expr': sql_random_generator, 'omit': True,
                                                                        'description': desc}))
                self._weightedBaseColumn = temp_name
            else:
                # create temporary expression mapping values to range of weights
                temp_name = f"_scaled_{self.name}"
                self.dependencies.append(temp_name)
                desc = f"adding temporary column {temp_name} required by {self.name}"
                self._initialBuildPlan.append(desc)

                # use a base expression based on mapping base column to size of data
                sql_scaled_generator = self._getScaledIntSQLExpression(self.name,
                                                                       scale=sum(self.weights),
                                                                       base_columns=self.baseColumns,
                                                                       base_datatypes=self._baseColumnDatatypes,
                                                                       compute_method=self._baseColumnComputeMethod,
                                                                       normalize=True)

                self.logger.debug("""building scaled sql expression : '%s' 
                                      with base column: %s, dependencies: %s""",
                                  sql_scaled_generator,
                                  self.baseColumn,
                                  self.dependencies)

                self.temporaryColumns.append((temp_name, DoubleType(), {'expr': sql_scaled_generator, 'omit': True,
                                                                        'baseColumn': self.baseColumn,
                                                                        'description': desc}))
                self._weightedBaseColumn = temp_name

    def _setup_logger(self):
        """Set up logging

        This will set the logger at warning, info or debug levels depending on the instance construction parameters
        """
        self.logger = logging.getLogger("DataGenerator")
        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        elif self.verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARNING)

    def _computeAdjustedRangeForColumn(self, colType, c_min, c_max, c_step, c_begin, c_end, c_interval, c_range,
                                       c_unique):
        """Determine adjusted range for data column
        """
        assert colType is not None, "`colType` must be non-None instance"

        if type(colType) is DateType or type(colType) is TimestampType:
            return self._computeAdjustedDateTimeRangeForColumn(colType, c_begin, c_end, c_interval, c_range, c_unique)
        else:
            return self._computeAdjustedNumericRangeForColumn(colType, c_min, c_max, c_step, c_range, c_unique)

    def _computeAdjustedNumericRangeForColumn(self, colType, c_min, c_max, c_step, c_range, c_unique):
        """Determine adjusted range for data column

        Rules:
        - if a datarange is specified , use that range
        - if begin and end are specified or minValue and maxValue are specified, use that
        - if unique values is specified, compute minValue and maxValue depending on type

        """
        if c_unique is not None:
            assert type(c_unique) is int, "unique_values must be integer"
            assert c_unique >= 1, "if supplied, unique values must be > 0"
            # TODO: set maxValue to unique_values + minValue & add unit test
            effective_min, effective_max, effective_step = None, None, None
            if c_range is not None and type(c_range) is NRange:
                effective_min = c_range.minValue
                effective_step = c_range.step
                effective_max = c_range.maxValue
            effective_min = coalesce_values(effective_min, c_min, 1)
            effective_step = coalesce_values(effective_step, c_step, 1)
            effective_max = coalesce_values(effective_max, c_max)

            # due to floating point errors in some Python floating point calculations, we need to apply rounding
            # if any of the components are float
            if type(effective_min) is float or type(effective_step) is float:
                unique_max = round(c_unique * effective_step + effective_min - effective_step, 9)
            else:
                unique_max = c_unique * effective_step + effective_min - effective_step
            result = NRange(effective_min, unique_max, effective_step)

            if result.maxValue is not None and effective_max is not None and result.maxValue > effective_max:
                self.logger.warning("Computed maxValue for column [%s] of %s is greater than specified maxValue %s",
                                    self.name,
                                    result.maxValue,
                                    effective_max)
        elif c_range is not None:
            result = c_range
        elif c_range is None:
            effective_min, effective_max, effective_step = None, None, None
            effective_min = coalesce_values(c_min, 0)
            effective_step = coalesce_values(c_step, 1)
            result = NRange(effective_min, c_max, effective_step)
        else:
            result = NRange(0, None, None)
        # assume numeric range of 0 to x, if no range specified
        self.logger.debug("Computing adjusted range for column: %s - %s", self.name, result)

        return result

    def _computeAdjustedDateTimeRangeForColumn(self, colType, c_begin, c_end, c_interval, c_range, c_unique):
        """Determine adjusted range for Date or Timestamp data column
        """
        effective_begin, effective_end, effective_interval = None, None, None
        if c_range is not None and type(c_range) is DateRange:
            effective_begin = c_range.begin
            effective_end = c_range.end
            effective_interval = c_range.interval
        effective_interval = coalesce_values(effective_interval, c_interval)
        effective_end = coalesce_values(effective_end, c_end)
        effective_begin = coalesce_values(effective_begin, c_begin)

        if type(colType) is DateType:
            result = DateRange.computeDateRange(effective_begin, effective_end, effective_interval, c_unique)
        else:
            result = DateRange.computeTimestampRange(effective_begin, effective_end, effective_interval, c_unique)

        self.logger.debug("Computing adjusted range for column: %s - %s", self.name, result)
        return result

    def _getUniformRandomExpression(self, col_name):
        """ Get random expression accounting for seed method

        :returns: expression of ColDef form - i.e `lit`, `expr` etc

        The value returned will be a number between 0 and 1 inclusive
        """
        assert col_name is not None, "`col_name` must not be None"
        if self._randomSeedMethod == RANDOM_SEED_FIXED and self._randomSeed != RANDOM_SEED_RANDOM:
            return expr(f"rand({self._randomSeed})")
        elif self._randomSeedMethod == RANDOM_SEED_HASH_FIELD_NAME:
            assert self.name is not None, " `self.name` must not be none"
            return expr(f"rand(hash('{self.name}'))")
        else:
            return rand()

    def _getRandomExpressionForDistribution(self, col_name, col_distribution):
        """ Get random expression accounting for seed method

        :returns: expression of ColDef form - i.e `lit`, `expr` etc

        The value returned will be a number between 0 and 1 inclusive
        """
        assert col_name is not None and len(col_name) > 0, "`col_name` must not be None and non empty"
        assert col_distribution is not None, "`col_distribution` must not be None"
        assert isinstance(col_distribution, DataDistribution), \
            "`distribution` object must be an instance of data distribution"

        self.executionHistory.append(f".. random number generation via distribution `{col_distribution}`")

        return col_distribution.generateNormalizedDistributionSample()

    def _getUniformRandomSQLExpression(self, col_name):
        """ Get random SQL expression accounting for seed method

        :returns: expression as a SQL string
        """
        assert col_name is not None, " `col_name` must not be None"
        if self._randomSeedMethod == RANDOM_SEED_FIXED and self._randomSeed != RANDOM_SEED_RANDOM:
            assert self._randomSeed is not None, "`randomSeed` must not be None"
            return f"rand({self._randomSeed})"
        elif self._randomSeedMethod == RANDOM_SEED_HASH_FIELD_NAME:
            assert self.name is not None, "`self.name` must not be none"
            return f"rand(hash('{self.name}'))"
        else:
            return "rand()"

    def _getScaledIntSQLExpression(self, col_name, scale, base_columns, base_datatypes=None, compute_method=None,
                                   normalize=False):
        """ Get scaled numeric expression

        This will produce a scaled SQL expression from the base columns



        :param col_name: = Column name used for error messages and debugging
        :param normalize: = If True, will normalize to the range 0 .. 1 inclusive
        :param scale: = Numeric value indicating scaling factor - will scale via modulo arithmetic
        :param base_columns: = list of base_columns
        :param base_datatypes: = list of Spark SQL datatypes for columns
        :param compute_method: = indicates how the value is be derived from base columns - i.e 'hash' or 'values'
                               - treated as hint only
        :returns: scaled expression as a SQL string

        """
        assert col_name is not None, "`col_name` must not be None"
        assert self.name is not None, "`self.name` must not be None"
        assert scale is not None, "`scale` must not be None"
        assert (compute_method is None or
                compute_method in COMPUTE_METHOD_VALID_VALUES), "`compute_method` must be valid value "
        assert (base_columns is not None and
                type(base_columns) is list
                and len(base_columns) > 0), "Base columns must be a non-empty list"

        effective_compute_method = compute_method

        # if we have multiple columns, effective compute method is always the hash of the base values
        if len(base_columns) > 1:
            if compute_method == VALUES_COMPUTE_METHOD:
                self.logger.warning(
                    "For column generation with values and multiple base columns,  data will  be computed with `hash`")
            effective_compute_method = HASH_COMPUTE_METHOD

        if effective_compute_method is None or effective_compute_method is AUTO_COMPUTE_METHOD:
            effective_compute_method = VALUES_COMPUTE_METHOD

        column_set = ",".join(base_columns)

        if effective_compute_method == HASH_COMPUTE_METHOD:
            result = f"cast( floor((hash({column_set}) % {scale}) + {scale}) % {scale} as double)"
        else:
            result = f"cast( ( floor(({column_set} % {scale}) + {scale}) % {scale}) as double) "

        if normalize:
            result = f"({result} / {(scale * 1.0) - 1.0})"

        self.logger.debug("computing scaled field [%s] as expression [%s]", col_name, result)
        return result

    @property
    def isWeightedValuesColumn(self):
        """ check if column is a weighed values column """
        return self['weights'] is not None and self.values is not None

    def getNames(self):
        """ get column names as list of strings"""
        min_num_columns, max_num_columns, struct_type = self._getMultiColumnDetails(validate=False)

        if max_num_columns > 1 and struct_type is None:
            return [f"{self.name}_{x}" for x in range(0, max_num_columns)]
        else:
            return [self.name]

    def getNamesAndTypes(self):
        """ get column names as list of tuples `(name, datatype)`"""
        min_num_columns, max_num_columns, struct_type = self._getMultiColumnDetails(validate=False)

        if max_num_columns > 1 and struct_type is None:
            return [(f"{self.name}_{x}", self.datatype) for x in range(0, max_num_columns)]
        else:
            return [(self.name, self.datatype)]

    def keys(self):
        """ Get the keys as list of strings """
        assert self._csOptions is not None, "self._csOptions should be non-empty"
        return self._csOptions.keys()

    def __getitem__(self, key):
        """ implement the built in dereference by key behavior """
        assert key is not None, "key should be non-empty"
        return self._csOptions.getOrElse(key, None)

    @property
    def isFieldOmitted(self):
        """ check if this field should be omitted from the output

        If the field is omitted from the output, the field is available for use in expressions etc.
        but dropped from the final set of fields
        """
        return self.omit

    @property
    def baseColumn(self):
        """get the base column used to generate values for this column"""
        return self['baseColumn']

    @property
    def datatype(self):
        """get the Spark SQL data type used to generate values for this column"""
        return self['type']

    @property
    def prefix(self):
        """get the string prefix used to generate values for this column

        When a string field is generated from this spec, the prefix is prepended to the generated string
        """
        return self['prefix']

    @property
    def suffix(self):
        """get the string suffix used to generate values for this column

        When a string field is generated from this spec, the suffix is appended to the generated string
        """
        return self['suffix']

    @property
    def min(self):
        """get the column generation `minValue` value used to generate values for this column"""
        return self._dataRange.minValue

    @property
    def max(self):
        """get the column generation `maxValue` value used to generate values for this column"""
        return self['maxValue']

    @property
    def step(self):
        """get the column generation `step` value used to generate values for this column"""
        return self['step']

    @property
    def exprs(self):
        """get the column generation `exprs` attribute used to generate values for this column.
        """
        return self['exprs']

    @property
    def expr(self):
        """get the `expr` attributed used to generate values for this column"""
        return self['expr']

    @property
    def text_separator(self):
        """get the `expr` attributed used to generate values for this column"""
        return self['text_separator']

    @property
    def begin(self):
        """get the `begin` attribute used to generate values for this column

        For numeric columns, the range (minValue, maxValue, step) is used to control data generation.
        For date and time columns, the range (begin, end, interval) are used to control data generation
        """
        return self['begin']

    @property
    def end(self):
        """get the `end` attribute used to generate values for this column

        For numeric columns, the range (minValue, maxValue, step) is used to control data generation.
        For date and time columns, the range (begin, end, interval) are used to control data generation
        """
        return self['end']

    @property
    def interval(self):
        """get the `interval` attribute used to generate values for this column

        For numeric columns, the range (minValue, maxValue, step) is used to control data generation.
        For date and time columns, the range (begin, end, interval) are used to control data generation
        """
        return self['interval']

    @property
    def numColumns(self):
        """get the `numColumns` attribute used to generate values for this column

        if a column is specified with the `numColumns` attribute, this is used to create multiple
        copies of the column, named `colName1` .. `colNameN`
        """
        return self['numColumns']

    @property
    def numFeatures(self):
        """get the `numFeatures` attribute used to generate values for this column

        if a column is specified with the `numFeatures` attribute, this is used to create multiple
        copies of the column, combined into an array or feature vector
        """
        return self['numFeatures']

    def structType(self):
        """get the `structType` attribute used to generate values for this column

        When a column spec is specified to generate multiple copies of the column, this controls whether
        these are combined into an array etc
        """
        return self['structType']

    def getOrElse(self, key, default=None):
        """ Get value for option key if it exists or else return default

        :param key: key name for option
        :param default: default value if option was not provided
        :return: option value or default

        """
        return self._csOptions.getOrElse(key, default)

    def getPlanEntry(self):
        """ Get execution plan entry for object

        :returns: String representation of plan entry
        """
        desc = self['description']
        if desc is not None:
            return " |-- " + desc
        else:
            return f" |-- building column generator for column {self.name}"

    def _makeWeightedColumnValuesExpression(self, values, weights, seed_column_name):
        """make SQL expression to compute the weighted values expression

        :returns: Spark SQL expr
        """
        from .function_builder import ColumnGeneratorBuilder
        assert values is not None, "`values` expression must be supplied as list of values"
        assert weights is not None, "`weights` expression must be list of weights"
        assert len(values) == len(weights), "`weights` and `values` lists must be of equal length"
        assert seed_column_name is not None, "`seed_column_name` must be explicit column name"
        expr_str = ColumnGeneratorBuilder.mkExprChoicesFn(values, weights, seed_column_name, self.datatype)
        return expr(expr_str).astype(self.datatype)

    def _isRealValuedColumn(self):
        """ determine if column is real valued

        :returns: Boolean - True if condition is true
        """
        col_type_name = self['type'].typeName()

        return col_type_name in ['double', 'float', 'decimal']

    def _isDecimalColumn(self):
        """ determine if column is decimal column

        :returns: Boolean - True if condition is true
        """
        col_type_name = self['type'].typeName()

        return col_type_name == 'decimal'

    def _isContinuousValuedColumn(self):
        """ determine if column generates continuous values

        :returns: Boolean - True if condition is true
        """
        is_continuous = self['continuous']

        return is_continuous

    def _getSeedExpression(self, base_column):
        """ Get seed expression for column generation

        This is used to generate the base value for every column
        if using a single base column, then simply use that, otherwise use either
        a SQL hash of multiple columns, or an array of the base column values converted to strings

        :returns: Spark SQL `col` or `expr` object
        """

        if type(base_column) is list:
            assert len(base_column) > 0, "`baseColumn` must be list of column names"
            if len(base_column) == 1:
                if self._baseColumnComputeMethod == HASH_COMPUTE_METHOD:
                    return expr(f"hash({base_column[0]})")
                else:
                    return col(base_column[0])
            elif self._baseColumnComputeMethod == VALUES_COMPUTE_METHOD:
                base_values = [f"string(ifnull(`{x}`, 'null'))" for x in base_column]
                return expr(f"array({','.join(base_values)})")
            else:
                return expr(f"hash({','.join(base_column)})")
        else:
            if self._baseColumnComputeMethod == HASH_COMPUTE_METHOD:
                return expr(f"hash({base_column})")
            else:
                return col(base_column)

    def _isStringField(self):
        return type(self.datatype) is StringType

    def _computeRangedColumn(self, datarange, base_column, is_random):
        """ compute a ranged column

        maxValue is maxValue actual value

        :returns: spark sql `column` or expression that can be used to generate a column
        """
        assert base_column is not None, "`baseColumn` must be specified"
        assert datarange is not None, "`datarange` must be specified"
        assert datarange.isFullyPopulated(), "`datarange` must be fully populated (minValue, maxValue, step)"

        if is_random:
            if self.distribution is not None:
                random_generator = self._getRandomExpressionForDistribution(self.name, self.distribution)
            else:
                random_generator = self._getUniformRandomExpression(self.name)
        else:
            random_generator = None

        if self._isContinuousValuedColumn() and self._isRealValuedColumn() and is_random:
            crange = datarange.getContinuousRange()
            baseval = random_generator * lit(crange)
        else:
            crange = datarange.getDiscreteRange()
            modulo_factor = lit(crange + 1)
            # following expression is needed as spark sql modulo of negative number is negative
            modulo_exp = ((self._getSeedExpression(base_column) % modulo_factor) + modulo_factor) % modulo_factor
            baseval = (modulo_exp * lit(datarange.step)) if not is_random else (
                    sql_round(random_generator * lit(crange)) * lit(datarange.step))

        if self._baseColumnComputeMethod == VALUES_COMPUTE_METHOD:
            new_def = self._adjustForMinValue(baseval, datarange)
        elif self._baseColumnComputeMethod == RAW_VALUES_COMPUTE_METHOD:
            new_def = baseval
        else:
            new_def = self._adjustForMinValue(baseval, datarange, force=True)

        # for ranged values in strings, use type of minValue, maxValue and step as output type
        if type(self.datatype) is StringType:
            if type(datarange.min) is float or type(datarange.max) is float or type(datarange.step) is float:
                if datarange.getScale() > 0:
                    new_def = sql_round(new_def.astype(FloatType()), datarange.getScale())
                else:
                    new_def = new_def.astype(DoubleType())
            else:
                new_def = new_def.astype(IntegerType())

        return new_def

    def _adjustForMinValue(self, baseval, datarange, force=False):
        """ Adjust for minimum value of data range
        :param baseval: base expression
        :param datarange: data range to conform to
        :param force: always adjust (possibly for implicit cast reasons)
        """
        if force and datarange is not None:
            new_def = baseval + lit(datarange.minValue)
        elif (datarange is not None) and (datarange.minValue != 0) and (datarange.minValue != 0.0):
            new_def = baseval + lit(datarange.minValue)
        else:
            new_def = baseval
        return new_def

    def _makeSingleGenerationExpression(self, index=None, use_pandas_optimizations=True):
        """ generate column data for a single column value via Spark SQL expression

            :param index: for multi column generation, specifies index of column being generated
            :param use_pandas_optimizations: if True, uses Pandas vectorized optimizations. Defaults to `True`
            :returns: spark sql `column` or expression that can be used to generate a column
        """
        self.logger.debug("building column : %s", self.name)

        # get key column specification properties
        col_is_rand, cdistribution = self[OPTION_RANDOM], self['distribution']
        percent_nulls = self['percentNulls']
        sformat = self['format']

        if self._dataRange is not None:
            self._dataRange.adjustForColumnDatatype(self.datatype)
            self.executionHistory.append(f".. using effective range: {self._dataRange}")

        new_def = None

        # generate expression
        if type(self.datatype) in [ArrayType, MapType, StructType] and self.expr is None:
            self.logger.warning("Array, Map or Struct type column with no SQL `expr` will result in NULL value")
            self.executionHistory.append(".. WARNING: Array, Map or Struct type column with no SQL `expr` ")

        # handle weighted values for weighted value columns
        # a weighted values column will use a base value denoted by `self._weightedBaseColumn`
        if self.isWeightedValuesColumn:
            self.executionHistory.append(".. building weighted volumn values expression")
            new_def = self._makeWeightedColumnValuesExpression(self.values, self.weights, self._weightedBaseColumn)

            if type(self.datatype) is StringType and self.textGenerator is not None:
                self.logger.warning("Template generation / text generation not supported for weighted columns")
                self.executionHistory.append(".. WARNING: Template & text generation not supported for weights")

            if type(self.datatype) is StringType and sformat is not None:
                self.logger.warning("Formatting not supported for weighted columns")
                self.executionHistory.append(".. WARNING: Formatting not supported for weighted columns")
        else:
            # rs: initialize the begin, end and interval if not initialized for date computations
            # defaults are start of day, now, and 1 minute respectively

            # for array, struct and map types, either value is provided via `expr` or via values
            if not type(self.datatype) in [ArrayType, MapType, StructType] or self.values is not None:
                self._computeImpliedRangeIfNeeded(self.datatype)

            # TODO: add full support for date value generation
            if self.expr is not None:
                # note use of SQL expression ignores range specifications
                new_def = expr(self.expr)
                self.executionHistory.append(f".. using SQL expression `{self.expr}` as base")

                if not self._inferDataType:
                    new_def = new_def.astype(self.datatype)
                    self.executionHistory.append(f".. casting to  `{self.datatype}`")
            elif type(self.datatype) in [ArrayType, MapType, StructType] and self.values is None:
                new_def = expr("NULL")
            elif self._dataRange is not None and self._dataRange.isFullyPopulated():
                self.executionHistory.append(f".. computing ranged value: {self._dataRange}")
                new_def = self._computeRangedColumn(base_column=self.baseColumn, datarange=self._dataRange,
                                                    is_random=col_is_rand)
            elif type(self.datatype) is DateType:
                # TODO: fixup for date generation

                # record execution history
                self.executionHistory.append(".. using random date expression")
                sql_random_generator = self._getUniformRandomSQLExpression(self.name)
                new_def = expr(f"date_sub(current_date, rounding({sql_random_generator}*1024))").astype(
                    self.datatype)
            else:
                if self._baseColumnComputeMethod == VALUES_COMPUTE_METHOD:
                    self.executionHistory.append(".. using values compute expression for seed")
                    new_def = self._getSeedExpression(self.baseColumn)
                elif self._baseColumnComputeMethod == RAW_VALUES_COMPUTE_METHOD:
                    self.executionHistory.append(".. using raw values compute expression for seed")
                    new_def = self._getSeedExpression(self.baseColumn)
                # TODO: resolve issues with hash when using templates
                # elif self._baseColumnComputeMethod == HASH_COMPUTE_METHOD:
                #    newDef = self._getSeedExpression(self.baseColumn)
                else:
                    self.logger.info("Assuming a seeded base expression with minimum value for column %s", self.name)
                    self.executionHistory.append(f".. seeding with minimum `{self._dataRange.minValue}`")
                    new_def = ((self._getSeedExpression(self.baseColumn) + lit(self._dataRange.minValue))
                               .astype(self.datatype))

            if self.values is not None:
                new_def = array([lit(x) for x in self.values])[new_def.astype(IntegerType())]
            elif type(self.datatype) is StringType and self.expr is None:
                new_def = self._applyPrefixSuffixExpressions(self.prefix, self.suffix, new_def)

            # use string generation template if available passing in what was generated to date
            if type(self.datatype) is StringType and self.textGenerator is not None:
                new_def = self._applyTextGenerationExpression(new_def, use_pandas_optimizations)

        if type(self.datatype) is StringType and sformat is not None:
            new_def = self._applyTextFormatExpression(new_def, sformat)

        new_def = self._applyFinalCastExpression(self.datatype, new_def)

        if percent_nulls is not None:
            new_def = self._applyComputePercentNullsExpression(new_def, percent_nulls)
        return new_def

    def _onSelect(self, df):
        """
        The _onSelect method is called when the column specifications expression as produced by the
        method ``_makeSingleGenerationExpression`` is used in a select statement.

        :param df: Dataframe in which expression is used
        :return: nothing

        .. note:: The purpose of this method is to allow for introspection of information such as datatype
                  which can only be determined when column specifications expression is used.
        """
        if self._inferDataType:
            inferred_type = df.schema[self.name].dataType
            self.logger.info("Inferred datatype for column %s as %s", self.name, str(inferred_type))
            self._csOptions.options['type'] = inferred_type

    def _applyTextFormatExpression(self, new_def, sformat):
        # note :
        # while it seems like this could use a shared instance, this does not work if initialized
        # in a class method
        self.executionHistory.append(f".. applying column format  `{sformat}`")
        new_def = format_string(sformat, new_def)
        return new_def

    def _applyPrefixSuffixExpressions(self, cprefix, csuffix, new_def):
        # string value generation is simply handled by combining with a suffix or prefix
        # TODO: prefix and suffix only apply to base columns that are numeric types
        text_separator = self.text_separator if self.text_separator is not None else '_'
        if cprefix is not None and csuffix is not None:
            self.executionHistory.append(".. applying column prefix and suffix")
            new_def = concat(lit(cprefix), lit(text_separator), new_def.astype(IntegerType()), lit(text_separator),
                             lit(csuffix))
        elif cprefix is not None:
            self.executionHistory.append(".. applying column prefix")
            new_def = concat(lit(cprefix), lit(text_separator), new_def.astype(IntegerType()))
        elif csuffix is not None:
            self.executionHistory.append(".. applying column suffix")
            new_def = concat(new_def.astype(IntegerType()), lit(text_separator), lit(csuffix))
        return new_def

    def _applyTextGenerationExpression(self, new_def, use_pandas_optimizations):
        """Apply text generation expression to column expression

        :param new_def : column definition being created
        :param use_pandas_optimizations: Whether Pandas optimizations should be applied
        :returns: new column definition
        """
        # note :
        # while it seems like this could use a shared instance, this does not work if initialized
        # in a class method
        tg = self.textGenerator
        if use_pandas_optimizations:
            self.executionHistory.append(f".. text generation via pandas scalar udf `{tg}`")
            u_value_from_generator = pandas_udf(tg.pandasGenerateText,
                                                returnType=StringType()).asNondeterministic()
        else:
            self.executionHistory.append(f".. text generation via udf `{tg}`")
            u_value_from_generator = udf(tg.classicGenerateText,
                                         StringType()).asNondeterministic()
        new_def = u_value_from_generator(new_def)
        return new_def

    def _applyFinalCastExpression(self, col_type, new_def):
        """ Apply final cast expression for column data

        :param col_type: final column type
        :param new_def:  column definition being created
        :returns: new column definition
        """
        self.executionHistory.append(f".. casting column [{self.name}] to  `{col_type}`")

        # cast the result to the appropriate type. For dates, cast first to timestamp, then to date
        if type(col_type) is DateType:
            new_def = new_def.astype(TimestampType()).astype(col_type)
        elif self._inferDataType:
            # dont apply cast when column has an inferred data type
            pass
        else:
            new_def = new_def.astype(col_type)

        return new_def

    def _applyComputePercentNullsExpression(self, newDef, probabilityNulls):
        """Compute percentage nulls for column being generated

           :param newDef: Column definition being created
           :param probabilityNulls: Probability of nulls to be generated for particular column. Values can be 0.0 - 1.0
           :returns: new column definition with probability of nulls applied
        """
        assert self.nullable, f"Column `{self.name}` must be nullable for `percent_nulls` option"
        self.executionHistory.append(".. applying null generator - `when rnd > prob then value - else null`")

        assert probabilityNulls is not None, "option 'percent_nulls' must not be null value or None"
        assert type(probabilityNulls) in [int, float], "option 'percent_nulls' must be int or float"
        assert 0.0 <= probabilityNulls <= 1.0, "option 'percent_nulls' must in the range [0.0 .. 1.0]"
        prob_nulls = probabilityNulls * 1.0  # for edge case where int was passed
        random_generator = self._getUniformRandomExpression(self.name)
        newDef = when(random_generator > lit(prob_nulls), newDef).otherwise(lit(None))
        return newDef

    def _computeImpliedRangeIfNeeded(self, col_type):
        """ Compute implied range if necessary
            :param col_type" Column type
            :returns: nothing
        """
        # check for implied ranges
        if self.values is not None:
            self._dataRange = NRange(0, len(self.values) - 1, 1)
        elif type(col_type) is BooleanType:
            self._dataRange = NRange(0, 1, 1)
        self.executionHistory.append(f".. using adjusted effective range: {self._dataRange}")

    def _getMultiColumnDetails(self, validate):
        """ Determine min and max number of columns to generate along with `structType` for columns
            with multiple columns / features

        :param validate:  If true, raises ValueError if there are bad option entries
        :return: tuple of (min_columns, max_columns, structType
        """
        num_columns = self['numColumns']
        struct_type = self['structType']

        if num_columns is None:
            num_columns = self['numFeatures']

        if num_columns is None:
            min_num_columns, max_num_columns = 1, 1
        elif isinstance(num_columns, int):
            min_num_columns, max_num_columns = int(num_columns), int(num_columns)
        elif isinstance(num_columns, tuple):
            if validate and ((len(num_columns) != 2) or not all(isinstance(c, int) for c in num_columns)):
                raise ValueError(f"Bad value [{num_columns}] for `numColumns` / `numFeatures` attribute")

            min_num_columns, max_num_columns = int(num_columns[0]), int(num_columns[1])

            if validate and (min_num_columns > max_num_columns):
                raise ValueError(f"Bad value [{num_columns}] for `numColumns` / `numFeatures` attribute")
        else:
            if validate:
                raise ValueError(f"Bad value [{num_columns}] for `numColumns` / `numFeatures` attribute")

            min_num_columns, max_num_columns = 1, 1

        if validate and (min_num_columns != max_num_columns) and (struct_type != self._ARRAY_STRUCT_TYPE):
            self.logger.warning(
                f"Varying number of features / columns specified for non-array column [{self.name}]")
            self.logger.warning(
                f"Lower bound for number of features / columns ignored for [{self.name}]")
            min_num_columns = max_num_columns

        return min_num_columns, max_num_columns, struct_type

    def makeGenerationExpressions(self):
        """ Generate structured column if multiple columns or features are specified

        if there are multiple columns / features specified using a single definition, it will generate
        a set of columns conforming to the same definition,
        renaming them as appropriate and combine them into a array if necessary
        (depending on the structure combination instructions)

            :param self: is ColumnGenerationSpec for column
            :returns: spark sql `column` or expression that can be used to generate a column
        """
        min_num_columns, max_num_columns, struct_type = self._getMultiColumnDetails(validate=True)

        self.executionHistory = []

        if (min_num_columns == 1) and (max_num_columns == 1) and struct_type != self._ARRAY_STRUCT_TYPE:
            # record execution history for troubleshooting
            self.executionHistory.append(f"generating single column - `{self.name}` having type `{self.datatype}`")

            retval = self._makeSingleGenerationExpression(use_pandas_optimizations=True)

            # record how column was generated
            exec_step_history = ".. computed from base values - "
            exec_step_history += f"`{self.baseColumn}`, method: `{self._baseColumnComputeMethod}`"
            self.executionHistory.append(exec_step_history)
        else:
            self.executionHistory.append(f"generating multiple columns {max_num_columns} - `{self.name}`")

            retval = []

            for ix in range(max_num_columns):
                with self._temporaryRename(f"{self.name}_{ix}") as renamed_cs:
                    retval.append(renamed_cs._makeSingleGenerationExpression(ix, use_pandas_optimizations=True))

            if struct_type == 'array':
                self.executionHistory.append(".. converting multiple columns to array")
                retval = array(retval)

                if min_num_columns != max_num_columns:
                    column_set = ",".join(self.baseColumns)
                    diff = max_num_columns - min_num_columns
                    expr_str = f"{min_num_columns} + (abs(hash({column_set})) % {diff + 1})"

                    retval = F.slice(retval, F.lit(1), F.expr(expr_str))

        return retval
