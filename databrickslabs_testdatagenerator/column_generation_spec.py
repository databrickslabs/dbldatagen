# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `ColumnGenerationSpec` class
"""

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round, array, expr, when, udf, format_string
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType
import math
from datetime import date, datetime, timedelta
from .utils import ensure
from .text_generators import TextGenerators

from pyspark.sql.functions import col, pandas_udf

class ColumnGenerationSpec:
    """ Column generation spec object - specifies how column is to be generated
    """

    required_props = {'name', 'type'}
    allowed_props = {'name', 'type', 'min', 'max', 'step',
                     'prefix', 'random', 'distribution',
                     'range', 'base_column', 'values',
                     'numColumns', 'numFeatures', 'structType',
                     'begin', 'end', 'interval', 'expr', 'omit',
                     'weights', 'description', 'continuous',
                     'percent_nulls', 'template', 'format',
                     'unique_values'

                     }
    forbidden_props = {
        'range'
    }

    max_type_range = {
        'byte': 256,
        'short': 65536
    }

    def __init__(self, name, colType=None, min=0, max=None, step=1, prefix='', random=False,
                 distribution="normal", base_column="id", random_seed=None, random_seed_method=None,
                 implicit=False, omit=False, nullable=True, **kwargs):

        if colType is None:
            colType = IntegerType()

        assert isinstance(colType, DataType)

        self.props = {'name': name, 'min': min, 'type': colType, 'max': max, 'step': step,
                      'prefix': prefix, 'base_column': base_column,
                      'random': random, 'distribution': distribution,
                      }
        self.props.update(kwargs)

        self._checkProps(self.props)

        self.implicit = implicit
        self.omit = omit
        self.name = name
        self.nullable = nullable
        self.random_seed = random_seed
        self.random_seed_method = random_seed_method

        # compute dependencies
        if base_column != "id":
            if type(base_column) is list:
                self.dependencies = base_column + ["id"]
            else:
                self.dependencies = [base_column, "id"]
        else:
            self.dependencies = ["id"]

        # compute required temporary values
        self.temporary_columns = []

        if self.isWeightedValuesColumn:
            # if its a weighted values column, then create temporary for it
            # not supported for feature / array columns for now
            ensure(self['numFeatures'] is None or self['numFeatures'] <= 1,
                   "weighted columns not supported for multi-column or multi-feature values")
            ensure(self['numColumns'] is None or self['numColumns'] <= 1,
                   "weighted columns not supported for multi-column or multi-feature values")
            temp_name = "_rnd_{}".format(self.name)
            self.dependencies.append(temp_name)
            desc = "adding temporary column {} required by {}".format(temp_name, self.name)
            print(desc)
            sql_random_generator = self.getUniformRandomSQLExpression()
            self.temporary_columns.append((temp_name, DoubleType(), {'expr': sql_random_generator, 'omit' : "True",
                                                                     'description': desc}))
            self.base_column = temp_name

    def getUniformRandomExpression(self):
        """ Get random expression accounting for seed method"""
        if self.random_seed_method == "fixed":
            return expr("rand({})".format(self.random_seed))
        elif self.random_seed_method == "hash_fieldname":
            assert self.name is not None
            return expr("rand(hash('{}'))".format(self.name))
        else:
            return rand()

    def getUniformRandomSQLExpression(self):
        """ Get random SQL expression accounting for seed method"""
        if self.random_seed_method == "fixed":
            assert self.random_seed is not None
            return "rand({})".format(self.random_seed)
        elif self.random_seed_method == "hash_fieldname":
            assert self.name is not None
            return "rand(hash('{}'))".format(self.name)
        else:
            return "rand()"

    @property
    def isWeightedValuesColumn(self):
        """ check if column is a weighed values column """
        return self.random and self['weights'] is not None and self.values is not None

    def getNames(self):
        """ get column names as list"""
        numColumns = self.props.get('numColumns', 1)
        structType = self.props.get('structType', None)

        if numColumns > 1 and structType is None:
            return ["{0}_{1}".format(self.name, x) for x in range(0, numColumns)]
        else:
            return [self.name]

    def getNamesAndTypes(self):
        """ get column names as list"""
        numColumns = self.props.get('numColumns', 1)
        structType = self.props.get('structType', None)

        if numColumns > 1 and structType is None:
            return [ ("{0}_{1}".format(self.name, x), self.datatype) for x in range(0, numColumns)]
        else:
            return [(self.name, self.datatype)]


    def keys(self):
        """ Get the keys or field names """
        ensure(self.props is not None, "self.props should be non-empty")
        return self.props.keys()

    def __getitem__(self, key):
        """ implement the built in derefernce by key behavior """
        ensure(key is not None, "key should be non-empty")
        return self.props.get(key, None)

    @property
    def isFieldOmitted(self):
        """ check if this field should be omitted from the output"""
        return self.omit

    @property
    def baseColumn(self):
        """get the base column used to generate values for this column"""
        return self['base_column']

    @property
    def datatype(self):
        """get the Spark SQL data type used to generate values for this column"""
        return self['type']

    @property
    def prefix(self):
        """get the string prefix used to generate values for this column"""
        return self['prefix']

    @property
    def suffix(self):
        """get the string suffix used to generate values for this column"""
        return self['suffix']

    @property
    def min(self):
        """get the column generation `min` value used to generate values for this column"""
        return self['min']

    @property
    def max(self):
        """get the column generation `max` value used to generate values for this column"""
        return self['max']

    @property
    def step(self):
        """get the column generation `step` value used to generate values for this column"""
        return self['step']

    @property
    def random(self):
        """get the column generation `random` attribute used to generate values for this column.
        If set, values should be generated randomly rather than from the seed `id` column
        """
        return self['random']

    @property
    def weights(self):
        """get the column generation `weights` value used to generate values for this column"""
        return self['weights']

    @property
    def values(self):
        """get the column generation `values` attribute used to generate values for this column"""
        return self['values']

    @property
    def exprs(self):
        """get the column generation `exprs` attribute used to generate values for this column.
        """
        return self['exprs']

    @property
    def distribution(self):
        """get the column generation `distribution` attribute used to generate values for this column"""
        return self['distribution']

    @property
    def expr(self):
        """get the base column used to generate values for this column"""
        return self['expr']

    @property
    def begin(self):
        """get the base column used to generate values for this column"""
        return self['begin']

    @property
    def end(self):
        """get the base column used to generate values for this column"""
        return self['end']

    @property
    def interval(self):
        """get the base column used to generate values for this column"""
        return self['interval']

    @property
    def numColumns(self):
        """get the base column used to generate values for this column"""
        return self['numColumns']

    @property
    def numFeatures(self):
        """get the base column used to generate values for this column"""
        return self['numFeatures']

    def structType(self):
        """get the base column used to generate values for this column"""
        return self['structType']

    def _getOrElse(self, key, default=None):
        """ Get val for key if it exists or else return default"""
        return self.props.get(key, default)

    def _checkProps(self, column_props):
        """
            check that column definition properties are recognized
            and that the column definition has required properties
        """
        ensure(column_props is not None, "coldef should be non-empty")

        colType = self['type']
        if colType.typeName() in self.max_type_range:
            min = self['min']
            max  = self['max']

            if min is not None and max is not None:
                effective_range = max - min
                if effective_range > self.max_type_range[colType.typeName()]:
                    raise ValueError("Effective range greater than range of type")

        for k in column_props.keys():
            ensure(k in self.allowed_props, 'invalid column option {0}'.format(k))

        for arg in self.required_props:
            ensure(arg in column_props.keys() and column_props[arg] is not None,
                   'missing column option {0}'.format(arg))

        for arg in self.forbidden_props:
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

    def computeTimestampIntervals(self, start, end, interval):
        """ Compute number of intervals between start and end date """
        ensure(type(start) is datetime, "Expecting start as type datetime.datetime")
        ensure(type(end) is datetime, "Expecting end as type datetime.datetime")
        ensure(type(interval) is timedelta, "Expecting interval as type datetime.timedelta")
        i1 = end - start
        ni1 = i1 / interval
        return math.floor(ni1)

    def getPlan(self):
        desc = self['description']
        if desc is not None:
            return " |-- " + desc
        else:
            return " |-- building column generator for column {}".format(self.name)

    def make_weighted_column_values_expression(self, values, weights):
        from .function_builder import ColumnGeneratorBuilder
        assert self.base_column is not None
        expr_str = ColumnGeneratorBuilder.mk_expr_choices_fn(values, weights, self.base_column, self.datatype)
        return expr(expr_str).astype(self.datatype)

    def _is_real_valued_column(self):
        """ determine if column is real valued """
        colTypeName = self['type'].typeName()

        return colTypeName == 'double' or colTypeName == 'float' or colTypeName == 'decimal'

    def _is_decimal_column(self):
        """ determine if column is decimal column"""
        colTypeName = self['type'].typeName()

        return colTypeName == 'decimal'

    def _is_continuous_valued_column(self):
        """ determine if column generates continuous values"""
        is_continuous = self['continuous']

        return is_continuous

    def get_seed_expression(self, base_column):
        """ Get seed expression for column generation
        if using a single base column, then simply use that, otherwise use a SQL hash of multiple columns
        """
        if type(base_column) is list:
            return expr("hash({})".format(",".join(base_column)))
        else:
            return col(base_column)

    def _compute_ranged_column(self, min, max, step, base_column, is_random):
        """ compute a ranged column

        max is max actual value
        """
        assert base_column is not None
        assert min is not None
        assert max is not None
        assert step is not None

        random_generator = self.getUniformRandomExpression() if is_random else None
        if self._is_continuous_valued_column() and self._is_real_valued_column() and is_random:
            crange = (max - min) * float(1.0)
            baseval = random_generator * lit(crange)
        else:
            crange = (max - min) * float(1.0 / step)
            baseval = (self.get_seed_expression(base_column) % lit(crange + 1) * lit(step)) if not is_random else (
                    round(random_generator * lit(crange)) * lit(step))
        newDef = (baseval + lit(min))

        # for ranged values in strings, use type of min, max and step as output type
        if type(self.datatype) is StringType:
            if type(min) is float or type(max) is float or type(step) is float:
                newDef = newDef.astype(DoubleType())
            else:
                newDef = newDef.astype(IntegerType())

        return newDef

    def _compute_default_ranges_for_type(self, min, max, col_type):
        """ computes default min and max based on data type"""
        if self._is_decimal_column():
            if min is None:
                min = 0.0
            if max is None:
                max = math.pow(10, col_type.precision - col_type.scale) - 1.0


        return min, max

    def make_single_generation_expression(self, index=None):
        """ generate column data via Spark SQL expression"""

        # get key column specification properties
        values, weights = self['values'], self['weights']
        sqlExpr = self['expr']
        ctype, cprefix = self['type'], self['prefix']
        cmin, cmax, cstep = self['min'], self['max'], self['step']
        csuffix = self['suffix']
        crand, cdistribution = self['random'], self['distribution']
        baseCol = self['base_column']
        c_begin, c_end, c_interval = self['begin'], self['end'], self['interval']
        string_generation_template=self['template']
        percent_nulls = self['percent_nulls']
        sformat=self['format']

        cmin, cmax = self._compute_default_ranges_for_type(min=cmin, max=cmax, col_type=ctype)

        newDef = None

        # handle weighted values
        if self.isWeightedValuesColumn:
            newDef=self.make_weighted_column_values_expression(values, weights)
        else:
            # rs: initialize the begin, end and interval if not initalized for date computations
            # defaults are start of day, now, and 1 minute respectively
            if c_begin is None:
                __c_begin = datetime.today()
                c_begin = datetime(__c_begin.year, __c_begin.month, __c_begin.day, 0, 0, 0)

            if c_end is None:
                c_end = datetime.today()

            if c_interval is None:
                c_interval = timedelta(days=0, hours=0, minutes=1)

            # check for implied ranges
            if values is not None:
                cmin, cstep, cmax = 0, 1, len(values) - 1
            elif type(ctype) is BooleanType:
                cmin, cstep, cmax = 0, 1, 1
            elif type(ctype) is TimestampType :
                # compute number of intervals in time ranges
                cmin = (c_begin - datetime(1970, 1, 1)).total_seconds()
                cstep = c_interval.total_seconds()
                cmax = cmin + c_interval.total_seconds() * self.computeTimestampIntervals(c_begin, c_end, c_interval)

            # TODO: add full support for date value generation
            if sqlExpr is not None:
                newDef = expr(sqlExpr).astype(ctype)
            elif cmin is not None and cmax is not None and cstep is not None:
                newDef=self._compute_ranged_column(base_column=baseCol,  min=cmin, max=cmax, step=cstep, is_random=crand)
            elif type(ctype) is DateType:
                sql_random_generator = self.getUniformRandomSQLExpression()
                newDef = expr("date_sub(current_date, round({}*1024))".format(sql_random_generator)).astype(ctype)
            else:
                newDef = (self.get_seed_expression(baseCol) + lit(cmin)).astype(ctype)

            # string value generation is simply handled by combining with a suffix or prefix
            if values is not None:
                newDef = array([lit(x) for x in values])[newDef.astype(IntegerType())]
            elif type(ctype) is StringType and sqlExpr is None:
                if cprefix is not None:
                    newDef = concat(lit(cprefix), lit('_'), newDef.astype(IntegerType()))
                elif csuffix is not None:
                    newDef = concat(newDef.astype(IntegerType(), lit('_'), lit(csuffix)))
                else:
                    newDef = newDef

            # use string generation template if available passing in what was generated to date
            if type(ctype) is StringType and string_generation_template is not None:
                # note :
                # while it seems like this could use a shared instance, this does not work if initialized
                # in a class method
                #u_value_from_template = udf(TextGenerators.value_from_template, StringType()).asNondeterministic()
                u_value_from_template = pandas_udf(TextGenerators.pandas_value_from_template, returnType=StringType()).asNondeterministic()
                newDef = u_value_from_template(newDef, lit(string_generation_template))

            if type(ctype) is StringType and sformat is not None:
                # note :
                # while it seems like this could use a shared instance, this does not work if initialized
                # in a class method
                newDef = format_string(sformat, newDef)

            newDef = newDef.astype(ctype)


        if percent_nulls is not None:
            prob_nulls=percent_nulls / 100.0
            random_generator = self.getUniformRandomExpression()
            newDef = when(random_generator > lit(prob_nulls),newDef).otherwise(lit(None))
        return newDef

    def make_generation_expressions(self):
        """ Generate structured column if multiple columns or features are specified

            :param self: is ColumnGenerationSpec for column
            :returns: spark sql `column` or expression that can be used to generate a column
        """
        numColumns = self['numColumns']
        structType = self['structType']

        if numColumns is None:
            numColumns = self['numFeatures']

        if numColumns == 1 or numColumns is None:
            # self.printVerbose("generating single column - `{0}`".format(self['name']))
            retval = self.make_single_generation_expression()
        else:
            # self.printVerbose("generating multiple columns {0} - `{1}`".format(numColumns, self['name']))
            retval = [self.make_single_generation_expression(x) for x in range(numColumns)]

            if structType == 'array':
                retval = array(retval)
            else:
                # TODO : update the output columns
                pass

        return retval
