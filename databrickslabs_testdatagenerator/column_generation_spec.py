#
# Copyright (C) 2019 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `DataGenError` and `DataGenerator` classes
"""

from pyspark.sql.functions import col, lit, concat, rand, ceil, floor, round, array, expr
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    StructType, StructField, TimestampType, DataType, DateType
import math
from datetime import date, datetime, timedelta
from .utils import ensure


class ColumnGenerationSpec:
    """ Column generation spec object - specifies how column is to be generated
    """

    required_props = {'name', 'type'}
    allowed_props = {'name', 'type', 'min', 'max', 'step',
                     'prefix', 'random', 'distribution', 'mean',
                     'range', 'median', 'base_column', 'values',
                     'numColumns', 'numFeatures', 'structType',
                     'begin', 'end', 'interval', 'expr', 'omit',
                     'weights', 'description', 'continuous'

                     }
    forbidden_props = {
        'range'
    }

    max_type_range = {
        'byte': 256,
        'short': 65536
    }

    def __init__(self, name, colType=None, min=0, max=None, step=1, prefix='', random=False,
                 distribution="normal", base_column="id",
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

        # compute dependencies
        if base_column != "id":
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
            self.temporary_columns.append((temp_name, DoubleType(), {'expr': 'rand()', 'omit' : "True",
                                                                     'description': desc}))
            self.base_column = temp_name

    @property
    def isWeightedValuesColumn(self):
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
        """get the base column used to generate values for this column"""
        return self['type']

    @property
    def prefix(self):
        """get the base column used to generate values for this column"""
        return self['prefix']

    @property
    def suffix(self):
        """get the base column used to generate values for this column"""
        return self['suffix']

    @property
    def min(self):
        """get the base column used to generate values for this column"""
        return self['min']

    @property
    def max(self):
        """get the base column used to generate values for this column"""
        return self['max']

    @property
    def step(self):
        """get the base column used to generate values for this column"""
        return self['step']

    @property
    def random(self):
        """get the base column used to generate values for this column"""
        return self['random']

    @property
    def weights(self):
        """get the base column used to generate values for this column"""
        return self['weights']

    @property
    def values(self):
        """get the base column used to generate values for this column"""
        return self['values']

    @property
    def exprs(self):
        """get the base column used to generate values for this column"""
        return self['exprs']

    @property
    def distribution(self):
        """get the base column used to generate values for this column"""
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

    def _compute_ranged_column(self, min, max, step, base_column, is_random):
        """ compute a ranged column

        max is max actual value
        """
        assert base_column is not None
        assert min is not None
        assert max is not None
        assert step is not None

        if self._is_continuous_valued_column() and self._is_real_valued_column() and is_random:
            crange = (max - min) * float(1.0)
            baseval = rand() * lit(crange)
        else:
            crange = (max - min) * float(1.0 / step)
            baseval = (col(base_column) % lit(crange + 1) * lit(step)) if not is_random else (
                    round(rand() * lit(crange)) * lit(step))
        newDef = (baseval + lit(min))

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

        cmin, cmax = self._compute_default_ranges_for_type(min=cmin, max=cmax, col_type=ctype)

        newDef = None

        # handle weighted values
        if self.isWeightedValuesColumn:
            return self.make_weighted_column_values_expression(values, weights)
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
            elif type(ctype) is TimestampType:
                # compute number of intervals in time ranges
                cmin = (c_begin - datetime(1970, 1, 1)).total_seconds()
                cstep = c_interval.total_seconds()
                cmax = cmin + c_interval.total_seconds() * self.computeTimestampIntervals(c_begin, c_end, c_interval)

            # TODO: add full support for date value generation
            if sqlExpr is not None:
                newDef = expr(sqlExpr).astype(ctype)
            elif cmin is not None and cmax is not None and cstep is not None:
                newDef=self._compute_ranged_column(base_column=baseCol,  min=cmin, max=cmax, step=cstep, is_random=crand).astype(ctype)
            elif type(ctype) is DateType:
                newDef = expr("date_sub(current_date, round(rand()*1024))").astype(ctype)
                print("data type for date", type(newDef))
            else:
                newDef = (col(baseCol) + lit(cmin)).astype(ctype)

            # string value generation is simplu handled by combining with a suffix or prefix
            if values is not None:
                newDef = array([lit(x) for x in values])[newDef.astype(IntegerType())]
            elif type(ctype) is StringType and sqlExpr is None:
                if cprefix is not None:
                    newDef = concat(lit(cprefix), lit('_'), newDef.astype(IntegerType()))
                elif csuffix is not None:
                    newDef = concat(newDef.astype(IntegerType(), lit('_'), lit(csuffix)))
                else:
                    newDef = newDef.astype(StringType())
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
