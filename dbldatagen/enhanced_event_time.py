# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `EnhancedEventTime` class

This defines helper methods for implementing enhanced event time
"""

import logging


from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.functions import lit, concat, rand, round as sql_round, array, expr, when, udf, \
    format_string
from pyspark.sql.types import FloatType, IntegerType, StringType, DoubleType, BooleanType, \
    TimestampType, DataType, DateType

from .column_spec_options import ColumnSpecOptions
from .datagen_constants import RANDOM_SEED_FIXED, RANDOM_SEED_HASH_FIELD_NAME, RANDOM_SEED_RANDOM
from .daterange import DateRange
from .distributions import Normal, DataDistribution
from .nrange import NRange
from .text_generators import TemplateGenerator
from .utils import ensure, coalesce_values

import dbldatagen as dg
import dbldatagen.distributions as dist

HASH_COMPUTE_METHOD = "hash"
VALUES_COMPUTE_METHOD = "values"
RAW_VALUES_COMPUTE_METHOD = "raw_values"
AUTO_COMPUTE_METHOD = "auto"
COMPUTE_METHOD_VALID_VALUES = [HASH_COMPUTE_METHOD,
                               AUTO_COMPUTE_METHOD,
                               VALUES_COMPUTE_METHOD,
                               RAW_VALUES_COMPUTE_METHOD]



class EnhancedEventTimeHelper(object):

    def init(self):
        pass

    def withEnhancedEventTime(self,
                              dataspec,
                              startEventTime=None,
                              acceleratedEventTimeInterval="10 minutes",
                              fractionLateArriving=0.1,
                              lateTimeInterval="6 hours",
                              jitter=(-0.25, 0.9999),
                              eventTimeName=None,
                              baseColumn="timestamp",
                              keepIntermediateColumns=False):
        """
        Implement enhanced event time

        :param dataspec:                     dataspec to apply enhanced event time to
        :param startEventTime:               start timestamp of output event time
        :param acceleratedEventTimeInterval: interval for accelerated event time (i.e "10 minutes")
        :param fractionLateArriving:         fraction of late arriving data. range [0.0, 1.0]
        :param lateTimeInterval:             interval for late arriving events (i.e "6 hours")
        :param jitter:                       jitter factor to avoid strictly increasing order in events
        :param eventTimeName:                Column name for generated event time column
        :param baseColumn:                   Base column name used for computations of adjusted event time
        :param keepIntermediateColumns:      Flag to retain intermediate columns in the output, [ debug / test only]

        This adjusts the dataframe to produce IOT style event time that normally increases over time but has a
        configurable fraction of late arriving data. It uses a base timestamp column (called `timestamp` by default)
        to compute data. There must be a column of this name in the source data frame and it should have the value of
        "now()".

        By default `rate` and `rate-micro-batch` streaming sources have this column but
        it can be added to other dataframes - either batch or streaming data frames.

        While the overall intent is to support synthetic IOT style simulated device data in a stream, it can be used
        with a batch data frame (as long as an appropriate timestamp column is added and designated as the base data
        timestamp column.
        """

        assert startEventTime is not None, "value for `startTime` must be specified"
        assert dataspec is not None, "dataspec must be specified"
        assert 0.0 <= fractionLateArriving <= 1.0, "fractionLateArriving must be in range [0.0, 1.0]"
        assert eventTimeName is not None, "eventTimeName argument must be supplied"

        # determine timestamp for start of generation
        start_of_generation = \
        dataspec.sparkSession.sql(f"select cast(now() as string) as start_timestamp").collect()[0][
            'start_timestamp']

        omitInterimColumns = not keepIntermediateColumns

        retval = (dataspec
                  .withColumn("_late_arrival_factor1", "double", minValue=0.0, maxValue=1.0, continuous=True,
                              random=True,
                              distribution=dist.Beta(2.0, 5.3), omit=omitInterimColumns)
                  .withColumn("_late_arrival_factor", "double", expr="least(_late_arrival_factor1 * 1.17, 1.0)",
                              baseColumn="_late_arrival_factor1",
                              omit=omitInterimColumns)
                  .withColumn("_stream_time", "timestamp", expr=f"{baseColumn}",
                              omit=omitInterimColumns, baseColumn=baseColumn)
                  .withColumn("_data_gen_time", "timestamp", expr="now()", omit=omitInterimColumns)
                  .withColumn("_difference_factor", "double",
                              expr=f"cast(_stream_time as double) - cast(TIMESTAMP '{start_of_generation}' as double)",
                              baseColumns=["_stream_time"],
                              omit=omitInterimColumns)
                  .withColumn("_jitter_within_event_interval", "double", minValue=jitter[0], maxValue=jitter[1],
                              continuous=True, random=True,
                              omit=omitInterimColumns)
                  .withColumn("_late_arrival_prob", "double", minValue=0.0, maxValue=1.0, continuous=True,
                              random=True,
                              omit=omitInterimColumns)
                  .withColumn("_late_arrival", "boolean",
                              expr=f"case when _late_arrival_prob <= {fractionLateArriving} then true else false end",
                              baseColumns=["_late_arrival_prob"],
                              omit=omitInterimColumns)
                  .withColumn("_ontime_event_ts", "timestamp",
                              expr=f"""greatest(TIMESTAMP '{startEventTime}' + 
                                         ((_difference_factor + _jitter_within_event_interval) 
                                            * INTERVAL {acceleratedEventTimeInterval}),
                                             TIMESTAMP '{startEventTime}') """,
                              baseColumns=["_difference_factor", "_jitter_within_event_interval"],
                              omit=omitInterimColumns)
                  .withColumn("_late_arrival_ts", "timestamp",
                              expr=f"""greatest(_ontime_event_ts - (_late_arrival_factor * INTERVAL {lateTimeInterval}),
                                                 TIMESTAMP '{startEventTime}')                                                                         
                                    """,
                              baseColumns=["_ontime_event_ts", "_late_arrival_factor"],
                              omit=omitInterimColumns)

                  # generate event time column
                  .withColumn(eventTimeName, "timestamp",
                              expr="case when _late_arrival then _late_arrival_ts else _ontime_event_ts end",
                              baseColumns=["_late_arrival", "_late_arrival_ts", "_ontime_event_ts"])
                  )
        return retval
