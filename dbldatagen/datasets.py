# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module defines the ``Datasets`` class.

This module supports the addition of standard datasets to the Synthetic Data Generator

These are standard datasets that can be synthesized with minimal coding to handle a variety of situations
for testing, benchmarking and other uses.

As the APIs return a data generation specification rather than a dataframe, additional columns can be added and further
manipulation can be performed before generation of actual data.

"""
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, DoubleType, BooleanType, ShortType, \
    TimestampType, DateType, DecimalType, ByteType, BinaryType, StructType, ArrayType, DataType

import pyspark.sql as ssql
import pyspark.sql.functions as F

from .utils import strip_margins
from .spark_singleton import SparkSingleton


class Datasets:
    """This class is used to generate standard data sets based on a plugin provider model.

       It allows for quick generation of data for common scenarios.

    :param sparkSession: Spark session instance to use when performing spark operations
    :param name: Dataset name to


    """
    _registered_providers = {}

    def __init__(self, sparkSession=None, name=None, streaming=False):
        """ Constructor:
        :param sparkSession: Spark session to use
        :param name: name of dataset to search for
        :param streaming: if True, validdates that dataset supports streaming data
        """
        assert name is not None, "Dataset name must be supplied"

        if sparkSession is None:
            sparkSession = SparkSingleton.getLocalInstance()

        self._sparkSession = sparkSession
        self._name = name
        self._streaming = streaming

    @classmethod
    def registerProvider(cls, name, providerType):
        pass

    @classmethod
    def list(cls, pattern=None, output="text/plain"):
        pass

    @classmethod
    def describe(cls, name, output="text/plain"):
        pass

    def get(self, tableName=None, rows=None, partitions=None):
        pass

