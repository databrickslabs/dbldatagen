# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `SparkSingleton` class

This is primarily meant for situations where the test data generator is run on a standalone environment
for use cases like unit testing rather than in a Databricks workspace environment
"""

import os
import logging
from pyspark.sql import SparkSession


class SparkSingleton:
    """A singleton class which returns one Spark session instance"""

    @classmethod
    def getInstance(cls):
        """Create a Spark instance for Datalib.

        :returns: A Spark instance
        """

        return SparkSession.builder.getOrCreate()

    @classmethod
    def getLocalInstance(cls, appName="new Spark session", useAllCores=True):
        """Create a machine local Spark instance for Datalib.
        By default, it uses `n-1` cores  of the available cores for the spark session,
        where `n` is total cores available.

        :param useAllCores:  If `useAllCores` is True, then use all cores rather than `n-1` cores
        :returns: A Spark instance
        """
        cpu_count = os.cpu_count()

        if useAllCores:
            spark_core_count = cpu_count
        else:
            spark_core_count = cpu_count - 1

        logger = logging.getLogger(__name__)
        logger.info("Spark core count: %d", spark_core_count)

        sparkSession = SparkSession.builder \
            .master(f"local[{spark_core_count}]") \
            .appName(appName) \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()

        return sparkSession
