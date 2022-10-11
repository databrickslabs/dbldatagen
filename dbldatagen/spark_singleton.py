# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `SparkSingleton` class

This is primarily meant for situations where the test data generator is run on a standalone environment
for use cases like unit testing rather than in a Databricks workspace environment
"""

import os
import math
import logging
from pyspark.sql import SparkSession


class SparkSingleton:
    """A singleton class which returns one Spark session instance"""

    @classmethod
    def getRecommendedSparkTaskCount(cls):
        cpu_count = max(os.cpu_count() - 1, 4)
        logging.info(f"recommended task count {cpu_count}")
        return cpu_count

    @classmethod
    def getInstance(cls):
        """Create a Spark instance for Datalib.

        :returns: A Spark instance
        """

        return SparkSession.builder.getOrCreate()

    @classmethod
    def getLocalInstance(cls, appName="new Spark session"):
        """Create a machine local Spark instance for Datalib.
        It uses 3/4 of the available cores for the spark session.

        :returns: A Spark instance
        """
        cpu_count = os.cpu_count() - 1
        logging.info("Spark core count: %d", cpu_count)

        return SparkSession.builder \
            .master(f"local[{cpu_count}]") \
            .appName(appName) \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
