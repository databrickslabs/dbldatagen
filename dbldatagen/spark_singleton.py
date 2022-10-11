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
    def getRecommendedSparkTaskCount(cls, limitToAvailableCores=False, useAllCores=False, minTasks=4):
        """
        Get recommended tasks for testing purposes

        :param limitToAvailableCores: if True, limit to available core count
        :param useAllCores: if True, use all cores
        :param minTasks: minimum number of tasks to use if not limited to available cores
        :return:
        """

        core_count = os.cpu_count()

        if useAllCores:
            task_count = core_count
        else:
            task_count = core_count - 1

        if not limitToAvailableCores:
            task_count = max(task_count, minTasks)

        logging.info(f"recommended task count {task_count}")
        return task_count

    @classmethod
    def getInstance(cls):
        """Create a Spark instance for Datalib.

        :returns: A Spark instance
        """

        return SparkSession.builder.getOrCreate()

    @classmethod
    def getLocalInstance(cls, appName="new Spark session", useAllCores=False):
        """Create a machine local Spark instance for Datalib.
        It uses 3/4 of the available cores for the spark session.

        :returns: A Spark instance
        """
        cpu_count = os.cpu_count()

        if useAllCores:
            spark_core_count = cpu_count
        else:
            spark_core_count = cpu_count - 1

        logging.info("Spark core count: %d", spark_core_count)

        return SparkSession.builder \
            .master(f"local[{spark_core_count}]") \
            .appName(appName) \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
