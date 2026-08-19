# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `SparkSingleton` class

This is primarily meant for situations where the test data generator is run on a standalone environment
for use cases like unit testing rather than in a Databricks workspace environment
"""

import logging
import os

from pyspark.sql import SparkSession

from dbldatagen.spark_compat import is_spark_connect_mode


class SparkSingleton:
    """A singleton class which returns one Spark session instance"""

    @classmethod
    def getInstance(cls: type["SparkSingleton"]) -> SparkSession:
        """Creates a `SparkSession` instance for Datalib.

        :returns: A Spark instance
        """
        active_session = cls.getActiveSession()
        if active_session is not None:
            return active_session

        return SparkSession.builder.getOrCreate()

    @classmethod
    def getActiveSession(cls: type["SparkSingleton"]) -> SparkSession | None:
        """Returns the active Spark session if one exists, without creating a new local session."""
        try:
            return SparkSession.getActiveSession()
        except AttributeError:
            return None

    @classmethod
    def getLocalInstance(
        cls: type["SparkSingleton"], appName: str = "new Spark session", useAllCores: bool = True
    ) -> SparkSession:
        """Creates a machine local `SparkSession` instance for Datalib.
        By default, it uses `n-1` cores  of the available cores for the spark session,
        where `n` is total cores available.

        :param appName: Name to use for the local `SparkSession` instance
        :param useAllCores:  If `useAllCores` is True, then use all cores rather than `n-1` cores
        :returns: A `SparkSession` instance
        """
        if is_spark_connect_mode():
            active_session = cls.getActiveSession()
            if active_session is not None:
                return active_session

            return SparkSession.builder.getOrCreate()

        cpu_count = os.cpu_count()

        if not cpu_count:
            spark_core_count = 1
        elif useAllCores:
            spark_core_count = cpu_count
        else:
            spark_core_count = cpu_count - 1

        logger = logging.getLogger(__name__)
        logger.info("Spark core count: %d", spark_core_count)

        sparkSession = (
            SparkSession.builder.master(f"local[{spark_core_count}]")
            .appName(appName)
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .getOrCreate()
        )

        return sparkSession
