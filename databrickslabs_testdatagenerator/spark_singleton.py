# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the `SparkSingleton` class

This is primarily meant for situations where the test data generator is run on a standalone environment
for use cases like unit testing rather than in a Databricks workspace environment
"""

from pyspark.sql import SparkSession
import os
import math


class SparkSingleton:
    """A singleton class which returns one Spark session instance"""

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
        cpu_count = int(math.floor(os.cpu_count() * 0.75))
        print("cpus", cpu_count)

        return SparkSession.builder \
            .master("local[{}]".format(cpu_count)) \
            .appName(appName) \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
