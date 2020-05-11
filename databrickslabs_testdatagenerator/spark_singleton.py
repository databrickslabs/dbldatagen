# See the License for the specific language governing permissions and
# limitations under the License.
#
from pyspark.sql import SparkSession
import os

class SparkSingleton:
    """A singleton class which returns one Spark session instance"""

    @classmethod
    def get_instance(cls):
        """Create a Spark instance for Datalib.
        :return: A Spark instance
        """

        return SparkSession.builder.getOrCreate()
