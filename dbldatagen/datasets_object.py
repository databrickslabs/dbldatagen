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

from .datasets import DatasetProvider
from .spark_singleton import SparkSingleton


class Datasets:
    """This class is used to generate standard data sets based on a plugin provider model.

       It allows for quick generation of data for common scenarios.

    :param sparkSession: Spark session instance to use when performing spark operations
    :param name: Dataset name to

    """

    DEFAULT_ROWS = 1000000
    DEFAULT_PARTITIONS = 4

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
        """This method lists the registered datasets
            It filters the list by a regular expression pattern if provided
        """
        summary_list = [(name, provider.summary) for name, provider in DatasetProvider.registered_providers.items()]

        # filter the list if a pattern is provided
        if pattern is not None:
            summary_list = [(name, summary) for name, summary in summary_list if re.match(pattern, name)]

        # now format the list for output
        if output == "text/plain":
            for name, summary in summary_list:
                print(f"{name}: {summary}")
        elif output == "text/html" or output == "html":
            # check if function named displayHtml is defined in the current context
            if "displayHtml" in globals():
                displayHtml(summary_list)
            else:
                print("<html><body>")
            print("<table>")
            for name, summary in summary_list:
                print(f"<tr><td>{name}</td><td>{summary}</td></tr>")
            print("</table>")
            print("</body></html>")
        else:
            raise ValueError(f"Output format '{output}' not supported")

    @classmethod
    def describe(cls, name, output="text/plain"):
        pass

    def get(self, tableName=None, rows=None, partitions=None):
        pass

    @property
    def registeredDatasets(self):
        return
