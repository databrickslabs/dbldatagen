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

import pprint
import re

from pyspark.sql.session import SparkSession

from dbldatagen.datasets.dataset_provider import DatasetProvider
from .spark_singleton import SparkSingleton
from .utils import strip_margins


class Datasets:
    """This class is used to generate standard data sets based on a plugin provider model.

       It allows for quick generation of data for common scenarios.

    :param sparkSession: Spark session instance to use when performing spark operations
    :param name: Dataset name to

    """

    DEFAULT_ROWS = 1000000
    DEFAULT_PARTITIONS = 4

    _registered_providers = {}

    @staticmethod
    def getGlobalSparkSession():

        def get_global_variable(ctx_globals, varname, typeCheck):
            """Get a global variable if available and optionally matches specific type or is subclass of type

            :param varname: name of variable to check for
            :param typeCheck: type to check against
            :returns: the variable if variable is available, otherwise None
            """
            assert varname is not None and len(varname) > 0, "Variable name must be specified"
            assert typeCheck is not None, "typeCheck type must be specified"

            try:
                if varname in ctx_globals:
                    candidate_var = ctx_globals[varname]
                    if candidate_var is not None and \
                            (type(candidate_var) == typeCheck) or issubclass(type(candidate_var), typeCheck):
                        return candidate_var
                    else:
                        return None
                return None
            except Exception as e:
                return None

        import inspect
        current = inspect.currentframe()

        while current is not None:
            fn = get_global_variable(current.f_globals, "spark", typeCheck=SparkSession)
            if fn is not None:
                return fn
            current = current.f_back
        return None

    @classmethod
    def registerProvider(cls, name, providerType):
        pass

    @classmethod
    def getProviderDefinitions(cls, name=None, pattern=None):
        """Get provider definition for a dataset

        :param name: name of dataset to get provider for, if None, returns all providers
        :param pattern: pattern to match dataset name, if None, returns all providers optionally matching name
        :return: list of tuples for provider definitions matching name and matching pattern

        Each tuple will be of the form (name, provider_definition)

        """
        if pattern is not None and name is not None:
            summary_list = [provider_definition
                            for provider_definition in DatasetProvider.getRegisteredDatasets().values()
                            if re.match(pattern, provider_definition.name) and name == provider_definition.name]
        elif pattern is not None:
            summary_list = [provider_definition
                            for provider_definition in DatasetProvider.getRegisteredDatasets().values()
                            if re.match(pattern, provider_definition.name)]
        elif name is not None:
            summary_list = [provider_definition
                            for provider_definition in DatasetProvider.getRegisteredDatasets().values()
                            if name == provider_definition.name]
        else:
            summary_list = [provider_definition
                            for provider_definition in DatasetProvider.getRegisteredDatasets().values()]
        return summary_list

    @classmethod
    def list(cls, pattern=None):
        """This method lists the registered datasets
            It filters the list by a regular expression pattern if provided

            :param pattern: Pattern to match dataset names. If None, all datasets are listed
        """
        summary_list = sorted([(providerDefinition.name, providerDefinition.summary) for
                               providerDefinition in cls.getProviderDefinitions(name=None, pattern=pattern)])

        print("The followed datasets are registered and available for use:")
        pprint.pprint(summary_list, indent=4, width=80)

    @classmethod
    def describe(cls, name):
        """This method lists the registered datasets
            It filters the list by a regular expression pattern if provided

            :param name: name of dataset to describe
        """
        providers = cls.getProviderDefinitions(name=name)

        assert [len(providers) >= 1], f"Dataset '{name}' not found"

        providerDef = providers[0]

        summaryAttributes = f""" 
                        | Dataset Name: {providerDef.name}
                        | Summary: {providerDef.summary}
                        | Supports Streaming: {providerDef.supportsStreaming}
                        | Provides Tables: {providerDef.tables}
                        | Primary Table: {providerDef.primaryTable}
                      |"""

        print(f"The dataset '{providerDef.name}' is described as follows:")
        print(strip_margins(summaryAttributes, '|'))
        print("\n".join([x.strip() for x in providers[0].description.split("\n")]))

    def __init__(self, sparkSession, name=None, streaming=False):
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

        providers = self.getProviderDefinitions(name=name)
        assert providers is not None and len(providers) > 0, f"Dataset '{name}' not found"

        if streaming:
            assert self._providerDefinition.supportsStreaming, f"Dataset '{name}' does not support streaming"

        self._providerDefinition = providers[0]

    def get(self, table=None, rows=None, partitions=None, **kwargs):
        provider = self._providerDefinition.providerClass
        print(f" provider: {provider}")
        assert provider is not None and issubclass(provider, DatasetProvider), "Invalid provider class"

        providerInstance = provider()

        if table is None:
            table = self._providerDefinition.primaryTable
            assert table is not None, "Primary table not defined"

        if rows is None:
            rows = Datasets.DEFAULT_ROWS
            assert rows is not None, "Number of rows not defined"

        if partitions is None or partitions == -1:
            partitions = Datasets.DEFAULT_PARTITIONS
            assert partitions is not None and partitions > 0, "Number of partitions not defined"

        tableDefn = providerInstance.getTable(self._sparkSession, tableName=table, rows=rows, partitions=partitions,
                                              **kwargs)
        return tableDefn
