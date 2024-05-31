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

from __future__ import annotations  # needed when using dataclasses in Python 3.8 with subscripts
import re

from dbldatagen.datasets.dataset_provider import DatasetProvider
from .spark_singleton import SparkSingleton
from .utils import strip_margins
from dataclasses import dataclass
from typing import Optional


class Datasets:
    """This class is used to generate standard data sets based on a plugin provider model.

       It allows for quick generation of data for common scenarios.

    :param sparkSession: Spark session instance to use when performing spark operations
    :param name: Dataset name to use

    Dataset names are registered with the DatasetProvider class. By convention, dataset names should be hierarchical
    and separated by slashes ('/')

    For example, the dataset name 'sales/retail' would indicate that the dataset is a retail dataset within the sales
    category.

    The dataset name is used to look up the provider class that will be used to generate the data.

    If a dataset provider supports multiple tables, the name of the table to retrieve is passed to the
    `get` method, along with any parameters that are required to generate the data.

    """

    @classmethod
    def getProviderDefinitions(cls, name=None, pattern=None, supportsStreaming=False):
        """Get provider definitions for one or more datasets

        :param name: name of dataset to get provider for, if None, returns all providers
        :param pattern: pattern to match dataset name, if None, returns all providers optionally matching name
        :param supportsStreaming: If true, filters out dataset providers that don't support streaming
        :return: list of provider definitions matching name and  pattern

        Each entry will be of the form DatasetProvider.DatasetProviderDefinition

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
            summary_list = list(DatasetProvider.getRegisteredDatasets().values())

        # filter for streaming
        if supportsStreaming:
            summary_list_filtered = [provider_definition
                                     for provider_definition in summary_list
                                     if provider_definition.supportsStreaming]
            return summary_list_filtered
        else:
            return summary_list

    @classmethod
    def list(cls, pattern=None, supportsStreaming=False):
        """This method lists the registered datasets
            It filters the list by a regular expression pattern if provided

            :param pattern: Pattern to match dataset names. If None, all datasets are listed
            :param supportsStreaming: if True, only return providerDefinitions that supportStreaming
        """
        summary_list = sorted([(providerDefinition.name, providerDefinition.summary) for
                               providerDefinition in cls.getProviderDefinitions(name=None, pattern=pattern,
                                                                                supportsStreaming=supportsStreaming)])

        print("The followed datasets are registered and available for use:")

        for entry in summary_list:
            print(f"  Provider: `{entry[0]}` - Summary description: {entry[1]}")

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
        print("")
        print("Detailed description:")
        print("")
        print(providerDef.description)

    def __init__(self, sparkSession, name=None, streaming=False):
        """ Constructor:
        :param sparkSession: Spark session to use
        :param name: name of dataset to search for
        :param streaming: if True, validdates that dataset supports streaming data
        """
        if not sparkSession:
            sparkSession = SparkSingleton.getLocalInstance()

        self._sparkSession = sparkSession
        self._name = name
        self._streamingRequired = streaming
        self._providerDefinition = None

        # build navigator for datasets

    def getNavigator(self):


    def _get(self, *, providerName, tableName, rowsRequested, partitionsRequested, **kwargs):
        assert providerName is not None and len(providerName), "Dataset provider name must be supplied"

        providers = self.getProviderDefinitions(name=providerName, supportsStreaming=self._streamingRequired)
        if not providers:
            raise ValueError(f"Dataset provider for '{providerName}' could not be found")

        self._providerDefinition = providers[0]

        if self._streamingRequired:
            if not self._providerDefinition.supportsStreaming:
                raise ValueError(f"Dataset '{providerName}' does not support streaming")

        assert datasetProvider is not None and DatasetProvider.isValidDataProviderType(datasetProvider), \
            "Expecting valid dataset provider"

        if tableName is None:
            table = self._providerDefinition.primaryTable
            assert table is not None, "Primary table not defined"

        assert tableName and len(tableName.strip() > 0), "Expecting valid table name"

        if datasetProvider is None or not DatasetProvider.isValidDataProviderType(datasetProvider):
            raise ValueError(f"Dataset provider could not be found for name {self._name}")

        providerInstance = datasetProvider()

        tableDefn = providerInstance.getTable(self._sparkSession, tableName=tableName, rows=rowsRequested,
                                              partitions=partitionsRequested,
                                              **kwargs)
        return tableDefn

    def get(self, table=None, rows=None, partitions=-1, **kwargs):
        """Get a table from the dataset
        If the dataset supports multiple tables, the table may be specified in the `table` parameter.
        If none is specified, the primary table is used.

        :param table: name of table to retrieve
        :param rows: number of rows to generate. if -1, provider should compute defaults.
        :param partitions: number of partitions to use.If -1, the number of partitions is computed automatically
        table size and partitioning.If applied to a dataset with only a single table, this is ignored.
        :param kwargs: additional keyword arguments to pass to the provider

        If `rows` or `partitions` are not specified, default values are supplied by the provider.

        For multi-table datasets, the table name must be specified. For single table datasets, the table name may
        be optionally supplied.

        Additionally, for multi-table datasets, the table name must be one of the tables supported by the provider.
        Default number of rows for multi-table datasets may differ - for example a 'customers' table may have a
        100,000 rows while a 'sales' table may have 1,000,000 rows.
        """

        if self._providerDefinition is None:
            self._locateProvider(self._name, supportsStreaming=self._streamingRequired)

        provider = self._providerDefinition.providerClass
        print(f" provider: {provider}")

        return self._get(provider)

    def __getattr__(self, path):
        # navigator = self.DatasetNavigator(self, self.name, path)

        # TODO: register paths and providers for registered data sets with the navigator

        # TODO: initiatlize navigator base
        return None


###
    class TreeNode:
        """"Tree node class for DatasetNavigator tree"""

        def __init__(self, nodeName, providerClass=None):
            assert nodeName is not None and len(nodeName.strip()) > 0, "Node name must be specified"
            self._nodeName = nodeName
            self._providerClass = providerClass  # provider class for provider
            self.children = {}  # dictionary of children

        def __str__(self):
            return f"Node: {self._nodeName}"

        @property
        def nodeName(self):
            return self._nodeName

        @property
        def providerClass(self):
            return self._providerClass

    class DatasetNavigator:
        """Dataset Navigator class for navigating datasets

        This class is used to navigate datasets and their tables via dotted notation.

        Ie X.dataset_grouping.dataset.table where X is an intance of the dataset navigator.

        The navigator is initialized with a set of paths and objects (usually providers) that are registered with the
        DatasetProvider class.

        When accessed via dotted notation, the navigator will use the pathSegment to locate the provider and create it.

        Any remaining pathSegment traversed will be used to locate the table within the provider.

        Overall, this just provides a syntactic layering over the creation of the provider instance
        and table generation.

        """

        def __init__(self):
            self._root = Datasets.TreeNode(None)
            self._currentPosition = self._root

        def __getattr__(self, pathSegment):
            """ Get the attribute """
            current = self._root
            segments = pathSegment.split('/')
            for segment in segments:
                if segment in current.children:
                    current = current.children[segment]
                else:
                    return None
            return current

        def __call__(self, path):
            segments = path.split('.')
            if len(segments) < 2:
                return None
            obj = self.__getattr__('.'.join(segments[:-1]))
            if obj is not None:
                return obj.getTable(segments[-1])
            return None

        def insertProvider(self, path):
            current = self._root
            segments = path.split('/')
            for segment in segments:
                if segment not in current.children:
                    current.children[segment] = Datasets.TreeNode(segment)
                current = current.children[segment]


####

    @dataclass
    class DatasetNavigator:
        """Dataset Navigator class for navigating datasets

        This class is used to navigate datasets and their tables via dotted notation.

        Ie X.dataset_grouping.dataset.table where X is an intance of the dataset navigator.

        The navigator is initialized with a set of paths and objects (usually providers) that are registered with the
        DatasetProvider class.

        When accessed via dotted notation, the navigator will use the pathSegment to locate the provider and create it.

        Any remaining pathSegment traversed will be used to locate the table within the provider.

        Overall, this just provides a syntactic layering over the creation of the provider instance
        and table generation.

        """
        children: Optional[dict[str, 'DatasetNavigator']] = None
        providerName: Optional[str] = None
        tableName: Optional[str] = None
        location: Optional[list[str]] = None

        def _addEntry(self, steps, providerName, tableName):
            results = self
            if steps is None or len(steps) == 0:
                self.tableName = tableName
                self.providerName = providerName
            else:
                new_location = self.location + [steps[0]] if self.location is not None else [steps[0]]
                if self.children is None:  # no children exist
                    self.children = {steps[0]: Node(location=new_location)._addEntry(steps[1:], providerName, tableName)}
                elif steps[0] in self.children:  # step is in the child dictionary
                    self.children[steps[0]]._addEntry(steps[1:], providerName, tableName)
                else:  # step is not in the child dictionary
                    self.children[steps[0]] = Node(location=new_location)._addEntry(steps[1:], providerName, tableName)

            return results

        def addEntry(self, provider_name, table_name):
            provider_steps = [x.strip() for x in provider_name.split("/") if x is not None and len(x) > 0]

            self._addEntry(provider_steps, provider_name, table_name)

            if table_name is not None:
                provider_steps.append(table_name)
                self._addEntry(provider_steps, provider_name, table_name)

        def find(self, step):
            if self.children is not None and step in self.children:
                return self.children[step]
            return None

        def isFinal(self):
            return self.providerName is not None

        def __getattr__(self, path):
            node = self.find(path)

            if node is None:
                location_path = ".".join(self.location) + "." + path
                raise ValueError(f"Provider / table not found {path} in sequence `{location_path}`")
            return node

        def __call__(self, **kwargs):
            print(self.providerName, kwargs)
            return self.isFinal()


#root = DatasetNavigator()

#for (provider, tables) in examples:
#    for tableName in tables:
#        root.addEntry(provider, tableName)
#    root.addEntry(provider, None)

# import pprint
# pprint.pprint(root, indent=4)
#print(root.basic.user(rows=4))