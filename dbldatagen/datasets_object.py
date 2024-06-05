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
        self._datasetsVersion = DatasetProvider.registeredDatasetsVersion
        self._navigator = None

    def getNavigator(self):
        latestVersion = DatasetProvider.registeredDatasetsVersion
        if self._datasetsVersion != latestVersion or not self._navigator:
            # create a navigator object to support x.y.z notation
            root = self.NavigatorNode(self)

            providersMap = DatasetProvider.getRegisteredDatasets()

            for providerName, providerDefn in providersMap.items():
                tables = providerDefn.providerClass.getDatasetTables()

                # root.addEntry(self, providerName, None)

                for table in tables:
                    root.addEntry(self, providerName, table)
            self._navigator = root
            self._datasetsVersion = latestVersion

        return self._navigator

    def _getProviderDefinition(self, providerName, supportsStreaming=False):
        assert providerName is not None and len(providerName), "Dataset provider name must be supplied"

        providers = self.getProviderDefinitions(name=providerName, supportsStreaming=self._streamingRequired)
        if not providers:
            raise ValueError(f"Dataset provider for '{providerName}' could not be found")

        providerDefn = providers[0]

        if supportsStreaming:
            if not providerDefn.supportsStreaming:
                raise ValueError(f"Dataset '{providerName}' does not support streaming")

        return providerDefn

    def _getProviderInstanceAndMetadata(self, providerName, supportsStreaming):
        providerDefinition = self._getProviderDefinition(providerName, supportsStreaming=supportsStreaming)
        providerClass = providerDefinition.providerClass

        if providerClass is None or not DatasetProvider.isValidDataProviderType(providerClass):
            raise ValueError(f"Dataset provider could not be found for name {self._name}")

        if providerClass is None or not DatasetProvider.isValidDataProviderType(providerClass):
            raise ValueError(f"Dataset provider could not be found for name {self._name}")

        providerInstance = providerClass()

        return providerInstance, providerDefinition

    def _get(self, *, providerName, tableName, rows=-1, partitions=-1, **kwargs):

        providerInstance, providerDefinition = \
            self._getProviderInstanceAndMetadata(providerName, supportsStreaming=self._streamingRequired)

        if tableName is None:
            tableName = providerDefinition.primaryTable
            assert tableName is not None, "Primary table not defined"

        tableDefn = providerInstance.getTableGenerator(self._sparkSession, tableName=tableName, rows=rows,
                                                       partitions=partitions,
                                                       **kwargs)
        return tableDefn

    def get(self, table=None, rows=-1, partitions=-1, **kwargs):
        """Get a table generator from the dataset provider

        These are DataGenerator instances that can be used to generate the data.
        The dataset providers also optionally can provide supporting tables which are computed tables based on
        parameters. These are retrieved using the `getAssociatedDataset` method

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

        return self._get(providerName=self._name, tableName=table, rows=rows, partitions=partitions,
                         **kwargs)

    def _getSupportingTable(self, *, providerName, tableName, rows=-1, partitions=-1, **kwargs):
        providerInstance, providerDefinition = \
            self._getProviderInstanceAndMetadata(providerName, supportsStreaming=self._streamingRequired)

        if tableName is None:
            raise ValueError("Name of supporting table must be provided")

        dfSupportingTable = providerInstance.getAssociatedDataset(self._sparkSession, tableName=tableName, rows=rows,
                                                                  partitions=partitions,
                                                                  **kwargs)
        return dfSupportingTable

    def getAssociatedDataset(self, table=None, rows=-1, partitions=-1, **kwargs):
        """Get a table generator from the dataset provider

        These are DataGenerator instances that can be used to generate the data.
        The dataset providers also optionally can provide supporting tables which are computed tables based on
        parameters. These are retrieved using the `getAssociatedDataset` method

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

        return self._getSupportingTable(providerName=self._name, tableName=table, rows=rows, partitions=partitions,
                                        **kwargs)

    # aliases

    """Alias for `getAssociatedDataset`"""
    getSupportingDataset = getAssociatedDataset

    """Alias for `getAssociatedDataset`"""
    getCombinedDataset = getAssociatedDataset

    def __getattr__(self, path):
        assert path is not None, "path should be non-empty"

        navigator = self.getNavigator()

        if self._name:
            navigator = navigator.find(self._name)

            if navigator:
                navigator = navigator.find(path)

        if not navigator:
            raise ValueError(f"Could not find registered provider for path: {path}")

        return navigator

    class NavigatorNode:
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

        def __init__(self, datasets, providerName=None, tableName=None, location=None):
            """ Initialization for node

            :param datasets: instance of datasets object
            :param providerName: provider name for node
            :param tableName: table name for node
            :param location: location for node - used in error reporting
            """
            self._datasets = datasets
            self._children = None
            self._providerName = providerName
            self._tableName = tableName
            self._location = location  # expected to be a list of the attributes used to navigate to the node

        def __repr__(self):
            return f"Node: (datasets: {self._datasets}, provider: {self._providerName}, loc: {self._location} )"

        def _addEntry(self, datasets, steps, providerName, tableName):

            results = self
            if steps is None or len(steps) == 0:
                self._tableName = tableName
                self._providerName = providerName
            else:
                new_location = self._location + [steps[0]] if self._location is not None else [steps[0]]
                if self._children is None:  # no children exist
                    newNode = datasets.NavigatorNode(datasets, location=new_location)
                    self._children = {steps[0]: newNode._addEntry(datasets, steps[1:], providerName, tableName)}
                elif steps[0] in self._children:  # step is in the child dictionary
                    self._children[steps[0]]._addEntry(datasets, steps[1:], providerName, tableName)
                else:  # step is not in the child dictionary
                    newNode = datasets.NavigatorNode(datasets, location=new_location)
                    self._children[steps[0]] = newNode._addEntry(datasets, steps[1:], providerName, tableName)

            return results

        def addEntry(self, datasets, providerName, tableName):
            provider_steps = [x.strip() for x in providerName.split("/") if x is not None and len(x) > 0]

            self._addEntry(datasets, provider_steps, providerName, tableName)

            # add an entry allowing navigation of the form `Datasets("basic").user()` with addition of table name
            if tableName is not None:
                provider_steps.append(tableName)
                self._addEntry(datasets, provider_steps, providerName, tableName)

        def find(self, attributePath):
            provider_steps = [x.strip() for x in attributePath.split("/") if x is not None and len(x) > 0]

            node = self
            for step in provider_steps:
                if node._children is not None and step in node._children:
                    node = node._children[step]
                else:
                    node = None
            return node

        def isFinal(self):
            return self._providerName is not None

        def __getattr__(self, path):
            node = self.find(path)

            if node is None:
                location_path = ".".join(self._location) + "." + path
                raise ValueError(f"Provider / table not found {path} in sequence `{location_path}`")
            return node

        def __call__(self, *args, **kwargs):
            if not self.isFinal():
                raise ValueError(f"Cant resolve provider / table name for sequence {self._location}")

            if self._tableName is not None:
                return self._datasets._get(*args, providerName=self._providerName, tableName=self._tableName, **kwargs)
            else:
                return self._datasets._get(*args, providerName=self._providerName, **kwargs)
