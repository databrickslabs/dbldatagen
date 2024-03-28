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

from .spark_singleton import SparkSingleton
from .utils import strip_margins
from dbldatagen.datasets.dataset_provider import DatasetProvider


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

    @staticmethod
    def getGlobalDisplayHtmlFn():
        def get_global_function(globals, fnName, packagePrefix=None):
            """Get a global function if available from module or package beginning with a specific prefix

            :param fnName: name of function to check for
            :param packagePrefix: prefix of package to search for function
            :returns: the function if function is available, otherwise None
            """
            assert fnName is not None and len(fnName) > 0, "Function name must be specified"
            assert packagePrefix is None or len(packagePrefix) > 0, "Package prefix must be either null or string"

            try:
                candidate_function = globals[fnName]

                if candidate_function is not None or fnName in globals:
                    candidate_function = globals[fnName]
                    if candidate_function is not None and callable(candidate_function):
                        if packagePrefix is not None:
                            if candidate_function.__module__.startswith(packagePrefix):
                                return candidate_function
                        else:
                            return candidate_function
                else:
                    print(f"Function {fnName} not found in globals")
                return None
            except Exception as e:
                return None

        import inspect
        current = inspect.currentframe()

        while current is not None:
            fn = get_global_function(current.f_globals, "displayHTML")
            if fn is not None:
                return fn
            current = current.f_back
        return None

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
                            for provider_definition, provider_cls in DatasetProvider.getRegisteredDatasets().values()
                            if re.match(pattern, provider_definition.name) and name == provider_definition.name]
        elif pattern is not None:
            summary_list = [provider_definition
                            for provider_definition, provider_cls in DatasetProvider.getRegisteredDatasets().values()
                            if re.match(pattern, provider_definition.name)]
        elif name is not None:
            summary_list = [provider_definition
                            for provider_definition, provider_cls in DatasetProvider.getRegisteredDatasets().values()
                            if name == provider_definition.name]
        else:
            summary_list = [provider_definition
                            for provider_definition, provider_cls in DatasetProvider.getRegisteredDatasets().values()]
        return summary_list

    @classmethod
    def list(cls, pattern=None, output="text/plain"):
        """This method lists the registered datasets
            It filters the list by a regular expression pattern if provided

            :param pattern: Pattern to match dataset names. If None, all datasets are listed
            :param output: output format to use - if 'auto', it will determine the format to use based on the context

            If output is 'text/plain', it will print the dataset description in plain text
            If output is 'text/html', it will print the dataset description in HTML format using the
            'displayHTML' global function if available
            if output is 'auto', it will determine the output format based on the context (i.e depending on whether
            the function is being called from a notebook or a script)
        """
        summary_list = sorted([(providerDefinition.name, providerDefinition.summary) for
                               providerDefinition in cls.getProviderDefinitions(name=None, pattern=pattern)])

        # determine the output format if auto
        if output == "auto":
            output = "text/html" if Datasets.getGlobalDisplayHtmlFn() is not None else "text/plain"

        # now format the list for output
        if output == "text/plain":
            print("The followed datasets are registered and available for use:")
            pprint.pprint(summary_list, indent=4, width=80)
        elif output == "text/html" or output == "html":
            # check if function named displayHtml is defined in the current context
            htmlListing = ["<html><body>",
                           "<h1>Registered Datasets</h1>",
                           "<table>"]
            htmlListing.extend([f"<tr><td>{name}</td><td>{summary}</td></tr>" for name, summary in summary_list])
            htmlListing.extend(["</table>", "</body></html>"])

            displayHtml = Datasets.getGlobalDisplayHtmlFn()
            if displayHtml is not None:
                displayHtml("\n".join(htmlListing))
            else:
                print("\n".join(htmlListing))
        else:
            raise ValueError(f"Output format '{output}' not supported")

    @classmethod
    def describe(cls, name, output="auto"):
        """This method lists the registered datasets
            It filters the list by a regular expression pattern if provided

            :param name: name of dataset to describe
            :param output: output format to use - if 'auto', it will determine the format to use based on the context

            If output is 'text/plain', it will print the dataset description in plain text
            If output is 'text/html', it will print the dataset description in HTML format using the
            'displayHTML' global function if available
            if output is 'auto', it will determine the output format based on the context (i.e depending on whether
            the function is being called from a notebook or a script)

        """
        providers = cls.getProviderDefinitions(name=name)

        assert [len(providers) >= 1], f"Dataset '{name}' not found"

        providerDef = providers[0]

        # determine the output format if auto
        if output == "auto":
            output = "text/html" if Datasets.getGlobalDisplayHtmlFn() is not None else "text/plain"

        # now format the list for output
        if output == "text/plain":
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
        elif output == "text/html" or output == "html":
            # check if function named displayHtml is defined in the current context
            displayHtml = Datasets.getGlobalDisplayHtmlFn()
            if displayHtml is not None:
                displayHtml(providers[0])
            else:
                print("<html><body>")
                print(providers[0])
                print("</body></html>")
        else:
            raise ValueError(f"Output format '{output}' not supported")

    def get(self, tableName=None, rows=None, partitions=None):
        pass

    @property
    def registeredDatasets(self):
        return
