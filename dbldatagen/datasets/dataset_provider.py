# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the DatasetProvider class
"""
from __future__ import annotations  # needed when using dataclasses in Python 3.8 with type of `list[str]`

import functools
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass


class DatasetProvider(ABC):
    """
    The DatasetProvider class acts as a base class for all dataset providers

    Implementors should override name,summary, description etc using the `dataset_definition` decorator.
    Subclasses should not require the constructor to take any arguments - arguments for the dataset should be
    passed to the getTableDataGenerator method.

    If no table name is specified, it defaults to a table name corresponding to the `DEFAULT_TABLE_NAME` constant.

    Dataset providers that produce multiple tables or need to explicitly name the table should list which tables are
    provided and what the primary table to be retrieved would be if no table name is specified.

    The intent of the `supportsStreaming` flag is mark a dataset as being streaming compatible. It does not require
    specific streaming support - only to note that the dataset provider does not have any operations that would
    be disallowed or very inefficient for a streaming dataframe.

    Examples of operations that might prevent a dataframe from being used in streaming include:
    - use of some types of windowing expressions
    - use of drop duplicates (not disallowed but can only be applied efficiently at the streaming microbatch level)

    Note that the DatasetDecoratorUtils inner class will be used as a decorator to subclasses of this and
    will overwrite the constants _DATASET_NAME, _DATASET_TABLES, _DATASET_DESCRIPTION, _DATASET_SUMMARY and
    _DATASET_SUPPORTS_STREAMING

    Derived DatasetProvider classes need to be registered with the DatasetProvider class to be used in the system
    (at least to be discoverable and creatable via the Datasets object). This is done by calling the
    `registerDataset` method with the dataset definition and the class object.

    Registration can be done manually or automatically by setting the `autoRegister` flag in the decorator to True.

    By default, all DatasetProvider classes should support batch usage. If a dataset provider supports streaming usage,
    the flag `supportsStreaming` should be set to `True` in the decorator.
    """
    DEFAULT_TABLE_NAME = "main"
    DEFAULT_ROWS = 100_000
    DEFAULT_PARTITIONS = 4

    # Note: the associated decorator will populate this with an instance of the `DatasetDefinition` class
    _DATASET_DEFINITION = None

    # the registered datasets will map from dataset names to a tuple of the dataset definition and the class
    # the implementation for dataset listing and describe will be driven by this
    _registeredDatasetsMetadata = {}

    # _registeredDatasetsVersion will contain a computed version number which is updated on new dataset
    # registration or when dataset provider is unregistered
    registeredDatasetsVersion = 0

    @dataclass
    class DatasetDefinition:
        """ Dataset Definition class - stores the attributes related to the dataset for use by the implementation
        of the decorator.

        This stores the name of the dataset (e.g. `basic/user`), the list of tables provided by the dataset,
        the primary table, a summary of the dataset, a detailed description of the dataset, whether the dataset
        supports streaming, and the provider class.
        """
        name: str
        tables: list[str]
        primaryTable: str
        summary: str
        description: str
        supportsStreaming: bool
        providerClass: type

    @classmethod
    def isValidDataProviderType(cls, candidateDataProvider):
        """Check if object is a valid data provider type

        :param candidateDataProvider: potential Dataset provider class
        :return: True if valid DatasetProvider type, False otherwise

        """
        return (candidateDataProvider is not None and
                isinstance(candidateDataProvider, type) and
                issubclass(candidateDataProvider, cls))

    @classmethod
    def getDatasetDefinition(cls):
        """ Get the dataset definition for the class """
        return cls._DATASET_DEFINITION

    @classmethod
    def getDatasetTables(cls):
        """ Get the dataset tables list for the class """
        datasetDefinition = cls.getDatasetDefinition()

        if datasetDefinition is None or datasetDefinition.tables is None:
            return [cls.DEFAULT_TABLE_NAME]

        return datasetDefinition.tables

    @classmethod
    def registerDataset(cls, datasetProvider):
        """ Register the dataset provider type using metadata defined in the dataset provider

        :param datasetProvider: Dataset provider class
        :return: None

        The dataset provider argument should be a subclass of the DatasetProvider class.

        It will retrieve the DatasetDefinition populated during creation by the decorator
        and should contain the name of the dataset, the list of tables provided by the dataset,
        the primary table, a summary of the dataset, a detailed description of the dataset,
        whether the dataset supports streaming, and the provider class.
        """
        if datasetProvider is None:
            raise ValueError("Valid dataset provider not supplied")

        if not cls.isValidDataProviderType(datasetProvider):
            raise ValueError(f"Supplied dataset provider {datasetProvider} is not a valid subclass of DatasetProvider")

        datasetDefinition = datasetProvider.getDatasetDefinition()

        assert isinstance(datasetDefinition, cls.DatasetDefinition), \
            "retrieved datasetDefinition must be an instance of DatasetDefinition"

        assert datasetDefinition.name is not None, \
            "datasetDefinition must contain a name for the data set"

        assert issubclass(datasetDefinition.providerClass, cls), \
            "datasetClass must be a subclass of DatasetProvider"

        if datasetDefinition.name in cls._registeredDatasetsMetadata:
            raise ValueError(f"Dataset provider is already registered for name `{datasetDefinition.name}`")

        cls._registeredDatasetsMetadata[datasetDefinition.name] = datasetDefinition
        cls.registeredDatasetsVersion = cls.registeredDatasetsVersion + 1

    @classmethod
    def unregisterDataset(cls, name):
        """ Unregister the dataset with the specified name

        :param name: Name of the dataset to unregister
        """
        assert name is not None and len(name.strip()) > 0, "name must be provided and not empty"

        # remove name from registered datasets if its already registered
        if name in cls._registeredDatasetsMetadata:
            del cls._registeredDatasetsMetadata[name]
            cls.registeredDatasetsVersion = cls.registeredDatasetsVersion + 1

    @classmethod
    def getRegisteredDatasets(cls):
        """
        Get the registered dataset definitions
        :return:  A dictionary of registered datasets metadata objects
        """
        return cls._registeredDatasetsMetadata

    @classmethod
    def getRegisteredDatasetsVersion(cls):
        """
        Get the registered datasets version indicator
        :return:  A dictionary of registered datasets
        """
        return cls._registeredDatasetsVersion

    @abstractmethod
    def getTableDataGenerator(self, sparkSession, *, tableName=None, rows=-1, partitions=-1,
                              **options):
        """Gets data generation instance that will produce table for named table

        :param sparkSession: Spark session to use
        :param tableName: Name of table to provide
        :param rows: Number of rows requested
        :param partitions: Number of partitions requested
        :param autoSizePartitions: Whether to automatically size the partitions from the number of rows
        :param options: Options passed to generate the table
        :return: DataGenerator instance to generate table if successful, throws error otherwise

        Implementors of the individual data providers are responsible for sizing partitions for the datasets based
        on the number of rows and columns. The number of partitions can be computed based on the number of rows
        and columns using the `autoComputePartitions` method.

        The Datasets object that serves as the entru point to the standard datasets will provide a default number of
        rows if none are provided.

        """
        raise NotImplementedError("Base data provider does not provide any tables!")

    @staticmethod
    def allowed_options(options=None):
        """ Decorator to enforce allowed options

            Used to document and enforce what options are allowed for each dataset provider implementation
            If the signature of the getTableDataGenerator method changes, change the DEFAULT_OPTIONS constant
            to include options that are always allowed
        """
        DEFAULT_OPTIONS = ["sparkSession", "tableName", "rows", "partitions"]

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                bad_options = [keyword_arg for keyword_arg in kwargs
                               if keyword_arg not in DEFAULT_OPTIONS and keyword_arg not in options]

                if len(bad_options) > 0:
                    errorMessage = f"""The following options are unsupported by provider: [{",".join(bad_options)}]"""
                    raise ValueError(errorMessage)

                return func(*args, **kwargs)

            return wrapper

        return decorator

    def checkOptions(self, options, allowedOptions):
        """ Check that options are valid

        :param options: options to check as dict
        :param allowedOptions: allowed options as list of strings
        :return: self
        """
        for key in options.keys():
            assert key in allowedOptions, f"Invalid option '{key}'"

        return self

    def autoComputePartitions(self, rows, columns):
        """ Compute the number of partitions based on rows and columns

        :param rows: number of rows
        :param columns: number of columns
        :return: number of partitions

        The equations is based on the number of rows and columns. It will produce 4 partitions as a minimum with
        12 partitions with 5,000,000 rows and 100 columns.

        For very large tables such as 1 billion rows and 10 columns, it will produce 18 partitions and increase
        logarithmically with the number of rows and columns.

        Implementors of standard datasets can chose to scale this value or use their own calculation.
        """
        return max(self.DEFAULT_PARTITIONS, int(math.log(rows / 350_000) * max(1, math.log(columns))))

    class DatasetDecoratorUtils:
        """ Defines the predefined_dataset decorator

            :param cls: target class to apply decorator to
            :param name: name of the dataset
            :param tables: list of tables provided by the dataset, if None, will default to [ DEFAULT_TABLE_NAME ]
            :param primaryTable: primary table provided by dataset. Defaults to first table of table list
            :param summary: Summary information for the dataset. If None, will be derived from target class name
            :param description: Detailed description of the class. If None, will use the target class doc string
            :param supportsStreaming: Whether data set can be used in streaming scenarios
        """

        def __init__(self, cls=None, *, name=None, tables=None, primaryTable=None, summary=None, description=None,
                     supportsStreaming=False):
            self._targetCls = cls

            # compute the data set provider name if not provided.
            # default name will be "providers/{classname}"
            self._datasetName = name if name is not None else f"providers/{cls.__name__}"

            # compute list of tables if not provided
            self._tables = tables if tables is not None else [DatasetProvider.DEFAULT_TABLE_NAME]

            # compute the primary table if not provided
            self._primaryTable = primaryTable if primaryTable is not None else self._tables[0]

            # compute the summary if not provided
            self._summary = summary if summary is not None else f"Dataset implemented by '{str(cls)}'"

            # compute the description if not provided
            # the default description will be the decorator target class's doc string
            if description is not None:
                self._description = description
            elif cls.__doc__ is not None and len(cls.__doc__.strip()) > 0:
                self._description = cls.__doc__
            else:
                # if theres no doc string, then compute a more detailed description for the class
                generated_description = [
                    f"The datasetProvider '{cls.__name__}' provides a data spec for the '{self._datasetName}' dataset",
                    "",  # empty line
                    f"Summary: {self._summary}"
                    "",  # empty line
                    f"Tables provided: {', '.join(self._tables)}",
                    "",  # empty line
                    f"Primary table: {self._primaryTable}",
                    ""  # empty line
                ]
                self._description = "\n".join(generated_description)

            self._supportsStreaming = supportsStreaming

        def mkClass(self, autoRegister=False):
            """ make the modified class for the Data Provider

            Applies the decorator args as a metadata object on the class.
            This is done at the class level as there is no instance of the target class at this point.

            :return: Returns the target class object
            """
            if self._targetCls is not None:
                # if self._targetCls is not None and (isinstance(self._targetCls, DatasetProvider) or
                #                                    issubclass(self._targetCls, DatasetProvider)):
                dataset_desc = DatasetProvider.DatasetDefinition(name=self._datasetName,
                                                                 tables=self._tables,
                                                                 primaryTable=self._primaryTable,
                                                                 summary=self._summary,
                                                                 description=self._description,
                                                                 supportsStreaming=self._supportsStreaming,
                                                                 providerClass=self._targetCls
                                                                 )
                setattr(self._targetCls, "_DATASET_DEFINITION", dataset_desc)
                retval = self._targetCls
            else:
                raise TypeError("Decorator must be applied to a class")

            if autoRegister:
                DatasetProvider.registerDataset(self._targetCls)

            return retval


def dataset_definition(cls=None, *args, autoRegister=False, **kwargs):  # pylint: disable=keyword-arg-before-vararg
    """ decorator to define standard dataset definition

    This is intended to be applied classes derived from DatasetProvider to simplify the implementation
    of the predefined datasets.

    :param cls: class object for subclass of DatasetProvider
    :param args: positional args
    :param autoRegister: whether to automatically register the dataset
    :param kwargs: keyword args
    :return: either instance of DatasetDecoratorUtils or function which will produce instance of this when called

    This function is intended to be used as a decorator.

    When applied without arguments, it will return a nested
    wrapper function which will take the subsequent class object and apply the DatasetDecoratorUtils to it.

    When applied with arguments, the arguments will be applied to the construct of the DatasetDecoratorUtils.

    This allows for the use of either syntax for decorators
    ```
    @dataset_definition
    class X(DatasetProvider)
    ```
    or

    ```
    @dataset_definition(name="basic/basic", tables=["primary"])
    class X(DatasetProvider)
    ```

    """

    def inner_wrapper(inner_cls=None, *inner_args, **inner_kwargs):  # pylint: disable=keyword-arg-before-vararg
        """ The inner wrapper function is used to handle the case where the decorator is used with arguments.
        It defers the application of the decorator to the target class until the target class is available.

        :param inner_cls: inner class object
        :param inner_args: inner args
        :param inner_kwargs: inner keyword args

        :return: Returns the target class object
        """
        try:
            assert DatasetProvider.isValidDataProviderType(inner_cls), \
                f"Target class of decorator ({inner_cls}) must inherit from DataProvider"
            return DatasetProvider.DatasetDecoratorUtils(inner_cls, *args, **kwargs).mkClass(autoRegister)
        except Exception as exc:
            raise TypeError(f"Invalid decorator usage: {exc}") from exc

    # handle the decorator syntax with no arguments - when there are no arguments, the only argument passed is an
    # implicit class object
    try:
        if cls is not None:
            # handle decorator syntax with no arguments
            # when no arguments are provided to the decorator, the only argument passed is an implicit class object
            assert DatasetProvider.isValidDataProviderType(cls), \
                f"Target class of decorator ({cls}) must inherit from DataProvider"
            return DatasetProvider.DatasetDecoratorUtils(cls, *args, **kwargs).mkClass(autoRegister)
        else:
            # handle decorator syntax with arguments - here we simply return the inner wrapper function
            # and the subsequent call with arguments will apply the decorator to the target class
            return inner_wrapper
    except Exception as e:
        raise TypeError(f"Invalid decorator usage: {e}") from e
