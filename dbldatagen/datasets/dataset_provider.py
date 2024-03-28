# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This file defines the DatasetProvider class
"""
from __future__ import annotations  # needed when using dataclasses in Python 3.8 with type of `list[str]`
from dataclasses import dataclass


class DatasetProvider:
    """
    The DatasetProvider class acts as a base class for all dataset providers

    Implementors should override name,summary, description etc using the `dataset_definition` decorator.
    Subclasses should not require the constructor to take any arguments - arguments for the dataset should be
    passed to the getTable method.

    If no table name is specified, it defaults to a table name of `primary`

    Note that the DatasetDefinitionDecorator class will be used as a decorator to subclasses of this and
    will overwrite the constants _DATASET_NAME, _DATASET_TABLES, _DATASET_DESCRIPTION, _DATASET_SUMMARY and
    _DATASET_SUPPORTS_STREAMING

    Derived DatasetProvider classes need to be registered with the DatasetProvider class to be used in the system
    (at least to be discoverable and creatable via the Datasets object). This is done by calling the
    `registerDataset` method with the dataset definition and the class object.

    Registration can be done manually or automatically by setting the `autoRegister` flag in the decorator to True.

    By default, all DatasetProvider classes should support batch usage. If a dataset provider supports streaming usage,
    the flag `supportsStreaming` should be set to True in the decorator.
    """
    DEFAULT_TABLE_NAME = "primary"
    _DATASET_DEFINITION = None

    # the registered datasets will map from dataset names to a tuple of the dataset definition and the class
    # the implementation for dataset listing and describe will be driven by this
    _registeredDatasets = {}

    @dataclass
    class DatasetDefinition:
        name: str = None
        summary: str = None
        description: str = None
        supportsStreaming: bool = False
        tables: list[str] = None
        primaryTable: str = None

    @classmethod
    def getDatasetDefinition(cls):
        return cls._DATASET_DEFINITION

    @classmethod
    def registerDataset(cls, datasetDefinition, datasetClass):
        """ Register the dataset with the given name

        :param name: Name of the dataset
        :param datasetDefinition: Dataset object
        :param datasetClass: Dataset object
        :return: None
        """
        assert isinstance(datasetDefinition, cls.DatasetDefinition), \
               "datasetDefinition must be an instance of DatasetDefinition"

        assert datasetDefinition.name is not None, \
               "datasetDefinition must contain a name for the data set"

        assert datasetDefinition.name is not None, \
               "datasetDefinition must contain a name for the data set"

        assert issubclass(datasetClass, cls) , \
               "datasetClass must be a subclass of DatasetProvider"

        cls._registeredDatasets[datasetDefinition.name] = (datasetDefinition, datasetClass)

    @classmethod
    def getRegisteredDatasets(cls):
        """
        Get the registered datasets
        :return:  A dictionary of registered datasets
        """
        return cls._registeredDatasets

    def get(self, tableName=None, **options):
        """ gets table for table name

        :param tableName:
        :param options:
        :return:

        Implementors should use this method but override the getTable method
        """

        if tableName is None:
            tableName = self.tables[0]

        if tableName in self.tables:
            return self.getTable(tableName, **options)
        else:
            raise ValueError(f"Data provider does not provide table named '{tableName}'")

    def getTable(self, tableName, rows=1000000, partitions=4, **options):
        """Gets table for named table

        :param tableName: Name of table to provide
        :param rows: Number of rows requested
        :param partitions: Number of partitions requested
        :param options: Options passed to generate the table
        :return: DataGenerator for table if successful, throws error otherwise
        """
        raise NotImplementedError("Base data provider does not provide any tables!")

    class DatasetDefinitionDecorator:
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
            self._datasetName = name if name is not None else f"providers/{cls.__name__}"

            self._tables = tables if tables is not None else [DatasetProvider.DEFAULT_TABLE_NAME]
            self._primaryTable = primaryTable if primaryTable is not None else self._tables[0]

            self._summary = summary if summary is not None else f"Dataset implemented by '{str(cls)}'"

            if description is not None:
                self._description = description
            elif cls.__doc__ is not None and len(cls.__doc__.strip()) > 0:
                self._description = cls.__doc__
            else:
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

            Applies the decorator args to class

            :return: Returns the target class object
            """
            if self._targetCls is not None:
                dataset_desc = DatasetProvider.DatasetDefinition(name=self._datasetName,
                                                                 tables=self._tables,
                                                                 primaryTable=self._primaryTable,
                                                                 summary=self._summary,
                                                                 description=self._description,
                                                                 supportsStreaming=self._supportsStreaming
                                                                 )
                setattr(self._targetCls, "_DATASET_DEFINITION", dataset_desc)
                retval = self._targetCls
            else:
                raise ValueError("Decorator must be applied to a class")

            if autoRegister:
                DatasetProvider.registerDataset(dataset_desc, retval)

            return retval


def dataset_definition(cls=None, *args, autoRegister=False, **kwargs):
    """ decorator to define standard dataset definition

    This is intended to be applied classes derived from DatasetProvider to simplify the implementation
    of the predefined datasets.

    :param cls: class object for subclass of DatasetProvider
    :param args: positional args
    :param autoRegister: whether to automatically register the dataset
    :param kwargs: keyword args
    :return: either instance of DatasetDefinitionDecorator or function which will produce instance of this when called

    This function is intended to be used as a decorator.

    When applied without arguments, it will return a nested
    wrapper function which will take the subsequent class object and apply the DatasetDefinitionDecorator to it.

    When applied with arguments, the arguments will be applied to the construct of the DatasetDefinitionDecorator.

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

    def inner_wrapper(inner_cls=None, *inner_args, **inner_kwargs):
        return DatasetProvider.DatasetDefinitionDecorator(inner_cls, *args, **kwargs).mkClass(autoRegister)

    # handle the decorator syntax with no arguments
    if cls is not None:
        # handle decorator syntax with no arguments
        # when no arguments are provided to the decorator, the only argument passed is an implicit class object
        assert issubclass(cls, DatasetProvider), f"Target class of decorator ({cls}) must inherit from DataProvider"
        return DatasetProvider.DatasetDefinitionDecorator(cls, *args, **kwargs).mkClass(autoRegister)
    else:
        # handle decorator syntax with arguments
        return inner_wrapper
