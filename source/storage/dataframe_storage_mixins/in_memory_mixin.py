from source.storage.dataframe_storage_mixins.interface import DataFrameStorageMixinInterface
from source.storage.io_handlers.in_memory import InMemoryIOHandler


class InMemoryDataFrameStorageMixin(DataFrameStorageMixinInterface):
    def __init__(self, io_handler):
        """
        Stores clean data as CSVs on the given IO handler
        @param io_handler: The IO handler to use to store data
        @type io_handler: L{InMemoryIOHandler}
        """
        self.__io_handler = io_handler

    def _store_dataframe(self, df, path, partitioning_columns=()):
        """
        Stores the given dataframe at the given path

        @param df: The dataframe to store.
        @type df: C{DataFrame}
        @param path: The path to store at.
        @type path: C{str}
        @param partitioning_columns: Columns to partition the data by.
        @type partitioning_columns: C{tuple} or C{list}
        """
        self.__io_handler[path] = df

    def _load_dataframe(self, path):
        """
        Load a dataframe from the given path

        @param path: The path to load from
        @type path: C{str}

        @return: The loaded dataframe.
        @rtype: C{DataFrame}
        """
        return self.__io_handler[path]
