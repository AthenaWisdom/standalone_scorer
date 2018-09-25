class DataFrameStorageMixinInterface(object):
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
        raise NotImplementedError()

    def _load_dataframe(self, path):
        """
        Load a dataframe from the given path

        @param path: The path to load from
        @type path: C{str}

        @return: The loaded dataframe.
        @rtype: C{DataFrame}
        """
        raise NotImplementedError()
