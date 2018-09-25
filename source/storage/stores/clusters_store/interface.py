from pandas import DataFrame

from source.storage.stores.clusters_store.types import ClustersMetaData, Clusters

# TODO(shahar): change design to FilteredClustersProvider in ml-runner.


class ClustersStoreInterface(object):
    def load_clusters(self, clusters_metadata):
        """
        Loads the clusters using the given metadata

        @param clusters_metadata: The metadata to use to load the clusters.
        @type clusters_metadata: L{ClustersMetaData}

        @return: The loaded clusters
        @rtype: L{Clusters}
        """
        raise NotImplementedError()

    def load_population(self, clusters_metadata):
        """
        Returns a set of the ids in the clusters

        @param clusters_metadata: The metadata to use to load the clusters.
        @type clusters_metadata: L{ClustersMetaData}

        @rtype: C{set}
        """
        raise NotImplementedError()

    def store_clusters_footprint_html(self, clusters_metadata, html_data):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @type html_data: C{str}
        @rtype: C{str}
        """
        raise NotImplementedError()

    def store_clusters_for_spark(self, clusters_metadata, dataframe):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @type dataframe: C{DataFrame}
        """
        raise NotImplementedError()

    def store_clusters_properties_for_spark(self, clusters_metadata, dataframe):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @type dataframe: C{DataFrame}
        """
        raise NotImplementedError()

    def store_population(self, clusters_metadata, population):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @type population: C{set}
        """
        raise NotImplementedError()


class InMemoryClustersStore(ClustersStoreInterface):
    def store_clusters_properties_for_spark(self, clusters_metadata, dataframe):
        pass

    def __init__(self):
        self.__storage = {}

    def store_clusters_for_spark(self, clusters_metadata, dataframe):
        pass

    def store_population(self, clusters_metadata, population):
        pass

    def load_population(self, clusters_metadata):
        pass

    def load_clusters(self, clusters_metadata):
        key = clusters_metadata.customer + "_" + clusters_metadata.id
        return self.__storage[key]

    def store_clusters_footprint_html(self, clusters_metadata, html_data):
        pass

    def store_clusters(self, clusters):
        """
        @type clusters: L{Clusters}
        """
        key = clusters.metadata.customer + "_" + clusters.metadata.id
        self.__storage[key] = clusters
