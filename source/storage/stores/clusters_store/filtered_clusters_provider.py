import os
from cStringIO import StringIO
import pandas as pd
from source.storage.io_handlers.s3 import S3IOHandler

from source.storage.stores.clusters_store.interface import ClustersStoreInterface
from source.storage.stores.clusters_store.types import Clusters

__author__ = 'user'


class FilteredClustersProvider(object):
    def __init__(self, io_handler, clusters_store):
        """
        @type io_handler: C{IOHandlerInterface}
        @type clusters_store: C{ClustersStoreInterface}
        """
        self.__io_handler = io_handler
        self.__clusters_store = clusters_store

    def load_clusters(self, clusters_metadata, use_specific_clusters):
        unfiltered_clusters_obj = self.__clusters_store.load_clusters(clusters_metadata)
        if use_specific_clusters:
            props_df, population_to_clusters_matrix = self.__get_selected_clusters_data(unfiltered_clusters_obj.clusters_properties,
                                                                                        unfiltered_clusters_obj.population_to_clusters_matrix,
                                                                                        clusters_metadata)
            return Clusters(clusters_metadata, props_df, population_to_clusters_matrix, unfiltered_clusters_obj.population_ids)

        return unfiltered_clusters_obj

    def __get_selected_clusters_data(self, clusters_props, all_pop_to_clust_map, clusters_metadata):
        selected_clusters_index = self.__get_selected_clusters_indices(clusters_metadata)
        clusters_props_selected = clusters_props[clusters_props.index.isin(selected_clusters_index)]
        clusters_props_selected.index = range(len(clusters_props_selected))
        top_pop_to_clust_map = all_pop_to_clust_map[selected_clusters_index, :]
        return clusters_props_selected, top_pop_to_clust_map

    def __get_selected_clusters_indices(self, clusters_metadata):
        file_path = os.path.join(self.__get_base_matlab_path(clusters_metadata), "chosen_clusters.csv")
        all_paths = list(self.__io_handler.list_dir(file_path))
        selected_clusters_path = filter(lambda x: x.endswith(".csv"), all_paths)[0]
        selected_clusters_indices = pd.read_csv(StringIO(self.__io_handler.load_raw_data(selected_clusters_path)),
                                                header=None, index_col=0).index.values
        return selected_clusters_indices

    @staticmethod
    def __get_base_matlab_path(clusters_metadata):
        return os.path.join('sandbox-{}'.format(clusters_metadata.customer), 'Spheres', clusters_metadata.id,
                            'artifacts', 'blocks')


