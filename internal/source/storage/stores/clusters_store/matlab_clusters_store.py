import os
import re
import csv
import struct
import tempfile
import logging
from cStringIO import StringIO
from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd
import scipy.io as scio
from functional import seq
from scipy.sparse import csr_matrix

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.clusters_store.interface import ClustersStoreInterface
from source.storage.stores.clusters_store.types import Clusters, ClustersMetaData

POPULATION_LINE_LENGTH = 10000


class MatlabClustersStore(ClustersStoreInterface):
    MAT_INIT_NAME = 'blk_data'
    MAT_FIELDS = {
        'src': 2,
        'type': 1,
        'blk_type': 1,
        'field': 1,
        'fieldby': 1,
        'N': 1,
        'WN': 1,
        'deg': 1,
        'thrs': 2,
        'SUB_CLUSTERS_FILE': 1,
        'percInternal': 1
    }

    def __init__(self, io_handler):
        """
        @type io_handler: C{IOHandlerInterface}
        """
        self.__io_handler = io_handler
        self.__logger = logging.getLogger('endor')
        self.__loaded_clusters = {}
        self.__loaded_populations = {}

    def store_population(self, clusters_metadata, population):
        with NamedTemporaryFile(delete=False) as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            seq(population).grouped(POPULATION_LINE_LENGTH).for_each(lambda x: writer.writerow(x.to_list()))
            local_path = csvfile.name
        remote_path = os.path.join(self.__get_base_matlab_path(clusters_metadata), "population.csv")
        self.__io_handler.save_raw_data(open(local_path).read(), remote_path, "text/csv")

    def load_clusters(self, clusters_metadata):
        """
        Loads the clusters using the given metadata

        @param clusters_metadata: The metadata to use to load the clusters.
        @type clusters_metadata: L{ClustersMetaData}

        @return: The loaded clusters
        @rtype: L{Clusters}
        """
        key = str(clusters_metadata)
        if key not in self.__loaded_clusters:
            props_df = self.__build_mat_props_df(clusters_metadata)
            population_to_clusters_matrix = self.__build_pop_to_clusters_map(clusters_metadata)

            pop = self.__get_translation_pop(clusters_metadata)
            self.__loaded_clusters[key] = Clusters(clusters_metadata, props_df, population_to_clusters_matrix, pop)

        return self.__loaded_clusters[key]

    def store_clusters_footprint_html(self, clusters_metadata, html_data):
        path = os.path.join('sandbox-{}'.format(clusters_metadata.customer), 'Spheres', clusters_metadata.id,
                            'artifacts', 'clusters_footprint.html')
        self.__io_handler.save_raw_data(html_data, path)
        self.__io_handler.add_public_read(path)
        return self.__io_handler.get_object_url(path)

    @staticmethod
    def __get_base_matlab_path(clusters_metadata):
        return os.path.join('sandbox-{}'.format(clusters_metadata.customer), 'Spheres', clusters_metadata.id,
                            'artifacts', 'blocks')

    def __get_files_by_name_in_path(self, path, name):
        all_files_in_dir = list(self.__io_handler.list_dir(path))

        relevant_files = [file_name for file_name in all_files_in_dir if name in file_name]
        return relevant_files

    def __get_mat_files_names(self, clusters_metadata):

        mat_names_list = self.__get_files_by_name_in_path(self.__get_base_matlab_path(clusters_metadata),
                                                          self.MAT_INIT_NAME)
        mat_names_list.sort(key=lambda x: int(re.search(r'\d+', x).group()))
        return mat_names_list

    def __get_mat_file(self, mat_file_name):
        # noinspection PyTypeChecker
        mat = scio.loadmat(StringIO(self.__io_handler.load_raw_data(mat_file_name)))
        return mat

    def __build_multiple_mat_clusters_properties(self, mat_files, prop_names, prop_names_count_dict):

        all_props_df = pd.DataFrame()
        for mat_file in mat_files:
            df = self.__build_single_mat_clusters_properties(mat_file, prop_names, prop_names_count_dict)
            all_props_df = all_props_df.append(df)
        all_props_df.index = range(len(all_props_df.index))
        return all_props_df

    def __build_single_mat_clusters_properties(self, mat_file, prop_names, prop_names_count_dict):
        try:
            clusters_props = mat_file[prop_names]
        except Exception:
            msg = "Field %s doesn't exist in mat file, but expected. Cannot continue" % prop_names
            self.__logger.error(msg)
            raise ValueError(msg)

        cluster_prop_dict = {}
        for prop_name, counts in prop_names_count_dict.iteritems():
            try:
                mat_values = clusters_props[prop_name][0][0]
            except (KeyError, IndexError):
                self.__logger.warning("Field %s doesn't exist in mat file, please remove it from config file."
                                      % prop_name)
                break
            if counts == 1:
                cluster_prop_dict[prop_name] = mat_values.flatten()
            else:
                for i in np.arange(counts):
                    cluster_prop_dict[prop_name + '_' + str(i)] = mat_values[:, i]
        cluster_prop_df = pd.DataFrame(cluster_prop_dict)
        # noinspection PyUnresolvedReferences
        cluster_prop_df.index.names = ['cluster']
        cluster_prop_df['W_prcntg'] = cluster_prop_df['WN'].astype(float) / cluster_prop_df['N'].astype(float)
        del mat_file
        return cluster_prop_df

    def __build_mat_props_df(self, clusters_metadata):
        mat_names_list = self.__get_mat_files_names(clusters_metadata)

        if len(mat_names_list) > 1:
            prop_names = 'temp_'
            mat_files = [self.__get_mat_file(mat_name) for mat_name in mat_names_list]
            props_df = self.__build_multiple_mat_clusters_properties(mat_files, prop_names, self.MAT_FIELDS)
        else:
            prop_names = 'blk_data'
            mat_file = self.__get_mat_file(mat_names_list[0])
            props_df = self.__build_single_mat_clusters_properties(mat_file, prop_names, self.MAT_FIELDS)
        return props_df

    def __build_pop_to_clusters_map(self, clusters_metadata):
        dimensions_for_sparse_mat = self.__get_dimensions_for_sparse_matrix(clusters_metadata)
        indices = self.__get_ir_list(clusters_metadata)
        indptr = self.__get_jc_list(clusters_metadata)
        pop_cluster_map = self.__build_pop_clust_matrix(dimensions_for_sparse_mat, indices, indptr)
        return pop_cluster_map

    def __get_dimensions_for_sparse_matrix(self, clusters_metadata):
        dimensions_file_name = "MtFile.spmat"
        all_files = self.__get_files_by_name_in_path(self.__get_base_matlab_path(clusters_metadata),
                                                     dimensions_file_name)

        full_path = all_files[0]
        f = StringIO(self.__io_handler.load_raw_data(full_path))
        mat_sizes = struct.unpack('<2IQ', f.read(16))
        return {'n_rows': mat_sizes[0], 'n_cols': mat_sizes[1], 'nnz': mat_sizes[2]}

    def __get_ir_list(self, clusters_metadata):
        ir_file_name = "IrFile.spmat"
        all_files = self.__get_files_by_name_in_path(self.__get_base_matlab_path(clusters_metadata), ir_file_name)

        full_path = all_files[0]
        data = self.__io_handler.load_raw_data(full_path)
        ir = np.frombuffer(data, dtype=np.int32)
        return ir

    def __get_jc_list(self, clusters_metadata):
        jc_file_name = "JcFile.spmat"

        all_files = self.__get_files_by_name_in_path(self.__get_base_matlab_path(clusters_metadata), jc_file_name)

        full_path = all_files[0]
        data = self.__io_handler.load_raw_data(full_path)
        jc = np.frombuffer(data, dtype=np.int64)
        return jc

    def __build_pop_clust_matrix(self, dimensions_for_sparse_mat, indices, indptr):

        nrows = dimensions_for_sparse_mat['n_rows']
        ncols = dimensions_for_sparse_mat['n_cols']
        nnz = dimensions_for_sparse_mat['nnz']

        data = np.ones(nnz)
        try:
            mat = csr_matrix((data, indices, indptr), shape=(ncols, nrows))
        except Exception as e:
            msg = "Couldn't build population to cluster match due to: %s, aborting." % str(e)
            self.__logger.error(msg)

            raise ValueError(msg)
        return mat

    def __get_translation_pop(self, clusters_metadata):
        spmat_help_name = ".spmathlp"
        all_files = self.__get_files_by_name_in_path(self.__get_base_matlab_path(clusters_metadata), spmat_help_name)

        full_path = all_files[0]

        f_spmathlp = StringIO(self.__io_handler.load_raw_data(full_path))

        num = struct.unpack('<Q', f_spmathlp.read(8))
        # noinspection PyTypeChecker
        ids = np.frombuffer(f_spmathlp.read(), dtype=np.double)

        if num[0] != len(ids):
            msg = "translating ids went wrong. Found %d ids, where expected %d ids, aborting" % (len(ids), num[0])
            self.__logger.error(msg)
            raise ValueError(msg)

        return ids

    def load_population(self, clusters_metadata):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @rtype: C{set}
        """
        key = str(clusters_metadata)
        if key not in self.__loaded_populations:
            self.__loaded_populations[key] = set(self.__get_translation_pop(clusters_metadata))
        return self.__loaded_populations[key]

    def store_clusters_for_spark(self, clusters_metadata, dataframe):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @type dataframe: C{DataFrame}
        """
        path = os.path.join(self.__get_base_matlab_path(clusters_metadata), 'clusters.csv')
        temp_output_file_path = tempfile.mkstemp()[1]
        dataframe.to_csv(temp_output_file_path, index=False, header=False)
        with open(temp_output_file_path) as data:
            self.__io_handler.save_raw_data_multipart(data, path)

    def store_clusters_properties_for_spark(self, clusters_metadata, dataframe):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @type dataframe: C{DataFrame}
        """
        path = os.path.join(self.__get_base_matlab_path(clusters_metadata), 'clusters_properties.csv')
        temp_output_file_path = tempfile.mkstemp()[1]
        dataframe.rename(columns={'cluster': 'id'}, inplace=True)
        dataframe.to_csv(temp_output_file_path, index=False, header=False)
        with open(temp_output_file_path) as data:
            self.__io_handler.save_raw_data_multipart(data, path)
