import logging
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix

__author__ = 'Shahars'


class MatlabHandler(object):
    def __init__(self):
        self.__logger = logging.getLogger('endor')

    def build_multiple_mat_clusters_properties(self, mat_files, prop_names, prop_names_count_dict):

        all_props_df = pd.DataFrame()
        for mat_file in mat_files:
            df = self.build_single_mat_clusters_properties(mat_file, prop_names, prop_names_count_dict)
            all_props_df = all_props_df.append(df)
        all_props_df.index = range(len(all_props_df.index))
        return all_props_df

    def build_single_mat_clusters_properties(self, mat_file, prop_names, prop_names_count_dict):
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
            except:
                self.__logger.warning("Field %s doesn't exist in mat file, please remove it from config file."
                                      % prop_name)
                break
            if counts == 1:
                cluster_prop_dict[prop_name] = mat_values.flatten()
            else:
                for i in np.arange(counts):
                    cluster_prop_dict[prop_name+"_"+str(i)] = mat_values[:, i]
        cluster_prop_df = pd.DataFrame(cluster_prop_dict)
        cluster_prop_df.index.names = ["cluster"]

        del mat_file
        return cluster_prop_df

    def build_pop_clust_matrix(self, dimensions_for_sparse_mat, indices, indptr):

        nrows = dimensions_for_sparse_mat["n_rows"]
        ncols = dimensions_for_sparse_mat["n_cols"]
        nnz = dimensions_for_sparse_mat["nnz"]

        data = np.ones(nnz)
        try:
            mat = csr_matrix((data, indices, indptr), shape=(ncols, nrows))
        except Exception as e:
            msg = "Couldn't build population to cluster match due to: %s, aborting." % str(e)
            self.__logger.error(msg)

            raise ValueError(msg)
        return mat



