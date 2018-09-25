from source.query.ml_phase.helper_funcs import get_first_mandatory_file
from source.utils.es_with_context_sender import send_es_error


__author__ = 'Shahars'

import os
import scipy.io as scio
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
import struct
import re


class BlkHandler(object):

    def __init__(self, data_path, mat_names, prop_names_count_dict, logger):

        self.logger = logger
        self.data_path = data_path
        self.prop_names_count_dict = prop_names_count_dict
        self.mat_names_list = mat_names
        # mat files order is important!
        self.mat_names_list.sort(key=lambda x: int(re.search(r'\d+', x).group()))
        if len(self.mat_names_list) > 1:
            self.logger.info("multiple mat files detected")
            self.prop_names = 'temp_'
            self.multiple_mat_bool = True
        else:
            self.logger.info("single mat file detected")
            self.prop_names = 'blk_data'
            self.multiple_mat_bool = False

    def build_mat_clusters_properties(self):
        """

        @return:
        @rtype:
        """
        if self.multiple_mat_bool:
            return self._build_multiple_mat_clusters_properties()
        else:
            mat = self._load_mat(self.mat_names_list[0])
            return self._build_single_mat_clusters_properties(mat)

    def _build_multiple_mat_clusters_properties(self):
        """

        @return:
        @rtype:
        """
        all_props_df = pd.DataFrame()
        for mat_name in self.mat_names_list:
            mat = self._load_mat(mat_name)
            df = self._build_single_mat_clusters_properties(mat)
            all_props_df = all_props_df.append(df)
        all_props_df.index = range(len(all_props_df.index))
        return all_props_df

    def _build_single_mat_clusters_properties(self, mat):
        try:
            clusters_props = mat[self.prop_names]
        except Exception:
            msg = "Field %s doesn't exist in mat file, but expected. Cannot continue" % self.prop_names
            self.logger.error(msg)
            send_es_error(msg)
            raise ValueError(msg)

        cluster_prop_dict = {}
        for prop_name, counts in self.prop_names_count_dict.iteritems():
            try:
                mat_values = clusters_props[prop_name][0][0]
            except:
                self.logger.warning("Field %s doesn't exist in mat file, please remove it from config file."
                                    % prop_name)
                break
            if counts == 1:
                cluster_prop_dict[prop_name] = mat_values.flatten()
            else:
                for i in np.arange(counts):
                    cluster_prop_dict[prop_name+"_"+str(i)] = mat_values[:, i]
        cluster_prop_df = pd.DataFrame(cluster_prop_dict)
        cluster_prop_df.index.names = ["cluster"]
        cluster_prop_df["W_prcntg"] = cluster_prop_df["WN"].astype(float)/cluster_prop_df["N"].astype(float)
        del mat
        return cluster_prop_df

    def _get_dimensions_for_sparse(self):
        dimensions_file_name = "MtFile.spmat"
        glob_mask = os.path.join(self.data_path, "*" + dimensions_file_name)
        full_path = get_first_mandatory_file(glob_mask)
        f = open(full_path, "rb")
        mat_sizes = struct.unpack('<2IQ', f.read(16))
        return mat_sizes

    def _get_ir_list(self):
        ir_file_name = "IrFile.spmat"
        glob_mask = os.path.join(self.data_path, "*"+ir_file_name)
        full_path = get_first_mandatory_file(glob_mask)
        ir = np.fromfile(full_path, dtype=np.int32)
        return ir

    def _get_jc_list(self):
        jc_file_name = "JcFile.spmat"
        glob_mask = os.path.join(self.data_path, "*" + jc_file_name)
        full_path = get_first_mandatory_file(glob_mask)
        jc = np.fromfile(full_path, dtype=np.int64)
        return jc

    def get_translation_pop(self):
        glob_mask = os.path.join(self.data_path, "*.spmathlp")
        file_path = get_first_mandatory_file(glob_mask)
        with open(file_path, 'rb') as f:
            num = struct.unpack('<Q', f.read(8))
            ids = np.fromfile(f, dtype=np.double)
            if num[0] != len(ids):
                msg = "translating ids went wrong. Found %d ids, where expected %d ids, aborting" % (len(ids), num[0])
                self.logger.error(msg)
                send_es_error(msg)
                raise ValueError(msg)

        return ids

    def build_pop_clust_matrix(self):
        nrows, ncols, nnz = self._get_dimensions_for_sparse()
        indices = self._get_ir_list()
        indptr = self._get_jc_list()
        data = np.ones(nnz)
        try:
            mat = csr_matrix((data, indices, indptr), shape=(ncols, nrows))
        except Exception as e:
            msg = "Couldn't build population to cluster match due to: %s, aborting." % str(e)
            self.logger.error(msg)
            send_es_error(msg)
            raise ValueError(msg)
        return mat

    def _load_mat(self, mat_name):
        """

        @param mat_name:
        @type mat_name:
        @return:
        @rtype:
        """

        self.logger.info("reading mat file named: %s" % mat_name)
        try:
            mat = scio.loadmat(os.path.join(self.data_path, mat_name))
            self.logger.info("finished reading mat file %s" % mat_name)
        except Exception as e:
            msg = "Error loading .mat file named %s from path %s, due to: %s" % (mat_name, self.data_path, str(e))
            self.logger.error(msg)
            send_es_error(msg)
            raise IOError(msg)

        return mat


def get_props_and_clust_to_pop_map(data_path, mat_names, prop_names_count_dict, logger):

    blk_h = BlkHandler(data_path, mat_names, prop_names_count_dict, logger)

    props_df = blk_h.build_mat_clusters_properties()

    pop_clust_map = blk_h.build_pop_clust_matrix()

    pop = blk_h.get_translation_pop()

    return props_df, pop_clust_map, pop