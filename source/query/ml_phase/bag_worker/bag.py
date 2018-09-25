import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix

__author__ = 'Shahars'


class Bag(object):

    def __init__(self, logger, bag_name, all_props_df, pop_clust_map, bag_pop, whites, universe, external_data=None):
        self.logger = logger
        self.name = bag_name
        self.whites = np.array(whites)
        self.universe = np.array(universe)
        self.bag_pop = np.array(bag_pop)
        self.nnz_in_cluster_to_pop_matrix = pop_clust_map.nnz

        self.props_df = self.__reassign_whites_to_clusters(all_props_df, pop_clust_map)
        self.whites_in_bag, self.universe_in_bag, self.pop_to_clusters_map = \
            self.remove_non_universe_from_bag(pop_clust_map)

        self.external_data = external_data
        self.scores_df = pd.DataFrame(index=self.universe_in_bag)
        self.weights_dict = {}
        self.unscored_handlers = []

    def remove_non_universe_from_bag(self, pop_clust_map):

        in_universe_indices = np.where(np.in1d(self.bag_pop, self.universe))
        in_whites_indices = np.where(np.in1d(self.bag_pop, self.whites))
        whites_in_bag = self.bag_pop[in_whites_indices]
        universe_in_bag = self.bag_pop[in_universe_indices]
        self.logger.info("%d universe in clusters" % len(universe_in_bag))
        self.logger.info("%d whites in clusters" % len(whites_in_bag))
        if len(in_universe_indices[0]) > 0:
            pop_to_clusters_map = pop_clust_map[:, in_universe_indices[0]]
        else:
            pop_to_clusters_map = csr_matrix((0, 0))
        return whites_in_bag, universe_in_bag, pop_to_clusters_map

    def set_unscored_handlers(self, unscored_handlers):
        self.unscored_handlers = unscored_handlers

    def add_bag_weight(self, weight_name, weight):
        self.weights_dict[weight_name] = weight

    def add_scores_by_function(self, scoring_func_name, scores_arr):

        if 0 in scores_arr.shape:
            self.scores_df[scoring_func_name] = np.nan
        else:
            new_scores_df = pd.DataFrame({scoring_func_name: scores_arr[:, 1]}, index=scores_arr[:, 0])
            new_scores_df[scoring_func_name] = new_scores_df[scoring_func_name].astype(float)
            self.scores_df = pd.merge(self.scores_df, new_scores_df, left_index=True, right_index=True, how="outer")

    def align_scores_by_function(self, scoring_func_name):
        self.scores_df[scoring_func_name] = -self.scores_df[scoring_func_name]

    def get_scores_df(self):
        return self.scores_df

    def retrieve_external_scores(self, external_file_name, external_col_name):
        full_col_name = '_'.join([external_file_name, external_col_name])
        return pd.DataFrame({full_col_name: self.external_data[full_col_name].values},
                            index=self.external_data["ID"].values)

    def __reassign_whites_to_clusters(self, initial_props_df, pop_clust_map):

        in_whites_indices = np.where(np.in1d(self.bag_pop, self.whites))
        if len(in_whites_indices[0]) > 0:
            only_whites_to_clusters_map = pop_clust_map[:, in_whites_indices[0]]
            ones_for_clusters = np.ones(len(self.bag_pop[in_whites_indices]))

            count_whites_in_clusters = only_whites_to_clusters_map.dot(ones_for_clusters).squeeze()
            initial_props_df["WN"] = count_whites_in_clusters
        initial_props_df["W_prcntg"] = initial_props_df["WN"].astype(float)/initial_props_df["N"].astype(float)
        return initial_props_df
