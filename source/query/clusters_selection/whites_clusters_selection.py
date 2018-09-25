import os
import numpy as np
from copy import deepcopy
import pandas as pd

from sklearn import preprocessing
from scipy.stats import binom
from sklearn import linear_model
from sklearn.ensemble.forest import RandomForestRegressor

from source.storage.io_handlers.s3 import S3IOHandler
from source.storage.stores.clusters_store.matlab_clusters_store import MatlabClustersStore
from source.storage.stores.clusters_store.types import ClustersMetaData
from source.storage.stores.split_kernel_store.csv_store import CSVSubKernelStore
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata

__author__ = 'user'


class ClusterSelectorByWhites(object):
    def __init__(self, io_handler, clusters_store, sub_kernel_store):
        self.io_handler = io_handler
        self.clusters_store = clusters_store

        self.__sub_kernel_store = sub_kernel_store

    def select_clusters(self, customer, quest_id, query_id, sphere_id, feature_flags, percent_of_new_clusters):

        clusters_metadata = ClustersMetaData(customer, sphere_id)
        clusters_obj = self.clusters_store.load_clusters(clusters_metadata)
        pop_ids = clusters_obj.population_ids
        clusters_properties = clusters_obj.clusters_properties
        population_to_clusters_matrix = clusters_obj.population_to_clusters_matrix

        whites, grays, universe = self.__get_kernel(customer, quest_id, query_id, feature_flags)

        clusters_properties = self.__reassign_whites(whites, clusters_properties, population_to_clusters_matrix, pop_ids)
        clusters_properties = self.__update_categorical(clusters_properties)

        the_output, reason, sort_ind, cluster_ind, sorted_y, clusters_label, number_of_new_clusters = \
            self.__the_calculation(len(whites), len(universe), clusters_properties, percent_of_new_clusters)
        df = pd.DataFrame(the_output, columns=['cluster_id'])

        output_path = os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id, 'artifacts', 'blocks',
                                   'chosen_clusters.csv')

        self.io_handler.save_raw_data(df.to_csv(encoding='utf-8'), output_path)

    def __the_calculation(self, n_whites, n_universe, clusters_properties, percent_of_new_clusters):
        # calculate p = whites/(whites + grays)
        p = float(n_whites) / float(n_universe)
        print('baseline probability: ', p)

        # get x = number of whites per cluster
        x = clusters_properties['WN']

        # get n = size of cluster
        n = clusters_properties['N']

        # get cluster_label = binomial(x, n, p)
        clusters_label = binom.pmf(x, n, p)

        # learn: cluster_properties --> cluster_label
        # IMPORTANT: label = 0, good; label = 1 bad
        estimators = {
            'linear_regression': {
              'model': linear_model.LinearRegression()
         },
         'random_forest': {
          'model': RandomForestRegressor()
            }
         }

        # exclude certain properties
        # ????
        clusters_features = deepcopy(clusters_properties)
        clusters_features.pop('WN')
        clusters_features.pop('W_prcntg')
        print(clusters_properties.shape, clusters_features.shape)


        for est_name, est in estimators.items():
            print('--- calculating ', est_name)
            est['model'].fit(clusters_properties, clusters_label)

            # re-label to each cluster --> learned_label
            est['y'] = est['model'].predict(clusters_properties)

            try:
                print('score:' , est['model'].score(clusters_properties, clusters_label))
            except:
                print('no .score')


        #   the reason
        reason = {}

        y = estimators['random_forest']['y']
        # sort the clusters
        sorted_y = np.sort(y)
        cluster_ind = np.argsort(clusters_label)  # the real label
        sort_ind = np.argsort(y)                  # the predicted label

        # the output: ids of clusters in descending order of importance
        number_of_new_clusters = int(percent_of_new_clusters * float(sort_ind.shape[0]))
        the_output = sort_ind[:number_of_new_clusters]

        return the_output, reason, sort_ind, cluster_ind, sorted_y, clusters_label, number_of_new_clusters


    def __update_categorical(self, clusters_properties):
        print('original number of features: ', clusters_properties.shape)
        categorical_columns = ['fieldby', 'SUB_CLUSTERS_FILE']
        le = preprocessing.LabelEncoder()

        for col in categorical_columns:

            clusters_properties[col] = le.fit_transform(clusters_properties[col])

        return clusters_properties

    def __get_kernel(self, customer, quest_id, query_id, feature_flags):

        sub_kernel = self.__sub_kernel_store.load_sub_kernel_by_ordinal_new(customer, quest_id, query_id, 0)
        whites = sub_kernel.whites
        universe = sub_kernel.all_ids
        grays = list(set(universe) - set(whites))
        return whites, grays, universe

    def __reassign_whites(self, whites, clusters_properties, population_to_clusters_matrix, pop_ids):

        in_whites_indices = np.where(np.in1d(pop_ids, whites))
        if len(in_whites_indices[0]) > 0:
            only_whites_to_clusters_map = population_to_clusters_matrix[:, in_whites_indices[0]]
            ones_for_clusters = np.ones(len(pop_ids[in_whites_indices]))
            count_whites_in_clusters = only_whites_to_clusters_map.dot(ones_for_clusters).squeeze()
            clusters_properties["WN"] = count_whites_in_clusters
        clusters_properties["W_prcntg"] = clusters_properties["WN"].astype(float)/clusters_properties["N"].astype(float)
        return clusters_properties

def process(customer, quest_id, query_id, sphere_id, feature_flags, percent_of_new_clusters):
    io_handler = S3IOHandler()
    clusters_selector = ClusterSelectorByWhites(io_handler, MatlabClustersStore(io_handler), CSVSubKernelStore(io_handler))
    clusters_selector.select_clusters(customer, quest_id, query_id, sphere_id, feature_flags, percent_of_new_clusters)