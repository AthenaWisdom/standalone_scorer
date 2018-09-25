__author__ = 'Igor'

import gc as gc

import numpy as np
import scipy as sp
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer


class SparseScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(SparseScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)

    def sparsify(self, D, k):
        class0 = type(D)
        D = sp.sparse.csr_matrix(D)
        D.eliminate_zeros()
        data = np.array(D.data)
        indices = np.array(D.indices)
        indptr = np.array(D.indptr)
        for i in range(D.shape[0]):
            tempData = data[indptr[i]:indptr[i+1]]
            if tempData.shape[0] <= k:
                continue
            thres = np.partition(tempData, -k)[-k]
            data[indptr[i]:indptr[i+1]] = tempData*(tempData > thres)
        D = sp.sparse.csr_matrix((data, indices, indptr), shape=D.shape)
        D.eliminate_zeros()
        D = class0(D)
        gc.collect()
        return D

    def score_bag(self, bag):
        users_x_clusters = bag.pop_to_clusters_map.T
        # 1/cluster_size, only if it has whites, else 0:
        cluster_weights = (1./bag.props_df['N'].values)*(bag.props_df['WN'].values > 0)
        # w_prcntg/cluster_size (why??), only if has whites, else 0:
        cluster_weights *= bag.props_df['W_prcntg'].values
        new_user_weights = users_x_clusters.dot(sp.sparse.diags(cluster_weights, 0))
        new_user_weights = self.sparsify(new_user_weights, 5)
        sparse_score = np.array([np.mean(row) if row else 0 for row in sp.sparse.lil_matrix(new_user_weights).data])
        guess = np.vstack([bag.universe_in_bag, sparse_score]).T
        return guess