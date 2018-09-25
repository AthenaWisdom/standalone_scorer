__author__ = 'Shahars'

import numpy as np
from bag_scorer import BagScorer


class HomeScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(HomeScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)

    def score_bag(self, bag):
        clust_to_pop_mat = bag.pop_to_clusters_map.T
        all_cluster_scores = bag.props_df["W_prcntg"].values
        pop_scores = clust_to_pop_mat.dot(all_cluster_scores).squeeze()
        score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()

        return scores_arr