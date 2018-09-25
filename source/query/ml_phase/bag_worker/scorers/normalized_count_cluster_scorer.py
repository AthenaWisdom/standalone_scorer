__author__ = 'Igor'
import numpy as np

from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer


class NormalizedCountClusterScorer(BagScorer):

    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(NormalizedCountClusterScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)

    def score_bag(self, bag):

        self.logger.info("start calculating scores by method: %s" % self.name)
        clust_to_pop_mat = bag.pop_to_clusters_map.T
        all_cluster_scores = 1./bag.props_df["N"].values
        pop_scores = clust_to_pop_mat.dot(all_cluster_scores).squeeze()
        score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
        return scores_arr



