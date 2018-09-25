__author__ = 'igor1'

from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer
import numpy as np


# DONE
class NaiveLikelihoodScorer(BagScorer):

    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(NaiveLikelihoodScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)

    def score_bag(self, bag):

        self.logger.info("start calculating scores by method: %s" % self.name)
        users_x_clusters = bag.pop_to_clusters_map.T

        wn = bag.props_df['WN'].values
        bn = bag.props_df['N'].values-wn
        smooth = 1

        num_whites = bag.whites.shape[0]
        num_all = users_x_clusters.shape[0]
        p_whites = num_whites*1./num_all

        # Each cluster receives a score: (WN / BN) / (normalize: #total_whites/#total_pop), and log it.
        # TODO: Perhaps should consider normalizing by number of expected whites in a cluster of this size.
        # each person gets the sum of these scores.

        cluster_scores = np.log((wn+smooth)*1./(bn+smooth)) - np.log(p_whites)
        pop_scores = users_x_clusters.dot(cluster_scores).squeeze()
        scores_arr = np.vstack([bag.universe_in_bag, pop_scores]).T
        return scores_arr
