__author__ = 'Shahars'
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer
import numpy as np


class CountClusterScorer(BagScorer):

    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(CountClusterScorer, self).__init__(
            name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)

    def score_bag(self, bag):
        self.logger.info("start calculating scores by method: %s" % self.name)

        clust_to_pop_mat = bag.pop_to_clusters_map.T
        all_cluster_scores = np.ones(len(bag.props_df.index))
        pop_scores = clust_to_pop_mat.dot(all_cluster_scores).squeeze()
        scores_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        # clust, pop = bag.pop_to_clusters_map.nonzero()
        # unique, counts = np.unique(pop, return_counts=True)
        #
        # scores_dict = dict(zip(bag.true_pop[unique], counts))
        # scores_dict = {person: len(clusters) for person, clusters in pop_to_clust_dict.iteritems()}
        scores_arr = np.array([scores_dict.keys(), scores_dict.values()]).transpose()
        self.logger.info("scored by %s scorer, number of whites in scores: %d" % (self.name, len(set(scores_dict.keys()).intersection(bag.whites))))
        # scores_arr = self.remove_non_universe_from_scores(bag, scores_arr)

        return scores_arr





