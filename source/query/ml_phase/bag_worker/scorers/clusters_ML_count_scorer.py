__author__ = 'Shahars'

import numpy as np

from source.query.ml_phase.bag_worker.scorers.clusters_ML_scorer import ClustersMLScorer


class ClustersMLCountScorer(ClustersMLScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(ClustersMLCountScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params)

    def score_bag(self, bag):
        self.logger.info("running Yaniv ML scorer with threshold 1: %.3f, threshold 2: %.3f, threshold 3: %.3f"
                         % (self.TH1, self.TH2, self.TH3))
        self._calc_clusters_scores(bag)
        if len(self.cluster_scores_df.index) > 0:
            universe_to_score = set(bag.universe)-set(bag.whites)
            self.logger.debug("universe to score size: %d" % len(universe_to_score))

            self.logger.debug("scored clusters size: %d" % len(self.cluster_scores_df.index))
            self.logger.debug("start calculating population scores")

            high_clust, pop = bag.pop_to_clusters_map[self.cluster_scores_df.index.values].nonzero()
            unique, counts = np.unique(pop, return_counts=True)
            scores_dict = dict(zip(bag.universe_in_bag[unique], counts))
            scores_arr = np.array([scores_dict.keys(), scores_dict.values()]).transpose()
            self.logger.debug("scored pop size: %d" % len(scores_arr))

        else:
            error_message = "no clusters scores, returning empty population scores"
            self.logger.warning(error_message)
            scores_arr = np.array([[], []]).transpose()
        return scores_arr

