__author__ = 'Shahars'

import numpy as np

from source.query.ml_phase.bag_worker.scorers.clusters_ML_scorer import ClustersMLScorer


class ClustersMLProbScorer(ClustersMLScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        self.RANDOM_ISOLATION_KEY = "MLProbScorer_random_isolation_key"

        super(ClustersMLProbScorer, self).__init__(
            name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params)

    def score_bag(self, bag):
        self.logger.info("running Yaniv ML scorer with threshold 1: %.3f, threshold 2: %.3f, threshold 3: %.3f"
                         % (self.TH1, self.TH2, self.TH3))
        self._calc_clusters_scores(bag)
        if len(self.cluster_scores_df.index) > 0:
            universe_to_score = set(bag.universe)-set(bag.whites)
            self.logger.debug("universe to score size: %d" % len(universe_to_score))

            self.logger.debug("scored clusters size: %d" % len(self.cluster_scores_df.index))
            self.logger.debug("start calculating population scores")

            clust_to_pop_mat = bag.pop_to_clusters_map.T
            all_cluster_scores = np.zeros(clust_to_pop_mat.shape[1])
            all_cluster_scores[self.cluster_scores_df.index.values] = self.cluster_scores_df["score"].values
            pop_scores = clust_to_pop_mat.dot(all_cluster_scores).squeeze()

            self.logger.debug("finished calculating population scores")
            score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
            self.logger.debug("finished creating scores dictionary")
            scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
            scores_arr = scores_arr[scores_arr[:, 1] > 0]

            self.logger.debug("scored pop size: %d" % len(scores_arr))

        else:
            warning_message = "no clusters scores, returning empty population scores"
            self.logger.warning(warning_message)
            scores_arr = np.array([[], []]).transpose()
        return scores_arr

