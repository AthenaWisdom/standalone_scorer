__author__ = 'Igor'

import numpy as np
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer
from sklearn.linear_model import SGDClassifier


class LogisticRegressionScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        self.RANDOM_ISOLATION_KEY = "LogRegScorer_random_isolation_key"
        super(LogisticRegressionScorer, self).__init__(
            name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)

    def score_bag(self, bag):

        self.logger.info("start calculating scores by method: %s" % self.name)
        users_x_clusters = bag.pop_to_clusters_map.T
        raw_whites = bag.whites
        pop = bag.universe_in_bag
        raw_whites_clean = raw_whites[np.in1d(raw_whites, pop)]
        whites = np.searchsorted(pop, raw_whites_clean)
        full_universe = np.arange(users_x_clusters.shape[0])
        is_white = np.in1d(full_universe, whites)

        y = is_white
        # each person's features are whether he's in a cluster or not, for all clusters.
        x_mat = users_x_clusters

        n_users = users_x_clusters.shape[0]
        n_whites = is_white.sum()
        seed = self._random_provider.get_randomizer_seed(self.RANDOM_ISOLATION_KEY)
        estimator = SGDClassifier(alpha=100000./n_users, loss='log', class_weight={0:1, 1:n_users/n_whites},
                                  penalty='l2', n_iter=50, random_state=seed)

        estimator.fit(x_mat, y)
        score = estimator.predict_proba(x_mat)[:, 1]

        pop_scores = score
        # score_dict = dict(zip(bag.true_pop[range(pop_scores.shape[0])], pop_scores))
        # scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
        scores_arr = np.vstack([bag.universe_in_bag, pop_scores]).T

        return scores_arr