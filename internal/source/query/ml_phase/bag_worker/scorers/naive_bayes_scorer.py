__author__ = 'Igor'

import numpy as np
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer
from sklearn.naive_bayes import MultinomialNB


# DONE
class NaiveBayesScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(NaiveBayesScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)
    """
        y_bar = argmax(y)(p(y)*prod(p(x_i|y)))
            (-) x_i is the i'th feature.
            (-) p(y) is is the relative frequency of class y in the training set.
            (-) p(x_i|y) is calculated using multinomial NB: p(x_i|y) =  (Ny_i + alpha)/(Ny +alpha*N)
                (-) Where Ny_i is the number of times feature i appeared in a sample class y in the training set.
                (-) Ny is the total count of features for class y.
    """
    def score_bag(self, bag):

        self.logger.info("start calculating scores by method: %s" % self.name)
        users_x_clusters = bag.pop_to_clusters_map.T
        raw_whites = bag.whites
        pop = bag.universe_in_bag
        raw_whites_clean = raw_whites[np.in1d(raw_whites,pop)]
        whites = np.searchsorted(pop, raw_whites_clean)
        full_universe = np.arange(users_x_clusters.shape[0])
        is_white = np.in1d(full_universe, whites)

        y = is_white
        # each person's features are whether he's in a cluster or not, for all clusters.
        x_mat = users_x_clusters

        estimator = MultinomialNB(alpha=1, fit_prior=False)
        estimator.fit(x_mat, y)
        score = estimator.predict_proba(x_mat)[:, 1]

        pop_scores = score

        scores_arr = np.vstack([bag.universe_in_bag, pop_scores]).T

        return scores_arr
