from source.query.ml_phase.bag_worker.bag import Bag

__author__ = 'Shahars'
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer
import numpy as np

LEGIT_FIELD = "ID"


class ExternalScorer(BagScorer):

    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(ExternalScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)
        self.file_name = params["external_file_name"]
        self.col_name = params["external_col_name"]

    def score_bag(self, bag):
        """
        Copied scores from external file only for candidates, i.e. (hashed) ID's in universe - whites.
        Note that even if an ID has a score in the file, its score isn't copied if the ID is white.

        @type bag: L{Bag}
        """
        # pop_scores = bag.retrieve_external_scores(self.file_name, self.col_name)
        # true_scores = pop_scores.ix[bag.true_pop]
        # scores_col = true_scores.columns[0]
        # res = np.array([true_scores.index.values, true_scores[scores_col].values]).transpose()
        # return res

        pop_scores = bag.retrieve_external_scores(self.file_name, self.col_name)
        candidate_ids_numeric = frozenset(bag.universe) - frozenset(bag.whites)
        # candidate_id_strings = (str(int(idnum)) for idnum in candidate_ids_numeric)
        candidate_scores = pop_scores.ix[candidate_ids_numeric].dropna()
        scores_col = candidate_scores.columns[0]
        res = np.array([
            candidate_scores.index.values.astype(int),
            candidate_scores[scores_col].values
        ]).transpose()
        return res
