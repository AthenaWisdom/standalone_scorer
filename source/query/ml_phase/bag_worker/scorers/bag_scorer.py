# from source.utils.event_sender import send_event
from source.utils.context_handling.context_provider import ContextProvider

__author__ = 'Shahars'
import numpy as np
from time import time


class BagScorer(object):

    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_holder, scorer_tags):
        """

        @type context_holder: L{ContextProvider}
        """
        self.__context_holder = context_holder
        self._random_provider = random_provider
        self._name = name
        self.normalize = normalize
        self.should_invert = invert
        self.unscored_handler = unscored_handler
        self.scores_arr = None
        self.tags = scorer_tags

    def get_default_scores(self, bag):
        return np.array([list(bag.universe_in_bag), [0]*len(bag.universe_in_bag)]).transpose()

    def score_population(self, bag, logger):
        self.logger = logger
        if len(bag.universe) > 0 and len(bag.universe_in_bag) > 0:
            try:
                time0 = time()
                self.logger.info("started score bag")
                scores_arr = self.score_bag(bag)
                duration = time()-time0
                self.logger.info("finished calculating scores by method: %s in %d seconds" % (
                    self.name, duration))
                self.scores_arr = self.remove_non_universe_from_scores(
                    bag, scores_arr)
                self.logger.info("scored by %s scorer" % self.name)
                self.logger.info("scored pop of size: %d" %
                                 len(self.scores_arr[:, 1]))
                self.logger.info("number of whites in scores: %d" % len(
                    set(self.scores_arr[:, 0]).intersection(bag.whites)))
            except Exception as e:
                self.logger.warning(str(e))
                self.logger.warning(
                    "Scorer %s failed, default scores will be added" % self.name, exc_info=True)
                self.scores_arr = self.get_default_scores(bag)
        else:
            self.logger.warning(
                "no universe inside clusters, empty scores added")
            self.scores_arr = np.array([[], []])
        if 0 in self.scores_arr.shape:
            self.logger.warning(
                "no scores were produced, default scores will be added to bag")
            self.scores_arr = self.get_default_scores(bag)

        self.logger.info("start handling unscored population")
        self.handle_unscored_pop(bag)
        self.logger.info("finished handling unscored population")

        if self.normalize:
            self.logger.info("normalizing scores")
            self.normalize_scores()
            self.logger.info("finished normalizing scores")
    #     TODO: remove writing to bag after checking no side effects
        copy_of_scores = np.copy(self.scores_arr)
        bag.add_scores_by_function(self.name, self.scores_arr)
        return copy_of_scores

    def score_bag(self, bag):
        raise NotImplementedError()

    def remove_non_universe_from_scores(self, bag, scores_arr):
        self.logger.info("removing non universe from scores")
        initial_pop_size = len(scores_arr[:, 1])
        universe_to_score = bag.universe
        scores_arr = scores_arr[np.in1d(scores_arr[:, 0], universe_to_score)]
        self.logger.debug("finished removing non %d universe from scores" % (
            initial_pop_size - len(scores_arr[:, 1])))
        return scores_arr

    def normalize_scores(self):
        avg = np.nanmean(self.scores_arr[:, 1])
        std = np.nanstd(self.scores_arr[:, 1])
        norm_scores = (self.scores_arr[:, 1] - avg) / float(std)
        self.scores_arr[:, 1] = norm_scores

    def handle_unscored_pop(self, bag):
        self.logger.info("handling unscored population using %s" %
                         self.unscored_handler.name)
        scores_df = self.unscored_handler.score_the_unscored_pop(
            self.scores_arr, bag.universe, self.name, self.logger)
        scorer_name = scores_df.columns[0]
        handled_scores_arr = np.array(
            [scores_df.index, scores_df[scorer_name].values]).transpose()

        self.scores_arr = handled_scores_arr

    def update_should_invert_by_past(self, should_invert):
        self.should_invert = should_invert
        return should_invert

    @property
    def name(self):
        return self._name

    @property
    def weight(self):
        return self.ensemble_weight
