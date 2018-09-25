import random
import numpy as np
from bag_scorer import BagScorer

__author__ = 'Inon'


MAX_POPULATION_SIZE = 2 ** 64
PRIME = 2 ** 89 - 1
RANDOM_SCORER_ISOLATION_KEY = 'RANDOM_SCORER_random_isolation_key'


class RandomScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(RandomScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)
        self.__random_provider = random_provider
        self._random_mapper = self.__random_mapping_float()

    def __random_hash_function(self):
        """
        Picks a hash function at random from a universal family
        (https://en.wikipedia.org/wiki/Universal_hashing#Hashing_integers).
        @return: the hash function.
        """
        a = self.__random_provider.get_next_random_int(RANDOM_SCORER_ISOLATION_KEY, 1, PRIME)
        b = self.__random_provider.get_next_random_int(RANDOM_SCORER_ISOLATION_KEY, 1, PRIME)

        def hash_function(x):
            return ((a * int(x) + b) % PRIME) % MAX_POPULATION_SIZE

        return hash_function

    def __random_mapping_float(self):
        hash_function = self.__random_hash_function()
        return lambda x: float(hash_function(x)) / MAX_POPULATION_SIZE

    def score_bag(self, bag):
        pop_scores = np.array([self._random_mapper(user_id) for user_id in bag.universe_in_bag])
        return np.array([bag.universe_in_bag, pop_scores]).transpose()
