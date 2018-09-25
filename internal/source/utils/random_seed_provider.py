from random import Random
import logging

RANDOM_MIN_INT = 0
RANDOM_MAX_INT = 2000000000


class RandomSeedProvider(object):
    def __init__(self, initial_seed=None):
        self.__logger = logging.getLogger('endor.utils.RandomSeedProvider')
        self.reset_to_seed(initial_seed)

    # noinspection PyAttributeOutsideInit
    def reset_to_seed(self, initial_seed=None):
        seed = initial_seed if initial_seed else Random().randint(0, 2000000000)
        self.__logger.info(
            'reseted RandomSeedProvider with initial_seed \'{}\''.format(initial_seed), extra={'randomOperation': 'reset'})
        self.__system_randomizer = Random(seed)
        self.__randomizers = {}

    STORED_NON_RANDOM_SEED = 123
    @classmethod
    def generate_contextual_seed(cls, seed, *contextual_values):
        return hash('{}-contextual-seed-{}'.format(seed, 'seedContextSeparator'.join([str(x) for x in contextual_values])))



    def get_next_random_int(self, isolation_key, min=RANDOM_MIN_INT, max=RANDOM_MAX_INT):
        return self.__get_isolated_randomizer(isolation_key).randint(min, max)

    def get_randomizer_seed(self, isolation_key):
        return self.__get_or_create(isolation_key)[0]

    def set_randomizer(self, isolation_key, seed):
        self.__logger.info('forced randomizer for \'{}\' with initial seed \'{}\''.format(isolation_key, seed), extra={'randomOperation': 'set_randomizer'})
        self.__randomizers[isolation_key] = (seed, Random(seed))

    def __get_isolated_randomizer(self, isolation_key):
        return self.__get_or_create(isolation_key)[1]

    def __get_or_create(self, isolation_key):
        if isolation_key not in self.__randomizers:
            initial_seed = self.__system_randomizer.randint(RANDOM_MIN_INT, RANDOM_MAX_INT)
            self.__randomizers[isolation_key] = (initial_seed, Random(initial_seed))
            self.__logger.info('created a new randomizer for \'{}\' with seed \'{}\''
                               .format(isolation_key, initial_seed), extra={'randomOperation': 'create'})
        return self.__randomizers[isolation_key]




class ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___(object):
    __provider = None

    @classmethod
    def force_provider(cls, provider):
        cls.__provider = provider

    @classmethod
    def get_provider(cls):
        if not cls.__provider:
            cls.force_provider(RandomSeedProvider())
        return cls.__provider
