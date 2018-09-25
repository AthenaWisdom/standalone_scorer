from collections import defaultdict
import logging

from sklearn.tree import DecisionTreeClassifier

from source.query.ml_phase.bag_worker import unscored_handlers, scorers
from source.query.ml_phase.bag_worker.scorers.external_scorer import ExternalScorer
from source.query.ml_phase.bag_worker.unscored_handlers.none_unscored_handler import LeaveUnscored

SCORER_TAG_INTERNAL = 'internal'
SCORER_TAG_EXTERNAL = 'external'
class ScorerGenerator(object):

    RUNNING_TIME_TABLE = {"light": 0, "medium": 1, "heavy": 2}
    DEFAULT_RUN_TIME = "heavy"
    CONFIG_RULES = [{"name": (basestring, True),
                    "normalize": (bool, True),
                     "run_time_weight": (basestring, False, "heavy"),
                     "conf": (object, True),
                     "default_ensemble_weight": (object, False, None),
                     "unscored_handler": (basestring, False, "RandomUnscoredHandler"),
                     "default_invert": (bool, False, False)
                     }]

    def __init__(self, random_provider, context_provider):
        self.__context_provider = context_provider
        self.RANDOM_ISOLATION_KEY = "scorer_generator_random_isolation_key"
        self._random_provider = random_provider


        self.logger = logging.getLogger('endor')

    def create_single_external_scorer(self, external_data_name):
       pass

    def create_single_scorer(self, scorer_data):
        scorer_name = scorer_data.name
        scorer_class = getattr(scorers, scorer_name)
        scorer_params = self.retrieve_scorer_params(scorer_data)
        scorer_tags = scorer_data.tags if hasattr(scorer_data, 'tags') else [SCORER_TAG_INTERNAL]
        normalize = scorer_data.normalize
        unscored_handler_name = scorer_data.unscored_handler
        unscored_handler = getattr(unscored_handlers, unscored_handler_name)
        scorer_default_invert = scorer_data.default_invert
        if SCORER_TAG_EXTERNAL in scorer_tags:
            scorer_full_name = "_".join([scorer_name, unscored_handler_name, scorer_params["external_file_name"], scorer_params["external_col_name"]])
        else:
            scorer_full_name = "_".join([scorer_name, unscored_handler_name])

        return scorer_class(scorer_full_name, normalize, scorer_default_invert, unscored_handler(),
                            self._random_provider, self.__context_provider, scorer_tags, scorer_params)

    def retrieve_scorer_run_time(self, scorer_data):
        try:
            scorer_run_time = self.RUNNING_TIME_TABLE[scorer_data.run_time_weight]
        except KeyError:
            self.logger.warning("'run_time_weight' for scorer %s section in ml_config is not one of "
                                "(light/medium/heavy). "
                                "Setting default scorer weight to heavy" % scorer_data.name)
            scorer_run_time = self.RUNNING_TIME_TABLE[self.DEFAULT_RUN_TIME]
        return scorer_run_time

    def create_scorers(self, config, run_time):
        self.logger.info("started generating scorers")
        run_type = self.__get_run_type(run_time)
        my_scorers = []
        scorer_tags = defaultdict(list)
        for s_id, scorer_data in enumerate(config):
            scorer_run_time = self.retrieve_scorer_run_time(scorer_data)
            if scorer_run_time <= run_type:
                scorer = self.create_single_scorer(scorer_data)
                my_scorers.append(scorer)
                self.logger.info("finished generating %s scorer" % scorer.name)

                for tag in scorer.tags:
                    scorer_tags[tag].append(scorer.name)
        self.logger.info("finished generating all scorers")
        return my_scorers, scorer_tags

    def retrieve_scorer_params(self, scorer_data):
        scorer_params = scorer_data.conf
        if "base_estimator" in scorer_params:
            base_estimator_name = scorer_params.base_estimator
            # noinspection PyTypeChecker
            scorer_params.base_estimator = self.retrieve_base_estimator_class(base_estimator_name)
        return scorer_params

    def retrieve_base_estimator_class(self, base_estimator_name):
        """
        @param base_estimator_name:
        @type base_estimator_name: C{str}
        @return:
        @rtype:
        """
        if base_estimator_name == "decision_tree":
            seed = self._random_provider.get_randomizer_seed(self.RANDOM_ISOLATION_KEY)
            return DecisionTreeClassifier(random_state=seed)
        else:
            self.logger.warning("unsupported base estimator %s!\n" % base_estimator_name)
            return None

    def __get_run_type(self, run_time):
        try:
            run_type = self.RUNNING_TIME_TABLE[run_time]
            self.logger.info("Will be creating %s and lower run-type scorers" % run_type)
        except KeyError:
            self.logger.warning("'run_time' section in ml_config file is not one of (light/medium/heavy). "
                                "Continuing with default heavy run (i.e. running all scorers)")
            run_type = self.RUNNING_TIME_TABLE[self.DEFAULT_RUN_TIME]
        return run_type
