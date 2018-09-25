from sklearn.linear_model import LogisticRegression
from source.query.ml_phase.bag_worker.ensemble.base_learning_ensemble import BaseLearningEnsemble


class EnsembleFactory(object):
    LOGISTIC_REGRESSION_RANDOM_ISOLATION_KEY = 'LogisticRegressionEnsemble_Randomizer'

    def __init__(self, random_provider):
        self.__random_provider = random_provider

    def create_ensemble(self, logger, ensemble_key):
        ensemble_schema = ensemble_key.scorer_name
        ensemble_params = ensemble_key.scorer_params
        classifier = self.__get_classifier_by_schema(ensemble_schema, ensemble_params)
        return BaseLearningEnsemble(logger, classifier)

    def __get_classifier_by_schema(self, ensemble_schema, params):
        if ensemble_schema == "logistic_ensemble":
            seed = self.__random_provider.get_randomizer_seed(self.LOGISTIC_REGRESSION_RANDOM_ISOLATION_KEY)
            return LogisticRegression(C=params["regularization_factor"], class_weight="auto", fit_intercept=False,
                                      random_state=seed)
        else:
            raise ValueError("not implemented ensemble schema: %s" % ensemble_schema)
