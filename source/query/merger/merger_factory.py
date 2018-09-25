from sklearn.ensemble import RandomForestClassifier, BaggingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import OneClassSVM
from sklearn.tree import DecisionTreeClassifier
from sklearn.lda import LDA
from source.query.merger.average_merger import AverageMerger
from source.query.merger.balanced_merger import BalancedMerger

from source.query.merger.base_learning_merger import BaseLearningMerger
from source.query.merger.one_class_learning_merger import OneClassLearningMerger
from source.query.merger.query0_merger import Query0Merger
from source.query.merger.std_merger import StdMerger

__author__ = 'Shahars'


class MergerFactory(object):
    LOGISTIC_REGRESSION_RANDOM_ISOLATION_KEY = 'LogisticRegression_Randomizer'
    DECISION_TREE_RANDOM_ISOLATION_KEY = 'DecisionTree_Randomizer'
    ONE_CLASS_SVM_RANDOM_ISOLATION_KEY = 'OneClassSVN_Randomizer'
    DECISION_FOREST_RANDOM_ISOLATION_KEY = 'DecisionForest_Randomizer'
    BAGGING_CLASSIFIER_RANDOM_ISOLATION_KEY = "BaggingClassifier_Randomizer"

    def __init__(self, random_provider):
        self.__random_provider = random_provider

    def create_merger(self, logger, merger_key):

        merger_schema = merger_key.model_name
        params = merger_key.model_params

        if merger_schema == "OneClassSVM":
            classifier = self.__get_classifier_by_schema(merger_schema, params)

            return OneClassLearningMerger(logger, str(merger_key), classifier)

        if merger_schema == "Query0Merger":
            return Query0Merger(logger, str(merger_key))
        if merger_schema == "AverageMerger":
            return AverageMerger(logger, str(merger_key))
        if merger_schema == "StdMerger":
            return StdMerger(logger, str(merger_key))
        else:

            classifier = self.__get_classifier_by_schema(merger_schema.replace("Balanced",""), params)
            if merger_schema.startswith("Balanced"):
                return BalancedMerger(logger, str(merger_key), classifier)
            else:
                return BaseLearningMerger(logger, str(merger_key), classifier)

    def __get_classifier_by_schema(self, merger_schema, params):
        if merger_schema == "LogisticRegression":

            seed = self.__random_provider.get_randomizer_seed(self.LOGISTIC_REGRESSION_RANDOM_ISOLATION_KEY)
            return LogisticRegression(C=float(params["regularization_factor"]), class_weight="auto",
                                      fit_intercept=False, random_state=seed)
        elif merger_schema == "DecisionTree":
            seed = self.__random_provider.get_randomizer_seed(self.DECISION_TREE_RANDOM_ISOLATION_KEY)
            return DecisionTreeClassifier(class_weight='auto', max_features="log2", max_depth=params["tree_depth"],
                                          random_state=seed)
        elif merger_schema == "OneClassSVM":
            seed = self.__random_provider.get_randomizer_seed(self.ONE_CLASS_SVM_RANDOM_ISOLATION_KEY)
            return OneClassSVM(kernel=str(params['kernel']), nu=float(params['nu']), random_state=seed)

        elif merger_schema == "RandomForest":
            seed = self.__random_provider.get_randomizer_seed(self.DECISION_FOREST_RANDOM_ISOLATION_KEY)
            return RandomForestClassifier(max_features="log2", oob_score=True, class_weight="auto",
                                          max_depth=int(params["tree_depth"]), random_state=seed)
        elif merger_schema == "BaggingRegression":
            seed = self.__random_provider.get_randomizer_seed(self.BAGGING_CLASSIFIER_RANDOM_ISOLATION_KEY)
            base_estimator = LogisticRegression(C=float(params["regularization_factor"]), class_weight="auto",
                                                random_state=seed)
            return BaggingClassifier(base_estimator=base_estimator, random_state=seed, max_samples=0.7)

        elif merger_schema == "LDA":
            # TODO(Shahar): compare with Igor's implementation
            return LDA(solver='eigen', store_covariance=True)
        else:
            raise ValueError("not implemented merger schema: %s" % merger_schema)
