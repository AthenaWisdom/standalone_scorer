from source.query.merger.base_learning_merger import BaseLearningMerger

__author__ = 'Shahars'


class OneClassLearningMerger(BaseLearningMerger):
    def __init__(self, logger, scorer_name, classifier):
        super(OneClassLearningMerger, self).__init__(logger, scorer_name, classifier)

    def predict_scores(self, x_test):
        res = self.classifier.predict(x_test)
        return res
