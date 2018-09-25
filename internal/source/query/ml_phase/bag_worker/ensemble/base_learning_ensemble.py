import numpy as np
from sklearn import preprocessing
__author__ = 'Shahars'


class BaseLearningEnsemble(object):

    def __init__(self, logger, classifier):
        self.logger = logger
        self.classifier = classifier

    def run(self, whites, scores_arr, pop):
        scorer_count = scores_arr.shape[1]
        if scorer_count == 1:
            self.logger.info("Only 1 scorer found, no ensemble required, returning scores of single scorer.")
            ensembled_arr = scores_arr.flatten()
            return ensembled_arr, pop, self.classifier
        elif scorer_count == 0:
            self.logger.info("no scorers found, no ensemble required.")
            return None, pop, self.classifier
        else:
            self.logger.info("%d scorers found, start ensembling." % scorer_count)
            ensembled_arr = self.learn_scores(scores_arr, pop, whites)

            self.logger.info("Finished computing ensembled scores list")
            return ensembled_arr, pop, self.classifier

    def _create_y_by_whites(self, pop, whites):
        self.logger.info("building Y by whites")

        y = np.in1d(pop, whites).astype(int)
        if len(set(y)) != 2:
            self.logger.warning("no two classes in scores population, merger will output default results")
            y = None

        self.logger.info("finished building Y by whites")
        return y

    def predict_scores(self, x_test):
        res = self.classifier.predict_proba(x_test)
        return res[:, 1]

    def learn_scores(self, scores_arr, pop, whites):
        x_mat = self._create_x(scores_arr)
        y = self._create_y_by_whites(pop, whites)
        if y is not None:
            self.train_ensemble(x_mat, y)
            ensembled_arr = self.predict_scores(x_mat)
        else:
            ensembled_arr = np.zeros(len(pop))
        return ensembled_arr

    def train_ensemble(self, x_train, y_train):
        self.classifier = self.classifier.fit(x_train, y_train)

    def _create_x(self, suspects_scores_arr):
        self.logger.info("building X by scorers scores")
        x_mat = self.handle_nan_scores(suspects_scores_arr)

        self.logger.info("normalizing X")
        x_mat_scaled = preprocessing.scale(x_mat, copy=True)
        self.logger.info("finished building X by scorers scores")
        return x_mat_scaled

    @staticmethod
    def handle_nan_scores(suspects_scores_arr):
        # NaN shouldn't be replaced by 0, but by min value in column
        nan_inds = np.where(np.isnan(suspects_scores_arr))
        col_min = np.nanmin(suspects_scores_arr, axis=0)

        # Some of the columns are all nan
        col_min[np.isnan(col_min)] = 0
        suspects_scores_arr[nan_inds] = np.take(col_min-1, nan_inds[1])
        #  if any nan or -inf entries remain, make them 0
        suspects_scores_arr[np.isnan(suspects_scores_arr) |
                            np.isneginf(suspects_scores_arr)] = 0
        #  if any inf or too large entries remain, make them 2**64-2
        suspects_scores_arr[np.isposinf(suspects_scores_arr) |
                            (suspects_scores_arr > (2**64-1))] = 2**64-2

        return suspects_scores_arr
