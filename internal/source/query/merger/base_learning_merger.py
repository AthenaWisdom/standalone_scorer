from sklearn import preprocessing
import numpy as np

__author__ = 'Shahars'

POP_ID_COL_NAME = "idnum"


class BaseLearningMerger(object):

    def __init__(self, logger, scorer_name, classifier):
        self.logger = logger
        self.merger_name = scorer_name
        self.classifier = classifier

    def run(self, whites, all_scores_df):
        self.logger.info("Starting to computed merged suspects list for scorer %s." % self.merger_name)

        # scores_arr, pop = self.merger_store.get_scores_et_pop_by_scorer(self.scorer_name)

        query_count = len(all_scores_df.columns)
        pop = all_scores_df.index.values
        if query_count == 1:
            self.logger.info("Only 1 query found, no merging required, returning scores of single query.")
            merged_arr = all_scores_df[all_scores_df.columns[0]].values

            del all_scores_df
            return merged_arr, pop, self.classifier
        elif query_count == 0:
            self.logger.info("no queries found, no merging required.")

            return None, pop, self.classifier
        else:
            self.logger.info("%d queries found, start merging." % query_count)
            all_scores_arr = all_scores_df.values
            del all_scores_df
            merged_arr = self.learn_scores(all_scores_arr, pop, whites)

            self.logger.info("Finished computing merged suspects list for scoring function %s." % self.merger_name)
            return merged_arr, pop, self.classifier

    def _create_y_by_whites(self, pop, whites):
        self.logger.info("building Y by whites")
        # whites = self.merger_store.get_whites()

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
            training_success = self.train_merger(x_mat, y)
            if training_success:
                merged_arr = self.predict_scores(x_mat)
            else:
                merged_arr = np.zeros(len(pop))
        else:
            merged_arr = np.zeros(len(pop))
        return merged_arr

    def train_merger(self, x_train, y_train):
        try:
            self.classifier = self.classifier.fit(x_train, y_train)
            return True
        except ValueError as ex:
            if ex.message.startswith("This solver needs samples of at least 2 classes in the data, but the data contains only one class"):
                self.logger.warning("merger %s failed " % self.merger_name, exc_info=True)
                return False
            else:
                raise

    def _create_x(self, suspects_scores_arr):
        self.logger.info("building X by queries scores")
        x_mat = self.handle_nan_scores(suspects_scores_arr)

        self.logger.info("normalizing X")
        x_mat_scaled = preprocessing.scale(x_mat, copy=False)
        self.logger.info("finished building X by queries scores")
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

