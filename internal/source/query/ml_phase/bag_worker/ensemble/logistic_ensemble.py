import pandas as pd
import numpy as np
from sklearn import linear_model, preprocessing


class LogisticEnsemble(object):
    def __init__(self, logger, scores_df, whites, name, random_provider):
        self._random_provider = random_provider
        self.RANDOM_ISOLATION_KEY = "logRegEnsemble_random_isolation_key"
        self.logger = logger
        self.scores_df = scores_df
        self.whites = whites
        self.name = name

    def create_x_by_all_scores(self):
        self.logger.info("start creating X sample by all scores")
        # self.scores_df = self.scores_df.fillna(0)
        self.scores_df = self.handle_nan_scores(self.scores_df)
        self.logger.info("sample size: %d" % len(self.scores_df.index))
        x_mat = self.scores_df.values
        self.logger.info("normalizing X")
        x_mat_scaled = preprocessing.scale(x_mat)
        self.logger.info("finished creating X sample by all scores")
        return x_mat_scaled

    def create_y_by_whites(self):
        self.logger.info("start creating y labels by whites")

        pop = self.scores_df.index.values
        y = np.in1d(pop, self.whites).astype(int)
        self.logger.info("finished creating y labels by whites")
        if len(set(y)) != 2:
            self.logger.warning("no two classes in scores population, ensemble will output default results")
            y = None
        return y

    def create_x_y_for_learning(self):
        x_mat = self.create_x_by_all_scores()
        y = self.create_y_by_whites()
        return x_mat, y

    def learn_logistic_reg_scores(self, x_mat, y, regularization_factor):
        self.logger.info("start calculating ensemble scores by logistic regression, with regularization "
                         "%s" % regularization_factor)
        seed = self._random_provider.get_randomizer_seed(self.RANDOM_ISOLATION_KEY)
        logreg = linear_model.LogisticRegression(C=regularization_factor, class_weight='auto', random_state=seed)
        logreg = logreg.fit(x_mat, y)
        z_mat = logreg.predict_proba(x_mat)
        self.logger.info("finished calculating ensemble scores by logistic regression")
        return z_mat[:, 1], logreg

    def calc_ensemble_scores(self, regularization_factor):
        ensemble_name = "%s_reg_%s" % (self.name, regularization_factor)
        x_mat, y = self.create_x_y_for_learning()

        if y is not None:
            try:
                reg_scores, model = self.learn_logistic_reg_scores(x_mat, y, regularization_factor)
                ensemble_scores = pd.DataFrame({ensemble_name: reg_scores},
                                               index=self.scores_df.index.values)
            except Exception as e:
                self.logger.warning("could not calc ensemble using %s with regularization factor %d, due to: %s."
                                    " Returning default scores" % (self.name, regularization_factor, str(e)))
                ensemble_scores = pd.DataFrame({ensemble_name: 0}, index=self.scores_df.index.values)
                model = None
        else:
            ensemble_scores = pd.DataFrame({ensemble_name: 0}, index=self.scores_df.index.values)
            model = None
        return ensemble_scores, [{"model_name": ensemble_name, "model": model}, ]

    def calc_discovery_mode_ensemble(self, discovery_reg_factors):
        self.logger.info("start calculating discovery mode ensemble")
        ensemble_scores = pd.DataFrame()
        ensemble_models = []
        for reg_factor in discovery_reg_factors:
            func_ensemble_scores, model = self.calc_ensemble_scores(reg_factor)
            ensemble_models.extend(model)
            ensemble_scores = pd.merge(ensemble_scores, func_ensemble_scores, left_index=True,
                                       right_index=True, how="outer")
        self.logger.info("finished calculating discovery mode ensemble")

        return ensemble_scores, ensemble_models

    @staticmethod
    def handle_nan_scores(all_suspects_df):
        all_suspects_df = all_suspects_df.replace([np.inf, -np.inf], np.nan)
        all_suspects_df = all_suspects_df.applymap(lambda val: np.nan if val > 2**53-1 else val)
        all_suspects_df = all_suspects_df.apply(lambda col:
                                                col.fillna(value=(0 if np.isnan(col.min()) else col.min()-1)))
        return all_suspects_df