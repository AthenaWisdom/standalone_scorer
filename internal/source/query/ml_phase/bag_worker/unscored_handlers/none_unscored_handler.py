__author__ = 'Shahars'

import pandas as pd

from source.query.ml_phase.bag_worker.unscored_handlers.unscored_handler import UnscoredHandler


class LeaveUnscored(UnscoredHandler):
    def __init__(self):
        super(LeaveUnscored, self).__init__("none")

    def score_the_unscored_pop(self, scores_arr, universe, scorer_name, logger):
        logger.info("leaving the unscored to be unscored")
        scores_df = pd.DataFrame({scorer_name: scores_arr[:, 1]}, index=scores_arr[:, 0])
        # universe_for_auc = set(universe) - set(whites)
        # scores_df = self.remove_non_universe_et_whites(scores_df, universe_for_auc, logger)
        return scores_df
