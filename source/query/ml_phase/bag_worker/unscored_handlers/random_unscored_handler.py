import pandas as pd
import numpy as np

from source.query.ml_phase.bag_worker.unscored_handlers.unscored_handler import UnscoredHandler


class RandomUnscoredHandler(UnscoredHandler):
    RANDOM_UNSCORED_RANDOM_ISOLATION_KEY = 'RandomUnscored_seed_isolation_key'

    def __init__(self, random_provider):
        super(RandomUnscoredHandler, self).__init__("random")
        self.__random_provider = random_provider

    def score_the_unscored_pop(self, scores_arr, universe, scorer_name, logger):
        logger.info("scoring the unscored pop using random scores beneath minimal score")
        scores_df = pd.DataFrame({scorer_name: scores_arr[:, 1]}, index=scores_arr[:, 0])
        universe_for_auc = set(universe)
        #
        # scores_df = self.remove_non_universe_et_whites(scores_df, universe_for_auc, logger)
        non_scored_index = self.retrieve_unscored_pop_df(scores_df, universe_for_auc)

        logger.info("%d unscored people found for scorer %s" % (len(non_scored_index),scorer_name))
        if len(non_scored_index) > 0:
            temp_scored_df = scores_df[~scores_df.index.isin(non_scored_index)]
            if len(temp_scored_df) > 0:
                min_score = min(temp_scored_df[scorer_name].values)
            else:
                logger.info("scorer %s gave no scores to the population" %scorer_name)
                min_score = 0
            logger.info("minimal score is: %.3f" % min_score)
            np.random.seed(self.__random_provider.get_randomizer_seed(self.RANDOM_UNSCORED_RANDOM_ISOLATION_KEY))
            # not_classified_score = (np.random.rand(len(non_scored_index)) * abs(min_score)) * 0.00001
            not_classified_score = min_score - (np.random.rand(len(non_scored_index)) * 0.00001)
            universe_for_auc_df = pd.DataFrame({scorer_name: not_classified_score}, index=non_scored_index)
            final_res_df = pd.concat([temp_scored_df, universe_for_auc_df])
            # noinspection PyTypeChecker
            logger.info("added %d random scores" % (len(final_res_df) - len(temp_scored_df)))

            return final_res_df
        return scores_df


