__author__ = 'Shahars'


class UnscoredHandler(object):

    def __init__(self, name):
        self.name = name

    def score_the_unscored_pop(self, scores_arr, universe, scorer_name, logger):
        raise NotImplementedError()

    # def remove_non_universe_et_whites(self, scores_df, universe_for_auc, logger):
    #     scored_pop = len(scores_df.index)
    #     scores_df = scores_df.ix[list(set(scores_df.index).intersection(universe_for_auc))].dropna()
    #     logger.info("removed %d population since not in universe or in whites" % (scored_pop-len(scores_df.index)))
    #     return scores_df

    def retrieve_unscored_pop_df(self, scores_df, universe_for_auc):
        non_scored_index = list(set(universe_for_auc)-set(scores_df.dropna().index))
        return non_scored_index



