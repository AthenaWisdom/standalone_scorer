__author__ = 'user'


class Query0Merger(object):
    def __init__(self, logger, scorer_name):
        self.logger = logger
        self.merger_name = scorer_name

    def run(self, whites, all_scores_df):
        self.logger.info("Starting to computed merged suspects list for scorer %s." % self.merger_name)
        pop = all_scores_df.index.values
        merged_arr = all_scores_df["sub_query_0"].values
        return merged_arr, pop, None
