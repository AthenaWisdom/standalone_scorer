from enum import IntEnum


class EvaluationResultsStoreInterface(object):
    def load_merger_results_evaluation_df(self, customer_id, quest_id, query_id, unscored_strategy):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type unscored_strategy: C{UnscoredStrategy}
        @rtype C{PandasDataFrame}
        """
        raise NotImplementedError()

    def load_scorer_results_evaluation_df(self, customer_id, quest_id, query_id, unscored_strategy):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type unscored_strategy: C{UnscoredStrategy}
        @rtype C{PandasDataFrame}
        """
        raise NotImplementedError()


class UnscoredStrategy(IntEnum):
    leave_unscored = 0
    after_scored = 1
