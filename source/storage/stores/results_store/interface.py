import hashlib
import json

from pandas import DataFrame as PandasDataFrame


class ResultsStoreInterface(object):
    def mutate_and_store_merger_results(self, results_df, customer_id, quest_id, query_id, merger_key):
        """
        @type results_df: C{PandasDataFrame}
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type scorer_id: C{str}
        @type merger_model: C{str}
        @type variant: C{str}
        """
        raise NotImplementedError()

    def mutate_and_store_scorer_results(self, results_df, customer_id, quest_id, query_id, sub_kernel_ordinal,
                                        scorer_key):
        """
        @type results_df: C{PandasDataFrame}
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type sub_kernel_ordinal: C{int}
        @type scorer_name: C{str}
        """
        raise NotImplementedError()

    def load_merger_results_df(self, customer_id, quest_id, query_id, merger_key):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type scorer_id: C{str}
        @type merger_model: C{str}
        @type merger_variant: C{dict}
        @rtype C{PandasDataFrame}
        """
        raise NotImplementedError()

    def load_scorer_results_df(self, customer_id, quest_id, query_id, sub_kernel_ordinal, scorer_key):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type sub_kernel_ordinal: C{int}
        @type scorer_id: C{str}
        @rtype C{PandasDataFrame}
        """
        raise NotImplementedError()

    @staticmethod
    def create_scorer_origin_id(sub_kernel_ordinal, scorer_key):
        """
        @type sub_kernel_ordinal: C{int}
        @type scorer_id: C{str}
        @rtype C{str}
        """
        return hashlib.md5('{}-{}'.format(sub_kernel_ordinal, str(scorer_key))).hexdigest()

    @staticmethod
    def create_merger_origin_id(merger_key):
        """
        @type scorer_id: C{str}
        @type merger_model: C{str}
        @type merger_variant: C{dict}
        @rtype C{str}
        """
        data = json.dumps(merger_key.model_params, separators=(',', ':'), sort_keys=True, allow_nan=False)
        return hashlib \
            .md5('{}-{}-{}'.format(merger_key.scorer_name, merger_key.model_name, data)) \
            .hexdigest()

