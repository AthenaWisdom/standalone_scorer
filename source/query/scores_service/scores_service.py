
import pandas as pd
import numpy as np
from source.query.scores_service.domain import MergerKey, ScorerKey
from source.storage.stores.evaluation_results_store.interface import EvaluationResultsStoreInterface, UnscoredStrategy
from source.storage.stores.merged_scores_store.interface import MergedScoresStoreInterface
from source.storage.stores.merged_scores_store.types import MergerMetadata, MergedScoresMetadata, \
    MergedPerformanceMetadata, MergedScores, MergerPerformance
from source.storage.stores.results_store.interface import ResultsStoreInterface
from source.storage.stores.scores_store.interface import ScoresStoreInterface
from source.storage.stores.scores_store.types import ScoresMetadata, Scores, PerformanceMetadata, ScorerPerformance

__author__ = 'user'


class ScoresService(object):
    def __init__(self, old_scores_store, old_merger_store, new_scores_store, new_performance_store):
        """
        @type old_scores_store: L{ScoresStoreInterface}
        @type old_merger_store: L{MergedScoresStoreInterface}
        @type new_scores_store: L{ResultsStoreInterface}
        @type new_performance_store: L{EvaluationResultsStoreInterface}
        """
        self._old_scores_store = old_scores_store
        self._old_merger_store = old_merger_store
        self._new_scores_store = new_scores_store
        self._new_performance_store = new_performance_store

    def load_scores(self, customer, quest_id, query_id, scorer_key, sub_kernel_ordinal, feature_flags):
        if feature_flags["useOldScoresStore"]:
            scorer_meta = ScoresMetadata(customer, quest_id, query_id, sub_kernel_ordinal,
                                         str(scorer_key))
            scorer_scores = self._old_scores_store.load_scores(scorer_meta)
            sub_query_scores_df = scorer_scores.all_scores
            sub_query_scores_df.columns = ['score']
            return sub_query_scores_df
        else:
            res = self._new_scores_store.load_scorer_results_df(customer, quest_id, query_id, sub_kernel_ordinal,
                                                                scorer_key)
            return pd.DataFrame(res['score'])

    def store_scores(self, customer, quest_id, query_id, sub_kernel_ordinal, scorer_key, all_scores_df, suspects_df):

        scores_metadata = ScoresMetadata(customer, quest_id, query_id, sub_kernel_ordinal,
                                         str(scorer_key))

        scores = Scores(all_scores_df, suspects_df, scores_metadata)
        self._old_scores_store.store_scores(scores)
        # Save in new format

        self._new_scores_store.mutate_and_store_scorer_results(all_scores_df.copy(), customer, quest_id,
                                                               query_id, sub_kernel_ordinal, scorer_key)

    def store_merged_scores(self, customer, quest_id, query_id, merger_key, merged_arr,
                            scored_pop, whites, unscored_population):
        scored_df = pd.DataFrame({'score': merged_arr}, index=scored_pop)
        unscored_df = pd.DataFrame({'score': np.nan}, index=unscored_population)
        results_df = pd.concat([scored_df, unscored_df])

        # merged_scores_df = pd.DataFrame({'score': merged_arr}, index=scored_pop)
        merged_suspects_df = results_df.ix[set(results_df.index) - set(whites)]
        merger_meta = MergerMetadata(str(merger_key), merger_key.model_name,
                                     merger_key.model_params, merger_key.scorer_name)
        merged_scores_meta = MergedScoresMetadata(merger_meta, customer, quest_id, query_id)
        merged_scores_obj = MergedScores(results_df, merged_suspects_df, merged_scores_meta)

        self._old_merger_store.store_merged_scores(merged_scores_obj)
    #     Save both in old and in new format
        results_df.index.name = 'user_id'
        self._new_scores_store.mutate_and_store_merger_results(results_df, customer, quest_id, query_id, merger_key)

    def load_merged_scores(self, customer, quest_id, query_id, feature_flags, merger_key):
        if feature_flags["useOldScoresStore"]:
            merger_metadata = MergerMetadata(str(merger_key), merger_key.model_name, merger_key.model_params,
                                             merger_key.scorer_name)
            merged_scores_meta = MergedScoresMetadata(merger_metadata, customer, quest_id, query_id)
            scores_obj = self._old_merger_store.load_merged_scores(merged_scores_meta)

            return scores_obj.all_merged_scores
        else:
            return self._new_scores_store.load_merger_results_df(customer, quest_id, query_id, merger_key)

    def load_mergers_precisions(self, customer_id, quest_id, query_id, feature_flags, merger_keys_str_map):
        if feature_flags["useOldScoresStore"]:
            all_merger_hr = pd.DataFrame()
            for merger_name, merger_key in merger_keys_str_map.items():
                merger_meta = MergerMetadata(merger_name, merger_key.model_name,
                                             merger_key.model_params, merger_key.scorer_name)
                merged_scores_meta = MergedScoresMetadata(merger_meta, customer_id, quest_id, query_id,)
                metadata = MergedPerformanceMetadata(merged_scores_meta, "hit_rate")
                merged_hr_df = self._old_merger_store.load_merged_performance(metadata).df
                merged_hr_df.columns = [merger_name]
                all_merger_hr = pd.merge(all_merger_hr, merged_hr_df, left_index=True, right_index=True, how="outer")
            return all_merger_hr
        else:
            df = self._new_performance_store.load_merger_results_evaluation_df(customer_id, quest_id, query_id,
                                                                                UnscoredStrategy.leave_unscored)
            hr_df = df[df["measurement"] == "precision"]
            hr_df["model_id"] = df.apply(lambda row: str(MergerKey(row['merger_model'], row['variant'],
                                                                   ScorerKey(row["scorer_id"]))),
                                         axis=1)

            formatted_performance = self.__get_merger_formatted_performance(hr_df)
            return formatted_performance

    def load_single_merger_performance(self, customer, quest_id, query_id, merger_key, performance_type, feature_flags):
        if feature_flags["useOldScoresStore"]:
            merger_metadata = MergerMetadata(str(merger_key),merger_key.model_name,
                                             merger_key.model_params, merger_key.scorer_name)
            merged_scores_meta = MergedScoresMetadata(merger_metadata, customer, quest_id, query_id)
            performance_metadata = MergedPerformanceMetadata(merged_scores_meta, performance_type)
            merger_performance_df = self._old_merger_store.load_merged_performance(performance_metadata).df.copy()
            return merger_performance_df
        else:
            all_perf_df = self._new_performance_store.load_merger_results_evaluation_df(customer, quest_id, query_id,
                                                                                         UnscoredStrategy.leave_unscored)
            if performance_type == 'baselines':
                return self.__calc_baseline_from_ven(all_perf_df, merger_key)
            if performance_type == 'hit_rate':
                performance_type = 'precision'
            performance_df = all_perf_df[all_perf_df["measurement"] == performance_type]
            performance_df["model_id"] = performance_df.apply(lambda row: str(MergerKey(row['merger_model'], row['variant'],
                                                                                        ScorerKey(row["scorer_id"]))),
                                                              axis=1)
            merger_performance_df = performance_df[performance_df["model_id"] == str(merger_key)]
            formatted_performance = self.__get_merger_formatted_performance(merger_performance_df)

            return formatted_performance

    def store_merger_performance(self, customer_id, quest_id, query_id, merger_key, df, performance_type, feature_flags,
                                 is_stable):

        merger_metadata = MergerMetadata(str(merger_key), merger_key.model_name,
                                         merger_key.model_params, merger_key.scorer_name)
        merged_scores_metadata = MergedScoresMetadata(merger_metadata, customer_id, quest_id, query_id)
        merged_performance_metadata = MergedPerformanceMetadata(merged_scores_metadata, performance_type)
        merger_performance = MergerPerformance(df, merged_performance_metadata)
        self._old_merger_store.store_merged_performance(merger_performance, is_stable)


    def store_scorer_performance(self, customer_id, quest_id, query_id, sub_query_id, scorer_key, stats_type,
                                 performance_df):
        scorer_metadata = ScoresMetadata(customer_id, quest_id, query_id, sub_query_id, str(scorer_key))
        performance_metadata = PerformanceMetadata(scorer_metadata, stats_type)
        performance_obj = ScorerPerformance(performance_df, performance_metadata)
        self._old_scores_store.store_performance(performance_obj)

    def load_scorer_performance(self, customer_id, quest_id, query_id, sub_kernel_ordinal, scorer_key,
                                performance_type):
        scorer_meta = ScoresMetadata(customer_id, quest_id, query_id, sub_kernel_ordinal, str(scorer_key))
        performance_metadata = PerformanceMetadata(scorer_meta, performance_type)
        return self._old_scores_store.load_performance(performance_metadata).df

    def __calc_baseline_from_ven(self, all_perf_df, merger_key):
        performance_df = all_perf_df[all_perf_df['measurement']=='venn']
        performance_df["model_id"] = performance_df.apply(lambda row: str(MergerKey(row['merger_model'], row['variant'],
                                                                                        ScorerKey(row["scorer_id"]))),
                                                          axis=1)
        merger_performance_df = performance_df[performance_df["model_id"] == str(merger_key)]

        scored_including_whites = merger_performance_df[merger_performance_df['name'].isin(['s','sv','sl','slv'])]['y'].sum()
        unscored_including_whites = merger_performance_df[merger_performance_df['name'].isin(['u', 'uv', 'ul', 'ulv'])]['y'].sum()
        scored_non_whites = merger_performance_df[merger_performance_df['name'].isin(['s', 'sv'])]['y'].sum()
        unscored_non_whites_count = merger_performance_df[merger_performance_df['name'].isin(['u','uv'])]['y'].sum()
        ground_in_unscored = merger_performance_df[merger_performance_df['name'].isin(['uv'])]['y'].sum()
        ground_in_scored = merger_performance_df[merger_performance_df['name'].isin(['sv'])]['y'].sum()

        baselines = {
            'scored': {
                'count': scored_including_whites,
                'count_non_whites': scored_non_whites,
                'ground_non_whites': ground_in_scored
            },
            'unscored': {
                'count': unscored_including_whites,
                'count_non_whites': unscored_non_whites_count,
                'ground_non_whites': ground_in_unscored
            }
        }
        return pd.DataFrame(baselines).T

    def __get_merger_formatted_performance(self, performance_df):
        performance_df['order_type'] = performance_df['name'].str.split(" ").str[0]
        performance_df = performance_df[performance_df['order_type'].isin(['top','bottom'])]
        performance_df['order_type'] = performance_df['order_type'].str.replace("top","").replace("bottom", "_bottom")
        performance_df['old_x'] = performance_df['name'].str.extract('(\d+)')
        performance_df['old_type'] = performance_df['name'].str[-1:].replace('[^%]$','count', regex=True).replace("%", "prcntg")
        performance_df['old_name'] = "hr" + performance_df['order_type'] + "_" + performance_df["old_type"] + performance_df["old_x"]
        performance_df = performance_df[["old_name", "y", "model_id"]]
        performance_df = performance_df.pivot(index="old_name", columns="model_id", values="y")



        return performance_df

