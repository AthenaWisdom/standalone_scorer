import logging
from time import time

import pandas as pd

from source.query.scores_analysis.stable_results_analyzer import StableResultsAnalyzer
from source.query.scores_service.domain import ScorerKey, MergerKey
from source.query.scores_service.scores_service import ScoresService
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types.quest.merger import ValidMergerPerformanceSummary, \
    InvalidMergerPerformanceSummary, MergerPricingDurationMetricArtifact
from source.storage.stores.split_kernel_store.interface import SubKernelStoreInterface
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.merger_task import MergerTask
from source.utils.configure_logging import configure_logger

__author__ = 'Shahars'


class ScoresMerger(TaskHandlerInterface):
    def __init__(self, merger_factory, scores_service, sub_kernel_store,
                 artifact_store, results_analyzer):
        """
        @type merger_factory:
        @type sub_kernel_store: L{SubKernelStoreInterface}
        @type artifact_store: L{ArtifactStoreInterface}
        @type scores_service: L{ScoresService}
        @type results_analyzer:
        """
        self.__sub_kernel_store = sub_kernel_store
        self.__results_analyzer = results_analyzer
        self.__artifact_store = artifact_store
        self.__merger_factory = merger_factory
        self.__scores_service = scores_service
        self.__logger = logging.getLogger('endor')

    def run_task(self, task):
        """
        @type task: L{MergerTask}
        """
        if task is None:
            raise RuntimeError('No task received')

        configure_logger(self.__logger, task.get_context())
        self.__logger.info('Got task')

        self.handle_task(task)

    def handle_task(self, task):
        """
        @type task: L{MergerTask}
        """
        with MergerPricingDurationMetricArtifact(task.customer, task.quest_id, 'merge_scores',
                                                 self.__artifact_store, query_id=task.query_id,
                                                 task_ordinal=task.task_ordinal):
            self.__run_merger_by_triplet(task)

    def __run_merger_by_triplet(self, task):
        """
        @type task: L{MergerTask}
        """

        merger_config = task.task_config

        scorer_key = ScorerKey(merger_config["scorer_id"])
        merger_key = MergerKey(merger_config["merger_model"], merger_config["variant"], scorer_key)

        self.__logger.info('start running merger %s '.format(merger_key))

        sub_kernel = self.__sub_kernel_store.load_sub_kernel_by_ordinal_new(task.customer, task.quest_id,
                                                                                task.query_id, 0)
        whites = sub_kernel.whites
        universe = sub_kernel.all_ids
        ground = sub_kernel.ground

        logger = logging.getLogger('endor.machine_learning.merger')
        merger = self.__merger_factory.create_merger(logger, merger_key)

        all_subqueries_scores_df = self.__get_sub_queries_scores_scorer(task, scorer_key)
        start_time = time()
        merged_arr, pop, cls = merger.run(whites, all_subqueries_scores_df)
        running_time = time() - start_time
        logger.info('running time of merger %s is %d seconds'.format(str(merger_key), running_time))
        if not (merged_arr is None):
            unscored = set(universe) - set(pop)
            self.__persist_merged_scores(task, merger_key, merged_arr, pop, whites, unscored)
            stats, valid_input, valid_statistics = self.__analyze_results(str(merger_key), merged_arr, pop, universe,
                                                                          whites, ground, self.__results_analyzer)
            stable_stats, _, _ = self.__analyze_results(str(merger_key), merged_arr, pop, universe, whites, ground,
                                                        StableResultsAnalyzer(self.__results_analyzer))
            self.__persist_stats(stats, merger_key, task, is_stable=False)
            self.__persist_stats(stable_stats, merger_key, task, is_stable=True)

            performance_artifact = self.__build_es_results_entity(task, str(merger_key), stats, whites, ground,
                                                                  universe,
                                                                  valid_input, valid_statistics, running_time)
            self.__artifact_store.store_artifact(performance_artifact)




    def __build_es_results_entity(self, task, scorer_name, results_analyze, whites, ground, universe, valid_input,
                                  valid_statistics, running_time):

        customer = task.customer
        quest_id = task.quest_id
        query_id = task.query_id
        merger_id = task.task_config["merger_id"]
        is_past = task.is_past
        valid_stats = 1 if valid_input and valid_statistics else 0
        if valid_stats:
            auc, hit_rates, recalls, lifts = self.__results_analyzer.prettify_results_summary_for_es(results_analyze)
            return ValidMergerPerformanceSummary(customer, quest_id, query_id, merger_id, scorer_name, 'merger',
                                                 len(ground), len(whites), len(universe), is_past, auc, hit_rates,
                                                 recalls, lifts, running_time, results_analyze['baselines'])
        else:
            return InvalidMergerPerformanceSummary(customer, quest_id, query_id, merger_id, scorer_name, 'merger',
                                                   len(ground), len(whites), len(universe), is_past, running_time,
                                                   results_analyze['baselines'])

    def __analyze_results(self, merger_name, merged_arr, pop, universe, whites, ground, results_analyzer):
        scores_df = pd.DataFrame({merger_name: merged_arr}, index=pop)

        stats = results_analyzer.calc_stats(scores_df, ground, whites, universe)
        stats = {"auc": stats[0], "hit_rate": stats[1], "lift": stats[2], "recall": stats[3], "baselines": stats[4]}
        valid_input = self.__results_analyzer.check_valid_input(scores_df, universe, whites, ground)
        valid_statistics = self.__results_analyzer.check_valid_stats(stats)

        return stats, valid_input, valid_statistics

    @staticmethod
    def calc_whites_percentage(whites, universe):
        return pd.DataFrame({"whites_perc": "%.3f" % (float(len(whites)) / float(len(universe)))}, index=[0])

    # def __load_new_scores(self, task, scorer_name, sub_kernel_ordinal):
    #     return pd.DataFrame(self.__results_store.load_scorer_results_df(task.customer, task.quest_id, task.query_id,
    #                                                                     sub_kernel_ordinal, scorer_name)
    #                         [scorer_name])
    #
    # def __load_old_scores(self, task, scorer_name, sub_kernel_ordinal):
    #     scorer_meta = ScoresMetadata(task.customer, task.quest_id, task.query_id, sub_kernel_ordinal, scorer_name)
    #     scorer_scores = self.__scorer_results_store.load_scores(scorer_meta)
    #     sub_query_scores_df = scorer_scores.all_scores
    #     return sub_query_scores_df

    def __get_sub_queries_scores_scorer(self, task, scorer_key):

        """
        @type task: L{MergerTask}
        @type scorer_key: L{ScorerKey}
        """

        all_df = pd.DataFrame()
        sub_kernel_ids = task.sub_kernel_ids
        for sub_kernel_ordinal in sub_kernel_ids:
            # noinspection PyBroadException
            # TODO: EN-1293 Fix broad exception
            try:
                sub_query_scores_df = self.__scores_service.load_scores(task.customer, task.quest_id, task.query_id,
                                                                        scorer_key, sub_kernel_ordinal,
                                                                        task.feature_flags)
                sub_query_scores_df.columns = ["sub_query_%d" % sub_kernel_ordinal]

            except Exception:
                sub_query_scores_df = pd.DataFrame({str(scorer_key): []}, index=[])

            all_df = pd.merge(all_df, sub_query_scores_df, left_index=True, right_index=True, how="outer")
            del sub_query_scores_df
        # If user has NaN entries in all Sub-queries: he shouldn't enter merger
        all_df = all_df.dropna(thresh=1, axis=0)
        # pop = all_df.index.values
        # scores_arr = all_df.values
        # del all_df
        return all_df

    def __persist_merged_scores(self, task, merger_key, merged_arr, scored_pop, whites, unscored):
        self.__scores_service.store_merged_scores(task.customer, task.quest_id, task.query_id, merger_key, merged_arr,
                                                  scored_pop, whites, unscored)

    def __persist_stats(self, stats, merger_key, task, is_stable):

        for stats_type, stats_object in stats.items():
            stats_df = self.__transform_stats_object_to_df(stats_type, stats_object)

            self.__scores_service.store_merger_performance(task.customer, task.quest_id, task.query_id, merger_key,
                                                           stats_df, stats_type, task.feature_flags, is_stable)

    def __transform_stats_object_to_df(self, stats_type, stats_object):
        if stats_type != 'baselines':
            return stats_object

        idx = []
        columns = {}
        for indexValue, value_map in stats_object.iteritems():
            idx.append(indexValue)

            for name, val in value_map.iteritems():
                if name not in columns:
                    columns[name] = []

                columns[name].append(val)

        return pd.DataFrame(columns, index=idx)


    @staticmethod
    def get_task_type():
        return MergerTask
