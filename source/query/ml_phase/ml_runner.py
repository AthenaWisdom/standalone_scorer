import logging

import numpy as np
import pandas as pd

from source.query.scores_service.domain import ScorerKey
from source.query.scores_service.scores_service import ScoresService
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types.quest.scores_assigner import \
    DatasetPropertiesArtifact, ScoreAssignerPricingDurationMetricArtifact
from source.storage.stores.clusters_store.filtered_clusters_provider import FilteredClustersProvider
from source.storage.stores.clusters_store.types import ClustersMetaData
from source.storage.stores.split_kernel_store.interface import SubKernelStoreInterface
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata, SubKernel
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.score_assigning_task import ScoreAssigningTask
from source.utils.config import ConfigParser
from source.utils.configure_logging import configure_logger

SCORER_TAG_INTERNAL = 'internal'
SCORER_TAG_RANDOM = 'random'


class ScoreAssigner(TaskHandlerInterface):
    def __init__(self, scores_service, sub_kernel_store, matlab_store, matlab_handler, bag_factory,
                 results_analyzer, scorer_factory, ensemble_factory, artifact_store, random_provider,
                 es_performance_storer):
        """
        @type artifact_store: L{ArtifactStoreInterface}
        @type scores_service:  L{ScoresService}
        @type sub_kernel_store: L{SubKernelStoreInterface}
        @type matlab_store: L{FilteredClustersProvider}
        @type matlab_handler:
        @type bag_factory:
        @type results_analyzer:
        @type scorer_factory:
        @type ensemble_factory:
        @type random_provider:
        @type es_performance_storer: L{StorePerformanceToElasticSearch}
        """
        self.__logger = logging.getLogger('endor')

        self.__results_analyzer = results_analyzer
        self.__matlab_handler = matlab_handler
        self.__random_provider = random_provider

        self.__bag_factory = bag_factory
        self.__scorer_factory = scorer_factory
        self.__ensemble_factory = ensemble_factory

        self.__matlab_store = matlab_store
        self.__sub_kernel_store = sub_kernel_store
        self.__artifact_store = artifact_store
        self.__scores_service = scores_service

        self.__es_performance_storer = es_performance_storer

    def run_task(self, task):
        """
        @type task: L{ScoreAssigningTask}
        """
        if task is None:
            raise RuntimeError('No task received')
        configure_logger(self.__logger, task.get_context())
        self.__logger.info('Got task')

        self.handle_task(task)

    def handle_task(self, task):
        is_past_mode = task.is_past
        with ScoreAssignerPricingDurationMetricArtifact(task.customer, task.quest_id, 'score_assigning',
                                                        self.__artifact_store, query_id=task.query_id,
                                                        task_ordinal=task.task_ordinal):
            self.__run_ml_and_calc_stats(task, is_past_mode)

    def __run_ml_and_calc_stats(self, task, is_past_mode):
        sub_kernel, sub_kernel_success = self.__load_sub_kernel(task, is_past_mode)
        if sub_kernel_success:

            bag, success = self.__create_bag(task, is_past_mode, sub_kernel)

            bag_properties = DatasetPropertiesArtifact(task.customer, task.quest_id, task.sphere_id, task.query_id,
                                                        task.sub_kernel_id, len(bag.whites), len(bag.universe),
                                                        len(bag.props_df.index), len(bag.bag_pop),
                                                        len(bag.universe_in_bag),
                                                        len(bag.whites_in_bag), bag.nnz_in_cluster_to_pop_matrix,
                                                        task.is_past)
            self.__artifact_store.store_artifact(bag_properties)
            if success:
                self.__logger.info("start running ml phase")
                config = ConfigParser.from_dict(task.task_config)
                scores_df, scorer_tags = self.__calc_all_scores(task, bag, config)
                scores_with_ensemble = self.__calc_ensemble_scores(task, config, bag, scores_df, scorer_tags)
                self.__logger.info("finished running ml phase")

                if scores_with_ensemble is not None and len(scores_with_ensemble) > 0:
                    # self.__persist_scores(task, scores_with_ensemble, bag.whites)
                    stats, valid_input, valid_statistics = self.__results_analyze(sub_kernel, scores_with_ensemble)
                    self.__persist_stats(task, stats)
            else:
                self.__logger.warning("bag worker didn't run")
        else:
            self.__logger.warning("bag worker didn't run")

    def __calc_all_scores(self, task, bag, config):
        self.__logger.info("started calculating scores for bag")
        bag_scorers, scorer_tags = self.__scorer_factory.create_scorers(config.scorers, config.run_time)

        for scorer in bag_scorers:
            self.__logger.info("started processing and storing results of {}".format(scorer))
            scores_arr = scorer.score_population(bag, self.__logger)
            results_df = pd.DataFrame({scorer.name: scores_arr[:, 1]}, index=scores_arr[:, 0])
            self.__persist_single_scorer_results(task, scorer.name, results_df, bag.universe, bag.whites)
            self.__logger.info("done processing and storing results of {}".format(scorer))

        self.__logger.info("finished calculating scores for bag")
        all_scores = bag.get_scores_df()
        return all_scores, scorer_tags

    def __calc_ensemble_scores(self, task, config, bag, scores_df, scorer_tags):
        scores_with_ensembles_df = scores_df
        if len(config.ensembles) > 0 and len(scores_df.columns) > 1:

            for ensemble_config in config.ensembles:
                if not hasattr(ensemble_config, 'tags'):
                    scorer_names_to_ensemble = scorer_tags[SCORER_TAG_INTERNAL]
                    tags_for_ensemble_name = None
                else:
                    scorer_names_to_ensemble = []
                    for tag in ensemble_config.tags:
                        scorer_names_to_ensemble.extend(scorer_tags[tag])
                    tags_for_ensemble_name = '_'.join(ensemble_config.tags)

                ensemble_key = ScorerKey(ensemble_config["ensemble_scheme"], ensemble_config["parameters"],
                                         tags_for_ensemble_name)
                ensemble_model = self.__ensemble_factory.create_ensemble(self.__logger, ensemble_key)
                scorers_for_ensemble = scores_df[scorer_names_to_ensemble]
                ensembled_arr, pop, classifier = ensemble_model.run(
                    bag.whites, scorers_for_ensemble.values, scores_df.index)

                ensemble_scores_df = pd.DataFrame({str(ensemble_key):  ensembled_arr}, index=pop)

                self.__persist_single_scorer_results(task, str(ensemble_key), ensemble_scores_df.copy(), bag.universe,
                                                     bag.whites)

                scores_with_ensembles_df = pd.merge(scores_with_ensembles_df, ensemble_scores_df, left_index=True,
                                                    right_index=True, how="inner")
        return scores_with_ensembles_df

    def __add_unscored_universe_to_results(self, scored_df, universe):
        scored_population = scored_df.index.values
        unscored_population = list(set(universe)-set(scored_population))
        unscored_df = pd.DataFrame({"score": np.nan}, index=unscored_population)
        res = pd.concat([scored_df, unscored_df])
        return res

    def __results_analyze(self, sub_kernel, scores_df):
        universe = sub_kernel.all_ids
        whites = sub_kernel.whites
        ground = sub_kernel.ground
        stats = self.__results_analyzer.calc_stats(scores_df, ground, whites, universe)
        stats = {"auc": stats[0], "hit_rate": stats[1], "lift": stats[2], "recall": stats[3]}
        valid_input = self.__results_analyzer.check_valid_input(scores_df, universe, whites, ground)
        valid_statistics = self.__results_analyzer.check_valid_stats(stats)

        return stats, valid_input, valid_statistics

    def __create_bag(self, current_task, is_past_mode, sub_kernel):
        """
        @type current_task: L{ScoreAssigningTask}
        @type is_past_mode:
        @type sub_kernel: L{SubKernel}
        """
        universe, universe_success = self.__get_universe(sub_kernel, is_past_mode)

        if universe_success:
            whites = self.__get_whites(sub_kernel)
            clusters_metadata = ClustersMetaData(current_task.customer, current_task.sphere_id)
            use_specific_clusters = current_task.feature_flags.get("useSpecificClusters", False)
            clusters_obj = self.__matlab_store.load_clusters(clusters_metadata, use_specific_clusters)
            external_data = sub_kernel.external_data
            bag, bag_success = self.__bag_factory.create_new_bag(current_task,
                                                                 is_past_mode, universe,
                                                                 whites, clusters_obj.clusters_properties,
                                                                 clusters_obj.population_to_clusters_matrix,
                                                                 clusters_obj.population_ids, external_data)
            return bag, bag_success
        else:
            return None, False

    def __handle_error_message(self, error_msg, is_past_mode):
        if is_past_mode:
            self.__logger.warning(error_msg)
            return False
        else:
            self.__logger.error(error_msg)
            raise ValueError(error_msg)

    def __load_sub_kernel(self, task, is_past_mode):
        """
        @type task: L{ScoreAssigningTask}
        """
        sub_kernel = None
        success = True
        try:
            sub_kernel = self.__sub_kernel_store.load_sub_kernel_by_ordinal_new(task.customer, task.quest_id,
                                                                                task.query_id, task.sub_kernel_id)
        except (LookupError, KeyError) as e:
            running_mode_error = "past" if is_past_mode else "present"
            error_msg = "Couldn't load sub kernel in %s running mode, due to %s" % \
                        (running_mode_error, str(e))
            success = self.__handle_error_message(error_msg, is_past_mode)
        return sub_kernel, success

    def __get_universe(self, sub_kernel, is_past_mode):
        universe = sub_kernel.all_ids
        success = True
        if len(universe) == 0:
            running_mode_error = "past" if is_past_mode else "present"
            error_msg = "Universe is empty in %s mode, bag not created, cannot continue." % running_mode_error
            success = self.__handle_error_message(error_msg, is_past_mode)
        return universe, success

    def __get_whites(self, sub_kernel):
        whites = sub_kernel.whites
        if len(whites) == 0:
            self.__logger.warning("Continuing with 0 whites")
        return whites

    def __persist_single_scorer_results(self, task, scorer_name, results_df, universe, whites):
        results_df.rename(columns={scorer_name: 'score'}, inplace=True)
        with_unscored_df = self.__add_unscored_universe_to_results(results_df, universe)
        with_unscored_df.index.name = 'user_id'
        suspects_df = with_unscored_df.ix[set(with_unscored_df.index) - set(whites)]
        scorer_key = ScorerKey(scorer_name)
        self.__scores_service.store_scores(task.customer, task.quest_id, task.query_id, task.sub_kernel_id, scorer_key, with_unscored_df, suspects_df)


    def __persist_stats(self, task, stats):

        for stats_type, stats_df in stats.items():
            for scorer_name in stats_df.columns.values:

                scorer_key = ScorerKey(scorer_name)
                performance_df = stats_df[scorer_name].to_frame()
                self.__scores_service.store_scorer_performance(task.customer, task.quest_id, task.query_id,
                                                               task.sub_kernel_id, scorer_key, stats_type,
                                                               performance_df)

    def __build_bag_properities(self, bag):
        pass

    @staticmethod
    def get_task_type():
        return ScoreAssigningTask
