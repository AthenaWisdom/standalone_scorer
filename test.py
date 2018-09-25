#!/usr/bin/python
import StringIO
import csv
import datetime
import hashlib
import json
import os
import shutil
from collections import namedtuple
from glob import glob

import numpy as np
import pandas as pd
import unittest2
from enum import Enum
from mock import Mock, MagicMock

from source import ContextManager
from source.jobnik_communicator.interface import NOPJobnikCommunicator
from source.query.ml_phase.bag_worker.scorers.random_scorer import RANDOM_SCORER_ISOLATION_KEY
from source.query.ml_phase.ml_bootstrap import build_score_assigner
from source.query.scores_service.domain import ScorerKey
from source.query.scores_service.scores_service import ScoresService
from source.storage.io_handlers.in_memory import InMemoryIOHandler
from source.storage.io_handlers.local_fs import LocalFSIOHandler
from source.storage.stores.artifact_store.in_memory import InMemoryArtifactStore
from source.storage.stores.clusters_store.filtered_clusters_provider import FilteredClustersProvider
from source.storage.stores.clusters_store.matlab_clusters_store import MatlabClustersStore
from source.storage.stores.evaluation_results_store.csv_store import CsvEvaluationResultsStore
from source.storage.stores.general_quest_data_store.json_store import JSONGeneralQuestDataStore
from source.storage.stores.merged_scores_store.csv_store import CSVMergedScoresStore
from source.storage.stores.merged_scores_store.in_memory import InMemoryMergedScoresStore
from source.storage.stores.results_store.csv_store import CsvResultsStore
from source.storage.stores.results_store.interface import ResultsStoreInterface
from source.storage.stores.scores_store.csv_store import CSVScoresStore
from source.storage.stores.scores_store.in_memory import InMemoryScoresStore
from source.storage.stores.scores_store.types import ScoresMetadata, PerformanceMetadata
from source.storage.stores.split_kernel_store.in_memory import InMemorySplitKernelStore
from source.storage.stores.split_kernel_store.in_memory import InMemorySubKernelStore
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata
from source.task_queues.publishers.in_memory import InMemoryTaskQueuePublisher
from source.task_queues.subscribers.in_memory import InMemoryAcknowledgableTaskQueueSubscriber
from source.task_runner.bootstrap import TaskRunnerBootstrapper
from source.task_runner.stop_notification_reader.in_memory import InMemoryStopNotificationReader
from source.task_runner.task_status_store.in_memory import InMemoryTaskStatusStore
from source.task_runner.tasks.score_assigning_task import ScoreAssigningTask
from source.utils.context_handling.context_provider import ContextProvider
from source.utils.random_seed_provider import ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___, \
    RandomSeedProvider
from source.storage.stores.split_kernel_store.in_memory import InMemoryWritableSubKernelStore
from infra.approximate_compare.approximate_comparator import is_almost_equal



class TestScoresService(ScoresService):
    def __init__(self, old_scores_store, old_merger_store, new_scores_store, new_performance_store):
        """
        @type general_quest_data_store: L{GeneralQuestDataStoreInterface}
        """
        super(TestScoresService, self).__init__(old_scores_store, old_merger_store, new_scores_store,
                                                new_performance_store)

    def store_merger_performance(self, customer_id, quest_id, query_id, merger_key, df, performance_type, feature_flags,
                                 is_stable):
        if feature_flags["useOldScoresStore"]:
            merger_metadata = MergerMetadata(str(merger_key), merger_key.model_name,
                                             merger_key.model_params, merger_key.scorer_name)
            merged_scores_metadata = MergedScoresMetadata(merger_metadata, customer_id, quest_id, query_id)
            merged_performance_metadata = MergedPerformanceMetadata(merged_scores_metadata, performance_type)
            merger_performance = MergerPerformance(df, merged_performance_metadata)
            self._old_merger_store.store_merged_performance(merger_performance, is_stable)
        else:
            self._new_performance_store.store_mergers_results_evaluation_df(customer_id, quest_id, query_id,
                                                                             UnscoredStrategy.leave_unscored, df)



class ComparisonPrecision(Enum):
    perfect = 'PERFECT'
    almost_perfect = 'ALMOST_PERFECT'
    very_similar = 'VERY_SIMILAR'
    loose = 'LOOSE'


class Scores(unittest2.TestCase):

    def test_home_scorer(self):
        context = {
            'customer': 'fake_test_customer_query_phase',
            'sphere_id': 'fake_sphere_for_test_query',
            'query_id': 'fake_query_id_test',
            'quest_id': 'fake_quest_id_test',
            'sub_kernel_id': 'some_subkernel_fake_id',
            "operation": "quest",
            "is_past": True,
            "tier": "sub_kernel",
        }
        ContextManager().context = context
        example_name = 'test1'

        base_path = os.path.join(example_name)
        meta_path = os.path.join(base_path, 'meta_HomeScorer')
        self.run_ml_example(base_path, meta_path, compare_exact_results=True)

    @staticmethod
    def __load_sphere_to_memory(customer, sphere_id, local_path, io_handler):
        for file_path in glob(os.path.join(local_path, 'newFormat*')):
            with open(file_path, 'rb') as f:
                path = os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id, 'artifacts',
                                    'blocks', file_path)
                io_handler.save_raw_data(f.read(), path)

    def run_ml_example(self, base_path, meta_path,
                       asserted_found_entity=None,
                       precision=ComparisonPrecision.very_similar, compare_exact_results=False):
        in_path = os.path.join(base_path, 'in')

        customer = 'fake_test_customer_query_phase'
        split_kernel_id = 'split_kernel'
        sphere_id = 'my_sphere'
        quest_id = 'my_quest'
        query_id = 'my_query'
        sub_kernel_ordinal = 0
        feature_flags = {"useOldScoresStore": True}
        artifact_store = InMemoryArtifactStore()
        context_provider = ContextProvider()
        scores_service, score_assigner, io_handler = self.__create_score_assigner(
            customer, quest_id, query_id, split_kernel_id, sphere_id, in_path, meta_path, artifact_store,
            sub_kernel_ordinal, context_provider)

        score_assigner.run_for_k_tasks(1)

        self.assert_all_scorers_expected_files_are_there_and_non_empty(base_path, meta_path, customer, quest_id,
                                                                       query_id, sub_kernel_ordinal, scores_service,
                                                                       precision=precision,
                                                                       compare_exact_results=compare_exact_results,
                                                                       feature_flags=feature_flags)
        self.assert_found_entities(asserted_found_entity, artifact_store)



    def compute_scores(self, example_name, sub_kernel_ordinal):
        base_path = os.getcwd()
        in_path = os.path.join(base_path, 'in')
        meta_path = os.path.join(base_path, 'meta')
        customer = 'fake_test_customer_query_phase'
        split_kernel_id = 'split_kernel'
        sphere_id = 'my_sphere'
        quest_id = 'my_quest'
        query_id = 'my_query'
        artifact_store = InMemoryArtifactStore()
        context_provider = ContextProvider()
        scores_service, score_assigner, _ = self.__create_score_assigner(customer, quest_id, query_id, split_kernel_id,
                                                                          sphere_id, in_path, meta_path, artifact_store,
                                                                          sub_kernel_ordinal, context_provider)
        score_assigner.run_for_k_tasks(1)

        Parameters = namedtuple('Parameters',
            'base_path, in_path, meta_path, customer, split_kernel_id,' +
            'sphere_id, quest_id, query_id, artifact_store, scores_service, score_assigner')
        return Parameters(base_path, in_path, meta_path, customer, split_kernel_id,
                          sphere_id, quest_id, query_id, artifact_store, scores_service, score_assigner)

    def __create_score_assigner(self, customer, quest_id, query_id, split_kernel_id, sphere_id, in_path, meta_path,
                                artifact_store, sub_kernel_ordinal, context_provider, feature_flags={}):
        # Toggle the following flag as needed.
        self.use_csv_stores = True

        in_memory_io_handler = InMemoryIOHandler()  # For all other stores

        # Initialize io_handler per use_csv_stores flag.
        if self.use_csv_stores:
            csvs_path = '/tmp/csv_stores' + datetime.datetime.now().strftime('_%Y%m%d_%H%M%S') + '/'
            if not os.path.isdir(csvs_path):
                os.mkdir(csvs_path)
            local_io_handler = LocalFSIOHandler(base_dir=csvs_path)
            scores_store = CSVScoresStore(local_io_handler)
            merger_store = CSVMergedScoresStore(local_io_handler)

        else:
            scores_store = InMemoryScoresStore(in_memory_io_handler)
            merger_store = InMemoryMergedScoresStore(in_memory_io_handler)

        results_store = CsvResultsStore(in_memory_io_handler)
        new_performance_store = CsvEvaluationResultsStore(in_memory_io_handler,
                                                          JSONGeneralQuestDataStore(in_memory_io_handler)
)

        scores_service = TestScoresService(scores_store, merger_store, results_store, new_performance_store)
        split_kernel_store = InMemorySplitKernelStore(in_memory_io_handler)
        sub_kernel_store = InMemoryWritableSubKernelStore()
        clusters_store = FilteredClustersProvider(in_memory_io_handler, MatlabClustersStore(in_memory_io_handler))
        publisher = InMemoryTaskQueuePublisher()
        subscriber = InMemoryAcknowledgableTaskQueueSubscriber(publisher.internal_queue)
        random_provider = RandomSeedProvider(42)

        random_provider.set_randomizer(RANDOM_SCORER_ISOLATION_KEY, 1)

        self.__load_kernel_from_in_path_to_store(in_path, split_kernel_store, customer, split_kernel_id, sub_kernel_store, quest_id, query_id, sub_kernel_ordinal)
        self.__load_sphere_to_memory(customer, sphere_id, in_path, in_memory_io_handler)
        with open(os.path.join(meta_path, 'ml_conf.json')) as conf_file:
            task = ScoreAssigningTask("", customer, quest_id, query_id, split_kernel_id, sphere_id, True,
                                      json.load(conf_file), sub_kernel_ordinal, sub_kernel_ordinal, 1, 42, None,
                                      feature_flags)
            publisher.publish_tasks([task], '')
        ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___.force_provider(random_provider)
        score_assigner = build_score_assigner(artifact_store, clusters_store, random_provider,
                                              scores_service, sub_kernel_store, context_provider)
        # noinspection PyTypeChecker
        task_runner = TaskRunnerBootstrapper.create_task_runner_from_external_dependencies(
            subscriber, InMemoryTaskStatusStore(), artifact_store, NOPJobnikCommunicator(),
            context_provider, InMemoryStopNotificationReader(forced_id='test-instance-id'), MagicMock(), random_provider, score_assigner)
        return scores_service, task_runner, in_memory_io_handler

    def __load_kernel_from_in_path_to_store(self, in_path, split_kernel_store, customer, split_kernel_id, sub_kernel_store, quest_id, query_id, sub_kernel_ordinal):
        kernel_df = pd.read_csv(os.path.join(in_path, 'kernel.csv'), dtype={'ID': np.int64})
        split_kernel_metadata = SplitKernelMetadata(customer, split_kernel_id, '123')
        split_kernel = MagicMock(name='split_kernel')
        split_kernel.metadata = split_kernel_metadata
        split_kernel.df.select().distinct().collect.return_value = (('0_all=all', ), ('0_all=all', ))
        if hasattr(self, 'num_of_rows'):
            split_kernel.df.where().toPandas.return_value = kernel_df.head(self.num_of_rows)
        else:
            split_kernel.df.where().toPandas.return_value = kernel_df
        split_kernel_store.store_split_kernel(split_kernel)
        sub_kernel_store.store_sub_kernel(customer, quest_id, query_id, sub_kernel_ordinal, kernel_df)

    @staticmethod
    def assert_found_entities(asserted_found_entity, artifact_store):
        if asserted_found_entity:
            entity_type, entity = asserted_found_entity
            artifact = artifact_store.load_artifact(entity_type)[0]

            assert is_almost_equal(entity, artifact.to_dict(), places=3)

    @staticmethod
    def __clean_folder(folder_str):
        for the_file in os.listdir(folder_str):
            file_path = os.path.join(folder_str, the_file)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    def assert_all_scorers_expected_files_are_there_and_non_empty(self, base_path, meta_path, customer, quest_id,
                                                                  query_id, sub_kernel_ordinal, scores_service,
                                                                  precision, compare_exact_results, feature_flags):
        print base_path
        print meta_path
        print "glagla"
        self.assert_all_stats_file_ok(base_path, meta_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                      scores_service, precision)
        self.assert_all_score_files_structure(base_path, meta_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                              scores_service, compare_exact_results, feature_flags)

        self.assert_all_results_files_structure(base_path, meta_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                                scores_service, feature_flags)

    def assert_all_stats_file_ok(self, base_path, meta_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                 scores_service, precision):
        all_expected_scorers_names = self.__get_all_expected_scorer_names(meta_path)
        for scorer_name in all_expected_scorers_names:
            self.assert_scorer_stats_file_ok(scorer_name, base_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                             scores_service, precision)

    def assert_all_score_files_structure(self, base_path, meta_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                         scores_service, compare_exact_results, feature_flags):
        all_expected_scorers_names = self.__get_all_expected_scorer_names(meta_path)
        for scorer_name in all_expected_scorers_names:
            self.assert_scores_file_structure(base_path, customer, quest_id, query_id, sub_kernel_ordinal, scorer_name,
                                              scores_service, compare_exact_results, feature_flags)

    @staticmethod
    def __get_all_expected_scorer_names(meta_path):
        conf_path = os.path.join(meta_path, 'ml_conf.json')
        with open(conf_path) as input_file:
            ml_conf = json.loads(input_file.read())

            scorers_conf = ml_conf['scorers']
            scorer_full_names = []
            for scorer_conf in scorers_conf:
                scorer_name = "_".join([scorer_conf["name"], scorer_conf["unscored_handler"]])
                scorer_tags = scorer_conf.get('tags')
                if scorer_tags and 'external' in scorer_tags:
                    scorer_name = "_".join([scorer_name, scorer_conf["conf"]["external_file_name"],
                                             scorer_conf["conf"]["external_col_name"]])
                scorer_full_names.append((scorer_name))
            # scorer_full_names = ["_".join([scorer_conf["name"], scorer_conf["unscored_handler"]]) for scorer_conf in
            #                      scorers_conf]

            ensemble_full_names = []
            if len(scorer_full_names) > 1:
                ensembles_conf = ml_conf['ensembles']
                for ensemble_conf in ensembles_conf:
                    ensemble_params = ensemble_conf["parameters"]
                    ensemble_tags = ensemble_conf.get('tags')
                    params_str = '_'.join("%s=%s" % (key, val) for (key, val) in ensemble_params.iteritems())
                    full_name = "_".join([ensemble_conf["ensemble_scheme"], params_str])
                    if ensemble_tags:
                        tags_for_full_name = '_'.join(ensemble_tags)
                        full_name += '_' + tags_for_full_name
                    ensemble_full_names.append(full_name)

            return scorer_full_names + ensemble_full_names

    def assert_scores_file_structure(self, base_path, customer, quest_id, query_id, sub_kernel_ordinal, scorer_name,
                                     scores_service, compare_exact_results, feature_flags):
        expected_file_path = os.path.join(base_path, 'expectedOut', scorer_name, 'scores', 'all_scores.csv')

        # actual_scores = read_actual_results(io_handler, customer, query_id, quest_id, sub_kernel_ordinal, scorer_name)
        scorer_key = ScorerKey(scorer_name)
        actual_scores = scores_service.load_scores(customer, quest_id, query_id, scorer_key, sub_kernel_ordinal, feature_flags)
        self.assert_not_all_zero_scores(actual_scores)
        expected_scores = pd.read_csv(expected_file_path, index_col=0)

        self.assertEqual(len(actual_scores), len(expected_scores), 'number of lines in scores file '
                                                                    'is different then expected')
        self.assertEqual(actual_scores.columns[0], expected_scores.columns[0], 'file headers are not same for file ' )
        non_nan_actual_scores = actual_scores.dropna(axis=0)
        non_nan_expected_scores = expected_scores.dropna(axis=0)
        self.assertEquals(len(non_nan_actual_scores), len(non_nan_expected_scores), 'size of scored population is '
                                                                                    'different than expected')
        if compare_exact_results:
            sorted_actual = non_nan_actual_scores.sort_values(by='score', ascending=True)
            sorted_expected = non_nan_expected_scores.sort_values(by='score', ascending=True)
            max_diff = max(abs(sorted_actual['score'] - sorted_expected['score']))
            self.assertTrue(max_diff<=1E-9, 'given scores are different than expected scores')

    def assert_all_results_files_structure(self, base_path, meta_path, customer_id, quest_id, query_id,
                                           sub_kernel_ordinal, scores_service, feature_flags):
        # type: (str, str, str, str, str, int, ResultsStoreInterface) -> ()

        all_expected_scorers_names = self.__get_all_expected_scorer_names(meta_path)
        universe_path = os.path.join(base_path, 'in', 'universe.txt')

        with open(universe_path, 'rb') as expected_f:
            universe_csv = list(csv.reader(expected_f))

            for scorer_name in all_expected_scorers_names:
                scorer_key = ScorerKey(scorer_name)
                actual_results_df = scores_service \
                    .load_scores(customer_id, quest_id, query_id, scorer_key, sub_kernel_ordinal, feature_flags)

                self.assertEqual(len(actual_results_df), len(universe_csv),
                                 'Number of lines is different then expected in sub_query: %d, scorer %s' %
                                 (sub_kernel_ordinal, scorer_name))
                self.assertEqual(actual_results_df.index.name, 'idnum',
                                 'Index header is incorrect in sub_query: %d, scorer %s' %
                                 (sub_kernel_ordinal, scorer_name))

                # #


                    # self.assertListEqual(list(actual_results_df), ['score', 'origin_id'],
                    #                      'File headers are incorrect in sub_query: %d, scorer %s' %
                    #                      (sub_kernel_ordinal, scorer_name))

    def assert_non_empty_file(self, file_path):
        self.assertFalse(os.stat(file_path).st_size == 0, msg='file does not exist or empty \'' + file_path + '\'')

    @staticmethod
    def __get_epsilons(precision):
        if precision == ComparisonPrecision.perfect:
            hr_epsilon = auc_epsilon = recall_epsilon = lift_epsilon = 0
        elif precision == ComparisonPrecision.almost_perfect:
            hr_epsilon = auc_epsilon = recall_epsilon = lift_epsilon = 1E-12
        elif precision == ComparisonPrecision.very_similar:
            hr_epsilon = auc_epsilon = recall_epsilon = lift_epsilon = 1E-6
        elif precision == ComparisonPrecision.loose:
            hr_epsilon = 0.3
            auc_epsilon = 0.15
            recall_epsilon = 0.3
            lift_epsilon = 2
        else:
            raise ValueError('Unknown precision %s' % precision)
        return hr_epsilon, auc_epsilon, recall_epsilon, lift_epsilon

    def assert_scorer_stats_file_ok(self, scorer_name, base_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                    scores_service, precision):
        hr_epsilon, auc_epsilon, recall_epsilon, lift_epsilon = self.__get_epsilons(precision)
        self.assert_scorer_expected_stats(scorer_name, base_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                          scores_service, "auc", auc_epsilon)
        self.assert_scorer_expected_stats(scorer_name, base_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                          scores_service, 'hit_rate', hr_epsilon)
        self.assert_scorer_expected_stats(scorer_name, base_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                          scores_service, 'lift', lift_epsilon)
        self.assert_scorer_expected_stats(scorer_name, base_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                          scores_service, 'recall', recall_epsilon)

    def assert_scorer_expected_stats(self, scorer_name, base_path, customer, quest_id, query_id, sub_kernel_ordinal,
                                     scores_service, performance_type, epsilon):

        scorer_key = ScorerKey(scorer_name)
        scorer_performance_df = scores_service.load_scorer_performance(customer, quest_id, query_id,
                                                                       sub_kernel_ordinal, scorer_key,
                                                                       performance_type)
        scorer_performance_df.columns = ['actual']
        glob_mask = os.path.join(base_path, 'expectedOut', scorer_name, 'artifacts', 'results_summary',
                                 performance_type + "*.csv")
        globbed_list = glob(glob_mask)
        expected_file_path = globbed_list[0]

        expected_df = pd.read_csv(expected_file_path, index_col=0, header=None)
        expected_df.columns = ['expected']

        merged = pd.merge(scorer_performance_df, expected_df, left_index=True, right_index=True, how='outer')
        res = (merged["expected"] - merged["actual"]).abs()
        is_good = (not (res > epsilon).any()) and (not np.isnan(res).any())
        self.assertTrue(is_good, 'some results were not within the margins for scorer %s. \n expected: \n '
                        % scorer_name + expected_df.to_string() + '\n actual:\n' + scorer_performance_df.to_string() +
                        '\ndifference:\n' + res.to_string())

    def assert_not_all_zero_scores(self, actual_scores):
        self.assertFalse((actual_scores[actual_scores.columns[0]] == 0).all(), 'all scores are zero')

testSuite = unittest2.TestSuite()
test_file_strings = 'ml_bootstrap.py'
module_strings = [str[0:len(str)-3] for str in test_file_strings]

if __name__ == "__main__":
     unittest2.main()