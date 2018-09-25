#!/usr/bin/python
#!/usr/bin/python
import StringIO
import csv
import datetime
import hashlib
import json
import os
import numpy as np
import shutil
import pandas as pd
from enum import Enum
from mock import Mock, MagicMock
import shutil
from collections import namedtuple
from glob import glob
from source.storage.io_handlers.local_fs import LocalFSIOHandler
from source.storage.stores.scores_store.csv_store import CSVScoresStore
from source.storage.stores.merged_scores_store.csv_store import CSVMergedScoresStore
from source.storage.stores.results_store.csv_store import CsvResultsStore
from source.storage.stores.evaluation_results_store.csv_store import CsvEvaluationResultsStore
from source.storage.stores.general_quest_data_store.json_store import JSONGeneralQuestDataStore
from test import Scores
from source import ContextManager
from source.storage.stores.artifact_store.in_memory import InMemoryArtifactStore
from source.utils.context_handling.context_provider import ContextProvider
from source.storage.io_handlers.in_memory import InMemoryIOHandler
from source.query.scores_service.scores_service import ScoresService



from source.storage.stores.split_kernel_store.in_memory import InMemoryWritableSubKernelStore
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


class StandAloneScoresService(ScoresService):
    def __init__(self, old_scores_store, old_merger_store, new_scores_store, new_performance_store):
        """
        @type general_quest_data_store: L{GeneralQuestDataStoreInterface}
        """
        super(StandAloneScoresService, self).__init__(old_scores_store, old_merger_store, new_scores_store,
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




def main():
    context = {
        'customer': 'a',
        'sphere_id': 'b',
        'query_id': 'c',
        'quest_id': 'd',
        'sub_kernel_id': 'e',
        "operation": "f",
        "is_past": True,
        "tier": "g",
    }
    ContextManager().context = context
    example_name = 'input'
    base_path = os.path.join(example_name)
    meta_path = os.path.join(base_path, 'meta_HomeScorer')
    run_ml(base_path, meta_path, compare_exact_results=False)


def __load_kernel_from_in_path_to_store(in_path, split_kernel_store, customer, split_kernel_id, sub_kernel_store, quest_id, query_id, sub_kernel_ordinal):
    kernel_df = pd.read_csv(os.path.join(in_path, 'kernel.csv'), dtype={'ID': np.int64})
    split_kernel_metadata = SplitKernelMetadata(customer, split_kernel_id, '123')
    split_kernel = MagicMock(name='split_kernel')
    split_kernel.metadata = split_kernel_metadata
    split_kernel.df.select().distinct().collect.return_value = (('0_all=all', ), ('0_all=all', ))
    split_kernel.df.where().toPandas.return_value = kernel_df
    split_kernel_store.store_split_kernel(split_kernel)
    sub_kernel_store.store_sub_kernel(customer, quest_id, query_id, sub_kernel_ordinal, kernel_df)


def __load_sphere_to_memory(customer, sphere_id, local_path, io_handler):
    for file_path in glob(os.path.join(local_path, 'newFormat*')):
        with open(file_path, 'rb') as f:
            path = os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id, 'artifacts',
                                'blocks', file_path)
            io_handler.save_raw_data(f.read(), path)

def __create_score_assigner(customer, quest_id, query_id, split_kernel_id, sphere_id, in_path, meta_path,
                            artifact_store, sub_kernel_ordinal, context_provider, feature_flags={}):
    in_memory_io_handler = InMemoryIOHandler()  # For all other stores

    # Initialize io_handler per use_csv_stores flag.
    csvs_path = os.path.join('output', datetime.datetime.now().strftime('_%Y%m%d_%H%M%S'))
    if not os.path.isdir(csvs_path):
        os.mkdir(csvs_path)
    local_io_handler = LocalFSIOHandler(base_dir=csvs_path)
    scores_store = CSVScoresStore(local_io_handler)
    merger_store = CSVMergedScoresStore(local_io_handler)

    results_store = CsvResultsStore(in_memory_io_handler)
    new_performance_store = CsvEvaluationResultsStore(in_memory_io_handler,
                                                      JSONGeneralQuestDataStore(in_memory_io_handler)
)

    scores_service = StandAloneScoresService(scores_store, merger_store, results_store, new_performance_store)
    split_kernel_store = InMemorySplitKernelStore(in_memory_io_handler)
    sub_kernel_store = InMemoryWritableSubKernelStore()
    clusters_store = FilteredClustersProvider(in_memory_io_handler, MatlabClustersStore(in_memory_io_handler))
    publisher = InMemoryTaskQueuePublisher()
    subscriber = InMemoryAcknowledgableTaskQueueSubscriber(publisher.internal_queue)
    random_provider = RandomSeedProvider(42)

    random_provider.set_randomizer(RANDOM_SCORER_ISOLATION_KEY, 1)
    __load_kernel_from_in_path_to_store(in_path, split_kernel_store, customer, split_kernel_id, sub_kernel_store, quest_id, query_id, sub_kernel_ordinal)
    __load_sphere_to_memory(customer, sphere_id, in_path, in_memory_io_handler)
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



def run_ml(base_path, meta_path, asserted_found_entity=None, compare_exact_results=False):

    in_path = os.path.join(base_path, 'input')
    customer = 'a'
    split_kernel_id = 'b'
    sphere_id = 'c'
    quest_id = 'd'
    query_id = 'e'
    sub_kernel_ordinal = 0
    feature_flags = {"useOldScoresStore": True}
    artifact_store = InMemoryArtifactStore()
    context_provider = ContextProvider()
    scores_service, score_assigner, io_handler = __create_score_assigner(
        customer, quest_id, query_id, split_kernel_id, sphere_id, in_path, meta_path, artifact_store,
        sub_kernel_ordinal, context_provider)

    score_assigner.run_for_k_tasks(1)

if __name__ == "__main__":
    main()