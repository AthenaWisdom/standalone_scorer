from source.query.merger.dispatch.merger_dispatch_executor import MergerDispatchExecutor
from source.query.merger.merger_triplets_generator import MergerStrategiesProvider
from source.query.merger.task import MergerTasksGenerator
from source.storage.stores.general_quest_data_store.json_store import JSONGeneralQuestDataStore
from source.storage.stores.split_kernel_store.csv_store import CSVSplitKernelStore


def prod_bootstrap(io_handler, artifact_store, task_submitter):
    # noinspection PyTypeChecker
    split_kernel_store = CSVSplitKernelStore(io_handler, None)
    general_quest_data_store = JSONGeneralQuestDataStore(io_handler)
    merger_strategies_provider = MergerStrategiesProvider()
    merger_task_generator = MergerTasksGenerator(general_quest_data_store, split_kernel_store,
                                                 merger_strategies_provider)

    quest_general_store = JSONGeneralQuestDataStore(io_handler)

    return MergerDispatchExecutor(merger_task_generator, task_submitter, artifact_store, quest_general_store)
