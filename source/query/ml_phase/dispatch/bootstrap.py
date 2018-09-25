from source.query.ml_phase.dispatch.score_assigning_dispatch_executor import ScoreAssigningDispatchHandler
from source.query.ml_phase.task import ScoreAssigningTaskGenerator
from source.storage.stores.general_quest_data_store.json_store import JSONGeneralQuestDataStore
from source.storage.stores.split_kernel_store.csv_store import CSVSplitKernelStore


def prod_bootstrap(io_handler, artifact_store, task_submitter):
    # noinspection PyTypeChecker
    split_kernel_store = CSVSplitKernelStore(io_handler, None)
    general_quest_data_store = JSONGeneralQuestDataStore(io_handler)

    return build_score_assigning_executor(artifact_store, general_quest_data_store,
                                          task_submitter, split_kernel_store)


def build_score_assigning_executor(artifact_store, general_quest_data_store, task_submitter, split_kernel_store):
    score_assigner_task_generator = ScoreAssigningTaskGenerator()
    return ScoreAssigningDispatchHandler(artifact_store, score_assigner_task_generator,
                                         general_quest_data_store, task_submitter,
                                         split_kernel_store)
