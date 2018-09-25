from source.query.merger.merger_triplets_generator import MergerStrategiesProvider
from source.query.ml_phase.task import ScoreAssigningTaskGenerator
from source.storage.stores.artifact_store.types import QueryMetadataSummary, ScoreAssignerExternalJobArtifact
from source.storage.stores.general_quest_data_store.types import QueryMetadata
from source.storage.stores.results_store.interface import ResultsStoreInterface
from source.storage.stores.split_kernel_store.interface import SplitKernelStoreInterface
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.score_assigning_dispatch_task import ScoreAssigningDispatchTask


class ScoreAssigningDispatchHandler(TaskHandlerInterface):
    def __init__(self, artifact_store, score_assigner_task_generator, quest_general_store,
                 task_submitter, split_kernel_store):
        """
        @type split_kernel_store: L{SplitKernelStoreInterface}
        @type score_assigner_task_generator: L{ScoreAssigningTaskGenerator}
        @type quest_general_store: L{GeneralQuestDataStoreInterface}
        """
        self.__split_kernel_store = split_kernel_store
        self.__task_submitter = task_submitter
        self.__quest_general_store = quest_general_store
        self.__score_assigner_task_generator = score_assigner_task_generator
        self.__artifact_store = artifact_store

    @staticmethod
    def get_task_type():
        return ScoreAssigningDispatchTask

    def handle_task(self, dispatch_task):
        """
        @type dispatch_task: L{ScoreAssigningDispatchTask}
        """
        query_execution_unit = dispatch_task.query_execution_unit

        feature_flags = dispatch_task.feature_flags
        feature_flags["useOldScoresStore"] = feature_flags.get("useOldScoresStore", True)

        num_sub_kernels = len(
                self.__split_kernel_store.get_sub_kernels_list(dispatch_task.customer, dispatch_task.quest_id,
                                                               query_execution_unit.query_id))

        query_summary = QueryMetadataSummary(dispatch_task.customer, dispatch_task.quest_id,
                                             query_execution_unit.query_id, query_execution_unit.sphere_id,
                                             query_execution_unit.query_id, query_execution_unit.timestamp,
                                             query_execution_unit.query_id, dispatch_task.is_past)

        self.__artifact_store.store_artifact(query_summary)

        generated_tasks = self.__score_assigner_task_generator.get_tasks(dispatch_task.job_id, dispatch_task.customer,
                                                                         dispatch_task.quest_id,
                                                                         query_execution_unit.query_id,
                                                                         num_sub_kernels,
                                                                         query_execution_unit.sphere_id,
                                                                         dispatch_task.ml_conf,
                                                                         dispatch_task.jobnik_session,
                                                                         query_execution_unit.seed,
                                                                         feature_flags, 1, dispatch_task.is_past)

        total_num_tasks = len(generated_tasks) + 1  # Add one task for the driver

        for score_assigning_task in generated_tasks:
            score_assigning_task.total_num_tasks = total_num_tasks

        # We need to specify how many tasks remain for when our caller reports progress
        dispatch_task.total_num_tasks = total_num_tasks

        self.__task_submitter.submit_tasks(dispatch_task.job_id, 'assign_scores', generated_tasks)

        query_metadata = QueryMetadata(dispatch_task.customer, dispatch_task.quest_id,
                                       query_execution_unit.query_id, query_execution_unit.query_id,
                                       query_execution_unit.sphere_id, query_execution_unit.timestamp)
        self.__quest_general_store.store_query_metadata(query_metadata)

        origin_index = self.__create_origin_index(generated_tasks)
        self.__quest_general_store.store_scorers_origin_index(dispatch_task.customer, dispatch_task.quest_id,
                                                              query_execution_unit.query_id, origin_index)
        # We remove this temporarily since it's too large for SQS and we don't currently use it as an artifact.
        # Restore it if needed, but with the understanding that SQS will not be able to send it through.
        # self.__artifact_store \
        #     .store_artifact(OriginIndexJobArtifact(customer, quest_id, query_id, 'scorers', origin_index))

        job_artifact = ScoreAssignerExternalJobArtifact(dispatch_task.customer, dispatch_task.quest_id,
                                                        query_execution_unit.query_id,
                                                        dispatch_task.job_id,
                                                        len(generated_tasks))
        self.__artifact_store.store_artifact(job_artifact)

    @staticmethod
    def __create_origin_index(tasks):
        """
        @type tasks: C{list} of C{ScoreAssigningTask}
        @rtype C{dict}
        """
        origin_index = {}

        for task in tasks:
            scorer_ids = MergerStrategiesProvider.get_all_scorer_names(task.task_config)

            for scorer_id in scorer_ids:
                origin_id = ResultsStoreInterface.create_scorer_origin_id(task.sub_kernel_id, scorer_id)
                origin_index[origin_id] = {
                    'sub_kernel_ordinal': task.sub_kernel_id,
                    'scorer_id': scorer_id,
                }

        return origin_index
