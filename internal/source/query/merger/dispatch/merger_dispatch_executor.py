import uuid

from source.query.merger.task import MergerTasksGenerator
from source.query.scores_service.domain import MergerKey
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types.quest.evaluation import OriginIndexJobArtifact
from source.storage.stores.results_store.interface import ResultsStoreInterface
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.submitter_interface import TaskSubmitterInterface
from source.task_runner.tasks.dispatch_mergers_task import DispatchMergersTask


class MergerDispatchExecutor(TaskHandlerInterface):
    def __init__(self, scores_merger_task_generator, task_submitter, artifact_store, quest_general_store):
        """
        @type scores_merger_task_generator: L{MergerTasksGenerator}
        @type task_submitter: L{TaskSubmitterInterface}
        @type artifact_store: L{ArtifactStoreInterface}
        @type quest_general_store: L{GeneralQuestDataStoreInterface}
        """
        self.__artifact_store = artifact_store
        self.__task_submitter = task_submitter
        self.__scores_merger_task_generator = scores_merger_task_generator
        self.__quest_general_store = quest_general_store

    def handle_task(self, dispatch_task):
        """
        @type dispatch_task: C{DispatchMergersTask}
        """
        if dispatch_task.jobnik_session is None:
            job_id = uuid.uuid4().get_hex()
        else:
            job_id = dispatch_task.jobnik_session.job_token['jobId']

        generated_tasks = self.__scores_merger_task_generator.get_tasks(job_id, dispatch_task.customer,
                                                                        dispatch_task.quest_id,
                                                                        dispatch_task.query_id, dispatch_task.ml_conf,
                                                                        dispatch_task.merger_conf,
                                                                        dispatch_task.is_past,
                                                                        dispatch_task.jobnik_session,
                                                                        dispatch_task.query_execution_unit.seed,
                                                                        dispatch_task.feature_flags)

        total_num_tasks = len(generated_tasks) + 1  # Add one task for us

        for generated_task in generated_tasks:
            generated_task.total_num_tasks = total_num_tasks

        # We need to specify how many tasks remain for when our caller reports progress
        dispatch_task.total_num_tasks = total_num_tasks

        self.__task_submitter.submit_tasks(dispatch_task.job_id, 'merge_scores', generated_tasks)

        origin_index = self.__create_origin_index(generated_tasks)
        self.__quest_general_store.store_mergers_origin_index(dispatch_task.customer, dispatch_task.quest_id, dispatch_task.query_id, origin_index)
        self.__artifact_store.store_artifact(
            OriginIndexJobArtifact(dispatch_task.customer, dispatch_task.quest_id, dispatch_task.query_id, 'mergers', origin_index))

    @staticmethod
    def get_task_type():
        return DispatchMergersTask

    @staticmethod
    def __create_origin_index(tasks):
        """
        @type tasks: C{list} of C{MergerTask}
        @rtype C{dict}
        """
        origin_index = {}

        for task in tasks:
            task_config = task.task_config
            merger_key = MergerKey(task_config['merger_model'], task_config['variant'], task_config['scorer_id'])
            origin_id = ResultsStoreInterface.create_merger_origin_id(merger_key)
            origin_index[origin_id] = {
                'scorer_id': task_config['scorer_id'],
                'merger_model': task_config['merger_model'],
                'variant': task_config['variant'],
            }

        return origin_index
