import functools

from source.jobnik_communicator.interface import JobnikSession
from source.storage.stores.split_kernel_store.interface import SplitKernelStoreInterface
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata
from source.task_runner.tasks.merger_task import MergerTask
from source.utils.random_seed_provider import RandomSeedProvider


class MergerTasksGenerator(object):
    def __init__(self, general_quest_store, split_kernel_store, merging_tasks_generator):
        """
        @type merging_tasks_generator: L{MergerStrategiesProvider}
        @type split_kernel_store: L{SplitKernelStoreInterface}
        @type general_quest_store: L{GeneralQuestDataStoreInterface}
        """
        self.__general_quest_data_store = general_quest_store
        self.__merging_tasks_generator = merging_tasks_generator
        self.__split_kernel_store = split_kernel_store

    def get_tasks(self, job_id, customer_id, quest_id, query_id, ml_config, merger_config,
                  is_past, jobnik_session, initial_seed, feature_flags):
        """
        @type job_id: C{str}
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type ml_config: C{dict}
        @type merger_config: C{dict}
        @type is_past: C{bool}
        @type jobnik_session: L{JobnikSession}
        @type initial_seed: C{int}
        @type feature_flags: C{dict}
        @rtype: C{list} of L{source.task_runner.tasks.merger_task.MergerTask}
        """
        query_metadata = self.__general_quest_data_store.load_query_metadata(customer_id, quest_id, query_id)
        sub_kernel_ids = range(len(self.__split_kernel_store.get_sub_kernels_list(customer_id, quest_id, query_id)))
        merge_tasks_confs = list(self.__merging_tasks_generator.get_merging_tasks(ml_config, merger_config))
        partial_merger_task = functools.partial(MergerTask, job_id=job_id, customer=customer_id, quest_id=quest_id,
                                                query_id=query_id, split_kernel_id=query_metadata.split_kernel_id,
                                                is_past=is_past, sub_kernel_ids=sub_kernel_ids,
                                                total_num_tasks=len(merge_tasks_confs), jobnik_session=jobnik_session,
                                                feature_flags=feature_flags)
        return [partial_merger_task(task_ordinal=task_ordinal + 1, task_config=task_config,
                                    task_seed=RandomSeedProvider.generate_contextual_seed(initial_seed, 'merger',
                                                                                          task_ordinal + 1))
                for task_ordinal, task_config in enumerate(merge_tasks_confs)]
