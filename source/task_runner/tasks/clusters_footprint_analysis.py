from source.task_runner.tasks.interface import TaskInterface


class ClustersFootprintAnalyzingTask(TaskInterface):
    items = TaskInterface.items + (
        'sphere_id',
        'sphere_id_to_dataset',
    )

    def __init__(self, job_id, customer, execution_id, task_ordinal, total_num_tasks, jobnik_session,
                 feature_flags, sphere_id_to_dataset=None):
        """
        @type customer: C{str}
        @type job_id: C{str}
        @type execution_id: C{str}
        @type sphere_id_to_dataset: C{dict}
        """
        super(ClustersFootprintAnalyzingTask, self).__init__(job_id, customer, execution_id, task_ordinal,
                                                             total_num_tasks, jobnik_session,
                                                             feature_flags=feature_flags)
        self.__sphere_id_to_dataset = sphere_id_to_dataset if sphere_id_to_dataset is not None else {}
        self.__sphere_id = execution_id

    def get_context(self):
        return {
            'customer': self.customer,
            'sphere_id': self.__sphere_id,
            'task_ordinal': self.task_ordinal,
            'operation': 'data_ingestion',
        }

    @property
    def sphere_id(self):
        return self.__sphere_id

    @property
    def sphere_id_to_dataset(self):
        try:
            return self.__sphere_id_to_dataset
        except AttributeError:
            return {}
