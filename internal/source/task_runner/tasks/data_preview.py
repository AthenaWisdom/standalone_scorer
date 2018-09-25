from source.task_runner.tasks.interface import TaskInterface


class DataPreviewTask(TaskInterface):
    items = TaskInterface.items + (
        'preview_id',
    )

    def __init__(self, job_id, customer, execution_id, task_ordinal, total_num_tasks, jobnik_session,
                 feature_flags):
        super(DataPreviewTask, self).__init__(job_id, customer, execution_id, task_ordinal, total_num_tasks,
                                              jobnik_session, feature_flags=feature_flags)
        self.__preview_id = execution_id

    def get_context(self):
        return {
            'customer': self.customer,
            'preview_id': self.__preview_id,
            'task_ordinal': self.task_ordinal,
            'operation': 'data_preview',
        }

    @property
    def preview_id(self):
        return self.__preview_id
