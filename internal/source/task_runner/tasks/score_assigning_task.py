from source.task_runner.tasks.interface import TaskInterface


class ScoreAssigningTask(TaskInterface):
    items = TaskInterface.items + (
        'quest_id',
        'query_id',
        'split_kernel_id',
        'is_past',
        'sphere_id',
        'task_config',
        'sub_kernel_id',
    )
    max_attempts = 5

    def __init__(self, job_id, customer, quest_id, query_id, split_kernel_id, sphere_id, is_past,
                 task_config, sub_kernel_id, task_ordinal, total_num_tasks, task_seed, jobnik_session, feature_flags):
        """
        @type feature_flags: C{dict}
        @type total_num_tasks: C{int}
        @type jobnik_session: L{JobnikSession}
        @type task_config: C{dict}
        @type customer: C{str}
        @type quest_id: C{str}
        @type job_id: C{str}
        @type query_id: C{str}
        @type split_kernel_id: C{str}
        @type sub_kernel_id: C{int}
        @type sphere_id: C{str}
        @type is_past: C{bool}
        @type task_ordinal: C{int}
        """
        super(ScoreAssigningTask, self).__init__(job_id, customer, quest_id, task_ordinal,
                                                 total_num_tasks, jobnik_session, task_seed, feature_flags)
        self.__task_config = task_config
        self.__is_past = is_past
        self.__sphere_id = sphere_id
        self.__split_kernel_id = split_kernel_id
        self.__query_id = query_id
        self.__quest_id = quest_id
        self.__sub_kernel_id = sub_kernel_id

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def query_id(self):
        return self.__query_id

    @property
    def split_kernel_id(self):
        return self.__split_kernel_id

    @property
    def sphere_id(self):
        return self.__sphere_id

    @property
    def is_past(self):
        return self.__is_past

    @property
    def task_config(self):
        return self.__task_config

    @property
    def sub_kernel_id(self):
        return self.__sub_kernel_id

    def get_context(self):
        return {
            'customer': self.customer,
            'quest_id': self.__quest_id,
            'query_id': self.__query_id,
            'sub_kernel_id': self.sub_kernel_id,
            'is_past': self.__is_past,
            'operation': 'assign_scores',
        }
