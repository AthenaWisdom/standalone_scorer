from source.task_runner.tasks.interface import TaskInterface

FEATURE_FLAG_MEMOIZE_BLOCKS_KEY = 'memoize_blocks'


class ClustersExtractionTask(TaskInterface):
    items = TaskInterface.items + (
        'sphere_id',
        'connecting_field',
        'input_csv_name',
    )

    def __init__(self, job_id, customer, sphere_id, connecting_field, input_csv_name, task_ordinal,
                 total_num_tasks, jobnik_session, task_seed, feature_flags=None):
        """


        @type jobnik_session: L{JobnikSession}
        @type total_num_tasks: C{int}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type connecting_field: C{str}
        @type input_csv_name: C{str}
        @type task_ordinal: C{int}
        @type feature_flags: C{dict}
        """
        super(ClustersExtractionTask, self).__init__(job_id, customer, sphere_id, task_ordinal, total_num_tasks,
                                                     jobnik_session, task_seed, feature_flags=feature_flags)
        self.__sphere_id = sphere_id
        self.__connecting_field = connecting_field
        self.__input_csv_name = input_csv_name

    @property
    def sphere_id(self):
        return self.__sphere_id

    @property
    def connecting_field(self):
        return self.__connecting_field

    @property
    def input_csv_name(self):
        return self.__input_csv_name

    def get_context(self):
        return {
            'customer': self.customer,
            'sphere_id': self.__sphere_id,
            'task_ordinal': self.task_ordinal,
            'connecting_field': self.__connecting_field,
            'input_csv_name': self.__input_csv_name,
            'operation': 'extract_clusters',
        }
