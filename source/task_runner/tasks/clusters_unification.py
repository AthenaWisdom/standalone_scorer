from source.task_runner.tasks.interface import TaskInterface


class ClustersUnificationTask(TaskInterface):
    items = TaskInterface.items + (
        'sphere_id',
        'input_spheres',
        'filter_only',
    )

    def __init__(self, job_id, customer, sphere_id, input_spheres, filter_only, task_ordinal, total_num_tasks,
                 jobnik_session):
        """
        @type filter_only: C{list} or None
        @type jobnik_session: L{JobnikSession}
        @type total_num_tasks: C{int}
        @type job_id: C{str}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_spheres: C{list} of C{str}
        @type task_ordinal: C{int}
        """
        super(ClustersUnificationTask, self).__init__(job_id, customer, sphere_id, task_ordinal,
                                                      total_num_tasks, jobnik_session)
        self.__filter_only = filter_only
        self.__sphere_id = sphere_id
        self.__input_spheres = input_spheres

    @property
    def filter_only(self):
        return self.__filter_only

    @property
    def sphere_id(self):
        return self.__sphere_id

    @property
    def input_spheres(self):
        return self.__input_spheres

    def get_context(self):
        return {
            'customer': self.customer,
            'sphere_id': self.__sphere_id,
            'task_ordinal': self.task_ordinal,
            'operation': 'unify_clusters',
        }
