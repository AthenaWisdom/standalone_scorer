from enum import Enum

from source.task_runner.tasks.interface import TaskInterface


class StatsSources(Enum):
    DATA_STATS = 0
    DATA_PREVIEW = 1


class InputStats(object):
    def __init__(self, source, stats_id):
        """
        @type source: L{StatsSources}
        @type stats_id: C{str}
        """
        self.__stats_id = stats_id
        self.__source = source

    @property
    def source(self):
        return self.__source

    @property
    def id(self):
        return self.__stats_id


class ClustersExtractionTasksDispatchTask(TaskInterface):
    items = TaskInterface.items + (
        'basic_config',
        'fde_conf',
        'schema',
        'data_preview_id',
    )

    def __init__(self, job_id, customer, sphere_id, jobnik_session, basic_config, fde_conf, schema, data_preview_id,
                 feature_flags):
        """
        @type job_id: C{str}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type jobnik_session: L{JobnikSession}
        @type basic_config: C{dict}
        @type fde_conf: C{dict}
        @type schema: C{list} of C{dict}
        @type data_preview_id: C{str}
        @type feature_flags: C{dict}
        """
        super(ClustersExtractionTasksDispatchTask, self).__init__(job_id, customer, sphere_id, 0, 1, jobnik_session,
                                                                  feature_flags=feature_flags)
        self.__fde_conf = fde_conf
        self.__basic_config = basic_config
        self.__schema = schema
        self.__data_preview_id = data_preview_id

    @property
    def fde_conf(self):
        return self.__fde_conf

    @property
    def basic_config(self):
        return self.__basic_config

    @property
    def schema(self):
        return self.__schema

    @property
    def data_preview_id(self):
        return self.__data_preview_id

    @property
    def input_stats(self):
        return InputStats(StatsSources.DATA_PREVIEW, self.__data_preview_id)

    def get_context(self):
        return {
            'customer': self.customer,
            'sphere_id': self.execution_id,
            'task_ordinal': self.task_ordinal,
            'operation': 'clusters_extraction_tasks_dispatch',
        }
