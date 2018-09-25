from source.storage.stores.general_quest_data_store.types import QueryExecutionUnit
from source.task_runner.tasks.interface import TaskInterface


class DispatchMergersTask(TaskInterface):
    items = TaskInterface.items + (
        'quest_id',
        'query_execution_unit_dict',
        'ml_conf',
        'merger_conf',
        'is_past',
    )

    def __init__(self, job_id, customer, quest_id, query_execution_unit_dict, ml_conf, merger_conf, is_past,
                 jobnik_session, feature_flags):
        """
        @type ml_conf: C{dict}
        @type merger_conf: C{dict}
        @type query_execution_unit_dict: C{dict}
        @type feature_flags: C{dict}
        @type jobnik_session: L{JobnikSession}
        @type customer: C{str}
        @type quest_id: C{str}
        @type job_id: C{str}
        @type is_past: C{bool}
        """
        super(DispatchMergersTask, self).__init__(job_id, customer, quest_id, 0, 1, jobnik_session,
                                                  feature_flags=feature_flags)
        self.__is_past = is_past
        self.__ml_conf = ml_conf
        self.__merger_conf = merger_conf
        self.__query_execution_unit_dict = query_execution_unit_dict
        self.__quest_id = quest_id

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def query_id(self):
        return self.query_execution_unit.query_id

    @property
    def ml_conf(self):
        return self.__ml_conf

    @property
    def merger_conf(self):
        return self.__merger_conf

    @property
    def query_execution_unit(self):
        return QueryExecutionUnit.from_dict(self.query_execution_unit_dict)

    @property
    def query_execution_unit_dict(self):
        return self.__query_execution_unit_dict

    @property
    def is_past(self):
        return self.__is_past

    def get_context(self):
        return {
            'customer': self.customer,
            'quest_id': self.__quest_id,
            'query_id': self.query_id,
            'task_ordinal': self.task_ordinal,
            'is_past': self.__is_past,
            'operation': 'dispatch_mergers',
        }
