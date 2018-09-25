from source.storage.stores.general_quest_data_store.types import QueryExecutionUnit
from source.task_runner.tasks.interface import TaskInterface


class ReportGenerationTask(TaskInterface):
    items = TaskInterface.items + (
        'quest_id',
        'past_query_execution_unit_dicts',
        'present_query_execution_unit_dict',
        'ml_conf',
        'merger_conf',
        'selected_merger',
        'hit_rate_thresholds',
        'selected_report_type'
    )

    def __init__(self, job_id, customer, quest_id, past_query_execution_unit_dicts, present_query_execution_unit_dict,
                 ml_conf, merger_conf, selected_merger, hit_rate_thresholds, jobnik_session,
                 feature_flags,
                 selected_report_type = 'NoWhitesLeaveUnscored'):
        """
        @type job_id: C{str}
        @type customer: C{str}
        @type quest_id: C{str}
        @type past_query_execution_unit_dicts: C{list} of C{dict}
        @type present_query_execution_unit_dict: C{dict}
        @type ml_conf: C{dict}
        @type merger_conf: C{dict}
        @type selected_merger: C{dict}
        @type hit_rate_thresholds: C{list} of C{int}
        @type jobnik_session: L{JobnikSession}
        @type feature_flags: C{dict}
        @type selected_report_type: C{str}
        """
        super(ReportGenerationTask, self).__init__(job_id, customer, quest_id, 0, 1, jobnik_session,
                                                   feature_flags=feature_flags)
        
        self.__quest_id = quest_id
        self.__past_query_execution_unit_dicts = past_query_execution_unit_dicts
        self.__present_query_execution_unit_dict = present_query_execution_unit_dict
        self.__ml_conf = ml_conf
        self.__merger_conf = merger_conf
        self.__selected_merger = selected_merger
        self.__hit_rate_thresholds = hit_rate_thresholds
        self.__selected_report_type = selected_report_type

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def selected_report_type(self):
        return self.__selected_report_type

    @property
    def present_query_execution_unit(self):
        return QueryExecutionUnit.from_dict(self.present_query_execution_unit_dict)

    @property
    def present_query_execution_unit_dict(self):
        return self.__present_query_execution_unit_dict

    @property
    def past_query_execution_units(self):
        return [QueryExecutionUnit.from_dict(qeu_dict) for qeu_dict in self.past_query_execution_unit_dicts]

    @property
    def past_query_execution_unit_dicts(self):
        return self.__past_query_execution_unit_dicts

    @property
    def ml_conf(self):
        return self.__ml_conf

    @property
    def merger_conf(self):
        return self.__merger_conf

    @property
    def selected_merger(self):
        return self.__selected_merger

    @property
    def hit_rate_thresholds(self):
        return self.__hit_rate_thresholds

    def get_context(self):
        return {
            'customer': self.customer,
            'quest_id': self.quest_id,
            'operation': 'report_generation',
        }
