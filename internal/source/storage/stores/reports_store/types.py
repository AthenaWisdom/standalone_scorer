from pandas import DataFrame
from source.query.scores_service.domain import MergerKey


class Report(object):
    def __init__(self, customer, quest_id, report_df, chosen_merger_key):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type report_df: C{DataFrame}
        @type chosen_merger_key: L{MergerKey}
        """
        self.__chosen_merger_key = chosen_merger_key
        self.__report_df = report_df
        self.__quest_id = quest_id
        self.__customer = customer

    @property
    def customer(self):
        return self.__customer

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def df(self):
        return self.__report_df

    @property
    def chosen_merger(self):
        return self.__chosen_merger_key


class SignificantSegmentsReport(object):
    def __init__(self, customer, quest_id, role, segmentation_execution_id, segmentation_df):
        self.__segmentation_df = segmentation_df
        self.__segmentation_execution_id = segmentation_execution_id
        self.__role = role
        self.__quest_id = quest_id
        self.__customer = customer

    @property
    def customer(self):
        return self.__customer

    @property
    def segmentation_df(self):
        return self.__segmentation_df

    @property
    def segmentation_execution_id(self):
        return self.__segmentation_execution_id

    @property
    def role(self):
        return self.__role

    @property
    def quest_id(self):
        return self.__quest_id
