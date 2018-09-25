from pandas import DataFrame


class ScoresMetadata(object):
    def __init__(self, customer, quest_id, query_id, sub_kernel_ordinal, scorer_name):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type sub_kernel_ordinal: C{int}
        @type scorer_name: C{str}
        """
        self.__sub_kernel_ordinal = sub_kernel_ordinal
        self.__query_id = query_id
        self.__quest_id = quest_id
        self.__customer = customer
        self.__scorer_name = scorer_name

    @property
    def scorer_name(self):
        return self.__scorer_name

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def customer(self):
        return self.__customer

    @property
    def query_id(self):
        return self.__query_id

    @property
    def sub_kernel_ordinal(self):
        return self.__sub_kernel_ordinal


class PerformanceMetadata(object):
    def __init__(self, scorer_metadata, performance_type):
        """
        @type scorer_metadata: L{ScoresMetadata}
        @type performance_type: C{str}
        """
        self.__performance_type = performance_type
        self.__scorer_metadata = scorer_metadata

    @property
    def performance_type(self):
        return self.__performance_type

    @property
    def scorer_metadata(self):
        return self.__scorer_metadata


class ScorerPerformance(object):
    def __init__(self, df, performance_metadata):
        """
        @type df: C{DataFrame}
        @type performance_metadata: L{PerformanceMetadata}
        """
        self.__performance_metadata = performance_metadata
        self.__df = df

    @property
    def df(self):
        return self.__df

    @property
    def metadata(self):
        return self.__performance_metadata


class Scores(object):
    def __init__(self, all_scores_df, suspects_df, metadata):
        """
        @type suspects_df: C{DataFrame}
        @type all_scores_df: C{DataFrame}
        @type metadata: L{ScoresMetadata}
        """
        self.__suspects_df = suspects_df
        self.__metadata = metadata
        self.__all_scores_df = all_scores_df

    @property
    def all_scores(self):
        return self.__all_scores_df

    @property
    def metadata(self):
        return self.__metadata

    @property
    def suspects_scores(self):
        return self.__suspects_df
