from pandas import DataFrame


class MergerMetadata(object):
    def __init__(self, merger_id, merger_name=None, merger_params=None, scorer_name=None):
        """

        @param merger_id: C{str}
        @param merger_name: C{str} Allowed None when unknown
        @param merger_params: C{str} Allowed None when unknown
        @param scorer_name: C{str} Allowed None when unknown
        """
        self.__merger_id = merger_id
        self.__merger_name = merger_name
        self.__merger_params = merger_params
        self.__scorer_name = scorer_name

    @property
    def merger_id(self):
        return self.__merger_id

    @property
    def merger_name(self):
        return self.__merger_name

    @property
    def merger_params(self):
        return self.__merger_params

    @property
    def scorer_name(self):
        return self.__scorer_name

    def get_bad_merger_name(self):
        params_str = '_'.join("%s=%s" % (key, val) for (key, val) in self.__merger_params.iteritems())
        full_merger_name = "_".join(["merged", self.__scorer_name, "by", self.__merger_name, params_str])
        return full_merger_name

    def to_dict(self):
        return {
            'merger_id': self.__merger_id,
            'merger_name': self.__merger_name,
            'merger_params': self.__merger_params,
            'scorer_name': self.__scorer_name,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class MergedScoresMetadata(object):
    def __init__(self, merger_metadata, customer, quest_id, query_id):
        """
        @type merger_metadata: L{MergerMetadata}
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        """

        self.__merger_metadata = merger_metadata
        self.__customer = customer
        self.__quest_id = quest_id
        self.__query_id = query_id

    @property
    def merger_metadata(self):
        return self.__merger_metadata

    @property
    def customer(self):
        return self.__customer

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def query_id(self):
        return self.__query_id


class MergedPerformanceMetadata(object):
    def __init__(self, merged_scores_metadata, performance_type):
        """
        @type merged_scores_metadata: L{MergedScoresMetadata}
        @type performance_type: C{str}
        """
        self.__performance_type = performance_type
        self.__merged_scores_metadata = merged_scores_metadata

    @property
    def performance_type(self):
        return self.__performance_type

    @property
    def merged_scores_metadata(self):
        return self.__merged_scores_metadata


class MergerModel(object):
    def __init__(self, merged_scores_metadata, model):
        """
        @type merged_scores_metadata: L{MergedScoresMetadata}
        @type model:
        """
        self.__model = model
        self.__merged_scores_metadata = merged_scores_metadata

    @property
    def metadata(self):
        return self.__merged_scores_metadata

    @property
    def model(self):
        return self.__model


class MergerPerformance(object):
    def __init__(self, df, merged_performance_metadata):
        """
        @type df: C{DataFrame}
        @type merged_performance_metadata: L{MergedPerformanceMetadata}
        """
        self.__performance_metadata = merged_performance_metadata
        self.__df = df

    @property
    def df(self):
        return self.__df

    @property
    def metadata(self):
        return self.__performance_metadata


class MergedScores(object):
    def __init__(self, all_merged_scores_df, merged_suspects_df, metadata):
        """
        @type merged_suspects_df: C{DataFrame}
        @type all_merged_scores_df: C{DataFrame}
        @type metadata: L{MergedScoresMetadata}
        """
        self.__merged_suspects_df = merged_suspects_df
        self.__metadata = metadata
        self.__all_merged_scores_df = all_merged_scores_df

    @property
    def all_merged_scores(self):
        return self.__all_merged_scores_df

    @property
    def metadata(self):
        return self.__metadata

    @property
    def merged_suspects_scores(self):
        return self.__merged_suspects_df
