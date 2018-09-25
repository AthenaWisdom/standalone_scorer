from datetime import datetime
from source.query.scores_service.domain import MergerKey

from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact


__all__ = [
    'QueryMetadataSummary',
    'QuestConfigurationArtifact',
    'MergerSummaryArtifact'
]


class QueryMetadataSummary(QuestBaseArtifact):
    type = 'quest_query_summary'

    def __init__(self, customer, quest_id, query_id, sphere_id, kernel_id, kernel_timestamp,
                 split_kernel_id, is_past):
        """
        @type split_kernel_id: C{str}
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type sphere_id: C{str}
        @type kernel_id: C{str}
        @type kernel_timestamp: C{datetime}
        @type is_past: C{bool}
        """
        super(QueryMetadataSummary, self).__init__(customer, quest_id)
        self.__split_kernel_id = split_kernel_id
        self.__quest_id = quest_id
        self.__query_id = query_id
        self.__sphere_id = sphere_id
        self.__kernel_id = kernel_id
        self.__kernel_timestamp = kernel_timestamp
        self.__is_past = is_past

    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'query_id': self.__query_id,
            'sphere_id': self.__sphere_id,
            'kernel_id': self.__kernel_id,
            'split_kernel_id': self.__split_kernel_id,
            'kernel_timestamp': self.__kernel_timestamp,
            'is_past': self.__is_past,
        }


class QuestConfigurationArtifact(QuestBaseArtifact):
    type = 'quest_runner_configuration'

    def __init__(self, customer, quest_id, present_data_unit, past_data_units, quest_config):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type present_data_unit: C{dict}
        @type past_data_units: C{list}
        @type quest_config: C{dict}
        """
        super(QuestConfigurationArtifact, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__present_data_unit = present_data_unit
        self.__past_data_units = past_data_units
        self.__quest_config = quest_config

    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'present_data_unit': self.__present_data_unit,
            'past_data_units': self.__past_data_units,
            'quest_config': self.__quest_config,
        }


class MergerSummaryArtifact(QuestBaseArtifact):
    type = 'merger_summary'

    def __init__(self, customer, quest_id, merger_key):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type merger_key: L{MergerKey}

        """
        super(MergerSummaryArtifact, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__merger_key = merger_key


    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'merger_id': str(self.__merger_key),
            'merger_model': self.__merger_key.model_name,
            'scorer_id': self.__merger_key.scorer_name,
            'variant': self.__merger_key.model_params,
        }


class ProbabilityGraphRepresentationArtifact(QuestBaseArtifact):
    type = 'probability_graph_representation'

    def __init__(self, customer, quest_id, graph_representation, num_scored, baselines):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type graph_representation: C{dict}
        @type num_scored: C{int}
        """
        super(ProbabilityGraphRepresentationArtifact, self).__init__(customer, quest_id)
        self.__num_scored = num_scored
        self.__graph_representation = graph_representation
        self.__quest_id = quest_id
        self.__baselines = baselines

    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'graph_representation': self.__graph_representation,
            'num_scored': self.__num_scored,
            'baselines': self.__baselines
        }
