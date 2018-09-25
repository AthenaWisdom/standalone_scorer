from source.storage.stores.artifact_store.types.common import DurationMetricArtifact
from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact

__all__ = [
    'ValidMergerPerformanceSummary',
    'InvalidMergerPerformanceSummary',
    'MergerPricingDurationMetricArtifact',
]


class ValidMergerPerformanceSummary(QuestBaseArtifact):
    type = 'valid_single_merger_results_summary'
    module = "merger"

    def __init__(self, customer, quest_id, query_id, merger_id, scorer_name, stats_type, ground_size, whites_size,
                 universe_size, is_past, auc, hit_rates, recalls, lifts, running_time, baselines):
        super(ValidMergerPerformanceSummary, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__query_id = query_id
        self.__merger_id = merger_id
        self.__scorer_name = scorer_name
        self.__stats_type = stats_type
        self.__ground_size = ground_size
        self.__whites_size = whites_size
        self.__universe_size = universe_size
        self.__is_past = is_past
        self.__auc = auc
        self.__hit_rates = hit_rates
        self.__recalls = recalls
        self.__lifts = lifts
        self.__running_time = running_time
        self.__baselines = baselines

    def _to_dict(self):
        res = {
            'module_name': self.module,
            'quest_id': self.__quest_id,
            'query_id': self.__query_id,
            'merger_id': self.__merger_id,
            'scorer_name': self.__scorer_name,
            'stats_type': self.__stats_type,
            'ground_size': self.__ground_size,
            'whites_size': self.__whites_size,
            'universe_size': self.__universe_size,
            'auc': self.__auc,
            "valid_stats": 1,
            'is_past': self.__is_past,
            'running_time': self.__running_time,
            'hit_rates': self.__hit_rates,
            'recalls': self.__recalls,
            'lifts': self.__lifts,
            'baselines': self.__baselines,
        }
        return res


class InvalidMergerPerformanceSummary(QuestBaseArtifact):
    type = 'invalid_single_merger_results_summary'
    module = "merger"

    def __init__(self, customer, quest_id, query_id, merger_id, scorer_name, stats_type, ground_size, whites_size,
                 universe_size, is_past, running_time, baselines):
        super(InvalidMergerPerformanceSummary, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__query_id = query_id
        self.__merger_id = merger_id
        self.__scorer_name = scorer_name
        self.__stats_type = stats_type
        self.__ground_size = ground_size
        self.__whites_size = whites_size
        self.__universe_size = universe_size
        self.__is_past = is_past
        self.__running_time = running_time
        self.__baselines = baselines

    def _to_dict(self):
        return {'module_name': self.module,
                'quest_id': self.__quest_id,
                "query_id": self.__query_id,
                "merger_id": self.__merger_id,
                'scorer_name': self.__scorer_name,
                "stats_type": self.__stats_type,
                "ground_size": self.__ground_size,
                "whites_size": self.__whites_size,
                "universe_size": self.__universe_size,
                "is_past": self.__is_past,
                "valid_stats": 0,
                "module_name": self.module,
                "running_time": self.__running_time,
                "baselines": self.__baselines
                }


class MergerPricingDurationMetricArtifact(DurationMetricArtifact):
    type = 'merger_pricing_duration_metric'
