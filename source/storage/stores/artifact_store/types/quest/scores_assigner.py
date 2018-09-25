from source.storage.stores.artifact_store.types.common import DurationMetricArtifact
from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact
from source.storage.stores.artifact_store.types.interface import ExternalJobArtifactInterface

__all__ = [
    'DatasetPropertiesArtifact',
    'ValidScorerPerformanceSummary',
    'InvalidScorerPerformanceSummary',
    'ScoreAssignerPricingDurationMetricArtifact',
    'ScoreAssignerExternalJobArtifact',
]


class DatasetPropertiesArtifact(QuestBaseArtifact):
    type = "dataset_properties"
    module = "ml_phase"

    def __init__(self, customer, quest_id, sphere_id, query_id, sub_kernel_id, whites_size, universe_size,
                 clusters_number, population_in_clusters, universe_in_clusters, whites_in_clusters,
                 nnz_in_cluster_to_pop_matrix, is_past):
        super(DatasetPropertiesArtifact, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__sub_kernel_id = sub_kernel_id
        self.__query_id = query_id
        self.__sphere_id = sphere_id

        self.__nnz_in_cluster_to_pop_matrix = nnz_in_cluster_to_pop_matrix
        self.__whites_in_clusters = whites_in_clusters
        self.__universe_in_clusters = universe_in_clusters

        self.__pop_in_clusters = population_in_clusters
        self.__clusters_number = clusters_number
        self.__universe_size = universe_size
        self.__whites_size = whites_size
        self.__is_past = is_past

    def _to_dict(self):
        return {
            "quest_id": self.__quest_id,
            "sphere_id": self.__sphere_id,
            "query_id": self.__query_id,
            "sub_kernel_id": self.__sub_kernel_id,

            "nnz_in_cluster_to_pop_matrix": self.__nnz_in_cluster_to_pop_matrix,
            "whites_in_clusters": self.__whites_in_clusters,
            "universe_in_clusters": self.__universe_in_clusters,

            "whites_size": self.__whites_size,
            "universe_size": self.__universe_size,
            "clusters_number": self.__clusters_number,
            "is_past": self.__is_past,
            "population_in_clusters": self.__pop_in_clusters
        }


class ValidScorerPerformanceSummary(QuestBaseArtifact):
    type = 'valid_single_scorer_results_summary'
    module = "ml_phase"

    def __init__(self, customer, quest_id, query_id, sub_query_id, scorer_name, stats_type, ground_size, whites_size,
                 universe_size, is_past, auc, hit_rates, recalls, lifts, ground_validation_id):
        super(ValidScorerPerformanceSummary, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__query_id = query_id
        self.__sub_query_id = sub_query_id
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
        self.__ground_validation_id = ground_validation_id

    def _to_dict(self):
        dct = {"quest_id": self.__quest_id,
               "query_id": self.__query_id,
               "sub_query_id": self.__sub_query_id,
               "scorer_name": self.__scorer_name,
               "stats_type": self.__stats_type,
               "ground_size": self.__ground_size,
               "whites_size": self.__whites_size,
               "universe_size": self.__universe_size,
               "auc": self.__auc,
               "is_past": self.__is_past,
               "valid_stats": 1,
               "module_name": self.module,
               "hit_rates": self.__hit_rates,
               "recalls": self.__recalls,
               "lifts": self.__lifts,
               "is_ground_validation": bool(self.__ground_validation_id)
               }

        if self.__ground_validation_id:
            dct['ground_validation_id'] = self.__ground_validation_id

        return dct


class InvalidScorerPerformanceSummary(QuestBaseArtifact):
    type = 'invalid_single_scorer_results_summary'
    module = "ml_phase"

    def __init__(self, customer, quest_id, query_id, sub_query_id, scorer_name, stats_type, ground_size, whites_size,
                 universe_size, is_past, ground_validation_id):
        super(InvalidScorerPerformanceSummary, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__query_id = query_id
        self.__sub_query_id = sub_query_id
        self.__scorer_name = scorer_name
        self.__stats_type = stats_type
        self.__ground_size = ground_size
        self.__whites_size = whites_size
        self.__universe_size = universe_size
        self.__is_past = is_past
        self.__ground_validation_id = ground_validation_id

    def _to_dict(self):
        dct = {
            "quest_id": self.__quest_id,
            "query_id": self.__query_id,
            "sub_query_id": self.__sub_query_id,
            "scorer_name": self.__scorer_name,
            "stats_type": self.__stats_type,
            "ground_size": self.__ground_size,
            "whites_size": self.__whites_size,
            "universe_size": self.__universe_size,
            "is_past": self.__is_past,
            "valid_stats": 0,
            "module_name": self.module,
            "is_ground_validation": bool(self.__ground_validation_id)
            }

        if self.__ground_validation_id:
            dct['ground_validation_id'] = self.__ground_validation_id

        return dct


class ScoreAssignerExternalJobArtifact(ExternalJobArtifactInterface, QuestBaseArtifact):
    type = 'score_assigner_external_job'

    def __init__(self, customer, quest_id, query_id, job_id, num_tasks):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type job_id: C{str}
        @type num_tasks: C{int}
        @type stage: C{int}
        @type query_id: C{str}
        """
        super(ScoreAssignerExternalJobArtifact, self).__init__(customer, quest_id, job_id, num_tasks)
        self.__query_id = query_id
        self.__quest_id = quest_id

    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'query_id': self.__query_id,
        }

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def query_id(self):
        return self.__query_id


class ScoreAssignerPricingDurationMetricArtifact(DurationMetricArtifact):
    type = 'score_assigner_pricing_duration_metric'
