from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact

__author__ = 'user'


class ValidationPerformanceSummary(QuestBaseArtifact):
    type = 'validation_results_summary'
    module = "validation_results"

    def __init__(self, customer, quest_id, execution_id, query_id, merger_id, scorer_name, stats_type,
                 ground, whites, universe, auc, hit_rates, recalls, lifts):
        super(ValidationPerformanceSummary, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__execution_id = execution_id
        self.__query_id = query_id
        self.__merger_id = merger_id
        self.__scorer_name = scorer_name
        self.__stats_type = stats_type

        self.__ground = frozenset(ground)
        self.__whites = frozenset(whites)
        self.__universe = frozenset(universe)
        self.__candidates = self.__universe - self.__whites
        self.__real_ground = self.__candidates & self.__ground

        self.__auc = auc
        self.__hit_rates = hit_rates
        self.__recalls = recalls
        self.__lifts = lifts

    def _to_dict(self):
        res = {
            'module_name': self.module,
            'quest_id': self.__quest_id,
            'execution_id': self.__execution_id,
            'query_id': self.__query_id,
            'merger_id': self.__merger_id,
            'scorer_name': self.__scorer_name,
            'stats_type': self.__stats_type,
            'ground_size': len(self.__ground),
            'whites_size': len(self.__whites),
            'universe_size': len(self.__universe),
            'candidates_size': len(self.__candidates),
            'real_ground_size': len(self.__real_ground),
            'auc': self.__auc,
            'hit_rates': self.__hit_rates,
            'recalls': self.__recalls,
            'lifts': self.__lifts,
        }
        return res