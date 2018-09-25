from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact

__all__ = [
    'ModelSelectionSummaryArtifact'
]


class ModelSelectionSummaryArtifact(QuestBaseArtifact):
    type = "model_selection_results"

    def __init__(self, customer, quest_id, query_id, query_role, best_merger_id, best_merger_name, best_scorer_name,
                 best_model_params, best_model_loss, selected_thresholds, models_losses):
        super(ModelSelectionSummaryArtifact, self).__init__(customer, quest_id)
        self.__models_losses = models_losses
        self.__selected_thresholds = selected_thresholds
        self.__best_model_loss = best_model_loss
        self.__best_model_params = best_model_params
        self.__best_scorer_name = best_scorer_name
        self.__best_merger_name = best_merger_name
        self.__best_merger_id = best_merger_id
        self.__query_role = query_role
        self.__query_id = query_id

    def _to_dict(self):
        return {
            "query_id": self.__query_id,
            "query_role": self.__query_role,
            "chosen_merger_id": self.__best_merger_id,
            "chosen_merger_name": self.__best_merger_name,
            "chosen_scorer_name": self.__best_scorer_name,
            "chosen_merger_params": self.__best_model_params,
            "chosen_model_loss": self.__best_model_loss,
            "selected_thresholds": self.__selected_thresholds,
            "models_losses": self.__models_losses
        }
