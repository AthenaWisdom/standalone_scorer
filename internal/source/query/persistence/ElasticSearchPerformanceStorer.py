from source.storage.stores.artifact_store.types import ValidScorerPerformanceSummary
from source.storage.stores.artifact_store.types import InvalidScorerPerformanceSummary


class ElasticSearchPerformanceStorer(object):
    def __init__(self, results_analyzer, artifact_store):
        """
        @type results_analyzer: L{ResultsAnalyzer}
        @type artifact_store: L{ArtifactStoreInterface}
        """
        self.__results_analyzer = results_analyzer
        self.__artifact_store = artifact_store

    def __build_es_results_entity(self, customer, quest_id, query_id, sub_kernel_id, is_past,
                                  scorer_name, scorer_stats, sub_kernel, ground_validation_id):
        try:
            auc, hit_rates, recalls, lifts = self.__results_analyzer.prettify_results_summary_for_es(scorer_stats)
            return ValidScorerPerformanceSummary(customer, quest_id, query_id, sub_kernel_id,
                                                 scorer_name, "phase_5", len(sub_kernel.ground),
                                                 len(sub_kernel.whites), len(sub_kernel.all_ids), is_past, auc,
                                                 hit_rates, recalls, lifts, ground_validation_id)
        except IndexError:
            return InvalidScorerPerformanceSummary(customer, quest_id, query_id, sub_kernel_id,
                                                   scorer_name, "phase_5", len(sub_kernel.ground),
                                                   len(sub_kernel.whites), len(sub_kernel.all_ids), is_past,
                                                   ground_validation_id)

    def store_performance_to_es(self, customer, quest_id, query_id, sub_kernel_id, is_past, stats, sub_kernel,
                                ground_validation_id, logger=None):
        scorer_names = stats['hit_rate'].columns.values
        for scorer_name in scorer_names:
            scorer_stats = {"auc": stats['auc'][scorer_name].to_frame(),
                            "hit_rate": stats['hit_rate'][scorer_name].to_frame(),
                            "lift": stats['lift'][scorer_name].to_frame(),
                            "recall": stats['recall'][scorer_name].to_frame()}
            performance_artifact = self.__build_es_results_entity(
                customer, quest_id, query_id, sub_kernel_id, is_past,
                scorer_name, scorer_stats, sub_kernel, ground_validation_id)
            self.__artifact_store.store_artifact(performance_artifact)
        if logger:
            logger.info(performance_artifact.to_dict())
            logger.info('Published performance artifacts for %d scorers' % len(scorer_names))
