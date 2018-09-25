import pandas as pd

from source.query.model_selection.model_selector import ModelSelector
from source.query.probability.report_probability_calculator import ReportProbabilityCalculator
from source.query.scores_service.domain import MergerKey
from source.query.scores_service.scores_service import ScoresService
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types import MergerSummaryArtifact
from source.storage.stores.artifact_store.types.quest.general import ProbabilityGraphRepresentationArtifact
from source.storage.stores.artifact_store.types.quest.report_preview import ReportPreviewArtifact
from source.storage.stores.general_quest_data_store.interface import GeneralQuestDataStoreInterface
from source.storage.stores.general_quest_data_store.runtime_query_execution_unit import RuntimeQueryExecutionUnit
from source.storage.stores.reporter_hash_dictionary_store.interface import ReporterHashDictionaryStoreInterface
from source.storage.stores.reports_store.interface import ReportsStoreInterface
from source.storage.stores.reports_store.types import Report
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.report_generation_task import ReportGenerationTask
from source.query.reporter.additional_reports_generator import AdditionalReportsGeneratorSelector


class ReportGenerationTaskHandler(TaskHandlerInterface):
    def __init__(self, quest_general_store, merger_model_selector, scores_service,
                 artifact_store, reports_store, report_probability_calculator, reporter_hash_dictionary_store,
                 additional_reports_generator_selector):
        """
        @type quest_general_store: L{GeneralQuestDataStoreInterface}
        @type merger_model_selector: L{ModelSelector}
        @type scores_service: L{ScoresService}
        @type artifact_store: L{ArtifactStoreInterface}
        @type reports_store: L{ReportsStoreInterface}
        @type report_probability_calculator: L{ReportProbabilityCalculator}
        @type reporter_hash_dictionary_store: L{ReporterHashDictionaryStoreInterface}
        @type additional_reports_generator_selector: L{AdditionalReportsGeneratorSelector}
        """
        self.__additional_reports_generator_selector = additional_reports_generator_selector
        self.__report_probability_calculator = report_probability_calculator
        self.__reports_store = reports_store
        self.__artifact_store = artifact_store
        self.__merger_model_selector = merger_model_selector
        self.__quest_general_store = quest_general_store
        self.__reporter_hash_dictionary_store = reporter_hash_dictionary_store
        self.__scores_service = scores_service

    @staticmethod
    def get_task_type():
        return ReportGenerationTask

    def handle_task(self, task):
        # type: (ReportGenerationTask) -> ()
        past_queries_metadata = RuntimeQueryExecutionUnit.build_runtime_data_units(task.past_query_execution_units)
        present_query_metadata = RuntimeQueryExecutionUnit("present", task.present_query_execution_unit.query_id,
                                                           task.present_query_execution_unit,
                                                           task.present_query_execution_unit.query_id)

        # TODO(izik): store runtime query execution units here with the general store.
        self.__quest_general_store \
            .store_runtime_query_execution_units(task.customer, task.quest_id,
                                                 [present_query_metadata] + past_queries_metadata)

        chosen_merger_key = self.__merger_model_selector \
            .retrieve_best_merger_key(task.customer, task.quest_id, past_queries_metadata, present_query_metadata,
                                      task.ml_conf,
                                      task.merger_conf, task.selected_merger, task.hit_rate_thresholds,
                                      task.feature_flags)

        self.__artifact_store.store_artifact(MergerSummaryArtifact(task.customer, task.quest_id, chosen_merger_key))

        unhashed_suspects = self.__get_unhashed_suspects(task.customer, task.quest_id, chosen_merger_key,
                                                         present_query_metadata, task.feature_flags)

        unscored_pop = pd.DataFrame({}, index=unhashed_suspects[unhashed_suspects['score'].isnull()].index)
        self.__reports_store.store_unscored_population(task.customer, task.quest_id, unscored_pop)

        sorted_scored_suspects = self.__get_sorted_scored_suspects(unhashed_suspects)

        if task.feature_flags.get('calculate_probability', False) and len(past_queries_metadata) > 0:
            self.__add_probability_to_sorted_scored_suspects(task.customer, task.quest_id, sorted_scored_suspects,
                                                             chosen_merger_key, past_queries_metadata[0].query_id,
                                                             task.feature_flags)

        report = Report(task.customer, task.quest_id, sorted_scored_suspects, chosen_merger_key)


        has_past = len(task.past_query_execution_units)>0

        reports_bundle = self.__additional_reports_generator_selector.get_additional_reports_generator(has_past).\
            create_and_store_additional_reports(report, task.feature_flags)
        selected_report = reports_bundle[task.selected_report_type]

        self.__reports_store.store_report(selected_report)

        report_preview = selected_report.df.head(30).to_csv(index=True, encoding='utf-8')
        self.__artifact_store.store_artifact(ReportPreviewArtifact(task.customer, task.quest_id, report_preview))

        return 0

    def __get_sorted_scored_suspects(self, unhashed_suspects):
        # type: (pd.DataFrame) -> pd.DataFrame
        unhashed_scored_suspects = unhashed_suspects.dropna(axis=0)
        sorted_scored_suspects = unhashed_scored_suspects.sort('score', ascending=False)
        sorted_scored_suspects['Rank'] = range(1, len(sorted_scored_suspects.index) + 1)
        sorted_scored_suspects = sorted_scored_suspects.drop('score', axis=1)
        # noinspection PyTypeChecker
        return sorted_scored_suspects

    def __add_probability_to_sorted_scored_suspects(self, customer, quest_id, sorted_scored_suspects, chosen_merger_key,
                                                    past_query_id, feature_flags):
        # type: (str, str, pd.DataFrame, MergerKey, str, dict) -> ()
        present_num_scored_candidates = len(sorted_scored_suspects.index)
        probability_vector, compressed_graph_representation, baselines = \
            self.__report_probability_calculator.calculate(chosen_merger_key, customer,
                                                           past_query_id, present_num_scored_candidates,
                                                           quest_id, feature_flags)
        sorted_scored_suspects['Probability'] = probability_vector
        self.__artifact_store.store_artifact(
            ProbabilityGraphRepresentationArtifact(customer, quest_id,
                                                   compressed_graph_representation, present_num_scored_candidates,
                                                   baselines))
        baseline = float(sum(baselines['ground_non_whites'].values())) / sum(baselines['count_non_whites'].values())
        sorted_scored_suspects['Lift'] = sorted_scored_suspects['Probability'] / baseline

    def __get_unhashed_suspects(self, customer, quest_id, chosen_merger_key, present_query_metadata, feature_flags):
        # type: (str, str, MergerKey, RuntimeQueryExecutionUnit, dict) -> pd.DataFrame
        kernel = self.__reporter_hash_dictionary_store.load_dictionary(customer, quest_id,
                                                                       present_query_metadata.query_id)

        suspects_scores = self.__load_suspects(customer, quest_id, present_query_metadata.query_id,
                                               chosen_merger_key, kernel, feature_flags)

        # Keep only the NORMAL_ID (ID is preserved because it's the index)
        hashing_df = kernel[['NORMAL_ID']]
        return self.__unhash_scores(suspects_scores, hashing_df)

    def __load_suspects(self, customer, quest_id, query_id, chosen_merger_key, kernel, feature_flags):
        # type: (str, str, str, MergerKey, pd.DataFrame, dict) -> pd.DataFrame

        scores = self.__scores_service.load_merged_scores(customer, quest_id, query_id, feature_flags,
                                                          chosen_merger_key)

        scores_w_whites = pd.merge(scores, kernel, how='left', left_index=True, right_index=True)
        suspects = pd.DataFrame(scores_w_whites[scores_w_whites['white'] == 0]["score"])

        return suspects

    def __unhash_scores(self, scores, hashing_df):
        # type: (pd.DataFrame, pd.DataFrame) -> pd.DataFrame
        translated_df = pd.merge(scores, hashing_df, how='inner', left_index=True, right_index=True)
        translated_df['ID'] = translated_df['NORMAL_ID']
        translated_df = translated_df \
            .drop(['NORMAL_ID'], axis=1) \
            .set_index(['ID'])

        return translated_df
