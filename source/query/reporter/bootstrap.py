from source.query.reporter.additional_reports_generator import KernelReader, \
    AdditionalReportsGeneratorSelector, PastBasedReportsGenerator, NoPastReportsGenerator
from source.query.merger.merger_triplets_generator import MergerStrategiesProvider
from source.query.model_selection.model_loss_calculator import ModelsLossCalculator
from source.query.model_selection.model_selector import ModelSelector
from source.query.probability.report_probability_calculator import ReportProbabilityCalculator
from source.query.probability.report_probability_interpolation import ReportProbabilityInterpolation
from source.query.reporter.report_generation_task_handler import ReportGenerationTaskHandler
from source.query.scores_service.scores_service import ScoresService
from source.storage.stores.evaluation_results_store.csv_store import CsvEvaluationResultsStore
from source.storage.stores.general_quest_data_store.json_store import JSONGeneralQuestDataStore
from source.storage.stores.merged_scores_store.csv_store import CSVMergedScoresStore
from source.storage.stores.reporter_hash_dictionary_store.csv import CsvReporterHashDictionaryStore
from source.storage.stores.reports_store.csv import CSVReportsStore
from source.storage.stores.results_store.csv_store import CsvResultsStore
from source.storage.stores.scores_store.csv_store import CSVScoresStore
from source.storage.stores.split_kernel_store.csv_store import CSVSplitKernelStore


def prod_bootstrap(io_handler, artifact_store):
    # noinspection PyTypeChecker
    split_kernel_store = CSVSplitKernelStore(io_handler, None)

    general_quest_data_store = JSONGeneralQuestDataStore(io_handler)

    scores_store = CSVScoresStore(io_handler)
    merged_scores_store = CSVMergedScoresStore(io_handler)
    results_store = CsvResultsStore(io_handler)
    new_performance_store = CsvEvaluationResultsStore(io_handler, general_quest_data_store)
    scores_service = ScoresService(scores_store, merged_scores_store, results_store, new_performance_store)
    merger_strategies_provider = MergerStrategiesProvider()

    model_selector = ModelSelector(scores_service, artifact_store, general_quest_data_store,
                                   split_kernel_store, ModelsLossCalculator(),
                                   merger_strategies_provider)

    reports_store = CSVReportsStore(io_handler)

    report_probability_calculator = ReportProbabilityCalculator(scores_service,
                                                                ReportProbabilityInterpolation())

    reporter_hash_dictionary_store = CsvReporterHashDictionaryStore(io_handler)
    past_based_reports_gen = PastBasedReportsGenerator(KernelReader(io_handler), scores_service, reports_store)
    no_past_based_reports_gen = NoPastReportsGenerator(KernelReader(io_handler), scores_service, reports_store)
    additional_reports_generator_selector = AdditionalReportsGeneratorSelector(past_based_reports_gen, no_past_based_reports_gen)

    return ReportGenerationTaskHandler(general_quest_data_store, model_selector, scores_service,
                                       artifact_store, reports_store, report_probability_calculator,
                                       reporter_hash_dictionary_store, additional_reports_generator_selector)
