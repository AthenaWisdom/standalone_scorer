from source.query.merger.merger_factory import MergerFactory
from source.query.merger.merger_runner import ScoresMerger
from source.query.ml_phase.bag_worker.unscored_handlers import RandomUnscoredHandler
from source.query.scores_analysis.auc_calculator import AucCalculator
from source.query.scores_analysis.hitrate_calculator import HitRateCalculator
from source.query.scores_analysis.lift_calculator import LiftCalculator
from source.query.scores_analysis.recall_calculator import RecallCalculator
from source.query.scores_analysis.results_analyzer import ResultsAnalyzer
from source.query.scores_service.scores_service import ScoresService
from source.storage.stores.evaluation_results_store.csv_store import CsvEvaluationResultsStore
from source.storage.stores.general_quest_data_store.json_store import JSONGeneralQuestDataStore
from source.storage.stores.merged_scores_store.csv_store import CSVMergedScoresStore
from source.storage.stores.results_store.csv_store import CsvResultsStore
from source.storage.stores.scores_store.csv_store import CSVScoresStore
from source.storage.stores.split_kernel_store.csv_store import CSVSubKernelStore


def build_scores_merger(artifact_store, random_provider, sub_kernel_store,
                        scores_service):

    hit_rate_calculator = HitRateCalculator()
    score_merger = ScoresMerger(MergerFactory(random_provider),
                                scores_service,
                                sub_kernel_store,
                                artifact_store,
                                ResultsAnalyzer(RandomUnscoredHandler(random_provider), hit_rate_calculator,
                                                LiftCalculator(hit_rate_calculator), RecallCalculator(),
                                                AucCalculator()))
    return score_merger


def build_prod_scores_merger(artifact_store, io_handler, random_seed_provider):
    merged_scores_store = CSVMergedScoresStore(io_handler)

    sub_kernel_store = CSVSubKernelStore(io_handler)
    scores_store = CSVScoresStore(io_handler)
    results_store = CsvResultsStore(io_handler)
    general_quest_data_store = JSONGeneralQuestDataStore(io_handler)
    results_evaluation_store = CsvEvaluationResultsStore(io_handler, general_quest_data_store)
    scores_service = ScoresService(scores_store, merged_scores_store, results_store, results_evaluation_store)
    return build_scores_merger(artifact_store, random_seed_provider, sub_kernel_store, scores_service)
