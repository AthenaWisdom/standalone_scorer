from source.query.ml_phase.bag_worker.bag_generator import BagGenerator
from source.query.ml_phase.bag_worker.scorer_generator import ScorerGenerator
from source.query.ml_phase.bag_worker.unscored_handlers import RandomUnscoredHandler
from source.query.ml_phase.ensemble_factory import EnsembleFactory
from source.query.ml_phase.matlab_res_handler import MatlabHandler
from source.query.ml_phase.ml_runner import ScoreAssigner
from source.query.scores_analysis.auc_calculator import AucCalculator
from source.query.scores_analysis.hitrate_calculator import HitRateCalculator
from source.query.scores_analysis.lift_calculator import LiftCalculator
from source.query.scores_analysis.recall_calculator import RecallCalculator
from source.query.scores_analysis.results_analyzer import ResultsAnalyzer
from source.query.scores_service.scores_service import ScoresService
from source.query.persistence.ElasticSearchPerformanceStorer import ElasticSearchPerformanceStorer
from source.storage.stores.clusters_store.filtered_clusters_provider import FilteredClustersProvider
from source.storage.stores.clusters_store.matlab_clusters_store import MatlabClustersStore
from source.storage.stores.evaluation_results_store.csv_store import CsvEvaluationResultsStore
from source.storage.stores.general_quest_data_store.json_store import JSONGeneralQuestDataStore
from source.storage.stores.merged_scores_store.csv_store import CSVMergedScoresStore
from source.storage.stores.results_store.csv_store import CsvResultsStore
from source.storage.stores.scores_store.csv_store import CSVScoresStore
from source.storage.stores.split_kernel_store.csv_store import CSVSubKernelStore
from source.utils.random_seed_provider import ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___


def build_score_assigner(artifact_store, matlab_store, random_provider, scores_service, sub_kernel_store,
                         context_holder):
    scorer_generator = ScorerGenerator(random_provider, context_holder)
    hit_rate_calculator = HitRateCalculator()
    results_analyzer = ResultsAnalyzer(RandomUnscoredHandler(random_provider), hit_rate_calculator,
                                       LiftCalculator(hit_rate_calculator), RecallCalculator(), AucCalculator())
    ensemble_generator = EnsembleFactory(random_provider)
    matlab_handler = MatlabHandler()
    bag_generator = BagGenerator()
    es_performance_storer = ElasticSearchPerformanceStorer(results_analyzer, artifact_store)
    score_assigner = ScoreAssigner(scores_service, sub_kernel_store, matlab_store, matlab_handler,
                                   bag_generator, results_analyzer, scorer_generator, ensemble_generator,
                                   artifact_store, random_provider, es_performance_storer)
    return score_assigner


def build_prod_score_assigner(artifact_store, io_handler, context_holder, random_seed_provider):
    scores_store = CSVScoresStore(io_handler)
    sub_kernel_store = CSVSubKernelStore(io_handler)
    filtered_clusters_provider = FilteredClustersProvider(io_handler, MatlabClustersStore(io_handler))
    results_store = CsvResultsStore(io_handler)
    general_quest_data_store = JSONGeneralQuestDataStore(io_handler)
    merger_store = CSVMergedScoresStore(io_handler)
    new_performance_store = CsvEvaluationResultsStore(io_handler, general_quest_data_store)
    scores_service = ScoresService(scores_store, merger_store, results_store, new_performance_store)
    return build_score_assigner(artifact_store, filtered_clusters_provider, random_seed_provider, scores_service, sub_kernel_store,
                                context_holder)
