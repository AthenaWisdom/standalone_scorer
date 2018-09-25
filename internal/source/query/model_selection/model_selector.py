import pandas as pd

from source.query.merger.merger_triplets_generator import MergerStrategiesProvider
from source.query.scores_service.domain import ScorerKey, MergerKey
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types.quest.model_selector import ModelSelectionSummaryArtifact
from source.storage.stores.general_quest_data_store.interface import GeneralQuestDataStoreInterface
from source.storage.stores.split_kernel_store.interface import SplitKernelStoreInterface


class ModelSelector(object):
    def __init__(self, scores_service, artifact_store, general_quest_data_store,
                 split_kernel_store, model_loss_calculator, merging_tasks_generator):
        """
        @type scores_service: L{ScoresService}

        @type split_kernel_store: L{SplitKernelStoreInterface}
        @type general_quest_data_store: L{GeneralQuestDataStoreInterface}
        @type merging_tasks_generator: L{MergerStrategiesProvider}

        @type artifact_store: L{ArtifactStoreInterface}
        @type model_loss_calculator:
        """
        self.__split_kernel_store = split_kernel_store
        self.__scores_service = scores_service
        self.__general_quest_data_store = general_quest_data_store
        self.__merging_tasks_generator = merging_tasks_generator
        self.__artifact_store = artifact_store

        self.__model_loss_calculator = model_loss_calculator

    def retrieve_best_merger_key(self, customer_id, quest_id, past_queries_metadata, present_query_metadata, ml_conf,
                                 merger_conf, preselected_merger, hit_rate_thresholds, feature_flags):
        merger_keys_to_str_map = self.__map_merger_keys_to_str(ml_conf, merger_conf)

        mergers_eligible_for_selection = {key: value for key, value in merger_keys_to_str_map.items()
                                          if 'ExternalScorer' not in key and 'RandomScorer' not in key}

        # If FDE overrides model selection by past, return his fixed scorer:
        if len(preselected_merger) > 0:
            # if selected scorer is ensemble
            if preselected_merger["scorer_name"] == 'logistic_ensemble':
                scorer_key = ScorerKey(preselected_merger["scorer_name"], preselected_merger.get('scorer_params', None),
                                       'internal')
            else:

                scorer_key = ScorerKey(preselected_merger["scorer_name"], preselected_merger.get('scorer_params', None))
            selected_merger_key = MergerKey(preselected_merger['merger_name'], preselected_merger['params'], scorer_key)

        else:
            # If past exists, apply model selection by past
            if self.__check_if_past_exists(past_queries_metadata, customer_id, quest_id, feature_flags):
                selected_merger_key = self.__get_best_model_by_past(mergers_eligible_for_selection,
                                                                    customer_id, quest_id,
                                                                    past_queries_metadata, hit_rate_thresholds,
                                                                    feature_flags)
            # Output default merger
            else:
                selected_merger_key = self.__get_default_model(mergers_eligible_for_selection)

        # Always try to run algorithm on present query
        # noinspection PyBroadException
        try:
            self.select_single_query_model(mergers_eligible_for_selection, customer_id, quest_id,
                                           present_query_metadata, hit_rate_thresholds, feature_flags)
        except Exception:
            pass

        return selected_merger_key

    @staticmethod
    def __get_triplet_by_merger_id(merger_tasks, merger_id):
        df = pd.DataFrame({name: [a[name] for a in merger_tasks] for name in ["merger_model", "scorer_id", "variant",
                                                                              "merger_id"]})
        res = df[df["merger_id"] == merger_id].values[0]
        return tuple(res)

    def __get_best_model_by_past(self, mergers_keys_map, customer_id, quest_id, queries_metadata, hit_rate_thresholds,
                                 feature_flags):

        present_selection = None
        for query_metadata in queries_metadata:
            # fetch data
            best_merger_key, thresholds, losses = self.select_single_query_model(mergers_keys_map, customer_id, quest_id,
                                                                                query_metadata, hit_rate_thresholds,
                                                                                feature_flags)

            if query_metadata.role == 'validation_past':
                present_selection = best_merger_key

        return present_selection

    @staticmethod
    def __remove_complex_models(merger_keys_str_map, thresh_hr_df):

        good_mergers = [merger_name for merger_name, merger_key in merger_keys_str_map.items() if
                        ((merger_key.model_name == "LogisticRegression" and
                            merger_key.model_params.values()[0] == 1e-06) or
                         (merger_key.model_name == "BaggingRegression") or
                         (merger_key.model_name == "RandomForest") or (merger_key.model_name == "Query0Merger"))]
        return thresh_hr_df[good_mergers]

    def extract_best_merger_per_query(self, merger_keys_str_map,  hit_rates, kernel_summary, hit_rate_thresholds):
        # merging_tasks_eligible_for_selection = \
        #     filter(lambda x: all(name not in x['scorer_id'] for name in ['ExternalScorer', 'RandomScorer']),
        #            merging_tasks)
        relevant_sorted_thresholds, models_losses = self.__model_loss_calculator.calc_models_losses(hit_rates,
                                                                                                    kernel_summary,
                                                                                                    hit_rate_thresholds)

        if models_losses is not None:

            best_model_name, best_model_loss = models_losses.idxmin().values[0], models_losses.min().values[0]
            best_merger_key = merger_keys_str_map[best_model_name]
        else:
            # TODO(Shahar): add tests for this flow: where max hit rates are 0 for all selected thresholds
            best_merger_key = self.__get_default_model(merger_keys_str_map)
            best_model_loss = -1

        return best_merger_key, best_model_loss, relevant_sorted_thresholds, models_losses

    def __get_all_mergers_hr(self, customer_id, quest_id, query_id, feature_flags, merger_keys_str_map):
        all_merger_hr = self.__scores_service.load_mergers_precisions(customer_id, quest_id, query_id,
                                                                      feature_flags, merger_keys_str_map)

        return all_merger_hr

    def __build_query_selection_artifact(self, query_metadata, customer_id, quest_id, best_merger_key,
                                         best_model_loss, thresholds, losses):

        best_merger_id = str(best_merger_key)
        best_merger_name = best_merger_key.model_name
        best_merger_params = best_merger_key.model_params
        best_scorer_name = best_merger_key.scorer_name

        if losses is not None:

            losses_artifact = losses.to_dict()
        else:
            losses_artifact = {}

        artifact = ModelSelectionSummaryArtifact(customer_id, quest_id, query_metadata.query_id, query_metadata.role,
                                                 best_merger_id, best_merger_name, best_scorer_name, best_merger_params,
                                                 best_model_loss, list(thresholds), losses_artifact)

        return artifact

    def select_single_query_model(self, merger_keys_str_map, customer_id, quest_id, query_role, hit_rate_thresholds,
                                  feature_flags):
        hit_rates = self.__get_all_mergers_hr(customer_id, quest_id, query_role.query_id, feature_flags,
                                              merger_keys_str_map)
        simple_models_hr = self.__remove_complex_models(merger_keys_str_map, hit_rates)

        kernel_summary = self.__general_quest_data_store.load_kernel_summary_new(customer_id, quest_id,
                                                                                 query_role.query_id)

        best_merger_key, best_model_loss, thresholds, losses = self.extract_best_merger_per_query(merger_keys_str_map,
                                                                                                 simple_models_hr,
                                                                                                 kernel_summary,
                                                                                                 hit_rate_thresholds)

        selection_artifact = self.__build_query_selection_artifact(query_role, customer_id,
                                                                   quest_id, best_merger_key, best_model_loss,
                                                                   thresholds, losses)

        self.__artifact_store.store_artifact(selection_artifact)
        return best_merger_key, thresholds, losses

    def __get_default_model(self, eligible_merger_keys_map):
        logistic_regressions_models = [model_key for model_key in eligible_merger_keys_map.values() if
                                       model_key.model_name == "LogisticRegression"]
        also_logistic_ensemble = [model_key for model_key in logistic_regressions_models if
                                  model_key.scorer_name.startswith('logistic_ensemble')]

        also_default_variants = [model_key for model_key in also_logistic_ensemble
                                 if model_key.model_params.get('regularization_factor', None) == 1e-06 and
                                 model_key.scorer_name.endswith('regularization_factor=0.000001')]

        if len(also_default_variants) > 0:
            best_effort_collection = also_default_variants
        elif len(also_logistic_ensemble) > 0:
            best_effort_collection = also_logistic_ensemble
        elif len(logistic_regressions_models) > 0:
            best_effort_collection = logistic_regressions_models
        else:
            best_effort_collection = eligible_merger_keys_map.values()

        return best_effort_collection[0]

    def __check_if_ground_in_validation_past(self, past_queries_metadata, customer_id, quest_id, feature_flags):
        validation_query_id = [query_metadata.query_id for query_metadata in past_queries_metadata if
                               query_metadata.role == 'validation_past'][0]
        kernel_summary = self.__general_quest_data_store.load_kernel_summary_new(customer_id, quest_id,
                                                                                 validation_query_id)
        ground_in_past = kernel_summary['summary']['num_ground']
        return ground_in_past>0

    def __get_model_id(self, merger_model, merger_variant, scorer_id):

        params_str = '_'.join("%s=%s" % (key, val) for (key, val) in
                                  merger_variant.iteritems())
        merger_id = "_".join([merger_model, params_str])
        return "_".join([merger_id, scorer_id])

    def __map_merger_keys_to_str(self, ml_conf, merger_conf):
        merging_tasks = list(self.__merging_tasks_generator.get_merging_tasks(ml_conf, merger_conf))
        merger_key_str_map = {}
        for merging_task in merging_tasks:
            merger_key = MergerKey(merging_task["merger_model"], merging_task["variant"],
                                   merging_task["scorer_id"])
            merger_key_str_map[str(merger_key)] = merger_key
        return merger_key_str_map

    def __check_if_past_exists(self, past_queries_metadata, customer_id, quest_id, feature_flags):
        return len(past_queries_metadata) > 0 and self.__check_if_ground_in_validation_past(past_queries_metadata,
                                                                                            customer_id, quest_id,
                                                                                            feature_flags)


