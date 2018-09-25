import json
import os
from source.query.scores_service.domain import MergerKey, ScorerKey
from source.storage.stores.merged_scores_store.interface import MergedScoresStoreInterface
from source.storage.stores.merged_scores_store.types import MergedScores, MergedScoresMetadata, MergerPerformance, \
    MergedPerformanceMetadata, MergerModel, MergerMetadata


class InMemoryMergedScoresStore(MergedScoresStoreInterface):

    def __init__(self, io_handler):
        self.__io_handler = io_handler

    def store_merged_scores(self, merged_scores):
        """
        @type merged_scores: L{MergedScores}
        """
        metadata = merged_scores.metadata
        base_output_path = self.__get_base_merged_scores_path(metadata)
        self.__io_handler.save_raw_data(merged_scores.all_merged_scores, os.path.join(base_output_path,
                                                                                      'merged_scores.csv'))
        self.__io_handler.save_raw_data(merged_scores.merged_suspects_scores,
                                        os.path.join(base_output_path, 'merged_suspects.csv'))
        metadata_path = self.__get_merger_metadata_path(metadata)
        self.__io_handler.save_raw_data(json.dumps(metadata.merger_metadata.to_dict(), allow_nan=False, indent=2),
                                        metadata_path)

    def load_merged_scores(self, metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        @return: Loaded scores according to the given metadata
        @rtype: L{MergedScores}
        @raise LookupError: When scores don't exist
        """
        base_output_path = self.__get_base_merged_scores_path(metadata)
        all_scores_df = self.__io_handler.load_raw_data(os.path.join(base_output_path, 'merged_scores.csv'))
        all_scores_df.index = all_scores_df.index.astype(float)
        suspects_scores_path = os.path.join(base_output_path, 'merged_suspects.csv')

        suspects_scores_df = self.__io_handler.load_raw_data(suspects_scores_path)
        suspects_scores_df.index = suspects_scores_df.index.astype(float)
        metadata_path = self.__get_merger_metadata_path(metadata)
        real_metadata = MergerMetadata.from_dict(json.loads(self.__io_handler.load_raw_data(metadata_path)))
        return MergedScores(all_scores_df, suspects_scores_df, real_metadata)

    def load_merger_metadata(self, metadata):
        metadata_path = self.__get_merger_metadata_path(metadata)
        real_metadata = MergerMetadata.from_dict(json.loads(self.__io_handler.load_raw_data(metadata_path)))
        return real_metadata

    def load_merged_performance(self, metadata):
        """
        @type metadata: L{MergedPerformanceMetadata}
        """
        input_path = self.__get_base_merged_performance_path(metadata, is_stable=False)
        return MergerPerformance(self.__io_handler.load_raw_data(input_path), metadata)

    def store_merged_performance(self, performance_object, is_stable):
        """
        @type performance_object: L{MergerPerformance}
        """
        output_path = self.__get_base_merged_performance_path(performance_object.metadata, is_stable)
        self.__io_handler.save_raw_data(performance_object.df, output_path)

    def store_merger_model(self, merger_model):
        """
        @type merger_model: L{MergerModel}
        """
        path = self.__get_base_merged_model_path(merger_model.metadata)
        self.__io_handler.save_raw_data(merger_model.model, path)

    @staticmethod
    def __get_base_merged_model_path(metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        """
        merger_meta = metadata.merger_metadata
        merger_key = MergerKey(merger_meta.merger_name, merger_meta.merger_params, ScorerKey(merger_meta.scorer_name))
        output_path_join = os.path.join('sandbox-{}'.format(metadata.customer), 'Quests',
                                        metadata.quest_id, metadata.query_id,
                                        'mergers', str(merger_key),
                                        'artifacts', 'models', 'model.pkl')
        return output_path_join

    @staticmethod
    def __get_base_merged_scores_path(metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        @rtype: C{str}
        """
        merger_meta = metadata.merger_metadata
        merger_key = MergerKey(merger_meta.merger_name, merger_meta.merger_params, ScorerKey(merger_meta.scorer_name))
        output_path_join = os.path.join('sandbox-{}'.format(metadata.customer), 'Quests', metadata.quest_id,
                                        metadata.query_id, 'mergers', str(merger_key), 'scores')
        return output_path_join

    @staticmethod
    def __get_base_mergers_path(customer, quest_id, query_id):
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id, query_id, 'mergers')
        return path

    @staticmethod
    def __get_merger_metadata_path(metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        @rtype: C{str}
        """
        merger_meta = metadata.merger_metadata
        merger_key = MergerKey(merger_meta.merger_name, merger_meta.merger_params, ScorerKey(merger_meta.scorer_name))
        output_path_join = os.path.join('sandbox-{}'.format(metadata.customer), 'Quests', metadata.quest_id,
                                        metadata.query_id, 'mergers', str(merger_key),
                                        'metadata.json')

        return output_path_join

    @staticmethod
    def __get_base_merged_performance_path(metadata, is_stable):
        """
        @type metadata: L{MergedPerformanceMetadata}
        @rtype: C{str}
        """
        merged_scores_metadata = metadata.merged_scores_metadata
        merger_meta = merged_scores_metadata.merger_metadata
        merger_key = MergerKey(merger_meta.merger_name, merger_meta.merger_params, ScorerKey(merger_meta.scorer_name))
        output_path_join = os.path.join('sandbox-{}'.format(merged_scores_metadata.customer), 'Quests',
                                        merged_scores_metadata.quest_id, merged_scores_metadata.query_id,
                                        'mergers' if not is_stable else 'stable_mergers', str(merger_key),
                                        'artifacts', 'results_summary', '{}.csv'.format(metadata.performance_type))
        return output_path_join

    def load_all_merger_ids(self, customer, quest_id, query_id):
        mergers_path = self.__get_base_mergers_path(customer, quest_id, query_id)
        all_merger_paths = list(self.__io_handler.list_dir(mergers_path))
        merger_ids = [os.path.basename(os.path.split(os.path.split(full_name)[0])[0]) for full_name in all_merger_paths
                      if "merged_scores.csv" in full_name]
        return merger_ids

