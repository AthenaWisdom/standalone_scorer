import json
import os
from cStringIO import StringIO
import shutil
import tempfile

import numpy as np
import pandas as pd
from sklearn.externals import joblib

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.merged_scores_store.interface import MergedScoresStoreInterface
from source.storage.stores.merged_scores_store.types import MergedScores, MergerPerformance, MergerModel, \
    MergedScoresMetadata, MergerMetadata


class CSVMergedScoresStore(MergedScoresStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
        self.__io_handler = io_handler

    def store_merged_scores(self, merged_scores):
        """
        @type merged_scores: L{MergedScores}
        """
        metadata = merged_scores.metadata
        base_output_path = self.__get_base_merged_scores_path(metadata)
        self.__io_handler.save_raw_data(self.__get_df_as_text(merged_scores.all_merged_scores),
                                        os.path.join(base_output_path, 'merged_scores.csv'))
        self.__io_handler.save_raw_data(self.__get_df_as_text(merged_scores.merged_suspects_scores),
                                        os.path.join(base_output_path, 'merged_suspects.csv'))
        metadata_path = self.__get_merger_metadata_path(metadata)
        self.__io_handler.save_raw_data(json.dumps(metadata.merger_metadata.to_dict(), indent=2, allow_nan=False),
                                        metadata_path)

    def load_merger_metadata(self, metadata):
        metadata_path = self.__get_merger_metadata_path(metadata)
        real_metadata = MergerMetadata.from_dict(json.loads(self.__io_handler.load_raw_data(metadata_path)))
        return real_metadata

    def load_merged_scores(self, metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        @return: Loaded merged scores according to the given metadata
        @rtype: L{MergedScores}
        @raise LookupError: When scores don't exist
        """
        base_output_path = self.__get_base_merged_scores_path(metadata)
        all_scores_df = pd.read_csv(StringIO(self.__io_handler.load_raw_data(os.path.join(base_output_path,
                                                                                          'merged_scores.csv'))),
                                    index_col=0)
        all_scores_df.index = all_scores_df.index.astype(int)
        suspects_scores_path = os.path.join(base_output_path, 'merged_suspects.csv')
        suspects_scores_df = pd.read_csv(StringIO(self.__io_handler.load_raw_data(suspects_scores_path)), index_col=0)
        suspects_scores_df.index = suspects_scores_df.index.astype(float)
        metadata_path = self.__get_merger_metadata_path(metadata)
        real_metadata = MergerMetadata.from_dict(json.loads(self.__io_handler.load_raw_data(metadata_path)))
        return MergedScores(all_scores_df, suspects_scores_df, real_metadata)

    def store_merged_performance(self, performance_object, is_stable):
        """
        @type performance_object: L{MergerPerformance}
        """
        output_path = self.__get_base_merged_performance_path(performance_object.metadata, is_stable)
        self.__io_handler.save_raw_data(performance_object.df.to_csv(encoding='utf-8'), output_path)

    def load_merged_performance(self, metadata):
        """
        @type metadata: L{MergedPerformanceMetadata}
        """
        input_path = self.__get_base_merged_performance_path(metadata, is_stable=False)
        performance_df = pd.read_csv(StringIO(self.__io_handler.load_raw_data(input_path)), index_col=0)
        return MergerPerformance(performance_df, metadata)

    def store_merger_model(self, merger_model):
        """
        @type merger_model: L{MergerModel}
        """
        path = self.__get_base_merged_model_path(merger_model.metadata)
        local_temp_path = tempfile.mkdtemp()
        joblib.dump(merger_model.model, os.path.join(local_temp_path, 'model.pkl'))
        for f in os.listdir(local_temp_path):
            with open(os.path.join(local_temp_path, f), 'rb') as binary_file:
                path = os.path.join(path, f)
                self.__io_handler.save_raw_data(binary_file.read(), path)
        shutil.rmtree(local_temp_path)

    @staticmethod
    def __get_base_mergers_path(customer, quest_id, query_id):
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id, query_id, 'mergers')
        return path

    @staticmethod
    def __get_base_merged_performance_path(metadata, is_stable):
        """
        @type metadata: L{MergedPerformanceMetadata}
        @rtype: C{str}
        """
        merged_scores_metadata = metadata.merged_scores_metadata
        path = os.path.join('sandbox-{}'.format(merged_scores_metadata.customer), 'Quests',
                            merged_scores_metadata.quest_id, merged_scores_metadata.query_id,
                            'mergers' if not is_stable else 'stable_mergers',
                            merged_scores_metadata.merger_metadata.merger_id,
                            'artifacts', 'results_summary', '{}.csv'.format(metadata.performance_type))
        return path

    @staticmethod
    def __get_base_merged_scores_path(metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        @rtype: C{str}
        """
        path = os.path.join('sandbox-{}'.format(metadata.customer), 'Quests', metadata.quest_id,
                            metadata.query_id, 'mergers', metadata.merger_metadata.merger_id, 'scores')
        return path

    @staticmethod
    def __get_base_merged_model_path(metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        """
        output_path_join = os.path.join('sandbox-{}'.format(metadata.customer), 'Quests',
                                        metadata.quest_id, metadata.query_id,
                                        'mergers', metadata.merger_metadata.merger_id,
                                        'artifacts', 'models')
        return output_path_join

    @staticmethod
    def __get_merger_metadata_path(metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        @rtype: C{str}
        """
        output_path_join = os.path.join('sandbox-{}'.format(metadata.customer), 'Quests', metadata.quest_id,
                                        metadata.query_id, 'mergers', metadata.merger_metadata.merger_id,
                                        'metadata.json')

        return output_path_join

    @staticmethod
    def __get_df_as_text(df):
        """
        @type df: C{DataFrame}
        @rtype: C{str}
        """
        scores_arr = np.c_[df.index, df.values]
        num_scorers = len(df.columns)
        save_format = ",".join(["%15.0f"] + ["%.25f"] * num_scorers)
        header = ",".join(["idnum"] + list(df.columns.values))
        all_scores_data = StringIO()
        np.savetxt(all_scores_data, scores_arr, fmt=save_format, header=header, comments="")
        return all_scores_data.getvalue()

    def load_all_merger_ids(self, customer, quest_id, query_id):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @return: The list of all merger ids
        @rtype: C{List}
        """
        mergers_path = self.__get_base_mergers_path(customer, quest_id, query_id)
        all_merger_paths = list(self.__io_handler.list_dir(mergers_path))
        # merger_ids = [os.path.basename(os.path.split(os.path.split(full_name)[0])[0]) for full_name in all_merger_paths
        #               if "merged_scores.csv" in full_name]
        merger_ids = [os.path.basename(full_name) for full_name in all_merger_paths]
        return merger_ids

