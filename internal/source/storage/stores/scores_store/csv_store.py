import os
from cStringIO import StringIO

import numpy as np
import pandas as pd

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.scores_store.interface import ScoresStoreInterface
from source.storage.stores.scores_store.types import Scores, PerformanceMetadata, ScorerPerformance


class CSVScoresStore(ScoresStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
        self.__io_handler = io_handler

    def store_scores(self, scores):
        """
        @type scores: L{Scores}
        """
        metadata = scores.metadata
        base_output_path = self.__get_base_scores_path(metadata)
        self.__io_handler.save_raw_data(self.__get_df_as_text(scores.all_scores),
                                        os.path.join(base_output_path, 'all_scores.csv'))
        self.__io_handler.save_raw_data(self.__get_df_as_text(scores.suspects_scores),
                                        os.path.join(base_output_path, 'enumerated_suspects.csv'))

    @staticmethod
    def __get_base_scores_path(metadata):
        output_path_join = os.path.join('sandbox-{}'.format(metadata.customer), 'Quests', metadata.quest_id,
                                        metadata.query_id, 'sub_kernels',
                                        'part-{0:05d}'.format(metadata.sub_kernel_ordinal), metadata.scorer_name,
                                        'scores')
        return output_path_join
    
    @staticmethod
    def __get_base_performance_path(metadata):
        """
        @type metadata: L{PerformanceMetadata}
        """
        scorer_metadata = metadata.scorer_metadata
        return os.path.join('sandbox-{}'.format(scorer_metadata.customer), 'Quests',
                            scorer_metadata.quest_id, scorer_metadata.query_id, 'sub_kernels',
                            'part-{0:05d}'.format(scorer_metadata.sub_kernel_ordinal),
                            scorer_metadata.scorer_name, 'artifacts', 'results_summary',
                            '{}.csv'.format(metadata.performance_type))

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

    def load_scores(self, metadata):
        """
        @type metadata: L{ScoresMetadata}
        @return: Loaded scores according to the given metadata
        @rtype: L{Scores}
        @raise LookupError: When scores don't exist
        """
        print "gla"
        base_input_path = self.__get_base_scores_path(metadata)
        scores_data = StringIO(self.__io_handler.load_raw_data(os.path.join(base_input_path, 'all_scores.csv')))
        all_scores_df = pd.read_csv(scores_data, index_col=0)
        all_scores_df.index = all_scores_df.index.astype(float)
        suspects_scores_path = os.path.join(base_input_path, 'enumerated_suspects.csv')
        suspects_scores_df = pd.read_csv(StringIO(self.__io_handler.load_raw_data(suspects_scores_path)), index_col=0)
        suspects_scores_df.index = suspects_scores_df.index.astype(float)
        return Scores(all_scores_df, suspects_scores_df, metadata)

    def store_performance(self, performance_object):
        """
        @type performance_object: L{ScorerPerformance}
        """
        output_path = self.__get_base_performance_path(performance_object.metadata)
        self.__io_handler.save_raw_data(performance_object.df.to_csv(encoding='utf-8'), output_path)

    def load_performance(self, metadata):
        """
        @type metadata: L{PerformanceMetadata}
        """
        input_path = self.__get_base_performance_path(metadata)
        performance_df = pd.read_csv(StringIO(self.__io_handler.load_raw_data(input_path)), index_col=0)
        return ScorerPerformance(performance_df, metadata)
