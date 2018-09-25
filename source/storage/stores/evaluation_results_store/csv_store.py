import os
from StringIO import StringIO

import pandas as pd
from functional import seq

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.evaluation_results_store.interface import EvaluationResultsStoreInterface, UnscoredStrategy
from source.storage.stores.general_quest_data_store.interface import GeneralQuestDataStoreInterface


class CsvEvaluationResultsStore(EvaluationResultsStoreInterface):
    __unscored_strategy_to_path_parts = {
        UnscoredStrategy.leave_unscored: 'leave-unscored',
        UnscoredStrategy.after_scored: 'after-scored',
    }

    def __init__(self, io_handler, general_quest_data_store):
        """
        @type io_handler: L{IOHandlerInterface}
        @type general_quest_data_store: L{GeneralQuestDataStoreInterface}
        """
        super(CsvEvaluationResultsStore, self).__init__()
        self.__io_handler = io_handler
        self.__general_quest_data_store = general_quest_data_store

    def load_scorer_results_evaluation_df(self, customer_id, quest_id, query_id, unscored_strategy):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type unscored_strategy: C{UnscoredStrategy}
        @rtype C{PandasDataFrame}
        """
        origin_index = self.__general_quest_data_store.load_scorers_origin_index(customer_id, quest_id, query_id)
        path = self._get_evaluation_results_path(customer_id, quest_id, query_id, 'scorers', unscored_strategy)
        return self.__load_evaluation_results_dataframe(path, origin_index)

    def load_merger_results_evaluation_df(self, customer_id, quest_id, query_id, unscored_strategy):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type unscored_strategy: C{UnscoredStrategy}
        @rtype C{PandasDataFrame}
        """
        origin_index = self.__general_quest_data_store.load_mergers_origin_index(customer_id, quest_id, query_id)
        path = self._get_evaluation_results_path(customer_id, quest_id, query_id, 'mergers', unscored_strategy)
        return self.__load_evaluation_results_dataframe(path, origin_index)

    def __load_evaluation_results_dataframe(self, path, origin_index):
        """
        @type path: C{str}
        @type origin_index: C{dict}
        @rtype C{PandasDataFrame}
        """
        # Load the file in the evaluation results folder
        dataframe = self._load_dataframe(path)

        # Reverse the hashing of origin_id to the actual columns
        dataframe = self.__unhash_origin_ids(dataframe, origin_index)

        return dataframe

    def _load_dataframe(self, path):
        """
        @type path: C{str}
        """
        raw_csv_path = seq(self.__io_handler.list_dir(path)) \
            .filter(lambda file_path: file_path[file_path.rfind('/') + 1:].startswith('part') and \
                                      file_path.endswith('.csv')) \
            .first()

        raw_csv = StringIO(self.__io_handler.load_raw_data(raw_csv_path))

        return pd.read_csv(raw_csv, encoding='utf-8')

    @staticmethod
    def _get_evaluation_results_path(customer_id, quest_id, query_id, originator, unscored_strategy):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type originator: C{str}
        @type unscored_strategy: C{UnscoredStrategy}
        @rtype C{str}
        """
        # We assume here that we'll always run on the evaluation against the default whites and ground populations.
        # Once this becomes an invalid assumption, we'll need to add these as parameters to this function and pass them.
        labelled_population_id = 'whites'
        validation_population_id = 'ground'

        return os.path.join('sandbox-{}'.format(customer_id), 'Quests', quest_id, query_id, 'evaluation_results',
                            originator, '{}_{}'.format(labelled_population_id, validation_population_id),
                            CsvEvaluationResultsStore.__unscored_strategy_to_path_parts[unscored_strategy])

    @staticmethod
    def __unhash_origin_ids(dataframe, origin_index):
        """
        @type dataframe: C{PandasDataFrame}
        @type origin_index: C{dict}
        @rtype C{PandasDataFrame}
        """
        origin_parts = seq(origin_index.values()).flat_map(lambda origin: origin.keys()).distinct().to_list()

        for part in origin_parts:
            dataframe[part] = dataframe['origin_id'] \
                .apply(lambda origin_id: origin_index.get(origin_id, {}).get(part, None))

        dataframe = dataframe.drop('origin_id', 1)

        return dataframe
