import os
from cStringIO import StringIO

import numpy as np
import pandas as pd

from source.storage.io_handlers.interface import ErrorCodes
from source.storage.stores.results_store.interface import ResultsStoreInterface


class CsvResultsStore(ResultsStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
        super(CsvResultsStore, self).__init__()
        self.__io_handler = io_handler

    def mutate_and_store_merger_results(self, results_df, customer_id, quest_id, query_id, merger_key):
        """
        @type results_df: C{PandasDataFrame}
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type scorer_id: C{str}
        @type merger_model: C{str}
        @type variant: C{dict}
        """
        origin_id = self.create_merger_origin_id(merger_key)

        results_df['origin_id'] = origin_id

        path = self.__get_merger_results_path(customer_id, quest_id, query_id, origin_id)
        self.__save_results_to_path(results_df, path)

    def mutate_and_store_scorer_results(self, results_df, customer_id, quest_id, query_id, sub_kernel_ordinal,
                                        scorer_key):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type sub_kernel_ordinal: C{int}
        @type scorer_name: C{str}
        @type results_df: C{PandasDataFrame}
        """
        origin_id = self.create_scorer_origin_id(sub_kernel_ordinal, str(scorer_key))

        results_df['origin_id'] = origin_id

        path = self.__get_scorer_results_path(customer_id, quest_id, query_id, origin_id)
        self.__save_results_to_path(results_df, path)

    def __save_results_to_path(self, results_df, results_path):
        results_csv = self.__get_df_as_text(results_df)
        try:
            self.__io_handler.save_raw_data(results_csv, results_path)
        except IOError as ex:
            if ex.args.get(0).get('code') == ErrorCodes.FILE_TOO_BIG:
                self.__io_handler.save_raw_data_multipart(StringIO(results_csv), results_path)
            else:
                raise

    @staticmethod
    def __get_merger_results_path(customer_id, quest_id, query_id, origin_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type origin_id: C{str}
        @rtype: C{str}
        """
        return CsvResultsStore.__get_results_path(customer_id, quest_id, query_id, 'merger_results', origin_id)

    @staticmethod
    def __get_scorer_results_path(customer_id, quest_id, query_id, origin_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type origin_id: C{str}
        @rtype: C{str}
        """
        return CsvResultsStore.__get_results_path(customer_id, quest_id, query_id, 'score_assigner_results', origin_id)

    @staticmethod
    def __get_results_path(customer_id, quest_id, query_id, entity_name, origin_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type entity_name: C{str}
        @type origin_id: C{str}
        @rtype: C{str}
        """
        return os.path.join('sandbox-{}'.format(customer_id), 'Quests', quest_id, query_id, entity_name,
                            '{}.csv'.format(origin_id))

    @staticmethod
    def __get_df_as_text(df):
        """
        @type df: C{DataFrame}
        @rtype: C{str}
        """
        # DO NOT CHANGE THE FORMAT.
        # PANDAS USE A SINGLE FORMAT FOR ALL NUMERIC COLUMN.
        # IN ORDER TO AVOID DATA LOSS ON THE FLOAT USER ID WE NEED 15 DIGITS BEFORE THE DECIMAL POINT
        # IN ORDER TO AVOID DATA LOSS ON THE FLOAT SCORE ID WE NEED 25 DIGITS AFTER THE DECIMAL POINT
        # float_format = "%15.25f"
        # df.to_csv(float_format=float_format)
        results_arr = np.c_[df.index, df.values]
        # user_id, score, origin_id
        save_format = ",".join(["%15.0f"] + ["%.25f"] + ["%s"])
        header = ",".join(["user_id"] + list(df.columns.values))
        all_results_data = StringIO()
        np.savetxt(all_results_data, results_arr, fmt=save_format, header=header, comments="")
        return all_results_data.getvalue()

    def load_merger_results_df(self, customer_id, quest_id, query_id, merger_key):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type scorer_id: C{str}
        @type merger_model: C{str}
        @type merger_variant: C{dict}
        @return: C{PandasDataFrame}
        """
        origin_id = self.create_merger_origin_id(merger_key)
        path = self.__get_merger_results_path(customer_id, quest_id, query_id, origin_id)
        return self.__load_results_dataframe(path)

    def load_scorer_results_df(self, customer_id, quest_id, query_id, sub_kernel_ordinal, scorer_key):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type sub_kernel_ordinal: C{int}
        @type scorer_id: C{str}
        @return: C{PandasDataFrame}
        """
        origin_id = self.create_scorer_origin_id(sub_kernel_ordinal, str(scorer_key))
        path = self.__get_scorer_results_path(customer_id, quest_id, query_id, origin_id)
        return self.__load_results_dataframe(path)

    def __load_results_dataframe(self, path):
        data = StringIO(self.__io_handler.load_raw_data(path))
        df = pd.read_csv(data, index_col=0)
        df.index = df.index.astype(float)
        return df



