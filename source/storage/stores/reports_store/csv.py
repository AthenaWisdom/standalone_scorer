import json
import os
from StringIO import StringIO

import numpy as np
import pandas as pd

from source.query.scores_service.domain import MergerKey, ScorerKey
from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.reports_store.interface import ReportsStoreInterface
from source.storage.stores.reports_store.types import Report


class CSVReportsStore(ReportsStoreInterface):

    def __init__(self, io_handler):
        """
            @type io_handler: L{IOHandlerInterface}
            """
        self.__io_handler = io_handler

    def store_report(self, report):
        """
        @type report: L{Report}
        """
        base_path = self.__get_base_path(report.customer, report.quest_id)
        report_path = os.path.join(base_path, 'report.csv')
        chosen_merger_path = os.path.join(base_path, 'chosen_merger.json')
        self.__io_handler.save_raw_data(report.df.to_csv(index=True, encoding='utf-8'), report_path)
        chosen_merger_dict = {"merger_model": report.chosen_merger.model_name,
                              "scorer_id": report.chosen_merger.scorer_name,
                              "variant": report.chosen_merger.model_params,
                              "merger_id": str(report.chosen_merger)}
        self.__io_handler.save_raw_data(json.dumps(chosen_merger_dict, allow_nan=False), chosen_merger_path)

    def store_mutated_report(self, mutation_name, report):
        base_path = self.__get_base_path(report.customer, report.quest_id)
        report_path = os.path.join(base_path, 'Reports', mutation_name, 'report.csv')
        self.__io_handler.save_raw_data(report.df.to_csv(index=True, encoding='utf-8'), report_path)

    def load_report(self, customer, quest_id):
        """
        @type quest_id: C{str}
        @type customer: C{str}
        @rtype: L{Report}
        """
        base_path = self.__get_base_path(customer, quest_id)
        report_path = os.path.join(base_path, 'report.csv')
        chosen_merger_path = os.path.join(base_path, 'chosen_merger.json')
        chosen_merger_dict = json.loads(self.__io_handler.load_raw_data(chosen_merger_path))
        merger_model = chosen_merger_dict["merger_model"]
        merger_params = chosen_merger_dict["variant"]
        scorer_id = chosen_merger_dict["scorer_id"]
        chosen_merger_key = MergerKey(merger_model, merger_params, ScorerKey(scorer_id))

        report_df = pd.read_csv(StringIO(self.__io_handler.load_raw_data(report_path)), encoding='utf-8')
        return Report(customer, quest_id, report_df, chosen_merger_key)

    @staticmethod
    def __get_base_path(customer, quest_id):
        return os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id)

    def store_unscored_population(self, customer, quest_id, unscored_pop):
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id, 'unscored_unhashed_suspects.csv')
        scores_arr = np.c_[unscored_pop.index]
        save_format = "%s"
        header = "idnum"
        all_scores_data = StringIO()
        np.savetxt(all_scores_data, scores_arr, fmt=save_format, header=header, comments="")
        data = all_scores_data.getvalue()
        self.__io_handler.save_raw_data(data, path)
