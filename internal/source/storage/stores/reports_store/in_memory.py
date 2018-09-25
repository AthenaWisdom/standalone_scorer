import json
import os
from StringIO import StringIO

import pandas as pd

from source.query.scores_service.domain import MergerKey, ScorerKey
from source.storage.dataframe_storage_mixins.in_memory_mixin import InMemoryDataFrameStorageMixin
from source.storage.io_handlers.in_memory import InMemoryIOHandler
from source.storage.stores.reports_store.interface import ReportsStoreInterface
from source.storage.stores.reports_store.types import Report


class InMemoryReportsStore(InMemoryDataFrameStorageMixin, ReportsStoreInterface):
    def __init__(self, io_handler):
        """
            @type io_handler: L{InMemoryIOHandler}
            """
        super(InMemoryReportsStore, self).__init__(io_handler)
        self.__io_handler = io_handler

    def store_report(self, report):
        """
        @type report: L{Report}
        """
        path = os.path.join('sandbox-{}'.format(report.customer), 'Quests', report.quest_id, 'report.csv')
        self.__io_handler.save_raw_data(report, path)

    def store_mutated_report(self, mutation_name, report):
        base_path = self.__get_base_path(report.customer, report.quest_id)
        report_path = os.path.join(base_path, 'Mutations', mutation_name, 'report.csv')
        self.__io_handler.save_raw_data(report.df.to_csv(index=True, encoding='utf-8'), report_path)

    def load_mutated_report_df(self, customer, quest_id, mutation_name):
        base_path = self.__get_base_path(customer, quest_id)
        report_path = os.path.join(base_path, 'Mutations', mutation_name, 'report.csv')
        chosen_merger_path = os.path.join(base_path, 'chosen_merger.json')
        chosen_merger_dict = json.loads(self.__io_handler.load_raw_data(chosen_merger_path))
        merger_model = chosen_merger_dict["merger_model"]
        merger_params = chosen_merger_dict["variant"]
        scorer_id = chosen_merger_dict["scorer_id"]
        chosen_merger_key = MergerKey(merger_model, merger_params, ScorerKey(scorer_id))

        report_df = pd.read_csv(StringIO(self.__io_handler.load_raw_data(report_path)), encoding='utf-8')
        return Report(customer, quest_id, report_df, chosen_merger_key)

    def store_chosen_merger(self, customer, quest_id, merger_model, variant, scorer_id):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type merger_model: C{str}
        @type variant: C{dict}
        @type scorer_id: C{str}
        """
        base_path = self.__get_base_path(customer, quest_id)
        chosen_merger_path = os.path.join(base_path, 'chosen_merger.json')
        chosen_merger_dict = {"merger_model": merger_model, "variant": variant, "scorer_id": scorer_id}
        self.__io_handler.save_raw_data(json.dumps(chosen_merger_dict), chosen_merger_path)

    def load_report(self, customer, quest_id):
        """
        @type quest_id: C{str}
        @type customer: C{str}
        @rtype: L{Report}
        """
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id, 'report.csv')
        return self.__io_handler.load_raw_data(path)

    def store_user_segments_results(self, segmentation_report):
        """
        @type segmentation_report: L{SignificantSegmentsReport}
        """
        path = os.path.join(
            self.__get_base_path(segmentation_report.customer, segmentation_report.quest_id),
            'segmentation_results',
            segmentation_report.segmentation_execution_id,
            segmentation_report.role,
            'segmentation_report'
        )
        self._store_dataframe(segmentation_report.segmentation_df, path)

    @staticmethod
    def __get_base_path(customer, quest_id):
        return os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id)

    def store_unscored_population(self, customer, quest_id, unscored_pop):
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id, 'unscored_unhashed_suspects.csv')
        self.__io_handler.save_raw_data(unscored_pop, path)
