import json
import os

from source.storage.io_handlers.in_memory import InMemoryIOHandler
from source.storage.stores.general_quest_data_store.interface import GeneralQuestDataStoreInterface
from source.storage.stores.general_quest_data_store.types import QueryMetadata
from source.storage.stores.kernel_store.types import KernelMetadata


class InMemoryGeneralQuestDataStore(GeneralQuestDataStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{InMemoryIOHandler}
        """
        self.__io_handler = io_handler



    def store_runtime_query_execution_units(self, customer, quest_id, query_execution_units):
        """
        @type customer:C{str}
        @type quest_id:C{str}
        @type query_execution_units: list of C{RuntimeQueryExecutionUnit}
        """
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id,
                            'query_execution_units.json')
        self.__io_handler.save_raw_data(query_execution_units, path)

    def load_runtime_query_execution_units(self, customer, quest_id):
        """
        @type customer:C{str}
        @type quest_id:C{str}
        @rtype query_execution_units: list of C{RuntimeQueryExecutionUnit}
        """
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id,
                            'query_execution_units.json')
        return self.__io_handler.load_raw_data(path)

    def store_query_metadata(self, query_metadata):
        """
        @type query_metadata: L{QueryMetadata}
        """
        path = os.path.join('sandbox-{}'.format(query_metadata.customer), 'Quests', query_metadata.quest_id,
                            query_metadata.query_id, 'metadata.json')
        self.__io_handler.save_raw_data(query_metadata, path)

    def load_query_metadata(self, customer, quest_id, query_id):
        path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id, query_id, 'metadata.json')
        return self.__io_handler.load_raw_data(path)

    def store_kernel_summary(self, kernel_metadata, kernel_summary):
        """
        @type kernel_metadata: L{KernelMetadata}
        @type kernel_summary: C{dict}
        """
        path = os.path.join('sandbox-{}'.format(kernel_metadata.customer), 'KernelsSummaries',
                            kernel_metadata.hash, 'metadata.json')
        self.__io_handler.save_raw_data(json.dumps(kernel_summary, indent=2, allow_nan=False), path)

    def store_kernel_summary_new(self, customer, quest_id, query_id, kernel_summary):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type kernel_summary: C{dict}
        """
        path = os.path.join('sandbox-{}'.format(customer), 'Quests',
                            quest_id, query_id, 'kernelSummary.json')
        self.__io_handler.save_raw_data(json.dumps(kernel_summary, indent=2, allow_nan=False), path)

    def load_kernel_summary_new(self, customer, quest_id, query_id):
        '''
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype: C{dict}
        '''
        path = os.path.join('sandbox-{}'.format(customer), 'Quests',
                            quest_id, query_id, 'kernelSummary.json')
        return json.loads(self.__io_handler.load_raw_data(path))

    def load_kernel_summary(self, kernel_metadata):
        path = os.path.join('sandbox-{}'.format(kernel_metadata.customer), 'KernelsSummaries',
                            kernel_metadata.hash, 'metadata.json')
        return json.loads(self.__io_handler.load_raw_data(path))

    def store_scorers_origin_index(self, customer_id, quest_id, query_id, origin_index):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type origin_index: C{dict}
        """
        path = self.__get_scorers_origin_index_path(customer_id, quest_id, query_id)
        self.__io_handler.save_raw_data(origin_index, path)

    def store_mergers_origin_index(self, customer_id, quest_id, query_id, origin_index):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type origin_index: C{dict}
        """
        path = self.__get_mergers_origin_index_path(customer_id, quest_id, query_id)
        self.__io_handler.save_raw_data(origin_index, path)

    def load_scorers_origin_index(self, customer_id, quest_id, query_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype C{dict}
        """
        path = self.__get_scorers_origin_index_path(customer_id, quest_id, query_id)
        return self.__io_handler.load_raw_data(path)

    def load_mergers_origin_index(self, customer_id, quest_id, query_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype C{dict}
        """
        path = self.__get_mergers_origin_index_path(customer_id, quest_id, query_id)
        return self.__io_handler.load_raw_data(path)

    @staticmethod
    def __get_scorers_origin_index_path(customer_id, quest_id, query_id):
        return os.path.join('sandbox-{}'.format(customer_id), 'Quests', quest_id, query_id, 'scorers_origin_index.json')

    @staticmethod
    def __get_mergers_origin_index_path(customer_id, quest_id, query_id):
        return os.path.join('sandbox-{}'.format(customer_id), 'Quests', quest_id, query_id, 'mergers_origin_index.json')