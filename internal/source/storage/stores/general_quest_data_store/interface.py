from source.storage.stores.general_quest_data_store.types import QueryMetadata
from source.storage.stores.general_quest_data_store.runtime_query_execution_unit import RuntimeQueryExecutionUnit
from source.storage.stores.kernel_store.types import KernelMetadata


class GeneralQuestDataStoreInterface(object):
    def store_query_metadata(self, query_metadata):
        """
        @type query_metadata: L{QueryMetadata}
        """
        raise NotImplementedError()

    def load_query_metadata(self, customer, quest_id, query_id):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @return: The query metadata for the given query
        @rtype: L{QueryMetadata}
        """
        raise NotImplementedError()

    def store_runtime_query_execution_units(self, customer, quest_id, query_execution_units):
        """
        @type customer:C{str}
        @type quest_id:C{str}
        @type query_execution_units: C{list} of L{RuntimeQueryExecutionUnit}
        """
        raise NotImplementedError()

    def load_runtime_query_execution_units(self, customer, quest_id):
        """
        @type customer:C{str}
        @type quest_id:C{str}
        @rtype query_execution_units: C{list} of L{RuntimeQueryExecutionUnit}
        """
        raise NotImplementedError()

    def store_kernel_summary(self, kernel_metadata, kernel_summary):
        """
        @type kernel_summary: C{dict}
        @type kernel_metadata: L{KernelMetadata}
        """
        raise NotImplementedError()

    def load_kernel_summary(self, kernel_metadata):
        """
        @type kernel_metadata: L{KernelMetadata}
        @rtype: C{dict}
        """
        raise NotImplementedError()

    def load_kernel_summary_new(self, customer, quest_id, query_id):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype: C{dict}
        """
        raise NotImplementedError()

    def store_scorers_origin_index(self, customer_id, quest_id, query_id, origin_index):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type origin_index: C{dict}
        """
        raise NotImplementedError()

    def store_mergers_origin_index(self, customer_id, quest_id, query_id, origin_index):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type origin_index: C{dict}
        """
        raise NotImplementedError()

    def load_scorers_origin_index(self, customer_id, quest_id, query_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype C{dict}
        """
        raise NotImplementedError()

    def load_mergers_origin_index(self, customer_id, quest_id, query_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype C{dict}
        """
        raise NotImplementedError()
