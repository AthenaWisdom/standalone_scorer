import pandas as pd


class ReporterHashDictionaryStoreInterface(object):
    def load_dictionary(self, customer_id, quest_id, query_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype: L{pd.DataFrame}
        """
        raise NotImplementedError()
