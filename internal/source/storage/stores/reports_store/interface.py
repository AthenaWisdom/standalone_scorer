from source.storage.stores.reports_store.types import Report


class ReportsStoreInterface(object):
    def store_report(self, report):
        """
        @type report: L{Report}
        """
        raise NotImplementedError()

    def load_report(self, customer, quest_id):
        """
        @type quest_id: C{str}
        @type customer: C{str}
        @rtype: L{Report}
        """
        raise NotImplementedError()

    def store_unscored_population(self, customer, quest_id, unscored_pop):
        raise NotImplementedError()

    def store_mutated_report(self, mutation_name, report):
        """
        @type mutation_name: C{str}
        @type report: L{Report}
        """
        raise NotImplementedError()