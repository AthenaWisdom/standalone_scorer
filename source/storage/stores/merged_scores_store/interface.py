from source.storage.stores.merged_scores_store.types import MergedScores, MergedScoresMetadata, MergerPerformance, \
    MergedPerformanceMetadata, MergerModel


class MergedScoresStoreInterface(object):

    def store_merged_scores(self, scores):
        """
        @param scores: The merged scores object to store
        @type scores: L{MergedScores}
        """
        raise NotImplementedError()

    def load_merged_scores(self, metadata):
        """
        @type metadata: L{MergedScoresMetadata}
        @return: Loaded merged scores according to the given metadata
        @rtype: L{MergedScores}
        @raise LookupError: When scores don't exist
        """
        raise NotImplementedError()

    def store_merged_performance(self, performance_object, is_stable):
        """
        @type performance_object: L{MergerPerformance}
        """
        raise NotImplementedError()

    def load_merged_performance(self, metadata):
        """
        @type metadata: L{MergedPerformanceMetadata}
        """
        raise NotImplementedError()

    def store_merger_model(self, merger_model):
        """
        @type merger_model: L{MergerModel}
        """
        raise NotImplementedError()

    def load_all_merger_ids(self, customer, quest_id, query_id):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @return: The list of all merger ids
        @rtype: C{List}
        """
        raise NotImplementedError()

    def load_merger_metadata(self, metadata):
        raise NotImplementedError()

