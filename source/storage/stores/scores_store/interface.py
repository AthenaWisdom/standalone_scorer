from source.storage.stores.scores_store.types import Scores, ScoresMetadata, PerformanceMetadata, ScorerPerformance


class ScoresStoreInterface(object):
    def store_scores(self, scores):
        """
        @param scores: The scores object to store
        @type scores: L{Scores}
        """
        raise NotImplementedError()

    def load_scores(self, metadata):
        """
        @type metadata: L{ScoresMetadata}
        @return: Loaded scores according to the given metadata
        @rtype: L{Scores}
        @raise LookupError: When scores don't exist
        """
        raise NotImplementedError()

    def store_performance(self, performance_object):
        """
        @type performance_object: L{ScorerPerformance}
        """

        raise NotImplementedError()

    def load_performance(self, metadata):
        """
        @type metadata: L{PerformanceMetadata}
        """
        raise NotImplementedError()
