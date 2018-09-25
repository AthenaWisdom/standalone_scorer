from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact


class OriginIndexJobArtifact(QuestBaseArtifact):
    type = 'origin_index'

    def __init__(self, customer, quest_id, query_id, results_source, index):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type results_source: C{str}
        @type index: C{dict}
        """
        super(OriginIndexJobArtifact, self).__init__(customer, quest_id)
        self.__customer = customer
        self.__query_id = query_id
        self.__quest_id = quest_id
        self.__results_source = results_source
        self.__index = index

    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'query_id': self.__query_id,
            'results_source': self.__results_source,
            'index': self.__index,
        }

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def query_id(self):
        return self.__query_id

    @property
    def results_source(self):
        return self.__results_source

    @property
    def index(self):
        return self.__index
