from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact

__all__ = [
    'SubKernelSummaryArtifact'
]


class SubKernelSummaryArtifact(QuestBaseArtifact):
    type = 'sub_kernel_summary'

    def __init__(self, customer, split_kernel_id, field, value, part, num_whites, num_ground, universe_size):
        """
        @type customer: C{str}
        @type split_kernel_id: C{str}
        @type field: C{str}
        @type value: C{str}
        @type part: C{int}
        @type num_whites: C{int}
        @type num_ground: C{int}
        @type universe_size: C{int}
        """
        super(SubKernelSummaryArtifact, self).__init__(customer, split_kernel_id)
        self.__universe_size = universe_size
        self.__num_ground = num_ground
        self.__num_whites = num_whites
        self.__part = part
        self.__value = value
        self.__field = field
        self.__split_kernel_id = split_kernel_id

    def _to_dict(self):
        return {
            'split_kernel_id': self.__split_kernel_id,
            'field': self.__field,
            'value': self.__value,
            'part': self.__part,
            'num_whites': self.__num_whites,
            'num_ground': self.__num_ground,
            'universe_size': self.__universe_size
        }
