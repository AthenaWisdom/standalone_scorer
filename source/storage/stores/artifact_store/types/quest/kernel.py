from datetime import datetime

from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact


__all__ = [
    'KernelSummaryArtifact',
    'SplitKernelPropertiesArtifact',
]


class KernelSummaryArtifact(QuestBaseArtifact):
    type = 'kernel_summary'

    def __init__(self, customer, quest_id, kernel_id, timestamp, summary,
                 intersections, sphere_id, sphere_intersections):
        """
        @type kernel_id: C{str}
        @type timestamp: C{datetime}
        @type sphere_intersections: C{dict}
        @type sphere_id: C{str}
        @type customer: C{str}
        @type quest_id: C{str}
        @type summary: C{dict}
        @type intersections: C{dict}
        """
        super(KernelSummaryArtifact, self).__init__(customer, quest_id)
        self.__kernel_id = kernel_id
        self.__timestamp = timestamp
        self.__sphere_intersections = sphere_intersections
        self.__sphere_id = sphere_id
        self.__customer = customer
        self.__quest_id = quest_id
        self.__summary = summary
        self.__intersections = intersections

    def _to_dict(self):
        return {
            'kernel_id': self.__kernel_id,
            'summary': self.__summary,
            'intersections': self.__intersections,
            'sphere_id': self.__sphere_id,
            'quest_id': self.__quest_id,
            'timestamp': self.__timestamp,
            'sphere_intersections': self.__sphere_intersections,
        }


class SplitKernelPropertiesArtifact(QuestBaseArtifact):
    type = 'split_kernel_properties'

    def __init__(self, customer, quest_id, split_kernel_id, num_sub_kernels):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type split_kernel_id: C{str}
        @type num_sub_kernels: C{int}
        """
        super(SplitKernelPropertiesArtifact, self).__init__(customer, quest_id)
        self.__quest_id = quest_id
        self.__split_kernel_id = split_kernel_id
        self.__num_sub_kernels = num_sub_kernels

    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'split_kernel_id': self.__split_kernel_id,
            'num_sub_kernels': self.__num_sub_kernels,
        }
