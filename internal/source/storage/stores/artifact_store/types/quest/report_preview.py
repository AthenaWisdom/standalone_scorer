from source.storage.stores.artifact_store.types.quest.base_artifact import QuestBaseArtifact

__author__ = 'izik'


class ReportPreviewArtifact(QuestBaseArtifact):
    type = 'report_preview'

    def __init__(self, customer, quest_id, report_preview):
        """
        @type report_preview: C{str}
        @type customer: C{str}
        @type quest_id: C{str}
        """
        super(ReportPreviewArtifact, self).__init__(customer, quest_id)
        self.__report_preview = report_preview
        self.__quest_id = quest_id

    def _to_dict(self):
        return {
            'quest_id': self.__quest_id,
            'report_preview': self.__report_preview
        }
