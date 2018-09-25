from source.storage.stores.artifact_store.types import ArtifactInterface
from source.storage.stores.artifact_store.types.interface import ExternalJobArtifactInterface

__all__ = [
    'HTMLDataPreviewArtifact',
    'PreviewDistinctCountArtifact',
    'DataPreviewExternalJobArtifact',
]


class DataPreviewBaseArtifact(ArtifactInterface):
    operation = 'data_preview'


class HTMLDataPreviewArtifact(DataPreviewBaseArtifact):
    type = "html_data_preview_artifact"

    def __init__(self, customer, preview_id, html_url):
        """
        @type customer: C{str}
        @type preview_id: C{str}
        @type html_url: C{str}
        """
        self.__html_url = html_url
        self.__preview_id = preview_id
        super(HTMLDataPreviewArtifact, self).__init__(customer, preview_id)

    def _to_dict(self):
        return {
            'preview_id': self.__preview_id,
            'html_url': self.__html_url,
        }


class PreviewDistinctCountArtifact(DataPreviewBaseArtifact):
    type = "data_preview_distinct_count"

    def __init__(self, customer, preview_id, dict_data):
        """
        @type customer: C{str}
        @type preview_id: C{str}
        @type dict_data: C{dict}
        """
        self.__dict_data = dict_data
        self.__preview_id = preview_id
        super(PreviewDistinctCountArtifact, self).__init__(customer, preview_id)

    def _to_dict(self):
        return {
            'preview_id': self.__preview_id,
            'dict_data': self.__dict_data,
        }


class DataPreviewExternalJobArtifact(ExternalJobArtifactInterface, DataPreviewBaseArtifact):
    type = 'data_preview_external_job'

    def __init__(self, customer, preview_id, job_id, num_tasks):
        """
        @type customer: C{str}
        @type preview_id: C{str}
        @type job_id: C{str}
        @type num_tasks: C{int}
        @type stage: C{int}
        """
        super(DataPreviewExternalJobArtifact, self).__init__(customer, preview_id, job_id, num_tasks)
        self.__preview_id = preview_id

    def _to_dict(self):
        return {
            'preview_id': self.__preview_id,
        }

    @property
    def sphere_id(self):
        return self.__preview_id
