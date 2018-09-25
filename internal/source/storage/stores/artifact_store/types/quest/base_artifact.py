from source.storage.stores.artifact_store.types.interface import ArtifactInterface


# noinspection PyAbstractClass
class QuestBaseArtifact(ArtifactInterface):
    operation = 'quest'


class UserSegmentationBaseArtifact(ArtifactInterface):
    operation = 'user_segmentation'
