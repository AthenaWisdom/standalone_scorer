from collections import defaultdict

from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types import ArtifactInterface, TYPES_TO_CLASSES


class InMemoryArtifactStore(ArtifactStoreInterface):
    def __init__(self):
        super(InMemoryArtifactStore, self).__init__('in-mem')
        self.__internal_dict = defaultdict(lambda: [])

    def store_artifact(self, artifact):
        """
        Store the given artifact on the store

        @param artifact: The artifact to store
        @type artifact: L{ArtifactInterface}
        """
        self.__internal_dict[artifact.type].append(artifact.to_dict())

    @property
    def internal_dict(self):
        return dict(self.__internal_dict)

    def load_artifact(self, artifact_type, **kwargs):
        filterset = kwargs.items()
        filtered = filter(lambda x: all(item in x.items() for item in filterset), self.__internal_dict[artifact_type])
        return map(lambda x: TYPES_TO_CLASSES[artifact_type].from_dict(x), filtered)
