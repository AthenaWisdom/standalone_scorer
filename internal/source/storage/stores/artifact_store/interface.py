from source.storage.stores.artifact_store.types import ArtifactInterface


class ArtifactStoreInterface(object):
    def __init__(self, service_name):
        self.__service_name = service_name

    @property
    def service_name(self):
        return self.__service_name

    def store_artifact(self, artifact):
        """
        Store the given artifact on the store

        @param artifact: The artifact to store
        @type artifact: L{ArtifactInterface}
        """
        raise NotImplementedError()

    def bulk_store_artifacts(self, artifacts):
        """
        Stores all of the artifacts given in a single bulk.
        @param artifacts: The iterable of artifacts
        """
        for artifact in artifacts:
            self.store_artifact(artifact)


class MultiStoresArtifactStore(ArtifactStoreInterface):
    def __init__(self, *stores, **kwargs):
        super(MultiStoresArtifactStore, self).__init__('composite')
        self.__stores = list(stores)

    def add_store(self, store):
        self.__stores.append(store)

    def remove_store(self, store):
        self.__stores.remove(store)

    def store_artifact(self, artifact):
        for store in self.__stores:
            store.store_artifact(artifact)

    def bulk_store_artifacts(self, artifacts):
        """
        Stores all of the artifacts given in a single bulk.
        @param artifacts: The iterable of artifacts
        """
        for store in self.__stores:
            store.bulk_store_artifacts(artifacts)
