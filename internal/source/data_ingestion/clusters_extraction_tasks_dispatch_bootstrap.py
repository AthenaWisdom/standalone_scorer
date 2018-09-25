from source.data_ingestion.clusters_extraction.task import ClustersExtractionTaskGenerator
from source.data_ingestion.clusters_extraction_tasks_dispatch_task_handler import \
    ClustersExtractionTasksDispatchTaskHandler
from source.storage.stores.engine_communication_store.uri_based import URIBasedEngineCommunicationStore
from source.storage.stores.preview_store.parquet_store import ParquetDataPreviewStore


def prod_bootstrap(io_handler, artifact_store, task_submitter):
    engine_communication_store = URIBasedEngineCommunicationStore(io_handler)
    data_preview_store = ParquetDataPreviewStore(io_handler)
    extractor_task_generator = ClustersExtractionTaskGenerator()

    return ClustersExtractionTasksDispatchTaskHandler(engine_communication_store, data_preview_store,
                                                      task_submitter,
                                                      extractor_task_generator, artifact_store)
