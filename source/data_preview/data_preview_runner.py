import codecs
import logging
import tempfile

import pandas_profiling

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types.data_preview import HTMLDataPreviewArtifact, \
    PreviewDistinctCountArtifact
from source.storage.stores.preview_store.interface import DataPreviewStoreInterface
from source.storage.stores.preview_store.parquet_store import ParquetDataPreviewStore
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.data_preview import DataPreviewTask


class DataPreviewAnalyzingHandler(TaskHandlerInterface):
    def __init__(self, data_preview_store, artifact_store):
        """
        @type data_preview_store: L{DataPreviewStoreInterface}
        @type artifact_store: L{ArtifactStoreInterface}
        """
        self.__data_preview_store = data_preview_store
        self.__logger = logging.getLogger('endor')

        self.__artifact_store = artifact_store

    @staticmethod
    def get_task_type():
        return DataPreviewTask

    def handle_task(self, task):
        """
        @type task: L{DataPreviewTask}
        """
        customer = task.customer
        preview_id = task.execution_id
        pandas_df = self.__data_preview_store.load_data_preview_pandas(customer, preview_id)
        if pandas_df is None:
            self.__logger.info('No pandas DF was loaded, probably empty batch')
            return
        profile = pandas_profiling.ProfileReport(pandas_df)
        temp_output_file_path = tempfile.mkstemp()[1]
        profile.to_file(temp_output_file_path)
        with codecs.open(temp_output_file_path, 'rb', encoding='utf8') as temp_output_file:
            html_data = temp_output_file.read()

        html_url = self.__data_preview_store.store_html_output(customer, preview_id, html_data)
        self.__artifact_store.store_artifact(HTMLDataPreviewArtifact(customer, preview_id, html_url))

        dict_data = profile.get_description()
        distinct_count = dict_data['variables']['distinct_count'].to_dict()

        if 'correlation' in dict_data['variables']:
            correlation_dict = dict_data['variables'][['correlation_var', 'correlation']].dropna().to_dict('index')

            for correlated_field in correlation_dict:
                correlated_to = get_correlated_to(correlation_dict, correlated_field)
                distinct_count[correlated_field] = distinct_count[correlated_to]

        self.__artifact_store.store_artifact(PreviewDistinctCountArtifact(customer, preview_id, distinct_count))
        self.__data_preview_store.store_uniques_dict(customer, preview_id,
                                                     {"unique_{}".format(k): v for k, v in distinct_count.iteritems()})


def prod_bootstrap(io_handler, artifact_store):
    """
    @type io_handler: L{IOHandlerInterface}
    @type artifact_store: L{ArtifactStoreInterface}
    """
    return DataPreviewAnalyzingHandler(ParquetDataPreviewStore(io_handler), artifact_store)


def get_correlated_to(correlation_dict, correlated_field):
    correlated_to = correlation_dict[correlated_field]['correlation_var']
    if correlated_to in correlation_dict:
        return get_correlated_to(correlation_dict, correlated_to)
    return correlated_to




