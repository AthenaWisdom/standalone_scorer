import codecs
import logging
import re
import tempfile
from functools import partial

import itertools
import pandas as pd
import pandas_profiling
from functional import seq

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types.data_ingestion import ClustersFootprintHTMLArtifact, \
    ClustersFootprintAssertionArtifact
from source.storage.stores.clusters_store.interface import ClustersStoreInterface
from source.storage.stores.clusters_store.matlab_clusters_store import MatlabClustersStore
from source.storage.stores.clusters_store.types import ClustersMetaData, Clusters
from source.storage.stores.engine_communication_store.interface import EngineCommunicationStoreInterface
from source.storage.stores.engine_communication_store.uri_based import URIBasedEngineCommunicationStore
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.clusters_footprint_analysis import ClustersFootprintAnalyzingTask

COLUMNS_FOR_ARTIFACT = ['N', 'deg', 'percInternal', 'src_0', 'src_1', 'thrs_0', 'thrs_1']
FUNCTIONS_FOR_ARTIFACT = ['min', 'max', '50%']


class FootPrintAnalyzerHandler(TaskHandlerInterface):
    def __init__(self, clusters_store, artifact_store, engine_communication_store):
        """
        @type clusters_store: L{ClustersStoreInterface}
        @type artifact_store: L{ArtifactStoreInterface}
        @type engine_communication_store: L{EngineCommunicationStoreInterface}
        """
        self.__engine_communication_store = engine_communication_store
        self.__clusters_store = clusters_store
        self.__logger = logging.getLogger('endor')
        self.__artifact_store = artifact_store

    @staticmethod
    def get_task_type():
        return ClustersFootprintAnalyzingTask

    def handle_task(self, task):
        """
        @type task: L{ClustersFootprintAnalyzingTask}
        """
        customer = task.customer
        sphere_id = task.execution_id
        clusters_meta_data = ClustersMetaData(customer, sphere_id)
        self.__store_population_dict(clusters_meta_data)
        clusters = self.__clusters_store.load_clusters(clusters_meta_data)
        clusters_properties = clusters.clusters_properties
        clusters_properties['cluster'] = clusters_properties.index

        try:
            file_hashes_map = self.__engine_communication_store.load_file_hashes_map(customer, sphere_id)
            float_to_int_regex = re.compile(r'\.\d+')
            data = map(lambda pair: (pair[0], float_to_int_regex.sub('', pair[1])), file_hashes_map.iteritems())
            file_hashes_df = pd.DataFrame(data, columns=['name', 'hash'])
            file_hashes_df['hash'] = file_hashes_df['hash'].astype(int)
            new_cols = file_hashes_df['name'].apply(lambda v: pd.Series(v.replace('.', '_').split("__sep__")))
            new_cols.columns = ['sphere', 'run_pair', 'shit']
            new_cols['dataset'] = new_cols['sphere'].apply(lambda x: task.sphere_id_to_dataset.get(x, 'unknown'))
            new_cols = new_cols.drop('shit', 1)
            file_hashes_df = file_hashes_df.join(new_cols)
            clusters_properties = pd.merge(clusters_properties, file_hashes_df, 'left',
                                           left_on='SUB_CLUSTERS_FILE', right_on='hash').drop('hash', 1)
        except LookupError:
            pass

        profile = pandas_profiling.ProfileReport(clusters_properties,
                                                 correlation_overrides={'SUB_CLUSTERS_FILE', 'run_pair',
                                                                        'sphere', 'name', 'percInternal'})
        description = profile.get_description()
        index_join = partial(pd.merge, left_index=True, right_index=True)
        agg_dict = reduce(index_join, [description['variables'][i].to_frame() for i in FUNCTIONS_FOR_ARTIFACT])\
            .ix[COLUMNS_FOR_ARTIFACT].fillna(0).to_dict('index')
        if 'run_pair' in clusters_properties.columns:
            clusters_per_dataset_and_run_pair = clusters_properties.groupby(['dataset', 'run_pair']).size().to_dict()
        else:
            clusters_per_dataset_and_run_pair = {}
        flattened_data = seq((k[0], k[1], v) for k, v in clusters_per_dataset_and_run_pair.iteritems())
        clusters_per_dataset_and_run_pair = flattened_data.group_by(lambda x: x[0])\
            .map(lambda x: (x[0], seq(x[1]).map(lambda y: seq(y).tail()).to_dict()))\
            .to_dict()
        temp_output_file_path = tempfile.mkstemp()[1]
        profile.to_file(temp_output_file_path)
        with codecs.open(temp_output_file_path, 'rb', encoding='utf8') as temp_output_file:
            html_data = temp_output_file.read()
        html_url = self.__clusters_store.store_clusters_footprint_html(clusters_meta_data, html_data)
        self.__artifact_store.store_artifact(ClustersFootprintHTMLArtifact(customer, sphere_id, html_url))
        self.__artifact_store.store_artifact(ClustersFootprintAssertionArtifact(customer,
                                                                                sphere_id,
                                                                                agg_dict,
                                                                                clusters_per_dataset_and_run_pair))
        # self.__store_clusters_for_spark(clusters_meta_data, clusters)
        self.__clusters_store.store_clusters_properties_for_spark(clusters_meta_data, clusters_properties)

    def __store_clusters_for_spark(self, clusters_metadata, clusters):
        """
        @type clusters_metadata: L{ClustersMetaData}
        @type clusters: L{Clusters}
        """
        self.__logger.info("Storing clusters for Spark")
        self.__logger.debug("Converting matrix to DF")
        clusters_pairs_iterator = itertools.izip(*clusters.population_to_clusters_matrix.nonzero())
        matrix_as_dataframe = pd.DataFrame.from_records(clusters_pairs_iterator, columns=['cluster', 'id_ordinal'])
        self.__logger.debug("Creating population DF")
        population_as_dataframe = pd.DataFrame(clusters.population_ids, columns=['hashed_id'])
        self.__logger.debug("Joining")
        merged_df = pd.merge(matrix_as_dataframe, population_as_dataframe, left_on='id_ordinal', right_index=True)
        final_df = merged_df[['hashed_id', 'cluster']]
        final_df['hashed_id'] = final_df.loc[:, 'hashed_id'].astype(long)
        self.__logger.debug("Storing")
        self.__clusters_store.store_clusters_for_spark(clusters_metadata, final_df)
        self.__logger.info("Done")

    def __store_population_dict(self, clusters_meta_data):
        population = self.__clusters_store.load_population(clusters_meta_data)
        self.__clusters_store.store_population(clusters_meta_data, population)


def prod_bootstrap(io_handler, artifact_store):
    """
    @type io_handler: L{IOHandlerInterface}
    @type artifact_store: L{ArtifactStoreInterface}
    """
    return FootPrintAnalyzerHandler(MatlabClustersStore(io_handler), artifact_store,
                                    URIBasedEngineCommunicationStore(io_handler))
