import functools
import itertools
import logging
from collections import defaultdict
from datetime import datetime

from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk

from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types.interface import ArtifactInterface
from source.storage.stores.artifact_store.types import ExceptionArtifact, ModelSelectionSummaryArtifact
from source.storage.stores.artifact_store.types import DurationMetricArtifact
from source.storage.stores.artifact_store.types import MergerSummaryArtifact
from source.storage.stores.artifact_store.types.quest.merger import InvalidMergerPerformanceSummary, \
    ValidMergerPerformanceSummary
from source.storage.stores.artifact_store.types.quest.scores_assigner import InvalidScorerPerformanceSummary, \
    ScoreAssignerExternalJobArtifact, ValidScorerPerformanceSummary

BLACKLISTED_ARTIFACTS = set()
WHITELISTED_ARTIFACTS = {
    ExceptionArtifact.type,
    DurationMetricArtifact.type,
    ScoreAssignerExternalJobArtifact.type,
    MergerSummaryArtifact.type,
    ValidMergerPerformanceSummary.type,
    ValidScorerPerformanceSummary.type,
    InvalidScorerPerformanceSummary.type,
    InvalidMergerPerformanceSummary.type,
    ModelSelectionSummaryArtifact.type
}


class ElasticsearchArtifactStore(ArtifactStoreInterface):
    def __init__(self, *es_args, **es_kwargs):
        """
        @type es_args: C{list}
        @type es_kwargs: C{dict}
        """
        super(ElasticsearchArtifactStore, self).__init__('elasticsearch')
        self.__es_kwargs = es_kwargs
        self.__es_args = es_args
        self.__connect_to_es()
        self.__logger = logging.getLogger('e')

    def __connect_to_es(self):
        self.__elasticsearch_driver = Elasticsearch(*self.__es_args, **self.__es_kwargs)

    @staticmethod
    def __prepare_entity(artifact):
        """
        @type artifact: L{ArtifactInterface}
        """
        copy_entity = artifact.to_dict()
        copy_entity.update({'time_sent': datetime.now(), 'endor_type': artifact.type})
        return prettify_entity(copy_entity)

    def store_artifact(self, artifact):
        """
        @type artifact: L{ArtifactInterface}
        """
        if artifact.type not in WHITELISTED_ARTIFACTS:
            return
        entity = self.__prepare_entity(artifact)
        for i in xrange(5):
            try:
                es_index = index_name()
                self.__elasticsearch_driver.index(es_index, artifact.type, entity)
                return
            except (exceptions.ConnectionTimeout, exceptions.TransportError):
                extra = {
                    'entity_type': artifact.type,
                    'entity': entity
                }
                if i == 4:
                    self.__logger.error('Could not send data to ES after 5 retries.', exc_info=True, extra=extra)
                    self.__connect_to_es()
                else:
                    self.__logger.warning('Could not send data to ES', exc_info=True, extra=extra)

    def bulk_store_artifacts(self, artifacts):
        filtered_artifacts = itertools.ifilter(lambda x: x.type in WHITELISTED_ARTIFACTS, artifacts)
        entities = itertools.imap(self.__prepare_entity, filtered_artifacts)
        add_metadata_func = functools.partial(clone_with_bulk_metadata, index_name())
        for i in xrange(5):
            try:
                ready_entities = itertools.imap(add_metadata_func, entities)
                bulk(self.__elasticsearch_driver, ready_entities)
                return
            except (exceptions.ConnectionTimeout, exceptions.TransportError):
                if i == 4:
                    self.__logger.error('Could not send data to ES after 5 retries.', exc_info=True)
                    self.__connect_to_es()
                else:
                    self.__logger.warning('Could not send data to ES', exc_info=True)


def prettify_key(s):
    return s.replace(".", "_").replace("-", "_").replace("=", "_")


def prettify_value(s):
    return s.replace("=", "_").replace("-", "_")


def prettify_entity(new_entity):
    prettify_funcs = defaultdict(lambda: (lambda y: y))
    prettify_funcs[str] = lambda x: prettify_value(x)
    prettify_funcs[unicode] = lambda x: prettify_value(x)
    prettify_funcs[dict] = lambda x: (prettify_entity(x))
    return {prettify_key(key): prettify_funcs[type(val)](val) for key, val in new_entity.iteritems()}


def index_name():
    return 'endor-' + datetime.now().strftime('%Y-%m-%d')


def clone_with_bulk_metadata(index, data):
    cloned = data.copy()
    cloned['_index'] = index
    cloned['_type'] = data['endor_type']
    return cloned

