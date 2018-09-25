import logging
import functools
from collections import defaultdict
from itertools import imap
from datetime import datetime
from contextlib import contextmanager

from elasticsearch.helpers import bulk

from source.utils.context import ContextManager
from source.utils.es_store import index_name, es_store as prod_es_store


class ESwContextEventSender(object):
    def __init__(self, es_store):
        self.__es_store = es_store
        self.__logger = logging.getLogger('endor')
        self.__context_manager = ContextManager()

    def send(self, entity_type, data):
        new_entity = self.__clone_with_context(data)
        new_entity = self.prettify_entity(new_entity)
        self.__es_store.index_entity(entity_type, new_entity)

    def __clone_with_context(self, data):
        data = data.copy()
        data.update(self.__context_manager.context)
        return data

    @staticmethod
    def __clone_with_bulk_metadata(index, entity_type, time_sent, data):
        cloned = data.copy()
        cloned['time_sent'] = time_sent
        cloned['_index'] = index
        cloned['_type'] = entity_type
        return cloned

    def bulk(self, entity_type, entities):
        time_sent = datetime.now()
        contextualized = imap(self.__clone_with_context, entities)
        prettified = imap(self.prettify_entity, contextualized)
        add_metadata_func = functools.partial(self.__clone_with_bulk_metadata, index_name(), entity_type, time_sent)
        pre_bulk = imap(add_metadata_func, prettified)
        bulk(self.__es_store, pre_bulk)

    def send_error(self, error_msg):
        self.send("error", {"error": error_msg})

    @classmethod
    def prettify_entity(cls, new_entity):
        prettify_funcs = defaultdict(lambda: (lambda y: y))
        prettify_funcs[str] = lambda x: prettify_value(x)
        prettify_funcs[unicode] = lambda x: prettify_value(x)
        prettify_funcs[dict] = lambda x: (cls.prettify_entity(x))
        return {prettify_key(key): prettify_funcs[type(val)](val) for key, val in new_entity.iteritems()}


sender = ESwContextEventSender(prod_es_store)
send_es_event = sender.send
send_es_error = sender.send_error
send_es_bulk = sender.bulk


@contextmanager
def time_long_metric(sub_operation):
    start = datetime.now()
    yield
    end = datetime.now()
    duration = (end - start).total_seconds()
    send_es_event('long_metric', {
        'duration_in_seconds': duration,
        'start': start,
        'end': end,
        'sub_operation': sub_operation
    })


def prettify_key(s):
    return s.replace(".", "_").replace("-", "_").replace("=", "_")


def prettify_value(s):
    return s.replace("=", "_").replace("-", "_")
