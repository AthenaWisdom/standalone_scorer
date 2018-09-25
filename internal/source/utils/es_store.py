import logging
from datetime import datetime

from elasticsearch import exceptions, Elasticsearch

ES_URI = 'https://edd3f902645e826bf5527642a043795e.us-east-1.aws.found.io:9243'



class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonElasticsearch(Elasticsearch):
    __metaclass__ = Singleton

    def __init__(self, *args, **kwargs):
        super(SingletonElasticsearch, self).__init__(*args, **kwargs)
        self.__logger = logging.getLogger('endor')

    def index_entity(self, entity_type, entity):
        copy_entity = entity.copy()
        for i in xrange(5):
            try:
                copy_entity.update({"time_sent": datetime.now()})
                self.index(index_name(), entity_type, copy_entity)
                return
            except (exceptions.ConnectionTimeout, exceptions.TransportError):
                extra = {
                    'entity_type': entity_type,
                    'entity': copy_entity
                }
                if i == 4:
                    self.__logger.error('Could not send data to ES after 5 retries.', exc_info=True, extra=extra)
                else:
                    self.__logger.warning('Could not send data to ES', exc_info=True, extra=extra)


es_store = SingletonElasticsearch([ES_URI])


def index_name():
    return 'endor-' + datetime.now().strftime('%Y-%m-%d')