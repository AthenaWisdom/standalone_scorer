import logging
import os

from mock import MagicMock

from source.utils.context import ContextManager

logging.addLevelName(25, 'PROGRESS')
PROGRESS_LEVEL = 25

IS_PROD = False
BASE_ES_URI = 'http://edd3f902645e826bf5527642a043795e.us-east-1.aws.found.io:9200'


class EndorLogger(logging.getLoggerClass()):
    def __init__(self, *args, **kwargs):
        super(EndorLogger, self).__init__(*args, **kwargs)
        self.__extra_data = {}

    @property
    def extra_data(self):
        return self.__extra_data.copy()

    @extra_data.setter
    def extra_data(self, value):
        self.__extra_data = value

    def progress(self, message, *args, **kwargs):
        self.__log_with_type(message, 'progress', PROGRESS_LEVEL, *args, **kwargs)

    def __log_with_type(self, message, log_type, level, *args, **kwargs):
        if 'extra' in kwargs:
            kwargs['extra']['kind'] = log_type
        else:
            kwargs['extra'] = {
                'kind': log_type
            }
        self.log(level, message, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].update(self.__extra_data)
        super(EndorLogger, self).log(level, msg, *args, **kwargs)

    def _log(self, level, msg, args, exc_info=None, extra=None):
        if extra is None:
            extra = {}
        extra.update(self.__extra_data)
        # noinspection PyProtectedMember
        super(EndorLogger, self)._log(level, msg, args, exc_info, extra)


class EndorMockLogger(EndorLogger):
    def log(self, level, msg, *args, **kwargs):
        pass

    def _log(self, level, msg, args, exc_info=None, extra=None):
        pass


if os.environ.get('TESTING', '0') == '1':
    logging.setLoggerClass(EndorMockLogger)
    from source.utils.es_store import es_store
    from source.utils import es_with_context_sender
    original_index_entity = es_store.index_entity
    original_bulk = es_with_context_sender.bulk
    es_with_context_sender.bulk = es_store.index_entity = MagicMock()
else:
    logging.setLoggerClass(EndorLogger)



