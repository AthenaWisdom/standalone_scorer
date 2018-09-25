import json
import logging
import urllib
import os
from socket import error as socket_error
from boto.utils import get_instance_identity
from logstash_formatter import LogstashFormatterV1
from logstash_handler import LogstashHandler

LOGSTASH_ADDRESS = 'listener.logz.io'
LOGSTASH_PORT = 5050
LOG_LEVEL = logging.DEBUG


def get_instance_id():
    try:
        if os.environ.get('FORCED_INSTANCE_ID') is not None:
            return os.environ['FORCED_INSTANCE_ID']
        return get_instance_identity(timeout=5, num_retries=3)['document']['instanceId']
    except IndexError:
        return "Non-AWS"



def configure_logger(logger, extra=None, log_level=LOG_LEVEL, to_logz_io=False):
    logger = logging.getLogger('endor')
    if len(logger.handlers):
        return

    extra_data = {
        'token': 'RQiCphmajBWxCVBfNoTTkaZWIYoDzCev',
        'instance_id': get_instance_id(),
    }

    try:
        with open('/meta/context_data') as context_data:
            extra_data.update(json.load(context_data))
    except (ValueError, IOError):
        pass

    fmt = json.dumps({'extra': extra_data}, allow_nan=False)

    formatter = logging.Formatter('[%(levelname)s][%(asctime)s][%(funcName)s]: %(message)s')
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler("/tmp/scorer.log", mode='a', encoding=None, delay=False)
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    stream_handler.setLevel(LOG_LEVEL)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.setLevel(log_level)
    logger.extra_data = extra if extra is not None else {}


def reconfigure_logger(logger, extra=None, log_level=LOG_LEVEL, to_logz_io=False):
    logger = logging.getLogger('scorer_endor')
    map(logger.removeHandler, logger.handlers[:])
    map(logger.removeFilter, logger.filters[:])
    configure_logger(logger, extra, log_level, to_logz_io)
