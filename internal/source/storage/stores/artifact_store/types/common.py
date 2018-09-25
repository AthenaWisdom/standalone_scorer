from datetime import datetime

from enum import IntEnum

from source.storage.stores.artifact_store.types import ArtifactInterface

__all__ = [
    'ExceptionArtifact',
    'DurationMetricArtifact',
    'ProgressMessageArtifact',
]


class ErrorCodes(IntEnum):
    no_errors = 0
    no_ground = 1
    no_whites = 2
    no_suspects = 3
    no_kernel = 3


class SeverityLevels(IntEnum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


class ExceptionArtifact(ArtifactInterface):
    type = 'exception'

    def __init__(self, customer, execution_id, msg, traceback, config, **kwargs):
        """
        @type customer: C{str}
        @type execution_id: C{str}
        @type msg: C{str}
        @type traceback: C{str}
        @type config: C{dict}
        """
        super(ExceptionArtifact, self).__init__(customer, execution_id)
        self.__config = config
        self.__extra_data = kwargs
        self.__traceback = traceback
        self.__msg = msg

    def _to_dict(self):
        base_dict = self.__extra_data.copy()
        base_dict['config'] = self.__config
        base_dict['traceback'] = self.__traceback
        base_dict['msg'] = self.__msg
        return base_dict


class WarningArtifact(ArtifactInterface):
    type = 'warningArtifact'

    def __init__(self, customer, execution_id, error_code, msg):
        """
        @type customer: C{str}
        @type execution_id: C{str}
        @type error_code: C{int}
        @type msg: C{str}
        """
        super(WarningArtifact, self).__init__(customer, execution_id)
        self.__error_code = error_code
        self.__msg = msg

    def _to_dict(self):
        return {
            'errorCode': self.__error_code,
            'msg': self.__msg
        }


# noinspection PyPep8Naming
class DurationMetricArtifact(ArtifactInterface):
    type = 'long_metric'

    def __init__(self, customer, execution_id, sub_operation, artifact_store=None, **kwargs):
        """
        @type artifact_store: L{ArtifactStoreInterface}
        @type sub_operation: C{str}
        @type customer: C{str}
        @type execution_id: C{str}
        @type kwargs: C{dict}
        """
        super(DurationMetricArtifact, self).__init__(customer, execution_id)
        self.__artifact_store = artifact_store
        self.__sub_operation = sub_operation
        self.__extra_data = kwargs
        self.__start_time = None
        self.__end_time = None

    def _to_dict(self):
        base_dict = self.__extra_data.copy()
        start_time = self.__start_time
        end_time = self.__end_time
        base_dict['duration_in_seconds'] = None \
            if None in (end_time, start_time) else (end_time - start_time).total_seconds()
        base_dict['sub_operation'] = self.__sub_operation
        base_dict['start'] = start_time
        base_dict['end'] = end_time
        return base_dict

    def __enter__(self):
        self.__start_time = datetime.now()
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__end_time = datetime.now()
        if self.__artifact_store is not None:
            self.__artifact_store.store_artifact(self)


class ProgressMessageArtifact(ArtifactInterface):
    type = 'progress_message'

    def __init__(self, customer, execution_id, message, code, severity, **kwargs):
        """
        @type severity: C{SeverityLevels}
        @type code: C{ErrorCodes}
        @type message: C{str}
        @type customer: C{str}
        @type execution_id: C{str}
        @type kwargs: C{dict}
        """
        super(ProgressMessageArtifact, self).__init__(customer, execution_id)
        self.__severity_level = severity
        self.__code = code
        self.__message = message
        self.__extra_data = kwargs

    @property
    def message(self):
        return self.__message

    @property
    def extra_data(self):
        return self.__extra_data

    def _to_dict(self):
        base_dict = self.__extra_data.copy()
        base_dict['message'] = self.__message
        base_dict['code'] = self.__code.value
        base_dict['severity'] = self.__severity_level.value
        base_dict['extra_data'] = self.__extra_data
        return base_dict

    def is_error(self):
        return self.__severity_level >= SeverityLevels.ERROR

    @classmethod
    def from_dict(cls, data):
        return cls(data['customer'], data['execution_id'], data['message'], data['code'],
                   data['severity'], **data['extra_data'])


class QueueLagsArtifact(ArtifactInterface):
    type = "queue_lags_artifact"
    operation = "any"

    def __init__(self, run_identifier, customer, execution_id, task_ordinal, time_in_queue, time_to_process,
                 task_class, environment='unknown'):
        """
        @type environment: C{str}
        @type task_class: C{str}
        @type run_identifier: C{str}
        @type customer: C{str}
        @type execution_id: C{str}
        @type task_ordinal: C{int}
        @type time_in_queue: C{float}
        @type time_to_process: C{float}
        """
        super(QueueLagsArtifact, self).__init__(customer, execution_id)
        self.__environment = environment
        self.__task_class = task_class
        self.__time_to_process = time_to_process
        self.__time_in_queue = time_in_queue
        self.__task_ordinal = task_ordinal
        self.__run_identifier = run_identifier

    @property
    def task_class(self):
        return self.__task_class

    @property
    def time_to_process(self):
        return self.__time_to_process

    @property
    def environment(self):
        return self.__environment

    def _to_dict(self):
        return {
            'time_to_process': self.__time_to_process,
            'time_in_queue': self.__time_in_queue,
            'task_class': self.__task_class,
            'task_ordinal': self.__task_ordinal,
            'run_identifier': self.__run_identifier,
            'environment': self.__environment,
        }
