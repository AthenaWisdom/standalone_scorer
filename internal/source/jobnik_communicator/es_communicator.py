import logging
from datetime import datetime

from elasticsearch import Elasticsearch

from source.jobnik_communicator.interface import JobnikCommunicatorInterface, JobnikSession


class ElasticsearchJobnikCommunicator(JobnikCommunicatorInterface):
    def __init__(self, *es_args, **es_kwargs):
        """
        @type es_args: C{list}
        @type es_kwargs: C{dict}
        """
        super(ElasticsearchJobnikCommunicator, self).__init__('elasticsearch')
        self.__es_kwargs = es_kwargs
        self.__es_args = es_args
        self.__connect_to_es()
        self.__logger = logging.getLogger('endor')

    def __connect_to_es(self):
        self.__elasticsearch_driver = Elasticsearch(*self.__es_args, **self.__es_kwargs)

    def send_driver_completion(self, jobnik_session):
        """
        @type jobnik_session: L{JobnikSession}
        """
        if jobnik_session is None:
            return

        message = {
            'type': 'driver_completion',
            'job_token': jobnik_session.job_token,
            'publish_date': datetime.utcnow(),
            'topic_name': self.__get_topic_name(jobnik_session),
            'jobnik_session': jobnik_session.to_dict()
        }
        self.__elasticsearch_driver.index('jobnik-comm', 'driver_completion', message)

    def send_progress_indication(self, identifier, total_tasks_count, failed, jobnik_session):
        if jobnik_session is None:
            return

        message = {
            'type': 'progress_indication',
            'job_token': jobnik_session.job_token,
            'publish_date': datetime.utcnow(),
            'identifier': identifier,
            'total_task_count': total_tasks_count,
            'has_errors': failed,
            'topic_name': self.__get_topic_name(jobnik_session),
            'jobnik_session': jobnik_session.to_dict()
        }
        self.__elasticsearch_driver.index('jobnik-comm', 'progress_indication', message)

    @staticmethod
    def __get_topic_name(jobnik_session):
        return '{}-jobnik-communication'.format(jobnik_session.jobnik_role)
