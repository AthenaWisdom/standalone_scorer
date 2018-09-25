import logging
from datetime import datetime

from source.jobnik_communicator.interface import JobnikCommunicatorInterface, JobnikSession
from source.topic_publisher.interface import TopicPublisherInterface

MAX_MESSAGE_RETRIES = 5


class JobnikTopicCommunicator(JobnikCommunicatorInterface):

    def __init__(self, message_publisher):
        """
        @type message_publisher: L{TopicPublisherInterface}
        """
        super(JobnikTopicCommunicator, self).__init__(message_publisher.service_name)
        self.__message_publisher = message_publisher
        self.__logger = logging.getLogger('endor')

    def send_driver_completion(self, jobnik_session):
        """
        @type jobnik_session: L{JobnikSession}
        """
        if jobnik_session is None:
            return

        message = {
            'type': 'driver_completion',
            'job_token': jobnik_session.job_token,
            'publish_date': datetime.utcnow()
        }
        extra = dict(pubsub="jobnik-communication", message_data=message,
                     jobnik_session=jobnik_session.to_dict())
        self.__logger.debug("Sending driver completion", extra=extra)
        message_id = self.__publish_message(jobnik_session, message)
        extra['message_id'] = message_id
        self.__logger.info("Sent driver completion", extra=extra)

    def __publish_message(self, jobnik_session, message):
        message_id = self.__message_publisher.publish_dict(self.__get_topic_name(jobnik_session), message)
        return message_id

    def send_progress_indication(self, identifier, total_tasks_count, failed, jobnik_session):
        if jobnik_session is None:
            return

        jobnik_return_code_name = 'UnknownError' if failed else 'Ok'

        message = {
            'type': 'progress_indication',
            'job_token': jobnik_session.job_token,
            'publish_date': datetime.utcnow(),
            'identifier': identifier,
            'total_task_count': total_tasks_count,
            'return_code_name': jobnik_return_code_name
        }
        extra = dict(pubsub="jobnik-communication", message_data=message,
                     jobnik_session=jobnik_session.to_dict())
        self.__logger.debug("Sending progress indication", extra=extra)
        message_id = self.__publish_message(jobnik_session, message)
        extra['message_id'] = message_id
        self.__logger.info("Sent progress indication", extra=extra)

    @staticmethod
    def __get_topic_name(jobnik_session):
        return '{}-jobnik-communication'.format(jobnik_session.jobnik_role)
