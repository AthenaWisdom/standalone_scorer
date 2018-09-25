import uuid

import os
from redis import Redis


class JobnikSession(object):
    def __init__(self, jobnik_role, job_token):
        self.__job_token = job_token
        self.__jobnik_role = jobnik_role

    @property
    def job_token(self):
        return self.__job_token

    @property
    def jobnik_role(self):
        return self.__jobnik_role

    def to_dict(self):
        return {
            'job_token': self.job_token,
            'jobnik_role': self.jobnik_role,
        }

    def __eq__(self, other):
        if other is None:
            return False
        return self.job_token == other.job_token and self.jobnik_role == other.jobnik_role


class JobnikCommunicatorInterface(object):
    def __init__(self, service_name):
        self.__service_name = service_name

    def send_driver_completion(self, jobnik_session):
        raise NotImplementedError()

    def send_progress_indication(self, identifier, total_tasks_count, failed, jobnik_session):
        """
        @type identifier: C{str}
        @type total_tasks_count: C{int}
        @type failed: C{bool}
        """
        raise NotImplementedError()

    @property
    def service_name(self):
        return self.__service_name


class NOPJobnikCommunicator(JobnikCommunicatorInterface):
    def __init__(self):
        super(NOPJobnikCommunicator, self).__init__("NOP-Communicator")

    def send_driver_completion(self, jobnik_session):
        pass

    def send_progress_indication(self, identifier, total_tasks_count, failed, jobnik_session):
        pass


class MultipleJobnikCommunicator(JobnikCommunicatorInterface):
    def __init__(self, communicators, with_redis):
        """
        @type with_redis: C{bool}
        @type communicators: C{list} of L{JobnikCommunicatorInterface}
        """
        super(MultipleJobnikCommunicator, self).__init__('composite')
        self.__redis_client = Redis(os.environ['MESSAGE_REDIS_HOST']) if with_redis else None
        self.__communicators = communicators

    def __add_to_redis(self, jobnik_session, service, message_id):
        if True or (None in (jobnik_session, self.__redis_client)):
            return

        set_name = '{}-{}-{}-communication-producer'.format(jobnik_session.jobnik_role,
                                                            jobnik_session.job_token['jobId'],
                                                            service)
        self.__redis_client.sadd(set_name.format(), message_id)

    def send_driver_completion(self, jobnik_session):
        """
        @type jobnik_session: L{JobnikSession}
        """
        message_id = uuid.uuid4().get_hex()
        self.__add_to_redis(jobnik_session, 'general', message_id)
        for communicator in self.__communicators:
            if communicator.service_name != 'sqs':
                continue
            try:
                communicator.send_driver_completion(jobnik_session)
                self.__add_to_redis(jobnik_session, communicator.service_name, message_id)
            except:
                pass

    def send_progress_indication(self, identifier, total_tasks_count, failed, jobnik_session):
        message_id = uuid.uuid4()
        self.__add_to_redis(jobnik_session, 'general', message_id)
        for communicator in self.__communicators:
            if communicator.service_name != 'sqs':
                continue
            try:
                communicator.send_progress_indication(identifier, total_tasks_count, failed, jobnik_session)
                self.__add_to_redis(jobnik_session, communicator.service_name, message_id)
            except:
                pass