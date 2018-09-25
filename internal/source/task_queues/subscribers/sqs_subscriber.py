import os
from contextlib import contextmanager
from datetime import datetime

import pytz

from source.aws.client import ReconnectingAWSClient
from source.storage.stores.artifact_store.types.common import QueueLagsArtifact
from source.task_queues.subscribers.interface import TaskQueueSubscriberInterface
from source.task_runner.tasks.interface import TaskInterface
from source.utils.run_repeatedly import RunRepeatedly, nop

DEFAULT_ACK_DEADLINE_IN_SECONDS = 60

GET_MESSAGE_WAIT_LIMIT = 2
RESOURCE_ALREADY_EXISTS_RESPONSE_CODE = 409
DEFAULT_MESSAGE_ATTRIBUTES = {
    'serialization':  {
        'DataType': 'String',
        'StringValue': 'json'
    }
}


class SQSTaskQueueSubscriber(TaskQueueSubscriberInterface):
    def __init__(self, queue_name):
        super(SQSTaskQueueSubscriber, self).__init__(True)
        self.__client = ReconnectingAWSClient('sqs', 'us-east-1')
        self.__queue_urls = [
            self.__client.get_queue_url(QueueName=queue_name + "-high")['QueueUrl'],
            self.__client.get_queue_url(QueueName=queue_name + "-low")['QueueUrl'],
        ]
        self.__environment_name = os.environ.get('ENVIRONMENT_NAME', 'unknown')

    def get_change_visibility_context_manager(self, queue_url, receipt_handle):
        return RunRepeatedly(self.__client.change_message_visibility,
                             DEFAULT_ACK_DEADLINE_IN_SECONDS / 3,
                             QueueUrl=queue_url,
                             ReceiptHandle=receipt_handle,
                             VisibilityTimeout=DEFAULT_ACK_DEADLINE_IN_SECONDS)

    @contextmanager
    def get(self):
        task = None
        for queue_url in self.__queue_urls:
            modify_deadline_cm, task, publish_time, receipt_handle = self.__get_task(queue_url)
            if task is not None:
                pulled_at = datetime.utcnow().replace(tzinfo=pytz.utc)
                try:
                    with modify_deadline_cm:
                        yield task, lambda: self.__client.delete_message(QueueUrl=queue_url,
                                                                         ReceiptHandle=receipt_handle)
                finally:
                    created_at = publish_time.replace(tzinfo=pytz.utc)
                    time_in_queue = (pulled_at - created_at).total_seconds()
                    time_to_process = (datetime.utcnow().replace(tzinfo=pytz.utc) - pulled_at).total_seconds()
                    run_identifier = task.customer.split('-')[-1]
                    self._store_lag_artifact(QueueLagsArtifact(run_identifier, task.customer, task.execution_id,
                                                               task.task_ordinal, time_in_queue, time_to_process,
                                                               task.__class__.__name__, self.__environment_name))
                break

        if task is None:
            yield None

    def __get_task(self, queue_url):
        response = self.__client.receive_message(QueueUrl=queue_url,
                                                 AttributeNames=['VisibilityTimeout', 'SentTimestamp'],
                                                 MessageAttributeNames=['serialization'],
                                                 WaitTimeSeconds=GET_MESSAGE_WAIT_LIMIT)
        received_messages = response.get('Messages')
        if received_messages is None:
            self._logger.debug('No message received on queue {} '
                                'after {} seconds'.format(queue_url, GET_MESSAGE_WAIT_LIMIT))
            return nop(), None, None, None

        self._logger.debug('Got message on queue {} '
                            'in {} seconds'.format(queue_url, GET_MESSAGE_WAIT_LIMIT))
        message = received_messages.pop()
        serialization_mode = message\
            .get('MessageAttributes', DEFAULT_MESSAGE_ATTRIBUTES)\
            .get('serialization', DEFAULT_MESSAGE_ATTRIBUTES['serialization'])\
            .get('StringValue', DEFAULT_MESSAGE_ATTRIBUTES['serialization']['StringValue'])
        publish_time = datetime.utcfromtimestamp(int(message['Attributes']['SentTimestamp']) / 1000.0)
        receipt_handle = message['ReceiptHandle']

        try:
            task = TaskInterface.deserialize(message['Body'], serialization_mode)
        except (KeyError, IndexError):
            self._logger.error('Got bad message on queue {}'.format(queue_url), extra={
                'actual_message': message['Body']
            })
            return nop(), None, None, None

        change_visibility_cm = self.get_change_visibility_context_manager(queue_url, receipt_handle)
        return change_visibility_cm, task, publish_time, receipt_handle
