import logging

from source.aws.client import ReconnectingAWSClient
from source.task_queues.publishers.interface import TaskQueuePublisherInterface
from source.task_runner.tasks.interface import TaskInterface


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


class SQSTaskQueuePublisher(TaskQueuePublisherInterface):
    mechanism_name = 'sqs'

    def __init__(self, environment=None, feature_flags=None):
        self.__environment = environment
        self.__feature_flags = feature_flags if feature_flags is not None else {}
        self.__client = ReconnectingAWSClient('sqs')
        self.__logger = logging.getLogger('endor')

    def publish_tasks(self, tasks, topic_name, is_immediate=False):
        """
        @type topic_name: C{str}
        @type tasks: C{list} of L{TaskInterface}
        @type is_immediate: C{bool}
        """
        if self.__environment is not None:
            topic_name = '{env}-{queue}-{priority}'.format(env=self.__environment, queue=topic_name,
                                                           priority='high' if is_immediate else 'low')
            self.__publish_message(tasks, topic_name)

    def __publish_message(self, tasks, topic_name):
        """
        @type topic_name: C{str}
        @type tasks: C{list} of L{TaskInterface}
        """
        serialization_mode = self.__feature_flags.get('task_serialization_mode', 'json')
        queue_url = self.__client.get_queue_url(QueueName=topic_name)['QueueUrl']
        messages = [
            {
                'MessageBody': task.serialize(serialization_mode),
                'MessageAttributes': {
                    'serializationMode': {'StringValue': serialization_mode, 'DataType': 'String'}
                }
            }
            for task in tasks]
        for chunk in chunks(messages, 10):
            for idx, message in enumerate(chunk):
                message['Id'] = str(idx)
            self.__client.send_message_batch(QueueUrl=queue_url, Entries=chunk)