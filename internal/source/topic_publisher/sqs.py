import copy
import json
from datetime import datetime
from uuid import uuid4

import boto3

from interface import TopicPublisherInterface


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")


class SQSTopicPublisher(TopicPublisherInterface):
    def __init__(self):
        super(SQSTopicPublisher, self).__init__('sqs')
        self.__sqs_client = boto3.client('sqs', region_name='us-east-1')

    def publish_dict(self, topic_name, dict_message):
        cloned_message = copy.deepcopy(dict_message)
        if 'messageId' not in cloned_message:
            cloned_message['messageId'] = uuid4().get_hex()
        queue_url = self.__sqs_client.get_queue_url(QueueName=topic_name)['QueueUrl']
        self.__sqs_client.send_message(QueueUrl=queue_url,
                                       MessageBody=json.dumps(dict_message, default=json_serial, allow_nan=False))
