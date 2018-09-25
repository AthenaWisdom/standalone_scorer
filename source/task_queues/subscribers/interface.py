import logging
from datetime import datetime

import pytz
from elasticsearch import Elasticsearch, exceptions
from functional import seq

from source import BASE_ES_URI
from source.aws.client import ReconnectingAWSClient
from source.storage.stores.artifact_store.types.common import QueueLagsArtifact


class TaskQueueSubscriberInterface(object):
    CLOUDWATCH_METRIC_TEMPLATE = {
        'MetricName': 'TaskProcessTime',
        'Unit': 'Seconds'
    }

    def __init__(self, should_send_metrics):
        """
        @param should_send_metrics: Should the subscriber send CW metrics 
        @type should_send_metrics: C{bool}
        """
        self.__cloudwatch_client = ReconnectingAWSClient('cloudwatch') if should_send_metrics else None
        self.__connect_to_es()
        self._logger = logging.getLogger('endor')

    def __connect_to_es(self):
        self.__elasticsearch_driver = Elasticsearch([BASE_ES_URI])

    def get(self):
        raise NotImplementedError()

    def __send_cloudwatch_metric(self, lag_artifact):
        """
        @type lag_artifact: L{QueueLagsArtifact}
        """
        if self.__cloudwatch_client is None:
            return

        metric_data = self.CLOUDWATCH_METRIC_TEMPLATE.copy()
        metric_data['Value'] = lag_artifact.time_to_process
        dimensions = {
            'Environment': lag_artifact.environment,
            'TaskClass': lag_artifact.task_class,
        }
        metric_data['Dimensions'] = seq(dimensions.iteritems())\
            .map(lambda x: {'Name': x[0], 'Value': x[1]})\
            .to_list()
        metric_data['Timestamp'] = datetime.utcnow().replace(tzinfo=pytz.utc)
        self.__cloudwatch_client.put_metric_data(
            Namespace='Endor/Custom',
            MetricData=[metric_data]
        )

    def _store_lag_artifact(self, lag_artifact):
        """
        @type lag_artifact: L{QueueLagsArtifact}
        """
        try:
            self.__send_cloudwatch_metric(lag_artifact)
            self._logger.info('Sent CloudWatch metric')
        except:
            self._logger.warning('Could not send CloudWatch metric', exc_info=True)
        entity = lag_artifact.to_dict()
        entity['time_sent'] = datetime.utcnow().replace(tzinfo=pytz.utc)
        for i in xrange(5):
            try:
                es_index = 'lag_artifacts_' + datetime.now().strftime('%Y-%m')
                self.__elasticsearch_driver.index(es_index, lag_artifact.type, entity)
                return
            except (exceptions.ConnectionTimeout, exceptions.TransportError):
                if i == 4:
                    self.__connect_to_es()
