import logging
from datetime import datetime

import os
from redis import Redis

from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.topic_publisher.interface import TopicPublisherInterface

PUBSUB_BLACKLISTED_ARTIFACTS = {
    'dataset_properties',
    'score_assigner_pricing_duration_metric',
    'merger_pricing_duration_metric'
}


class TopicArtifactStore(ArtifactStoreInterface):
    def __init__(self, context_provider, message_publisher, with_redis=True):
        """
        @type message_publisher: L{TopicPublisherInterface}
        """
        super(TopicArtifactStore, self).__init__(message_publisher.service_name)
        self.__context_provider = context_provider
        self.__message_publisher = message_publisher
        self.__redis_client = Redis(os.environ['MESSAGE_REDIS_HOST']) if with_redis else None
        self.__logger = logging.getLogger('endor')

    def __get_topic_name_for_context(self, context):
        return '{}-artifacts-eventbus'.format(context.jobnik_role)

    def __add_to_redis(self, jobnik_session, service, message_id):
        if None in (jobnik_session, self.__redis_client):
            return

        set_name = '{}-{}-{}-artifacts-producer'.format(jobnik_session.jobnik_role,
                                                        jobnik_session.job_token['jobId'],
                                                        service)
        self.__redis_client.sadd(set_name.format(), message_id)

    def store_artifact(self, artifact):
        if artifact.type in PUBSUB_BLACKLISTED_ARTIFACTS:
            return
        try:
            context = self.__context_provider.get_current_context()
            if context is None or context.jobnik_session is None:
                self.__logger.warn('No context was defined, will not send artifacts to pubsub.')
                return
            self.__add_to_redis(context.jobnik_session, 'general', artifact.message_id)

            message = {
                'job_token': context.jobnik_session.job_token,
                'messageId': artifact.message_id,
                'publish_date': datetime.utcnow(),
                'type': artifact.type,
                'artifact': artifact.to_dict()
            }
            topic_name = self.__get_topic_name_for_context(context.jobnik_session)
            message_id = self.__message_publisher.publish_dict(topic_name, message)
            self.__add_to_redis(context.jobnik_session, self.service_name, artifact.message_id)
            extra = dict(pubsub="jobnik-artifacts", artifact_type=artifact.type,
                         jobnik_session=context.jobnik_session.to_dict(), message_id=message_id)
            self.__logger.info("Sent artifact to Pub/Sub", extra=extra)
        except:
            self.__logger.error("failed to publish artifact to pubsub publisher", exc_info=True)


