import os

from mock import Mock
from redis import Redis

from source import BASE_ES_URI
from source.jobnik_communicator.es_communicator import ElasticsearchJobnikCommunicator
from source.jobnik_communicator.interface import MultipleJobnikCommunicator
from source.jobnik_communicator.topic_communicator import JobnikTopicCommunicator
from source.storage.io_handlers.s3 import S3IOHandler
from source.storage.stores.artifact_store.elasticsearch_store import ElasticsearchArtifactStore
from source.storage.stores.artifact_store.in_memory import InMemoryArtifactStore
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface, MultiStoresArtifactStore
from source.storage.stores.artifact_store.pubsub_artifact_store import TopicArtifactStore
from source.task_queues.publishers.sqs_publisher import SQSTaskQueuePublisher
from source.task_queues.subscribers.interface import TaskQueueSubscriberInterface
from source.task_queues.subscribers.sqs_subscriber import SQSTaskQueueSubscriber
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.runner import TaskRunner
from source.task_runner.stop_notification_reader.interface import StopNotificationReaderInterface
from source.task_runner.stop_notification_reader.redis_based import RedisBasedStopNotificationReader
from source.task_runner.submitter_interface import QueueBasedTaskSubmitter
from source.task_runner.task_status_store.interface import TaskStatusStoreInterface
from source.task_runner.task_status_store.redis_store import RedisTaskStatusStore
from source.topic_publisher.sqs import SQSTopicPublisher
from source.utils.context_handling.context_provider import ContextProvider
from source.utils.random_seed_provider import RandomSeedProvider


class TaskRunnerBootstrapper(object):
    @staticmethod
    def create_task_runner_from_external_dependencies(tasks_subscriber, task_status_store, artifact_store,
                                                      jobnik_communicator, context_holder,
                                                      stop_notification_reader, instance_protector,
                                                      random_seed_provider, *handlers):
        """
        @type instance_protector: L{SpotinstInstanceProtector}
        @type context_holder: L{ContextProvider}
        @type jobnik_communicator: L{JobnikCommunicatorInterface}
        @type stop_notification_reader: L{StopNotificationReaderInterface}
        @type tasks_subscriber: L{TaskQueueSubscriberInterface}
        @type task_status_store: L{TaskStatusStoreInterface}
        @type artifact_store: L{ArtifactStoreInterface}
        @type random_seed_provider: L{RandomSeedProvider}
        @type handlers: C{list} of L{TaskHandlerInterface}
        """
        return TaskRunner(tasks_subscriber, task_status_store, artifact_store, jobnik_communicator,
                          context_holder, stop_notification_reader, instance_protector, random_seed_provider, *handlers)

    @classmethod
    def __get_queueing_mechanism(cls, environment_name, queue_name):
        return SQSTaskQueueSubscriber('{env}-{queue}'.format(env=environment_name, queue=queue_name))

    # TODO (izik): Initialize a JobnikCommunicator and MultiArtifactStore
    # TODO (izik): Add flag indicating whether to use Jobnik (Proxy?)
    # TODO (izik): Change TaskRunner __init__ to accept JobnikCommunicator and add under run() the usage of the Jobnik
    @classmethod
    def create_prod_task_runner(cls, redis_host, environment_name, queue_name, is_writer, all_handlers=False):
        io_handler = S3IOHandler()
        tasks_subscriber = cls.__get_queueing_mechanism(environment_name, queue_name)
        context_holder = ContextProvider()

        redis_driver = Redis(redis_host)
        task_status_store = RedisTaskStatusStore(redis_driver)
        artifact_store = MultiStoresArtifactStore()
        random_seed_provider = RandomSeedProvider()
        if is_writer:
            artifact_store.add_store(ElasticsearchArtifactStore([BASE_ES_URI]))
            artifact_store.add_store(TopicArtifactStore(context_holder, SQSTopicPublisher()))
            # task_status_store.is_task_done = Mock(name='is_task_done', return_value=False)
            # task_status_store.mark_task_as_done = Mock(name='mark_task_as_done')
        else:
            artifact_store.add_store(InMemoryArtifactStore())
            io_handler.save_raw_data = Mock()
            io_handler.store_folder_contents = Mock()
        matlab_found = os.path.isdir('/usr/local/MATLAB')
        task_submitter = QueueBasedTaskSubmitter(SQSTaskQueuePublisher(environment_name, {}))
        handlers = []

        if matlab_found or all_handlers:
            from source.data_ingestion.clusters_extraction.bootstrap import build_prod_clusters_extractor
            clusters_extractor = build_prod_clusters_extractor(artifact_store, io_handler)

            from source.data_ingestion.clusters_unification.clusters_unifier import build_prod_clusters_unifier
            clusters_unifier = build_prod_clusters_unifier(artifact_store, io_handler)

            from source.data_ingestion.clusters_extraction_tasks_dispatch_bootstrap \
                import prod_bootstrap as clusters_extraction_tasks_dispatch_bootstrap
            clusters_extraction_tasks_dispatch_handler = \
                clusters_extraction_tasks_dispatch_bootstrap(io_handler, artifact_store, task_submitter)

            handlers += [clusters_extractor, clusters_unifier, clusters_extraction_tasks_dispatch_handler]
        if not matlab_found or all_handlers:
            from source.query.merger.merger_bootstrap import build_prod_scores_merger
            scores_merger = build_prod_scores_merger(artifact_store, io_handler, random_seed_provider)

            from source.query.ml_phase.ml_bootstrap import build_prod_score_assigner
            scores_assigner = build_prod_score_assigner(artifact_store, io_handler, context_holder,
                                                        random_seed_provider)

            from source.query.ml_phase.dispatch.bootstrap import prod_bootstrap as score_assigner_dispatch_bootstrap
            score_assigner_dispatch_handler = score_assigner_dispatch_bootstrap(io_handler, artifact_store,
                                                                                task_submitter)

            from source.query.merger.dispatch.bootstrap import prod_bootstrap as mergers_dispatch_bootstrap
            mergers_dispatch_handler = mergers_dispatch_bootstrap(io_handler, artifact_store, task_submitter)

            from source.data_preview.data_preview_runner import prod_bootstrap as preview_bootstrap
            data_preview_handler = preview_bootstrap(io_handler, artifact_store)

            from source.data_ingestion.clusters_footprint_analyzer.footprint_analyzer_runner \
                import prod_bootstrap as footprint_analyzer_bootstrap
            footprint_analyzer_handler = footprint_analyzer_bootstrap(io_handler, artifact_store)

            from source.query.reporter.bootstrap import prod_bootstrap as reporter_bootstrap
            report_generation_handler = reporter_bootstrap(io_handler, artifact_store)

            handlers += [scores_assigner, scores_merger, data_preview_handler, footprint_analyzer_handler,
                         score_assigner_dispatch_handler, mergers_dispatch_handler, report_generation_handler]

        sqs_jobnik_communicator = JobnikTopicCommunicator(SQSTopicPublisher())
        es_jobnik_communicator = ElasticsearchJobnikCommunicator([BASE_ES_URI])
        jobnik_communicator = MultipleJobnikCommunicator([sqs_jobnik_communicator, es_jobnik_communicator], True)
        instance_protector = SpotinstInstanceProtector()

        return cls.create_task_runner_from_external_dependencies(tasks_subscriber, task_status_store, artifact_store,
                                                                 jobnik_communicator, context_holder,
                                                                 RedisBasedStopNotificationReader(redis_driver),
                                                                 instance_protector, random_seed_provider, *handlers)
