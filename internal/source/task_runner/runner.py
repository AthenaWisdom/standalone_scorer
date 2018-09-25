import logging
import time
import traceback
from threading import Event, Thread

from handler_interface import TaskHandlerInterface
from source.jobnik_communicator.interface import JobnikCommunicatorInterface
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types import ExceptionArtifact
from source.task_queues.subscribers.interface import TaskQueueSubscriberInterface
from source.task_runner.stop_notification_reader.interface import StopNotificationReaderInterface
from source.task_runner.task_status_store.interface import TaskStatusStoreInterface
from source.task_runner.tasks.clusters_extaction import ClustersExtractionTask
from source.task_runner.tasks.clusters_unification import ClustersUnificationTask
from source.task_runner.tasks.interface import TaskInterface
from source.utils.configure_logging import reconfigure_logger
from source.utils.context_handling.context_provider import ContextProvider
from source.utils.random_seed_provider import RandomSeedProvider

FATAL_EXCEPTIONS = {

}


class TaskRunner(object):
    def __init__(self, task_subscriber, task_status_store, artifact_store, jobnik_communicator,
                 context_holder, stop_notification_reader, instance_protector, random_seed_provider, *handlers):
        """
        @type stop_notification_reader: L{StopNotificationReaderInterface}
        @type context_holder: L{ContextProvider}
        @type jobnik_communicator: L{JobnikCommunicatorInterface}
        @type artifact_store: L{ArtifactStoreInterface}
        @type task_status_store: L{TaskStatusStoreInterface}
        @type task_subscriber: L{TaskQueueSubscriberInterface}
        @type instance_protector: L{SpotinstInstanceProtector}
        @type random_seed_provider: L{RandomSeedProvider}
        @type handlers: C{list} of L{TaskHandlerInterface}
        """
        self.__instance_protector = instance_protector
        self.__stop_notification_reader = stop_notification_reader
        self.__context_holder = context_holder
        self.__jobnik_communicator = jobnik_communicator
        self.__artifact_store = artifact_store
        self.__task_status_store = task_status_store
        self.__task_subscriber = task_subscriber
        self.__random_seed_provider = random_seed_provider
        self.__handlers = {handler.get_task_type(): handler for handler in handlers}
        self.__stop_event = Event()
        self.__run_thread = None
        self.__logger = logging.getLogger('endor')
        self.__tasks_counter = 0

    def __should_run(self, number_of_tasks_to_process=None):
        if self.__stop_event.is_set():
            self.__logger.info("Stopping because of event")
            return False
        if number_of_tasks_to_process is not None:
            if self.__tasks_counter >= number_of_tasks_to_process:
                self.__logger.info("Stopping because of num tasks")
                return False
        if self.__stop_notification_reader.should_stop():
            self.__logger.info("Stopping because of notification")
            return False
        return True

    def run(self, number_of_tasks_to_process=None):
        self.__stop_event.clear()
        self.__tasks_counter = 0
        self.__logger.info('Starting to get tasks..')
        while self.__should_run(number_of_tasks_to_process):
            with self.__task_subscriber.get() as task_and_ack:
                if task_and_ack is None:
                    self.__logger.info('No available tasks found.')
                    continue
                self.__logger.info('found task on sqs')
                self.__handle_single_task(*task_and_ack)

    def __handle_single_task(self, task, ack_func):
        """
        @type task: L{TaskInterface}
        @type ack_func: C{LambdaType}
        """
        extra_data = task.get_context()
        extra_data['replication_controller'] = True
        reconfigure_logger(self.__logger, extra=extra_data)
        self.__logger.debug('Got task {}'.format(task.get_key()))
        self.__context_holder.set_current_context(task.jobnik_session, task.feature_flags)

        # When a job or its blueprint are aborted, tasks should be not be run, but they should be ack'd
        environment = task.jobnik_session.jobnik_role if task.jobnik_session is not None else 'tests'

        if self.__task_status_store.is_task_aborted(environment, task.job_id):
            self.__logger.info('Task is for an aborted job {} at {}. Skipping it.'.format(task.job_id, environment))
            self.__tasks_counter += 1
            ack_func()
            return

        if self.__task_status_store.is_task_done(task.get_key()):
            self.__logger.warning('Task already marked as done.')
            self.__tasks_counter += 1
            ack_func()
            return

        self.__random_seed_provider.reset_to_seed(task.task_seed)
        try:
            if type(task) in (ClustersExtractionTask, ClustersUnificationTask):
                acquired_lock = self.__task_status_store.acquire_task(task.get_key())
                self.__logger.debug('Acquired lock - {}'.format(acquired_lock is not None))
                if acquired_lock is not None:
                    with acquired_lock, self.__instance_protector.protect_me():
                        self.__attempt_task(task)
                else:
                    return
            else:
                self.__logger.info('notifying task start.')
                self.__attempt_task(task)
            if task.jobnik_session is not None:
                self.__jobnik_communicator.send_progress_indication(str(task.task_ordinal),
                                                                    task.total_num_tasks,
                                                                    False, task.jobnik_session)
            ack_func()
            self.__logger.debug('Acked task {}'.format(task.get_key()))
            self.__task_status_store.mark_task_as_done(task.get_key())
            self.__logger.debug('Marked task as done {}'.format(task.get_key()))
        except Exception as ex:
            task_tries = self.__task_status_store.get_try_count(task.get_key())
            if task_tries < 2:
                self.__logger.warning('Exception caught in main', exc_info=True, extra={
                    'task_tries': task_tries
                })
                # noinspection PyBroadException
                try:
                    exception_artifact = ExceptionArtifact(execution_id=task.execution_id, msg=ex.message,
                                                           traceback=traceback.format_exc(), config={},
                                                           final_retry=False,
                                                           **task.get_context())
                    self.__artifact_store.store_artifact(exception_artifact)
                except:
                    self.__logger.error('Failed to send exception artifact on a non final retry.', exc_info=True)
            else:
                self.__logger.error('Exception caught in main', exc_info=True)
                exception_artifact = ExceptionArtifact(execution_id=task.execution_id, msg=ex.message,
                                                       traceback=traceback.format_exc(), config={}, final_retry=True,
                                                       **task.get_context())
                self.__artifact_store.store_artifact(exception_artifact)
                self.__mark_task_failure(ack_func, task)
            raise
        finally:
            self.__tasks_counter += 1
            self.__context_holder.release_context()

    def __attempt_task(self, task):
        self.__task_status_store.increment_try_count(task.get_key())
        self.__task_status_store.notify_attempting_task(task)
        self.__logger.debug('Notified task attempt started {}'.format(task.get_key()))

        try:
            self.__handlers[type(task)].handle_task(task)
        finally:
            self.__task_status_store.notify_task_attempted(task)
            self.__logger.debug('Notified task attempt completed {}'.format(task.get_key()))

    def __mark_task_failure(self, ack_func, task):
        if task.jobnik_session is not None:
            self.__jobnik_communicator.send_progress_indication(str(task.task_ordinal),
                                                                task.total_num_tasks,
                                                                True, task.jobnik_session)
        ack_func()
        self.__logger.debug('Acked task {}'.format(task.get_key()))
        self.__task_status_store.mark_task_as_done(task.get_key())
        self.__logger.debug('Marked task as failed {}'.format(task.get_key()))

    def start(self):
        self.__run_thread = Thread(target=self.run)
        self.__run_thread.start()

    def stop(self, signal, frame):
        self.__logger.info("Stopped with signal {}".format(signal))
        self.__stop_event.set()

    def run_for_k_tasks(self, number_of_tasks_to_process):
        self.run(number_of_tasks_to_process)

    def wait_for_completion(self):
        while self.__run_thread.is_alive():
            time.sleep(1)
        self.__run_thread.join()
