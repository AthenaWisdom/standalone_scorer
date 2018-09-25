from source.task_queues.publishers.in_memory import InMemoryTaskQueuePublisher
from source.task_queues.publishers.interface import TaskQueuePublisherInterface
from source.task_queues.subscribers.in_memory import InMemoryAcknowledgableTaskQueueSubscriber
from source.task_runner.tasks.interface import TaskInterface


class TaskSubmitterInterface(object):
    def submit_tasks(self, job_id, job_type, tasks, is_immediate=False):
        """
        @param is_immediate: Should the task be processes ASAP as opposed to waiting in the queue
        @type is_immediate: C{bool}
        @type job_id: C{str}
        @type job_type: C{str}
        @type tasks: C{list} of L{TaskInterface}
        @type stage: C{int}
        """
        raise NotImplementedError()


class QueueBasedTaskSubmitter(TaskSubmitterInterface):
    JOB_TYPE_TO_QUEUE_NAME = {
        'assign_scores': 'ml-tasks',
        'merge_scores': 'ml-tasks',
        'data_preview': 'ml-tasks',
        'footprint_analyzing': 'ml-tasks',
        'extract_clusters': 'clusters-tasks',
        'unify_clusters': 'clusters-tasks',
    }

    def __init__(self, task_publisher):
        """
        @type task_publisher: L{TaskQueuePublisherInterface}
        """
        self.__task_publisher = task_publisher

    def submit_tasks(self, job_id, job_type, tasks, is_immediate=False):
        self.__task_publisher.publish_tasks(tasks, self.JOB_TYPE_TO_QUEUE_NAME[job_type], is_immediate)


class LocalTaskSubmitter(TaskSubmitterInterface):
    def __init__(self, task_runner_generator):
        """
        @type task_runner_generator: C{function}
        """
        self.__task_publisher = InMemoryTaskQueuePublisher()
        subscriber = InMemoryAcknowledgableTaskQueueSubscriber(self.__task_publisher.internal_queue)
        self.__task_runner = task_runner_generator(subscriber, self)

    def submit_tasks(self, job_id, job_type, tasks, is_immediate=False):
        self.__task_publisher.publish_tasks(tasks, job_id, is_immediate)
        self.__task_runner.run_for_k_tasks(len(tasks))


class MultipleSubmittersTaskSubmitter(TaskSubmitterInterface):
    def __init__(self):
        self.__submitters = []

    def submit_tasks(self, job_id, job_type, tasks, is_immediate=False):
        for submitter in self.__submitters:
            submitter.submit_tasks(job_id, job_type, tasks, is_immediate)

    def add_submitter(self, submitter):
        """
        @type submitter: L{TaskSubmitterInterface}
        """
        self.__submitters.append(submitter)

    def remove_submitter(self, submitter):
        """
        @type submitter: L{TaskSubmitterInterface}
        """
        self.__submitters.remove(submitter)


class ExceptionSwallowingTaskSubmitter(TaskSubmitterInterface):
    def __init__(self, real_submitter):
        """
        @type real_submitter: L{TaskSubmitterInterface)
        """
        self.__real_submitter = real_submitter

    def submit_tasks(self, job_id, job_type, tasks, is_immediate=False):
        # noinspection PyBroadException
        try:
            self.__real_submitter.submit_tasks(job_id, job_type, tasks, is_immediate)
        except:
            pass
