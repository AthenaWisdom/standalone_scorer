from Queue import Queue

from source.task_queues.publishers.interface import TaskQueuePublisherInterface


class InMemoryTaskQueuePublisher(TaskQueuePublisherInterface):
    mechanism_name = 'memory'

    def __init__(self):
        self.internal_queue = Queue()

    def publish_tasks(self, tasks, topic_id, is_immediate=False):
        for task in tasks:
            self.internal_queue.put(task)

