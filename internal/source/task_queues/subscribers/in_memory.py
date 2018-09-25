from contextlib import contextmanager

from source.task_queues.subscribers.interface import TaskQueueSubscriberInterface


class InMemoryAcknowledgableTaskQueueSubscriber(TaskQueueSubscriberInterface):
    def __init__(self, internal_queue):
        super(InMemoryAcknowledgableTaskQueueSubscriber, self).__init__(False)
        self.internal_queue = internal_queue

    @contextmanager
    def get(self):
        yield self.internal_queue.get(), lambda: None
