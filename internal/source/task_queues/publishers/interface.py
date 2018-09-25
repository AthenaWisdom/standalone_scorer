class TaskQueuePublisherInterface(object):
    mechanism_name = 'none'

    def publish_tasks(self, tasks, topic_id, is_immediate=False):
        """
        @param is_immediate: Should the task be processes ASAP as opposed to waiting in the queue
        @type is_immediate: C{bool}
        @type topic_id: C{str}
        @type tasks: C{list}
        """
        raise NotImplementedError()

