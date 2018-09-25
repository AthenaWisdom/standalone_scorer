class TaskHandlerInterface(object):
    def handle_task(self, task):
        raise NotImplementedError()

    @staticmethod
    def get_task_type():
        """
        @rtype: C{type}
        """
        raise NotImplementedError()
