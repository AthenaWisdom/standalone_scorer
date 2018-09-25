from source.task_runner.task_status_store.types import TaskKey
from source.utils.run_repeatedly import RunRepeatedly


class TaskStatusStoreInterface(object):
    def mark_task_as_done(self, task_key):
        """
        @type task_key: L{TaskKey}
        """
        raise NotImplementedError()

    def is_task_done(self, task_key):
        """
        @type task_key: L{TaskKey}
        @rtype: C{bool}
        """
        raise NotImplementedError()

    def increment_try_count(self, task_key):
        """
        @type task_key: L{TaskKey}
        @return: The updated try count
        @rtype: C{int}
        """
        raise NotImplementedError()

    def get_try_count(self, task_key):
        """
        @type task_key: L{TaskKey}
        @return: The try count
        @rtype: C{int}
        """
        raise NotImplementedError()

    def acquire_task(self, task_key):
        """
        @type task_key: L{TaskKey}
        @rtype: L{RunRepeatedly}
        """
        raise NotImplementedError()

    def notify_attempting_task(self, task):
        """
        @type task: L{TaskInterface}
        """
        raise NotImplementedError()

    def notify_task_attempted(self, task):
        """
        @type task: L{TaskInterface}
        """
        raise NotImplementedError()

    def is_task_aborted(self, env, job_id):
        """
        @type env: C{str}
        @type job_id: C{str}
        @rtype: C{bool}
        """
        raise NotImplementedError()

    def get_all_finished_tasks(self, dummy_task_key):
        """
        @param dummy_task_key: A task key that conforms to the job
        @type dummy_task_key: L{TaskKey}
        """
        raise NotImplementedError()
