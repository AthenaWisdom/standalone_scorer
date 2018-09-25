from collections import defaultdict

from source.task_runner.task_status_store.interface import TaskStatusStoreInterface
from source.task_runner.task_status_store.types import TaskKey
from source.task_runner.tasks.interface import TaskInterface
from source.utils.run_repeatedly import nop


class InMemoryTaskStatusStore(TaskStatusStoreInterface):
    def acquire_task(self, task_key):
        nop()

    def __init__(self):
        self.__sets = defaultdict(set)
        self.__dicts = defaultdict(lambda: defaultdict(lambda: 0))
        self.__aborted_job_keys = set()

    @staticmethod
    def __get_finished_set_name(task_key):
        return '{}_finished'.format(task_key.job_id)

    @staticmethod
    def __get_tries_hash_name(task_key):
        return '{}_tries'.format(task_key.job_id)

    def mark_task_as_done(self, task_key):
        """
        @type task_key: L{TaskKey}
        """
        set_name = self.__get_finished_set_name(task_key)
        self.__sets[set_name].add(task_key.task_ordinal)

    def is_task_done(self, task_key):
        """
        @type task_key: L{TaskKey}
        @rtype: C{bool}
        """
        set_name = self.__get_finished_set_name(task_key)
        return task_key.task_ordinal in self.__sets[set_name]

    def increment_try_count(self, task_key):
        """
        @type task_key: L{TaskKey}
        """
        hash_name = self.__get_tries_hash_name(task_key)
        self.__dicts[hash_name][task_key.task_ordinal] += 1
        return self.__dicts[hash_name][task_key.task_ordinal]

    def get_try_count(self, task_key):
        """
        @type task_key: L{TaskKey}
        @rtype: C{int}
        """
        hash_name = self.__get_tries_hash_name(task_key)
        return self.__dicts[hash_name][task_key.task_ordinal]

    def is_task_aborted(self, env, job_id):
        """
        @type env: C{str}
        @type job_id: C{str}
        @rtype: C{bool}
        """
        return '{}-{}-aborted-job'.format(env, job_id) in self.__aborted_job_keys

    def add_aborted_job_key(self, env, job_id):
        """
        @type env: C{str}
        @type job_id: C{str}
        """
        self.__aborted_job_keys.add('{}-{}-aborted-job'.format(env, job_id))

    def notify_attempting_task(self, task):
        """
        @type task: L{TaskInterface}
        """
        running_tasks = self.__dicts['running_tasks']
        running_tasks['job_{}_attempt_{}'.format(task.job_id, task.task_ordinal)] = task
        running_jobs = self.__dicts['running_jobs']
        running_jobs['job_{}'.format(task.job_id)] = task

    def notify_task_attempted(self, task):
        """
        @type task: L{TaskInterface}
        """
        running_tasks = self.__dicts['running_tasks']
        del running_tasks['job_{}_attempt_{}'.format(task.job_id, task.task_ordinal)]
        running_jobs = self.__dicts['running_jobs']
        del running_jobs['job_{}'.format(task.job_id)]

    def get_all_finished_tasks(self, dummy_task_key):
        """
        @type dummy_task_key: L{TaskKey}
        @rtype: C{bool}
        """
        set_name = self.__get_finished_set_name(dummy_task_key)
        return self.__sets[set_name]
