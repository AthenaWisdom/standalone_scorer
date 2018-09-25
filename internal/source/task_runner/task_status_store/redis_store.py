import functools
import json

from redis import Redis

from source.task_runner.task_status_store.interface import TaskStatusStoreInterface
from source.task_runner.task_status_store.types import TaskKey
from source.task_runner.tasks.interface import TaskInterface
from source.utils.run_repeatedly import RunRepeatedly


class RedisTaskStatusStore(TaskStatusStoreInterface):
    TASK_ACQUIRED_VALUE = "acquired"
    TASK_ACQUIRE_TTL = 300

    def __init__(self, redis_driver):
        """
        @type redis_driver: C{Redis}
        """
        self.__redis_driver = redis_driver

    @staticmethod
    def __get_finished_set_name(task_key):
        return '{}_finished'.format(task_key.job_id)

    @staticmethod
    def __get_tries_hash_name(task_key):
        return '{}_tries'.format(task_key.job_id)

    @staticmethod
    def __get_lock_key_name(task_key):
        return '{}_{}_lock'.format(task_key.job_id, task_key.task_ordinal)

    @staticmethod
    def __get_unique_task_key(task):
        """
        @type task: L{TaskInterface}
        """
        return '{}-{}'.format(task.job_id, task.task_ordinal)

    @staticmethod
    def __get_task_environment(task):
        return task.jobnik_session.jobnik_role if task.jobnik_session is not None else 'tests'

    def mark_task_as_done(self, task_key):
        """
        @type task_key: L{TaskKey}
        """
        # TODO: This key is never removed
        set_name = self.__get_finished_set_name(task_key)
        self.__redis_driver.sadd(set_name, task_key.task_ordinal)

    def is_task_done(self, task_key):
        """
        @type task_key: L{TaskKey}
        @rtype: C{bool}
        """
        set_name = self.__get_finished_set_name(task_key)
        return self.__redis_driver.sismember(set_name, task_key.task_ordinal)

    def increment_try_count(self, task_key):
        """
        @type task_key: L{TaskKey}
        """
        hash_name = self.__get_tries_hash_name(task_key)
        # TODO: This key is never removed
        return self.__redis_driver.hincrby(hash_name, task_key.task_ordinal)

    def get_try_count(self, task_key):
        """
        @type task_key: L{TaskKey}
        @rtype: C{int}
        """
        hash_name = self.__get_tries_hash_name(task_key)
        num_tries = self.__redis_driver.hget(hash_name, task_key.task_ordinal)
        return int(num_tries) if num_tries is not None else 0

    def acquire_task(self, task_key):
        """
        @type task_key: L{TaskKey}
        @rtype: C{None} Or L{RunRepeatedly}
        """
        key_name = self.__get_lock_key_name(task_key)
        if self.__redis_driver.set(key_name, self.TASK_ACQUIRED_VALUE,
                                   ex=self.TASK_ACQUIRE_TTL, nx=True):
            refresh_key = functools.partial(self.__redis_driver.expire, key_name, self.TASK_ACQUIRE_TTL)
            return RunRepeatedly(refresh_key, self.TASK_ACQUIRE_TTL / 6)
        return None

    def is_task_aborted(self, env, job_id):
        """
        @type env: C{str}
        @type job_id: C{str}
        @rtype: C{bool}
        """
        return self.__redis_driver.exists('{}-{}-aborted-job'.format(env, job_id))

    def __get_redis_time(self):
        # We use Redis's TIME command so we'll have a single source of truth when it comes to timestamps
        return self.__redis_driver.time()[0]

    def notify_attempting_task(self, task):
        """
        @type task: L{TaskInterface}
        """
        unique_task_key = self.__get_unique_task_key(task)

        # Collect tasks and count attempts
        self.__redis_driver.sadd('{}-tasks-started'.format(self.__get_task_environment(task)), unique_task_key)
        self.__redis_driver.hincrby('{}-attempts-started-by-job'.format(self.__get_task_environment(task)),
                                    task.job_id, 1)

        # Collect tasks that are incomplete
        self.__redis_driver.hset('{}-incomplete-tasks'.format(self.__get_task_environment(task)), unique_task_key,
                                 json.dumps({
                                     'task': task.to_dict(),
                                     'attempt': (self.get_try_count(task.get_key())),
                                     'started': self.__get_redis_time()
                                 }, allow_nan=False))

    def notify_task_attempted(self, task):
        """
        @type task: L{TaskInterface}
        """
        unique_task_key = self.__get_unique_task_key(task)

        # Collect completions and count attempts
        self.__redis_driver.sadd('{}-tasks-completed'.format(self.__get_task_environment(task)), unique_task_key)
        self.__redis_driver.hincrby('{}-attempts-completed-by-job'.format(self.__get_task_environment(task)),
                                    task.job_id, 1)

        # Task is no longer incomplete
        self.__redis_driver.hdel('{}-incomplete-tasks'.format(self.__get_task_environment(task)), unique_task_key)

    def get_all_finished_tasks(self, dummy_task_key):
        """
        @type dummy_task_key: L{TaskKey}
        """
        set_name = self.__get_finished_set_name(dummy_task_key)
        return set(map(int, self.__redis_driver.smembers(set_name)))
