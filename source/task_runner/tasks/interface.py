import json
import pickle

from source.jobnik_communicator.interface import JobnikSession
from source.task_runner.task_status_store.types import TaskKey
from source.utils.random_seed_provider import RandomSeedProvider


def get_all_bases(cls):
    """
    @type cls: C{type}
    @rtype: C{list} of C{type}
    """
    result = []
    for base in cls.__bases__:
        if base == object:
            continue
        result.append(base)
        result.extend(get_all_bases(base))
    return result


class TaskInterface(object):
    items = (
        'customer',
        'jobnik_session',
        'feature_flags',
        'job_id',
        'total_num_tasks',
        'task_ordinal',
        'execution_id',
        'task_seed',
    )
    max_attempts = 2

    def __init__(self, job_id, customer, execution_id, task_ordinal, total_num_tasks, jobnik_session,
                 task_seed=RandomSeedProvider.STORED_NON_RANDOM_SEED,
                 feature_flags=None):
        """
        @type job_id: C{str}
        @type jobnik_session: L{JobnikSession}
        @type total_num_tasks: C{int}
        @type task_ordinal: C{int}
        @type execution_id: C{str}
        @type customer: C{str}
        @type task_seed: C{int}
        """
        self.__job_id = job_id
        self.__feature_flags = feature_flags if feature_flags is not None else {}
        self.__jobnik_session = jobnik_session
        self.__total_num_tasks = total_num_tasks
        self.__customer = customer
        self.__task_ordinal = task_ordinal
        self.__execution_id = execution_id
        self.__task_seed = task_seed

    def get_context(self):
        """
        Returns the context of the task.
        This will be the logging extra data and extra data on exception artifacts

        @rtype: C{dict}
        """
        raise NotImplementedError()

    def get_key(self):
        """
        @rtype: L{TaskKey}
        """
        return TaskKey(self.__execution_id, self.__task_ordinal, self.__job_id)

    @property
    def execution_id(self):
        return self.__execution_id

    @property
    def job_id(self):
        return self.__job_id

    @property
    def task_seed(self):
        return self.__task_seed

    @property
    def task_ordinal(self):
        return self.__task_ordinal

    @property
    def customer(self):
        return self.__customer

    @property
    def jobnik_session(self):
        return self.__jobnik_session

    @property
    def total_num_tasks(self):
        return self.__total_num_tasks

    @total_num_tasks.setter
    def total_num_tasks(self, value):
        self.__total_num_tasks = value

    @property
    def feature_flags(self):
        return self.__feature_flags

    def serialize(self, mode='pickle'):
        if mode == 'pickle':
            return pickle.dumps(self)
        elif mode == 'json':
            task_as_dict = self.to_dict()
            task_as_dict['task_class'] = self.__class__.__name__
            return json.dumps(task_as_dict, allow_nan=False)

    @classmethod
    def deserialize(cls, data, mode='pickle'):
        if mode == 'pickle':
            return pickle.loads(data)
        elif mode == 'json':
            task_as_dict = json.loads(data)
            class_name = task_as_dict['task_class']
            clazz = dict(map(lambda x: (x.__name__, x), cls.__subclasses__()))[class_name]
            args = set(clazz.__init__.im_func.func_code.co_varnames[1:clazz.__init__.im_func.func_code.co_argcount])
            partial_dict = {key: value for key, value in task_as_dict.iteritems()
                            if len({key, 'kwargs'}.intersection(args))}
            if partial_dict['jobnik_session'] is not None:
                partial_dict['jobnik_session'] = JobnikSession(**partial_dict['jobnik_session'])
            return clazz(**partial_dict)

    def _to_dict(self):
        result = {item: getattr(self, item) for item in self.items}
        result['jobnik_session'] = result['jobnik_session'].to_dict() if result['jobnik_session'] is not None else None
        return result

    def to_dict(self):
        """
        Returns an dictionary representation of this task.

        **Note: Do no override this, rewrite _to_dict instead.
        @rtype: C{dict}
        """
        bases = get_all_bases(self.__class__)
        bases.reverse()
        result_dict = {}
        for base in bases:
            # noinspection PyUnresolvedReferences,PyProtectedMember
            result_dict.update(base._to_dict(self))

        result_dict.update(self._to_dict())
        return result_dict

    def __eq__(self, other):
        return all(map(lambda x: getattr(self, x) == getattr(other, x), self.items))
