import json
from contextlib import contextmanager


class Singleton(type):
    _instances = {}

    def __call__(cls, *args):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args)
        return cls._instances[cls]


class ContextManager(object):
    __metaclass__ = Singleton

    def __init__(self):
        try:
            with open('/meta/context_data') as context_data:
                self.__default_context = json.load(context_data)
        except IOError:
            self.__default_context = {}
        self.__context = self.__default_context

    @property
    def context(self):
        return self.__context.copy()

    @context.setter
    def context(self, value):
        self.__context.update(value)

    def reset_context(self):
        self.__context = self.__default_context

    def __sub_context(self, **kwargs):
        original_context = self.__context.copy()
        self.__context.update(kwargs)
        yield self
        self.__context = original_context

    @contextmanager
    def query_context(self, query_id):
        return self.__sub_context(query_id=query_id)

    @contextmanager
    def module_context(self, module_name):
        return self.__sub_context(module_name=module_name)

    @contextmanager
    def kernel_context(self, kernel_hash):
        return self.__sub_context(kernel_hash=kernel_hash)
