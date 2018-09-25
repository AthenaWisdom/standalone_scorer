from contextlib import contextmanager
from threading import Thread, Event


class RunRepeatedly(object):
    def __init__(self, func, interval, *args, **kwargs):
        """
        @param func: The function to run repeatedly
        @type func: C{function}
        @param interval: The interval between each run
        @type interval: C{float}
        """
        self.__stop_event = Event()

        def run():
            while not self.__stop_event.wait(interval):
                func(*args, **kwargs)

        self.__run_thread = Thread(target=run)

    def __enter__(self):
        self.__run_thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__stop_event.set()
        self.__run_thread.join()


@contextmanager
def nop():
    yield
