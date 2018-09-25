from source.task_runner.stop_notification_reader.interface import StopNotificationReaderInterface


class InMemoryStopNotificationReader(StopNotificationReaderInterface):
    def __init__(self, forced_id=None):
        self.__instances_to_stop = set()
        if forced_id is None:
            self.__my_id = self._get_id()
        else:
            self.__my_id = forced_id

    def should_stop(self):
        return self.__my_id in self.__instances_to_stop

    def stop_this(self):
        self.__instances_to_stop.add(self.__my_id)