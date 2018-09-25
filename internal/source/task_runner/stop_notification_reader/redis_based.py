from redis import Redis

from source.task_runner.stop_notification_reader.interface import StopNotificationReaderInterface


class RedisBasedStopNotificationReader(StopNotificationReaderInterface):
    def __init__(self, redis_driver):
        """
        @type redis_driver: C{Redis}
        """
        self.__redis_driver = redis_driver
        self.__my_id = self._get_id()

    def should_stop(self):
        return self.__redis_driver.exists(self.__my_id)
