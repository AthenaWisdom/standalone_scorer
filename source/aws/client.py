import socket

import boto3


class ReconnectingAWSClient(object):
    def __init__(self, service, region_name='us-east-1'):
        """
        @param service: The service to connect to
        @type service: C{str}
        """
        self.__region_name = region_name
        self.__service = service
        self.__client_obj = None

    @property
    def region_name(self):
        return self.__region_name

    def __create_client(self):
        self.__client_obj = boto3.client(self.__service, region_name=self.__region_name)

    @property
    def __client(self):
        if self.__client_obj is None:
            self.__create_client()

        return self.__client_obj

    def __getattr__(self, item):
        actual_attribute = getattr(self.__client, item)
        if not callable(actual_attribute):
            return actual_attribute

        def wrapped(*args, **kwargs):
            try:
                return actual_attribute(*args, **kwargs)
            except socket.error as ex:
                if ex.errno == socket.errno.ECONNRESET:
                    self.__client_obj = None
                    new_actual_attribute = getattr(self.__client, item)
                    return new_actual_attribute(*args, **kwargs)
                else:
                    raise

        return wrapped