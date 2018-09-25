class TopicPublisherInterface(object):
    def __init__(self, service_name):
        self.__service_name = service_name

    def publish_dict(self, topic_name, dict_message):
        """
        @type topic_name: C{str}
        @type dict_message: C{dict}
        """
        raise NotImplementedError()

    @property
    def service_name(self):
        return self.__service_name
