__author__ = 'izik'


class Context(object):
    def __init__(self, jobnik_session, feature_flags):
        self.__feature_flags = feature_flags
        self.__jobnik_session = jobnik_session

    @property
    def jobnik_session(self):
        return self.__jobnik_session

    @property
    def feature_flags(self):
        return self.__feature_flags
