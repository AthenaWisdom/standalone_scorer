from source.utils.context_handling.context import Context

__author__ = 'izik'


class ContextProvider(object):
    def get_current_context(self):
        return self.__context

    def set_current_context(self, jobnik_session, feature_flags=None):
        # noinspection PyAttributeOutsideInit
         self.__context = Context(jobnik_session, feature_flags)

    def release_context(self):
        self.set_current_context(None, None)


