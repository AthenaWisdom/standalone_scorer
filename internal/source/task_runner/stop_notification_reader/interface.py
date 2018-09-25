import socket

from boto.utils import get_instance_identity


class StopNotificationReaderInterface(object):
    def should_stop(self):
        """
        @return: Returns True if the runner should stop
        @rtype: C{bool}
        """
        raise NotImplementedError()

    def _get_id(self):
        try:
            return get_instance_identity(timeout=5, num_retries=2)['document']['instanceId']
        except IndexError:
            return socket.gethostname()
