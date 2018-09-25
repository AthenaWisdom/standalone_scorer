from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.io_handlers.local_fs import LocalFSIOHandler
from source.storage.io_handlers.s3 import S3IOHandler


class IOHandlerComponent(object):
    def __init__(self, feature_flags):
        self._io_handler = self.get_io_handler(feature_flags)

    @staticmethod
    def get_io_handler(feature_flags):
        """
        @type feature_flags: C{dict}
        @rtype: L{IOHandlerInterface}
        """
        if feature_flags['io'] == 's3':
            return S3IOHandler()
        elif feature_flags['io'] == 'local':
            return LocalFSIOHandler('/tmp/tmp.wMbWjFFaWt')