import os
import tempfile
from datetime import datetime

import pytz

from source.storage.io_handlers.interface import IOHandlerInterface


class LocalFSIOHandler(IOHandlerInterface):
    def get_created_at(self, path):
        real_path = os.path.join(self.__base_dir, path)
        return datetime.utcfromtimestamp(os.stat(real_path).st_ctime).replace(tzinfo=pytz.utc)

    def __init__(self, base_dir=None):
        self.__base_dir = base_dir if base_dir is not None else tempfile.mkdtemp()

    @property
    def base_dir(self):
        return self.__base_dir

    def get_hadoop_base_path(self):
        return self.base_dir + '/'

    def save_raw_data(self, data, path, content_type=None):
        print "save_raw_data"
        print path
        real_path = os.path.join(self.__base_dir, path)
        dir_name = os.path.dirname(real_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(real_path, 'wb') as f:
            f.write(data)

    def load_raw_data(self, path):
        print "loadraw"
        print path
        return self.open(path).read()

    def path_exists(self, path):
        real_path = os.path.join(self.__base_dir, path)
        return os.path.exists(real_path)

    def list_dir(self, path):
        print "listing"
        print path
        real_path = os.path.join(self.__base_dir, path)
        return [os.path.join(real_path, f).replace(self.__base_dir + '/', '') for f in os.listdir(real_path)]

    def open(self, path):
        real_path = os.path.join(self.__base_dir, path)
        print "opening"
        print real_path
        if not os.path.isfile(real_path):
            raise LookupError('"{}" in base dir "{}" is not a file or does not exist'.format(path, self.__base_dir))

        return open(real_path, 'rb')

    def add_public_read(self, path):
        real_path = os.path.join(self.__base_dir, path)
        os.chmod(real_path, (os.stat(real_path).st_mode & 0xFFF8) + 0x4)

    def get_object_url(self, path):
        return os.path.join(self.__base_dir, path)
