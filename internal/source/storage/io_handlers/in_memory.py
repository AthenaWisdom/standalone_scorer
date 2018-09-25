import copy
import os
import re
from cStringIO import StringIO
from datetime import datetime

import pytz
from pandas import DataFrame

from source.storage.io_handlers.interface import IOHandlerInterface


class InMemoryIOHandler(IOHandlerInterface):
    def get_created_at(self, path):
        return self.__created_at_dict[path]

    def __init__(self):
        self.__internal_dict = {}
        self.__created_at_dict = {}

    def path_exists(self, path):
        return path in self.__internal_dict

    def save_raw_data(self, data, path, content_type=None):
        self.__internal_dict[path] = data
        if path not in self.__created_at_dict:
            self.__created_at_dict[path] = datetime.utcnow().replace(tzinfo=pytz.utc)

    def get_hadoop_base_path(self):
        return ''

    def load_raw_data(self, path):
        try:
            data = self.__internal_dict[path]
            if isinstance(data, DataFrame):
                data = data.copy(True)
            else:
                data = copy.deepcopy(data)
            return data
        except KeyError:
            raise LookupError('Path {} was not found'.format(path))

    def __setitem__(self, key, value):
        self.__internal_dict.__setitem__(key, value)
        if key not in self.__created_at_dict:
            self.__created_at_dict[key] = datetime.utcnow().replace(tzinfo=pytz.utc)

    def __getitem__(self, item):
        if item.endswith('*'):
            regex = re.compile(item.replace('*', '.*'))
            key = [m[0] for m in [regex.findall(key) for key in self.__internal_dict.iterkeys()] if m].pop()
            return self.__internal_dict.__getitem__(key)
        return self.__internal_dict.__getitem__(item)

    def list_dir(self, path):
        regex = re.compile('^' + path + '.*')
        return [m[0] for m in [regex.findall(key) for key in self.__internal_dict.iterkeys()] if m]

    def open(self, path):
        return StringIO(self.load_raw_data(path))

    def copy_contents_from_remote_folder_to_remote_folder(self, source_path, target_path):
        files_in_source = self.list_dir(source_path)
        files_count_in_source = len(files_in_source)

        if files_count_in_source == 0:
            return 0

        for file_path in files_in_source:
            target_file_path = os.path.join(target_path, os.path.relpath(file_path, source_path))
            self.save_raw_data(self.load_raw_data(file_path), target_file_path)

        return files_count_in_source

    def download_fileobj(self, remote_path, file_like):
        file_like.write(self.load_raw_data(remote_path))