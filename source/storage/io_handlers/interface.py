import os
from datetime import datetime

from enum import IntEnum


class File(object):
    def __init__(self, path, size):
        """
        @type path: C{str}
        @type size: C{int}
        """
        self.__size = size
        self.__path = path

    @property
    def size(self):
        return self.__size

    @property
    def path(self):
        return self.__path


class IOHandlerInterface(object):
    def get_hadoop_base_path(self):
        """
        Returns the hadoop base path for Spark to load DataFrames
        e.g.:
            - s3a://
            - gs://
        @rtype: C{str}
        """
        raise NotImplementedError()

    def save_raw_data(self, data, path, content_type=None):
        """
        Saves raw data on the storage service

        @param content_type: A standard MIME type describing the format of the object data.
        @type content_type: C{str}
        @param data: The data to save.
        @type data: C{str}
        @param path: The path on the storage service where the data should be saved.
        @type path: C{str}
        """
        raise NotImplementedError()

    def save_raw_data_multipart(self, data, path, content_type=None):
        self.save_raw_data(data.read(), path, content_type)

    def load_raw_data(self, path):
        """
        Loads raw data from the storage service.

        @param path: The path on the remote storage where the data should be loaded from.
        @type path: C{str}
        @return: The loaded data
        @raise LookupError: When the path does not exist.
        """
        raise NotImplementedError()

    def path_exists(self, path):
        """
        Determines whether the given path exists on the storage service.
        @param path: The path to test
        @type path: C{str}
        @return: True if the path exists False otherwise
        @rtype: C{bool}
        """
        raise NotImplementedError()

    def list_dir(self, path):
        """
        Returns a list of full paths of the contents of the given path

        @param path: The path to list contents of
        @type path: C{str}
        @rtype C{list} of C{str}
        """
        raise NotImplementedError()

    def list_files(self, path):
        """
        Returns a list of Files of the contents of the given path

        @param path: The path to list contents of
        @type path: C{str}
        @rtype C{list} of L{File}
        """
        raise NotImplementedError()

    def open(self, path):
        """
        Returns a file-like object to read the data from.

        @param path: The path on the remote storage where the data should be loaded from.
        @type path: C{str}
        @rtype: C{file}
        @raise LookupError: When the path does not exist.
        """
        raise NotImplementedError()

    def store_folder_contents(self, local_folder_path, remote_folder_path):
        """
        Stores the contents of the given local folder on the remote folder of the IO service

        @type local_folder_path: C{str}
        @type remote_folder_path: C{str}
        """
        for file_name in os.listdir(local_folder_path):
            with open(os.path.join(local_folder_path, file_name), 'rb') as local_file:
                self.save_raw_data(local_file.read(), os.path.join(remote_folder_path, file_name))

    def add_public_read(self, path):
        """
        Adds public read permissions on the remote file
        @param path: The path on the remote storage where the data should be loaded from.
        @type path: C{str}
        """
        raise NotImplementedError()

    def get_object_url(self, path):
        """
        @param path: The path on the remote storage where the data should be loaded from.
        @type path: C{str}

        """
        raise NotImplementedError()

    def get_created_at(self, path):
        """
        @type path: C{str}
        @rtype: C{datetime}
        """
        raise NotImplementedError()

    def copy_contents_from_remote_folder_to_remote_folder(self, source_path, target_path):
        """
        @type source_path: C{str}
        @type target_path: C{str}
        @rtype C{int}
        @return number of files copied
        """
        raise NotImplementedError()

    def download_fileobj(self, remote_path, file_like):
        """
        @see boto3.s3.inject.object_download_fileobj
        @type remote_path: C{str}
        """
        raise NotImplementedError()


class ErrorCodes(IntEnum):
    FILE_TOO_BIG = 1
