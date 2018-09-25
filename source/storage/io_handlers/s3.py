import functools
import hashlib
import itertools
import logging
import os
import threading
from Queue import Queue
from cStringIO import StringIO
from contextlib import closing
from datetime import datetime
from httplib import IncompleteRead
from os.path import join, relpath

import boto3
from boto3.s3.transfer import TransferConfig, MB
from botocore.exceptions import ClientError, IncompleteReadError
from botocore.vendored.requests.packages.urllib3.exceptions import ProtocolError

from source.aws.client import ReconnectingAWSClient
from source.storage.io_handlers.interface import IOHandlerInterface, File, ErrorCodes


class S3IOHandler(IOHandlerInterface):
    def get_created_at(self, path):
        """
        Actually is modification time

        @type path: C{str}
        @rtype: C{datetime}
        """
        self.__logger.debug('Loading last modified at from path "{}"'.format(path))
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        client = self.__client
        try:
            key = client.get_object(Bucket=bucket_name, Key=key_path)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                raise LookupError('Key "{}" was not found in bucket "{}".'.format(key_path, bucket_name))
            raise
        return key['LastModified']

    def __init__(self, max_keys_for_list_operation=1000):
        self.__client = ReconnectingAWSClient('s3', 'us-east-1')
        self.__logger = logging.getLogger('endor.io_handler.s3')
        self.__max_keys_for_list_operation = max_keys_for_list_operation

    def get_hadoop_base_path(self):
        return 's3a://'

    def path_exists(self, path):
        """
        Determines whether the given path exists on the storage service.
        @param path: The path to test
        @type path: C{str}
        @return: True if the path exists False otherwise
        @rtype: C{bool}
        """
        client = self.__client
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        try:
            client.get_object(Bucket=bucket_name, Key=key_path)
            return True
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                return False
            raise

    def save_raw_data(self, data, path, content_type='text/html'):
        """
        Saves raw data on the storage service

        @param data: The data to save.
        @type data: C{str}
        @param path: The path on the storage service where the data should be saved.
        @type path: C{str}
        @param content_type: A standard MIME type describing the format of the object data
        @type content_type: C{str}
        """
        client = self.__client
        self.__logger.debug('Saving to path "{}"'.format(path))
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        try:
            client.put_object(
                Bucket=bucket_name,
                Key=key_path,
                Body=data,
                ContentType=content_type,
                ServerSideEncryption='AES256')
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'EntityTooLarge':
                raise IOError({'code': ErrorCodes.FILE_TOO_BIG})
            else:
                raise

    def save_raw_data_multipart(self, data, path, content_type=None):
        """
        Saves raw data on the storage service

        @param data: The data to save.
        @type data: C{file}
        @param path: The path on the storage service where the data should be saved.
        @type path: C{str}
        @param content_type: A standard MIME type describing the format of the object data
        @type content_type: C{str}
        """
        client = self.__client
        self.__logger.debug('Saving to path "{}" with multipart'.format(path))
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        config = TransferConfig(multipart_threshold=5 * 1024 * MB)
        extra_args = {'ServerSideEncryption': 'AES256'}
        if content_type is not None:
            extra_args['ContentType'] = content_type

        client.upload_fileobj(
            Bucket=bucket_name,
            Key=key_path,
            Fileobj=data,
            Config=config,
            ExtraArgs=extra_args)

    def load_raw_data(self, path):
        """
        Loads raw data from the storage service.

        @param path: The path on the remote storage where the data should be loaded from.
        @type path: C{str}
        @return: The loaded data
        @rtype: C{str}
        @raise LookupError: When the path does not exist.
        """
        try:
            with closing(self.open(path)) as file_handle:
                data = file_handle.read()
                data_md5 = hashlib.md5(data).hexdigest()
                self.__logger.debug('Loaded {} bytes from path "{}" with md5 "{}"'.format(len(data), path, data_md5))
                return data
        except ProtocolError as ex:
            if isinstance(ex.args[1], IncompleteRead):
                return self.load_raw_data(path)
        except IncompleteReadError:
            return self.load_raw_data(path)

    @staticmethod
    def __split_to_bucket_and_path(path):
        split_path = path.split('/')
        bucket_name, key_path = split_path[0], join(*split_path[1:])
        return bucket_name, key_path

    def list_files(self, path):
        self.__logger.debug('Listing path "{}"'.format(path))
        client = self.__client
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        query = join(key_path, '')
        query_len = len(query)

        continuation_token = None
        fetch_more_objects = True

        while fetch_more_objects:
            list_objects = functools.partial(client.list_objects_v2,
                                             Bucket=bucket_name,
                                             Prefix=query,
                                             Delimiter='/',
                                             MaxKeys=self.__max_keys_for_list_operation)

            if continuation_token is not None:
                response = list_objects(ContinuationToken=continuation_token)
            else:
                response = list_objects()

            if 'Contents' in response:
                keys = (File(i['Key'], i['Size']) for i in response['Contents'])
            else:
                keys = []

            if 'CommonPrefixes' in response:
                folders = (File(i['Prefix'], 0) for i in response['CommonPrefixes'])
            else:
                folders = []

            for obj in itertools.chain(keys, folders):
                result_key = obj.path[query_len:].strip('/')
                if result_key:
                    yield File(join(path, result_key), obj.size)

            fetch_more_objects = response['IsTruncated']

            if fetch_more_objects:
                continuation_token = response['NextContinuationToken']

    def list_dir(self, path):
        return (f.path for f in self.list_files(path))

    def open(self, path):
        self.__logger.debug('Loading from path "{}"'.format(path))
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        client = self.__client
        try:
            key = client.get_object(Bucket=bucket_name, Key=key_path)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                raise LookupError('Key "{}" was not found in bucket "{}".'.format(key_path, bucket_name))
            raise

        return key['Body']

    def store_folder_contents(self, local_folder_path, remote_folder_path):
        exception_queue = Queue()
        s3_region = self.__client.region_name

        def upload_file(local_path, remote_path):
            try:
                client = boto3.client('s3', region_name=s3_region)
                bucket_name, key_path = self.__split_to_bucket_and_path(remote_path)
                with open(local_path, 'rb') as local_file:
                    client.put_object(Bucket=bucket_name, Key=key_path, Body=local_file.read(),
                                      ServerSideEncryption='AES256')
            except Exception as ex:
                exception_queue.put(ex)

        upload_jobs_generator = ((join(local_folder_path, file_name), join(remote_folder_path, file_name))
                                 for file_name in os.listdir(local_folder_path))
        threads = []
        for local_file_path, remote_file_path in upload_jobs_generator:
            self.__logger.debug('Uploading "{}" to "{}"'.format(local_file_path, remote_file_path))
            thread = threading.Thread(target=upload_file, args=(local_file_path, remote_file_path))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        if not exception_queue.empty():
            raise exception_queue.get()

    def add_public_read(self, path):
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        client = self.__client
        try:
            client.put_object_acl(Bucket=bucket_name, Key=key_path, ACL='public-read')
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                raise LookupError('Key "{}" was not found in bucket "{}".'.format(key_path, bucket_name))
            raise

    def get_object_url(self, path):
        bucket_name, key_path = self.__split_to_bucket_and_path(path)
        return 'https://{bucket}.s3.amazonaws.com/{key}'.format(bucket=bucket_name, key=key_path)

    def copy_contents_from_remote_folder_to_remote_folder(self, source_path, target_path):
        _, source_key_path = self.__split_to_bucket_and_path(source_path)
        target_bucket_name, target_key_path = self.__split_to_bucket_and_path(target_path)

        source_file_paths = list(self.list_dir(source_path))

        if len(source_file_paths) == 0:
            return 0

        client = self.__client

        for single_source_file_path in source_file_paths:
            bucket_name, key_path = self.__split_to_bucket_and_path(single_source_file_path)
            single_target_key_path = join(target_key_path, relpath(key_path, source_key_path))
            client.copy({'Bucket': bucket_name, 'Key': key_path}, target_bucket_name, single_target_key_path,
                        ExtraArgs={
                            'ServerSideEncryption': 'AES256'
                        })

        return len(source_file_paths)

    def download_fileobj(self, remote_path, file_like):
        bucket_name, key_path = self.__split_to_bucket_and_path(remote_path)
        self.__logger.debug('Downloading {} as file object'.format(remote_path))
        self.__client.download_fileobj(Bucket=bucket_name, Key=key_path, Fileobj=file_like)
