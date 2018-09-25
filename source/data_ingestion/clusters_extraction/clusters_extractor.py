import errno
import functools
import logging
import os
import shutil
import statvfs
import subprocess
import tempfile
from datetime import datetime
from hashlib import md5

import pytz
from elasticsearch import Elasticsearch
from elasticsearch import exceptions

from source import BASE_ES_URI
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types import DurationMetricArtifact
from source.storage.stores.artifact_store.types.data_ingestion import ClustersExtractionPricingDurationMetricArtifact, \
    RunPairDiskUsageArtifact, InputCsvSizeArtifact
from source.storage.stores.engine_communication_store.interface import EngineCommunicationStoreInterface
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.clusters_extaction import ClustersExtractionTask, FEATURE_FLAG_MEMOIZE_BLOCKS_KEY
from source.utils.configure_logging import configure_logger
from source.utils.run_repeatedly import RunRepeatedly

IRRELEVANT_PARAM_FIELDS = {
    'filename',
    'prms_num_of_CPUs',
}

PHASE_NUM_TO_ARGS = {
    1: '1',
    2: '2',
    8.6: '8.6,/out/BlockDataAll_new,CLUST'
}

WORKSPACE_FOLDERS = {
    'in': '/in',
    'meta': '/meta',
    'out': '/out',
    'work_dir': '/work_dir',  # This is being used by MATLAB as a constant
}


# noinspection PyArgumentList
class ClustersExtractor(TaskHandlerInterface):
    def __init__(self, artifact_store, engine_communication_store):
        """
        @type artifact_store: L{ArtifactStoreInterface}
        @type engine_communication_store: L{EngineCommunicationStoreInterface}
        """
        self.__engine_communication_store = engine_communication_store
        self.__artifact_store = artifact_store
        self.__logger = logging.getLogger('endor')

    def run_task(self, task):
        """
        @type task: L{ClustersExtractionTask}
        """

        if task is None:
            raise RuntimeError('No task received')

        configure_logger(self.__logger, task.get_context())
        self.__logger.info('Got task')
        self.handle_task(task)

    def handle_task(self, task):
        """
        @type task: L{ClustersExtractionTask}
        """
        self.__connect_to_es()
        self.__clean_workspace()
        self.__initialize_workspace()
        self.send_disk_usage_artifact(task.customer, task.sphere_id, task.connecting_field, task.input_csv_name)
        send_usage_func = functools.partial(self.send_disk_usage_artifact, task.customer,
                                            task.sphere_id, task.connecting_field, task.input_csv_name)
        # noinspection PyTypeChecker
        with ClustersExtractionPricingDurationMetricArtifact(task.customer, task.sphere_id, 'extract_clusters',
                                                             self.__artifact_store, task_ordinal=task.task_ordinal,
                                                             connecting_field=task.connecting_field,
                                                             input_csv_name=task.input_csv_name), \
                RunRepeatedly(send_usage_func, 20):
            self.__process_task(task)
        self.__clean_workspace()

    @staticmethod
    def __initialize_workspace():
        for folder in WORKSPACE_FOLDERS.itervalues():
            os.mkdir(folder)

    @staticmethod
    def __clean_workspace():
        for folder in WORKSPACE_FOLDERS.itervalues():
            try:
                shutil.rmtree(folder)
            except OSError as ex:
                if ex.errno != errno.ENOENT:
                    raise

    def __process_task(self, task):
        # type: (ClustersExtractionTask) -> None
        partial_duration_artifact = functools.partial(DurationMetricArtifact, task.customer, task.sphere_id,
                                                      'clusters_extraction', artifact_store=self.__artifact_store,
                                                      input_csv_name=task.input_csv_name,
                                                      connecting_field=task.connecting_field)

        input_csv_hash = self.__engine_communication_store.get_input_csv_hash(task.customer, task.sphere_id,
                                                                              task.input_csv_name)

        params_data = self.__engine_communication_store.load_params_data(task.customer, task.sphere_id,
                                                                         task.input_csv_name, task.connecting_field)
        params_file_path = os.path.join(WORKSPACE_FOLDERS['meta'], 'params.txt')

        # Write the params file into the expected location and parse it too
        with open(params_file_path, 'w') as params_file:
            params_file.write(params_data)
            # noinspection PyTypeChecker
            params = dict(row.strip().split('\t')
                          for row in params_data.split('\n')
                          if len(row.strip()) > 0)

        block_memoization_enabled = task.feature_flags.get(FEATURE_FLAG_MEMOIZE_BLOCKS_KEY, False)
        block_checksum = self.__create_block_checksum(input_csv_hash, params)
        fetched_memoized_block = False

        if block_memoization_enabled:
            # Check if the complete checksum exists as a key in our store. If it doesn't, we'll get zero files copied.
            files_copied = self.__engine_communication_store \
                .copy_memoized_block_to_intermediate(task.customer, task.sphere_id, task.input_csv_name,
                                                     task.connecting_field, block_checksum)
            fetched_memoized_block = (files_copied != 0)
            if fetched_memoized_block:
                self.__logger.debug('Block cache hit', extra={
                    'input_csv_name': task.input_csv_name,
                    'connecting_field': task.connecting_field,
                    'customer': task.customer,
                    'sphere_id': task.sphere_id,
                    'block_key': block_checksum
                })

        if not block_memoization_enabled or not fetched_memoized_block:
            with partial_duration_artifact(phase='download'):
                input_csv_temp_path, num_rows_in_input_csv = self.__engine_communication_store.load_input_csv(
                    task.customer, task.sphere_id, task.input_csv_name, task.connecting_field)

            input_csv_size = os.stat(input_csv_temp_path).st_size
            input_csv_size_artifact = InputCsvSizeArtifact(task.customer, task.sphere_id, task.connecting_field,
                                                           task.input_csv_name, input_csv_size, num_rows_in_input_csv)
            self.__artifact_store.store_artifact(input_csv_size_artifact)
            self.__create_and_upload_block(task, params['filename'], input_csv_temp_path, partial_duration_artifact)
            if block_memoization_enabled:
                self.__logger.debug("Memoizing block")
                with partial_duration_artifact(phase='memoization'):
                    self.__engine_communication_store.memoize_block(task.customer, task.sphere_id, task.input_csv_name,
                                                                    task.connecting_field, block_checksum)
                self.__logger.debug("Finished memoizing block")

    def __create_and_upload_block(self, task, input_csv_filename, input_csv_temp_path, partial_duration_artifact):
        # Move input csv into the input directory
        shutil.move(input_csv_temp_path, os.path.join(WORKSPACE_FOLDERS['in'], input_csv_filename))

        self.__create_block_in_out_dir(partial_duration_artifact, task)

        with partial_duration_artifact(phase='upload'):
            self.__engine_communication_store.store_block(task.customer, task.sphere_id, task.input_csv_name,
                                                          task.connecting_field, WORKSPACE_FOLDERS['out'])

        self.__logger.debug('Finished storing results')

    @staticmethod
    def __create_block_checksum(input_csv_checksum, params):
        checksum = md5()

        for key in sorted(params):
            # The file name of the input csv has no implication on the output of the MATLAB process, so it doesn't
            # go into the signature of the params file.
            if key not in IRRELEVANT_PARAM_FIELDS:
                checksum.update('{}: {}\n'.format(key, params[key]))

        params_checksum = checksum.hexdigest()
        return params_checksum + '-' + input_csv_checksum

    def __create_block_in_out_dir(self, partial_duration_artifact, task):
        for phase_num, phase_args in PHASE_NUM_TO_ARGS.iteritems():
            self.__logger.debug('Running Phase {}'.format(phase_num))
            log_name = 'phase_{}'.format(phase_num)
            _, stdout_path = tempfile.mkstemp('_stdout')
            _, stderr_path = tempfile.mkstemp('_stderr')
            with partial_duration_artifact(phase=str(phase_num)), \
                    RunRepeatedly(lambda: self.__log_file_size(stdout_path,
                                                               'stdout_phase_{}'.format(phase_num)), 180), \
                    RunRepeatedly(lambda: self.__log_file_size(stderr_path,
                                                               'stderr_phase_{}'.format(phase_num)), 180), \
                    open(stderr_path, 'w') as stderr, \
                    open(stdout_path, 'w') as stdout:
                exit_code = self.__run_phase(phase_args, stdout, stderr)

            self.__engine_communication_store.store_matlab_logs(task.customer, task.sphere_id, task.input_csv_name,
                                                                task.connecting_field, log_name, stdout_path)
            self.__engine_communication_store.store_matlab_logs(task.customer, task.sphere_id, task.input_csv_name,
                                                                task.connecting_field, log_name, stderr_path)
            if exit_code != 0:
                raise subprocess.CalledProcessError(exit_code, phase_args)

            self.__logger.debug('Finished Phase {}'.format(phase_num))

        self.__logger.debug('Storing results')

        return None

    def __log_file_size(self, file_path, logical_name):
        file_size = os.path.getsize(file_path)
        self.__logger.debug('File {} is sized {}'.format(logical_name, file_size), extra={
            'file_path': file_path,
            'logical_name': logical_name,
            'file_size': file_size
        })

    @staticmethod
    def __run_phase(phase_args, stdout_fd, stderr_fd):
        phase_process = subprocess.Popen(['/bin/sh', '/home/ubuntu/ENGINE/run_AthenaMain.sh',
                                          '/usr/local/MATLAB/MATLAB_Compiler_Runtime/v83', phase_args],
                                         stdout=stdout_fd, stderr=stderr_fd,
                                         cwd='/home/ubuntu/ENGINE')
        return phase_process.wait()

    @staticmethod
    def get_task_type():
        return ClustersExtractionTask

    def __connect_to_es(self):
        self.__elasticsearch_driver = Elasticsearch([BASE_ES_URI])

    def __store_disk_usage_artifact(self, disk_usage_artifact):
        """
        @type disk_usage_artifact: L{RunPairDiskUsageArtifact}
        """
        entity = disk_usage_artifact.to_dict()
        entity['time_sent'] = datetime.utcnow().replace(tzinfo=pytz.utc)
        for i in xrange(5):
            try:
                es_index = 'disk_usage_artifacts'
                self.__elasticsearch_driver.index(es_index, disk_usage_artifact.type, entity)
                return
            except (exceptions.ConnectionTimeout, exceptions.TransportError):
                if i == 4:
                    self.__connect_to_es()

    def send_disk_usage_artifact(self, customer, sphere_id, connecting_field, input_csv_name):
        stats = os.statvfs('/')
        disk_usage_percent = 1 - float(stats[statvfs.F_BFREE]) / stats[statvfs.F_BLOCKS]
        self.__store_disk_usage_artifact(RunPairDiskUsageArtifact(customer, sphere_id, connecting_field,
                                                                  input_csv_name, disk_usage_percent))
