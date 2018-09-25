import functools
import logging
import os
import shutil
import subprocess
import tempfile

from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types import DurationMetricArtifact
from source.storage.stores.artifact_store.types.data_ingestion import ClustersUnificationPricingDurationMetricArtifact,\
    ClustersUnificationSourceHashArtifact
from source.storage.stores.engine_communication_store.interface import EngineCommunicationStoreInterface
from source.storage.stores.engine_communication_store.uri_based import URIBasedEngineCommunicationStore
from source.task_runner.handler_interface import TaskHandlerInterface
from source.task_runner.tasks.clusters_unification import ClustersUnificationTask
from source.utils.configure_logging import configure_logger
from source.utils.run_repeatedly import RunRepeatedly

WORKSPACE_FOLDERS = [
    '/in',
    '/meta',
    '/out',
    '/work_dir',
]


class ClustersUnifier(TaskHandlerInterface):
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
        @type task: L{ClustersUnificationTask}
        """

        if task is None:
            raise RuntimeError('No task received')

        configure_logger(self.__logger, task.get_context())
        self.__logger.info('Got task')

        self.handle_task(task)

    def handle_task(self, task):
        """
        @type task: L{ClustersUnificationTask}
        """
        self.__clean_workspace()
        self.__initialize_workspace()
        with ClustersUnificationPricingDurationMetricArtifact(task.customer, task.sphere_id, 'unify_clusters',
                                                              self.__artifact_store,
                                                              task_ordinal=task.task_ordinal):
            self.__process_task(task)
        self.__clean_workspace()

    @staticmethod
    def __initialize_workspace():
        for folder in WORKSPACE_FOLDERS:
            os.mkdir(folder)

    @staticmethod
    def __clean_workspace():
        for folder in WORKSPACE_FOLDERS:
            try:
                shutil.rmtree(folder)
            except OSError as ex:
                if ex.errno != 2:
                    raise

    def __process_task(self, task):
        """
        @type task: L{ClustersUnificationTask}
        """
        partial_duration_artifact = functools.partial(DurationMetricArtifact, task.customer, task.sphere_id,
                                                      'clusters_unification', artifact_store=self.__artifact_store)

        with partial_duration_artifact(phase='download'):
            for sphere_id in task.input_spheres:
                self.__engine_communication_store.download_sphere_blocks(task.customer, sphere_id, '/in',
                                                                         task.filter_only)
            if not self.__engine_communication_store.download_single_params(task.customer,
                                                                            task.input_spheres[0],
                                                                            '/meta'):
                raise ValueError('Could not download params file.')

        self.__logger.debug('Running Phase 8.1')
        log_name = 'phase_unifier'
        stdout_fd, stdout_path = tempfile.mkstemp('_stdout')
        stderr_fd, stderr_path = tempfile.mkstemp('_stderr')
        with partial_duration_artifact(phase='8.1'), \
                RunRepeatedly(lambda: self.__log_file_size(stdout_path, 'unifier'), 180), \
                RunRepeatedly(lambda: self.__log_file_size(stderr_path, 'unifier'), 180):
            exit_code = self.__run_phase('8.1,BlockDataAll_new,unified_blocks', stdout_fd, stderr_fd)
        self.__logger.debug('Finished Phase 8.1')
        os.close(stderr_fd)
        os.close(stdout_fd)
        self.__engine_communication_store.store_unifier_logs(task.customer, task.sphere_id,
                                                             log_name + '_out', stdout_path)
        self.__engine_communication_store.store_unifier_logs(task.customer, task.sphere_id,
                                                             log_name + '_err', stderr_path)
        if exit_code != 0:
            raise subprocess.CalledProcessError(exit_code, '8.1,BlockDataAll_new,unified_blocks')

        self.__logger.debug('Storing results')
        with partial_duration_artifact(phase='upload'):
            self.__engine_communication_store.store_unification_results(task.customer, task.sphere_id,
                                                                        '/out', '/meta/params.txt')
        with open('/out/translation.txt') as translation_file:
            file_hashes_map = {k: v for k, v in map(lambda x: x.strip().split("="), translation_file.readlines())}
        self.__engine_communication_store.store_file_hashes_map(task.customer, task.sphere_id, file_hashes_map)
        file_hashes_artifact = ClustersUnificationSourceHashArtifact(task.customer, task.execution_id,
                                                                     file_hashes_map)
        self.__artifact_store.store_artifact(file_hashes_artifact)
        self.__logger.debug('Finished storing results')

    @staticmethod
    def __run_phase(phase_args, stdout_fd, stderr_fd):
        phase_process = subprocess.Popen(['/bin/sh', '/home/ubuntu/ENGINE/run_AthenaMain.sh',
                                          '/usr/local/MATLAB/MATLAB_Compiler_Runtime/v83', phase_args],
                                         stdout=stdout_fd, stderr=stderr_fd,
                                         cwd='/home/ubuntu/ENGINE')
        return phase_process.wait()

    def __log_file_size(self, file_path, logical_name):
        file_size = os.path.getsize(file_path)
        self.__logger.debug('File {} is sized {}'.format(logical_name, file_size), extra={
            'file_path': file_path,
            'logical_name': logical_name,
            'file_size': file_size
        })

    @staticmethod
    def get_task_type():
        return ClustersUnificationTask

    @staticmethod
    def hash_djb2(inp):
        result = 5381
        for x in inp:
            result = ((result << 5) + result) + ord(x)
        return result & 0xFFFFFFFF


def build_prod_clusters_unifier(artifact_store, io_handler):
    return ClustersUnifier(artifact_store, URIBasedEngineCommunicationStore(io_handler))