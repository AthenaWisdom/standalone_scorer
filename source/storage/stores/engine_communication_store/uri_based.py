import gzip
import json
import logging
import os
import re
import tempfile
from cStringIO import StringIO
from contextlib import closing

from functional import seq

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.engine_communication_store.interface import EngineCommunicationStoreInterface
from source.storage.stores.engine_communication_store.types import RunPair


class URIBasedEngineCommunicationStore(EngineCommunicationStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
        self.__io_handler = io_handler
        self.__logger = logging.getLogger('endor.storage.URIBasedEngineCommunicationStore')

    def _save_input_csv(self, customer, sphere_id, input_csv):
        raise NotImplementedError()

    def get_input_csv_hash(self, customer, sphere_id, input_csv_name):
        _, _, hash_path = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        return self.__io_handler.load_raw_data(hash_path)

    def load_params_data(self, customer, sphere_id, input_csv_name, connecting_field):
        params_path = os.path.join(self.__get_intermediate_run_pairs_path(customer, sphere_id),
                                   '{}_{}'.format(input_csv_name, connecting_field), 'params.txt')
        return self.__io_handler.load_raw_data(params_path)

    def __get_customer_sphere_base_path(self, customer, sphere_id):
        return os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id)

    def __get_intermediate_path(self, customer, sphere_id):
        return os.path.join(self.__get_customer_sphere_base_path(customer, sphere_id), 'intermediate')

    def __get_artifacts_path(self, customer, sphere_id):
        return os.path.join(self.__get_customer_sphere_base_path(customer, sphere_id), 'artifacts')

    def __get_intermediate_input_csv_path(self, customer, sphere_id):
        return os.path.join(self.__get_intermediate_path(customer, sphere_id), 'input_csv')

    def __get_intermediate_run_pairs_path(self, customer, sphere_id):
        return os.path.join(self.__get_intermediate_path(customer, sphere_id), 'run_pairs')

    def __get_intermediate_blocks_path(self, customer, sphere_id):
        return os.path.join(self.__get_intermediate_path(customer, sphere_id), 'run_pair_results')

    def __get_file_hashes_map_json_path(self, customer, sphere_id):
        return os.path.join(self.__get_artifacts_path(customer, sphere_id), 'file_hashes_map.json')

    def __get_intermediate_block_path(self, customer, sphere_id, input_csv_name, connecting_field):
        return os.path.join(self.__get_intermediate_blocks_path(customer, sphere_id),
                            '{}_{}'.format(input_csv_name, connecting_field))

    def __get_memoized_block_path(self, block_checksum):
        return os.path.join('memoized-blocks', block_checksum)

    def __get_input_csv_path(self, customer, sphere_id, input_csv_name):
        output_path = os.path.join(self.__get_intermediate_input_csv_path(customer, sphere_id), input_csv_name)
        header_path = os.path.join(output_path, 'header.csv')
        hash_path = os.path.join(output_path, 'hash.md5')
        return header_path, output_path, hash_path

    def save_run_pair(self, customer, sphere_id, run_pair, should_save_input_csv=True):
        """
        Saves the given run pair for the engine to use
        @type should_save_input_csv: C{bool}
        @param customer: The relevant customer
        @type customer: C{str}
        @param sphere_id: The relevant sphere
        @type sphere_id: C{str}
        @param run_pair: The run pair to save.
        @type run_pair: L{RunPair}
        """
        if should_save_input_csv:
            self._save_input_csv(customer, sphere_id, run_pair.input_csv)
        params_path = os.path.join(self.__get_intermediate_run_pairs_path(customer, sphere_id), run_pair.name,
                                   'params.txt')
        self.__io_handler.save_raw_data(run_pair.params.data, params_path)

    def store_file_hashes_map(self, customer, sphere_id, file_hashes_map):
        self.__io_handler.save_raw_data(json.dumps(file_hashes_map, allow_nan=False),
                                        self.__get_file_hashes_map_json_path(customer, sphere_id),
                                        'application/json')

    def load_file_hashes_map(self, customer, sphere_id):
        return json.loads(self.__io_handler.load_raw_data(self.__get_file_hashes_map_json_path(customer, sphere_id)))

    class SanitizingFileLike(object):
        def __init__(self, output_file_path):
            super(URIBasedEngineCommunicationStore.SanitizingFileLike, self).__init__()

            self.__num_written_rows = 0
            self.__output_file = open(output_file_path, 'w')
            self.__replace_regex = re.compile('"(?P<full>-\d+(\.\d+)?)"')

        def write(self, data_buffer):
            sanitized_data_buffer = self.__replace_regex.sub('\g<full>', data_buffer).replace('"', '')

            # Every write to the input csv file will be accompanied by a write to the MD5 checksum
            self.__num_written_rows += sanitized_data_buffer.count('\n')
            self.__output_file.write(sanitized_data_buffer)

        def close(self):
            self.__output_file.close()

        @property
        def num_written_rows(self):
            return self.__num_written_rows

    def load_input_csv(self, customer, sphere_id, input_csv_name, connecting_field):
        # Download the merged input csv to a local directory
        header_path, input_csv_path, _ = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        local_input_csv_path = tempfile.mkstemp()[1]

        with closing(self.SanitizingFileLike(local_input_csv_path)) as local_input_csv_file:
            header_data = self.__io_handler.load_raw_data(header_path).strip() + os.linesep
            local_input_csv_file.write(header_data)

            # Find out which files comprise the final csv file
            data_files = self.__io_handler.list_dir(os.path.join(input_csv_path, 'data'))
            part_files = filter(lambda path: 'part' in path, data_files)

            for data_file_path in sorted(part_files):
                # Download and process each part file
                self.__io_handler.download_fileobj(data_file_path, local_input_csv_file)
            num_rows_in_input_csv = local_input_csv_file.num_written_rows

        return local_input_csv_path, num_rows_in_input_csv

    def get_input_csv_size(self, customer, sphere_id, input_csv_name):
        header_path, input_csv_path, _ = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        data_files = self.__io_handler.list_files(os.path.join(input_csv_path, 'data'))
        part_files = filter(lambda f: 'part' in f.path, data_files)

        return sum(f.size for f in part_files)

    def store_block(self, customer, sphere_id, input_csv_name, connecting_field, results_folder):
        output_folder = self.__get_intermediate_block_path(customer, sphere_id, input_csv_name, connecting_field)
        self.__io_handler.store_folder_contents(results_folder, output_folder)

    def download_sphere_blocks(self, customer, sphere_id, local_folder, filter_only=None):
        run_pairs = self.__io_handler.list_dir(self.__get_intermediate_blocks_path(customer, sphere_id))
        if filter_only is not None:
            run_pairs = seq(run_pairs).filter(lambda x: os.path.basename(x) in filter_only).to_list()
        for run_pair in run_pairs:
            for file_name in self.__io_handler.list_dir(run_pair):
                if 'BlockDataAll_' in file_name:
                    local_file_name = '{}__sep__{}__sep__{}'.format(sphere_id, os.path.basename(run_pair),
                                                                    os.path.basename(file_name))
                    with open(os.path.join(local_folder, local_file_name), 'wb') as local_file:
                        local_file.write(self.__io_handler.load_raw_data(file_name))
                    self.__logger.debug('Saved "{}" as "{}"'.format(file_name, local_file_name))

    def download_single_params(self, customer, sphere_id, local_folder):
        for run_pair in self.__io_handler.list_dir(self.__get_intermediate_run_pairs_path(customer, sphere_id)):
            for file_name in self.__io_handler.list_dir(run_pair):
                if 'params' in file_name:
                    with open(os.path.join(local_folder, 'params.txt'), 'wb') as local_file:
                        local_file.write(self.__io_handler.load_raw_data(file_name))
                    return True
        return False

    def store_unification_results(self, customer, sphere_id, results_folder, params_path):
        remote_results_path = os.path.join(self.__get_artifacts_path(customer, sphere_id), 'blocks')
        self.__io_handler.store_folder_contents(results_folder, remote_results_path)

        with open(params_path, 'rb') as params_file:
            arbitrary_params_path = os.path.join(self.__get_intermediate_run_pairs_path(customer, sphere_id), 'temp',
                                                 'params.txt')
            self.__io_handler.save_raw_data(params_file.read(), arbitrary_params_path)

    def list_input_csvs(self, customer, sphere_id):
        return seq(self.__io_handler.list_dir(self.__get_intermediate_input_csv_path(customer, sphere_id)))\
            .map(os.path.basename)\
            .filter_not(lambda x: "$folder$" in x)\
            .to_list()

    def get_input_csv_header(self, customer, sphere_id, input_csv_name):
        header_path, _, _ = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        return self.__io_handler.load_raw_data(header_path).strip().split(',')

    def copy_memoized_block_to_intermediate(self, customer, sphere_id, input_csv_name, connecting_field,
                                            block_checksum):
        remote_input_folder = self.__get_memoized_block_path(block_checksum)
        remote_output_folder = self.__get_intermediate_block_path(customer, sphere_id, input_csv_name, connecting_field)

        return self.__io_handler.copy_contents_from_remote_folder_to_remote_folder(remote_input_folder,
                                                                                   remote_output_folder)

    def memoize_block(self, customer, sphere_id, input_csv_name, connecting_field,
                      block_checksum):
        remote_output_folder = self.__get_memoized_block_path(block_checksum)
        remote_input_folder = self.__get_intermediate_block_path(customer, sphere_id, input_csv_name, connecting_field)

        return self.__io_handler.copy_contents_from_remote_folder_to_remote_folder(remote_input_folder,
                                                                                   remote_output_folder)

    def store_matlab_logs(self, customer, sphere_id, input_csv_name, connecting_field, log_name, log_file_path):
        log_filename = '{}.txt'.format(log_name)
        remote_logs_path = os.path.join('matlab-logs', self.__get_intermediate_blocks_path(customer, sphere_id),
                                        '{}_{}'.format(input_csv_name, connecting_field),
                                        log_filename + '.gz')
        logs_data = self.gzip_file_to_string(log_file_path, log_filename)
        self.__io_handler.save_raw_data(logs_data, remote_logs_path)

    @staticmethod
    def gzip_file_to_string(log_file_path, log_filename):
        data_sio = StringIO()
        with open(log_file_path) as log_file, gzip.GzipFile(log_filename, 'w', fileobj=data_sio) as log_gzip:
            log_gzip.write(log_file.read())
        logs_data = data_sio.getvalue()
        return logs_data

    def store_unifier_logs(self, customer, sphere_id, log_name, log_file_path):
        log_filename = 'unifier.log'.format(log_name)
        remote_logs_path = os.path.join('matlab-logs', self.__get_artifacts_path(customer, sphere_id),
                                        'blocks', log_filename + '.gz')
        logs_data = self.gzip_file_to_string(log_file_path, log_filename)
        self.__io_handler.save_raw_data(logs_data, remote_logs_path)
