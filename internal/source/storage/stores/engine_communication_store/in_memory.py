import gzip
import os
import tempfile
from cStringIO import StringIO
from hashlib import md5

from source.storage.io_handlers.in_memory import InMemoryIOHandler
from source.storage.stores.engine_communication_store.interface import EngineCommunicationStoreInterface
from source.storage.stores.engine_communication_store.types import RunPair, InputCSV

DATAFRAME_CSV_FILE_NAME = 'dataframe.csv'


class InMemoryEngineCommunicationStore(EngineCommunicationStoreInterface):
    def get_input_csv_size(self, customer, sphere_id, input_csv_name):
        header_path, input_csv_path, _ = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        input_csv_single_file_contents = self.__io_handler[os.path.join(input_csv_path, DATAFRAME_CSV_FILE_NAME)]
        return len(input_csv_single_file_contents)

    def __init__(self):
        self.__io_handler = InMemoryIOHandler()

    def _save_input_csv(self, customer, sphere_id, input_csv):
        """
        Saves the given input CSV if it does not exist
        @param customer: The relevant customer
        @type customer: C{str}
        @param sphere_id: The relevant sphere
        @type sphere_id: C{str}
        @param input_csv: The input CSV to save
        @type input_csv: L{InputCSV}
        """
        header_path, output_path, hash_path = self.__get_input_csv_path(customer, sphere_id, input_csv.name)

        if self.__io_handler.path_exists(output_path):
            return

        try:
            # Assume it's a Pandas DataFrame
            input_csv_as_csv = input_csv.df.to_csv()
        except AttributeError:
            # Assume it's a Spark DataFrame
            input_csv_as_csv = input_csv.df.toPandas().to_csv()

        self.__io_handler.save_raw_data(input_csv_as_csv, os.path.join(output_path, DATAFRAME_CSV_FILE_NAME))

        headers = ','.join(input_csv.df.columns) + os.linesep
        self.__io_handler.save_raw_data(headers, header_path)
        checksum = md5()
        checksum.update(headers)
        checksum.update(input_csv_as_csv)
        self.__io_handler.save_raw_data(checksum.hexdigest(), hash_path)

    @staticmethod
    def __get_input_csv_path(customer, sphere_id, input_csv_name):
        output_path = '{}_{}_{}'.format(customer, sphere_id, input_csv_name)
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
        self._save_input_csv(customer, sphere_id, run_pair.input_csv)
        params_path = os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id, 'intermediate',
                                   'run_pairs', run_pair.name, 'params.txt')
        self.__io_handler.save_raw_data(run_pair.params.data, params_path)

    def load_input_csv(self, customer, sphere_id, input_csv_name, connecting_field):
        """
        Downloads the relevant input_csv
        Returns the input csv path

        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @rtype: C{tuple}
        """

        header_path, input_csv_path, _ = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        input_csv_local_path = tempfile.mkstemp()[1]

        with open(input_csv_local_path, 'w') as input_csv_file:
            # Read header file
            header_file_contents = self.__io_handler[header_path]
            input_csv_file.write(header_file_contents)

            # Read contents file
            input_csv_single_file_contents = self.__io_handler[os.path.join(input_csv_path, DATAFRAME_CSV_FILE_NAME)]
            input_csv_file.write(input_csv_single_file_contents)

        with open(input_csv_local_path) as input_csv_file:
            for i, l in enumerate(input_csv_file):
                pass
            # noinspection PyUnboundLocalVariable
            num_rows_in_input_csv = i + 1

        return input_csv_local_path, num_rows_in_input_csv

    def load_params_data(self, customer, sphere_id, input_csv_name, connecting_field):
        """
        Downloads the relevant params
        Returns params content

        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @rtype: C{str}
        """
        params_path = os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id, 'intermediate',
                                   'run_pairs', '{}_{}'.format(input_csv_name, connecting_field), 'params.txt')
        params_txt_contents = self.__io_handler.load_raw_data(params_path)
        return params_txt_contents

    def load_all_run_pairs(self, customer, sphere_id):
        run_pairs_folder = os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id,
                                        'intermediate', 'run_pairs')
        return list(self.__io_handler.list_dir(run_pairs_folder))

    def store_block(self, customer, sphere_id, input_csv_name, connecting_field, results_folder):
        """
        Stores the results of a run pair

        @param results_folder: The folder in which the results are stored
        @type results_folder: C{str}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        """
        output_folder = self.get_run_pair_results_output_folder(customer, sphere_id, input_csv_name, connecting_field)
        self.__io_handler.store_folder_contents(results_folder, output_folder)

    @staticmethod
    def get_run_pair_results_output_folder(customer, sphere_id, input_csv_name, connecting_field):
        return os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id, 'intermediate',
                            'run_pair_results', '{}_{}'.format(input_csv_name, connecting_field))

    def list_run_pair_result_files(self, customer, sphere_id, input_csv_name, connecting_field):
        output_folder = self.get_run_pair_results_output_folder(customer, sphere_id, input_csv_name, connecting_field)
        return self.__io_handler.list_dir(output_folder)

    def get_input_csv_header(self, customer, sphere_id, input_csv_name):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @rtype: C{list}
        """
        header_path, _, _ = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        return self.__io_handler.load_raw_data(header_path).strip().split(',')

    def get_input_csv_hash(self, customer, sphere_id, input_csv_name):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @rtype: C{str}
        """
        _, _, hash_path = self.__get_input_csv_path(customer, sphere_id, input_csv_name)
        return self.__io_handler.load_raw_data(hash_path)

    def list_input_csvs(self, customer, sphere_id):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @rtype: C{list}
        """
        dir_path = os.path.join('sandbox-{}'.format(customer), 'Spheres', sphere_id,
                                'intermediate', 'input_csv')
        return map(os.path.basename, self.__io_handler.list_dir(dir_path))

    def __get_memoized_block_path(self, block_checksum):
        return os.path.join('memoized-blocks', block_checksum)

    def copy_memoized_block_to_intermediate(self, customer, sphere_id, input_csv_name, connecting_field,
                                            block_checksum):
        remote_input_folder = self.__get_memoized_block_path(block_checksum)
        remote_output_folder = self.get_run_pair_results_output_folder(customer, sphere_id, input_csv_name,
                                                                       connecting_field)

        return self.__io_handler.copy_contents_from_remote_folder_to_remote_folder(remote_input_folder,
                                                                                   remote_output_folder)

    def memoize_block(self, customer, sphere_id, input_csv_name, connecting_field, block_checksum):
        remote_output_folder = self.__get_memoized_block_path(block_checksum)
        remote_input_folder = self.get_run_pair_results_output_folder(customer, sphere_id, input_csv_name,
                                                                      connecting_field)

        return self.__io_handler.copy_contents_from_remote_folder_to_remote_folder(remote_input_folder,
                                                                                   remote_output_folder)

    def store_matlab_logs(self, customer, sphere_id, input_csv_name, connecting_field, log_name, log_file_path):
        log_filename = '{}.txt'.format(log_name)
        remote_logs_path = os.path.join('matlab-logs',
                                        self.get_run_pair_results_output_folder(customer, sphere_id,
                                                                                input_csv_name, connecting_field),
                                        log_filename + '.gz')
        logs_data = self.__gzip_file_to_string(log_file_path, log_filename)
        self.__io_handler.save_raw_data(logs_data, remote_logs_path)

    def __gzip_file_to_string(self, log_file_path, log_filename):
        data_sio = StringIO()
        with open(log_file_path) as log_file, gzip.GzipFile(log_filename, 'w', fileobj=data_sio) as log_gzip:
            log_gzip.write(log_file.read())
        logs_data = data_sio.getvalue()
        return logs_data

    def store_unifier_logs(self, customer, sphere_id, log_name, log_file_path):
        log_filename = 'unifier.log'.format(log_name)
        remote_logs_path = os.path.join('matlab-logs', 'sandbox-{}'.format(customer), 'Spheres',
                                        sphere_id, 'artifacts', 'blocks', log_filename + '.gz')
        logs_data = self.__gzip_file_to_string(log_file_path, log_filename)
        self.__io_handler.save_raw_data(logs_data, remote_logs_path)
