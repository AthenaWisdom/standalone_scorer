from source.storage.stores.engine_communication_store.types import RunPair, InputCSV


class EngineCommunicationStoreInterface(object):
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
        raise NotImplementedError()

    def save_run_pair(self, customer, sphere_id, run_pair, should_save_input_csv=True):
        """
        Saves the given run pair for the engine to use
        @type should_save_input_csv: C{bool}
        @param customer: The relevant customer
        @type customer: C{str}\
        @param sphere_id: The relevant sphere
        @type sphere_id: C{str}
        @param run_pair: The run pair to save.
        @type run_pair: L{RunPair}
        """
        raise NotImplementedError()

    def load_input_csv(self, customer, sphere_id, input_csv_name, connecting_field):
        """
        Downloads the relevant input_csv and params
        Returns the input csv path

        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @rtype: C{tuple}
        """
        raise NotImplementedError()

    def get_input_csv_size(self, customer, sphere_id, input_csv_name):
        """
        Returns the size in bytes of an input-csv

        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @rtype: C{int}
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def download_sphere_blocks(self, customer, sphere_id, local_folder, filter_only=None):
        """
        @param filter_only: Only download run pairs in the given list
        @type filter_only: C{list} of C{str}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type local_folder: C{str}
        """
        raise NotImplementedError()

    def download_single_params(self, customer, sphere_id, local_folder):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type local_folder: C{str}
        """
        raise NotImplementedError()

    def store_unification_results(self, customer, sphere_id, results_folder, params_path):
        """
        @param params_path:
        @type params_path:
        @type customer: C{str}
        @type sphere_id: C{str}
        @type results_folder: C{str}
        """
        raise NotImplementedError()

    def store_file_hashes_map(self, customer, sphere_id, file_hashes_map):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type file_hashes_map: C{dict}
        """
        raise NotImplementedError()

    def load_file_hashes_map(self, customer, sphere_id):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @rtype: C{dict}
        """
        raise NotImplementedError()

    def list_input_csvs(self, customer, sphere_id):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @rtype: C{list}
        """
        raise NotImplementedError()

    def get_input_csv_header(self, customer, sphere_id, input_csv_name):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @rtype: C{list}
        """
        raise NotImplementedError()

    def copy_memoized_block_to_intermediate(self, customer, sphere_id, input_csv_name, connecting_field,
                                            block_checksum):
        """
        Copies a memoized block to the intermediate directory of a customer's sphere

        @param block_checksum: The checksum of the block
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @type block_checksum: C{str}
        @rtype C{int}
        @return number of files copied
        """
        raise NotImplementedError()

    def memoize_block(self, customer, sphere_id, input_csv_name, connecting_field,
                      block_checksum):
        """
        Fetches a block from the intermediate directory of a customer's sphere and memoizes it

        @param block_checksum: The checksum of the block
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @type block_checksum: C{str}
        @rtype C{int}
        @return number of files copied
        """
        raise NotImplementedError()

    def get_input_csv_hash(self, customer, sphere_id, input_csv_name):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @rtype: C{str}
        """
        raise NotImplementedError()

    def store_matlab_logs(self, customer, sphere_id, input_csv_name, connecting_field, log_name, log_file_path):
        """
        Save a log file from MATLAB
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @type log_name: C{str}
        @type log_file_path: C{str}
        """
        raise NotImplementedError()

    def store_unifier_logs(self, customer, sphere_id, log_name, log_file_path):
        """
        Save a log file from MATLAB
        @type customer: C{str}
        @type sphere_id: C{str}
        @type log_name: C{str}
        @type log_file_path: C{str}
        """
        raise NotImplementedError()
