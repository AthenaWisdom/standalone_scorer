import os

TRANSLATION_TABLES_FOLDER = 'translation_tables'

__author__ = 'izik'
DATA_FOLDER = "data"
DATASET_FOLDER = "datasets"

STATS_FOLDER = "csv_stats"
GLOBAL_STATS_FOLDER = "global_stats"
USER_STATS_FOLDER = "user_stats"
COLUMN_STATS_FOLDER = "column_stats"
INPUT_CSV_FOLDER = "input_csv"


class UriResolver(object):
    def __init__(self, base_uri, post_resolve_callback=None):
        self.__base_uri = base_uri
        if post_resolve_callback is not None and not callable(post_resolve_callback):
            raise ValueError('Post resolve callback is not callable')
        self.__post_resolve_callback = post_resolve_callback

    @property
    def base_uri(self):
        return self.__base_uri

    def get_uri_for_raw_data(self, customer_id, dataset_id):
        return os.path.join(self.__base_uri, customer_id, 'data', dataset_id, 'raw', '*')

    def get_input_csv_uri(self, customer_id, dataset_id, execution_id, input_csv_name):
        return os.path.join(self.__base_uri, customer_id, DATASET_FOLDER, dataset_id,
                            'input_csv', execution_id, input_csv_name)

    def get_params_uri(self, customer_id, dataset_id, execution_id, params_folder_name):
        return os.path.join(self.__base_uri, customer_id, DATASET_FOLDER, dataset_id, 'intermediate', 'raw_blocks',
                            execution_id, params_folder_name)

    def __get_stats_folder_uri(self, customer_id, dataset_id, execution_id, folder_name):
        return os.path.join(self.__base_uri, customer_id, DATA_FOLDER, dataset_id,
                            STATS_FOLDER, execution_id, folder_name)

    def get_hashing_translation_table_uri(self, field_name, customer_id, dataset_id, execution_id):
        return os.path.join(self.__base_uri, customer_id, DATA_FOLDER, dataset_id,
                            TRANSLATION_TABLES_FOLDER, execution_id, field_name)

    def get_global_stats_uri(self, *arg, **kwargs):
        return self.__get_stats_folder_uri(*arg, folder_name=GLOBAL_STATS_FOLDER, **kwargs)

    def get_user_stats_uri(self, *arg, **kwargs):
        return self.__get_stats_folder_uri(*arg, folder_name=USER_STATS_FOLDER, **kwargs)

    def get_column_stats_uri(self, *arg, **kwargs):
        return self.__get_stats_folder_uri(*arg, folder_name=COLUMN_STATS_FOLDER, **kwargs)

    def get_num_tasks_uri(self, execution_id):
        return os.path.join(self.__base_uri, execution_id, 'num_launched')

    def __getattribute__(self, item):
        attribute = super(UriResolver, self).__getattribute__(item)

        if callable(attribute) and item.startswith('get_') and self.__post_resolve_callback is not None:
            def wrapped(*args, **kwargs):
                return_value = attribute(*args, **kwargs)
                self.__post_resolve_callback(return_value)
                return return_value
            return wrapped
        return attribute