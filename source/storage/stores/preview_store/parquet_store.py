import json
import logging
from cStringIO import StringIO

import pandas as pd

from source.storage.stores.preview_store.interface import DataPreviewStoreInterface


class ParquetDataPreviewStore(DataPreviewStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
        self.__logger = logging.getLogger('endor')
        self.__io_handler = io_handler

    def store_uniques_dict(self, customer, preview_id, uniques_dict):
        path = "sandbox-{}/DataPreviews/{}/uniques.json".format(customer, preview_id)
        return self.__io_handler.save_raw_data(json.dumps(uniques_dict, allow_nan=False), path)

    def load_uniques_dict(self, customer, preview_id):
        path = "sandbox-{}/DataPreviews/{}/uniques.json".format(customer, preview_id)
        return json.loads(self.__io_handler.load_raw_data(path))

    def store_html_output(self, customer, preview_id, html_data):
        path = "sandbox-{}/DataPreviews/{}/profile.html".format(customer, preview_id)
        self.__io_handler.save_raw_data(html_data, path)
        self.__io_handler.add_public_read(path)
        return self.__io_handler.get_object_url(path)

    def load_data_preview_pandas(self, customer, preview_id):
        path = "sandbox-{}/DataPreviews/{}/data-csv".format(customer, preview_id)
        date_col_indices_path = "sandbox-{}/DataPreviews/{}/date_indices.json".format(customer, preview_id)
        if not self.__io_handler.path_exists(date_col_indices_path):
            return None
        date_indices = json.loads(self.__io_handler.load_raw_data(date_col_indices_path))
        local_data = StringIO()
        for f in self.__io_handler.list_dir(path):
            if 'part' in f:
                remote_file = self.__io_handler.open(f)
                data = remote_file.read(10 * 1024 * 1024)
                while data != '':
                    local_data.write(data)
                    data = remote_file.read(10 * 1024 * 1024)
        local_data.seek(0)
        return pd.read_csv(local_data, parse_dates=date_indices, encoding='utf-8', error_bad_lines=False)
