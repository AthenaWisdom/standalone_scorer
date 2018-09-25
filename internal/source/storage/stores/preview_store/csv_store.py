import json
import tempfile

import pandas as pd

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.preview_store.interface import DataPreviewStoreInterface


class CSVDataPreviewStore(DataPreviewStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
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
        path = "sandbox-{}/DataPreviews/{}/pandas_df.pickle".format(customer, preview_id)
        temp_path = tempfile.mkstemp()[1]
        with open(temp_path, 'wb') as f:
            f.write(self.__io_handler.load_raw_data(path))
        return pd.read_pickle(temp_path)
