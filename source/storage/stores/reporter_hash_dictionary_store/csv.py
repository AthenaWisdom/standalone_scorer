import os
from cStringIO import StringIO

import pandas as pd

from source.storage.stores.reporter_hash_dictionary_store.interface import ReporterHashDictionaryStoreInterface


class CsvReporterHashDictionaryStore(ReporterHashDictionaryStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
        super(ReporterHashDictionaryStoreInterface, self).__init__()
        self.__io_handler = io_handler

    class UnseekableFileLike(object):
        def __init__(self, file_like):
            super(CsvReporterHashDictionaryStore.UnseekableFileLike, self).__init__()
            self.__output_file = file_like

        def write(self, data_buffer):
            self.__output_file.write(data_buffer)

    def load_dictionary(self, customer_id, quest_id, query_id):
        """
        @type customer_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype: L{pd.DataFrame}
        """
        path = os.path.join('sandbox-{}'.format(customer_id), 'Quests', quest_id, query_id,
                            'reporterHashDictionary.csv')
        data = StringIO()
        header = "NORMAL_ID,ID,white" + os.linesep
        data.write(header)

        # Find out which files comprise the final csv file
        data_files = self.__io_handler.list_dir(path)
        part_files = filter(lambda p: 'part' in p, data_files)

        # This hack is here because boto3 seeks to the beginning of the file object if possible
        unseekable_file_like = self.UnseekableFileLike(data)
        for data_file_path in sorted(part_files):
            # Download and process each part file
            self.__io_handler.download_fileobj(data_file_path, unseekable_file_like)
        data.seek(0)

        return pd.read_csv(data, index_col=1)
