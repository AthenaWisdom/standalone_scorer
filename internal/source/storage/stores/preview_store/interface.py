from pandas import DataFrame as PandasDataFrame


class DataPreviewStoreInterface(object):
    def store_uniques_dict(self, customer, preview_id, uniques_dict):
        """
        @type customer: C{str}
        @type preview_id: C{str}
        @type uniques_dict: C{dict}
        """
        raise NotImplementedError()

    def load_uniques_dict(self, customer, preview_id):
        """
        @type customer: C{str}
        @type preview_id: C{str}
        @rtype: C{dict}
        """
        raise NotImplementedError()

    def store_html_output(self, customer, preview_id, html_data):
        """
        @type customer: C{str}
        @type preview_id: C{str}
        @type html_data: C{unicode}
        @return: The URL to the remote file
        @rtype: C{str}
        """
        raise NotImplementedError()

    def load_data_preview_pandas(self, customer, preview_id):
        """
        @type customer: C{str}
        @type preview_id: C{str}
        @rtype: C{PandasDataFrame}
        """
        raise NotImplementedError()
