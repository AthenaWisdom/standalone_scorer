from datetime import datetime, date

from interface import ArtifactInterface


__all__ = [
    'DataSummaryArtifact',
    'DataStatsArtifact',
    'BatchColumnStatsArtifact',
    'NewValuesPerColumnArtifact',
    'OldValuesPerColumnArtifact',
    'BatchFrequentValuesPerColumnArtifact',
    'RawDataSummaryArtifact',
    'DataGatewayConfigurationArtifact',
]


# noinspection PyAbstractClass
class DataGatewayBaseArtifact(ArtifactInterface):
    operation = 'data_gateway'


class DataSummaryArtifact(DataGatewayBaseArtifact):
    type = 'data_summary'

    def __init__(self, customer, dataset, batch_id, num_legal_rows, day):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type batch_id: C{str}
        @type num_legal_rows: C{int}
        @type day: C{datetime}
        """
        super(DataSummaryArtifact, self).__init__(customer, batch_id)
        self.__batch_id = batch_id
        self.__day = day
        self.__num_legal_rows = num_legal_rows
        self.__dataset = dataset

    def _to_dict(self):
        return {
            'day': self.__day,
            'dataset': self.__dataset,
            'batch_id': self.__batch_id,
            'num_legal_rows': self.__num_legal_rows,
        }


class DataStatsArtifact(DataGatewayBaseArtifact):
    type = 'data_stats'

    def __init__(self, customer, dataset, batch_id, min_time, max_time, percent_invalid_rows, num_invalid_rows):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type batch_id: C{str}
        @type min_time: C{datetime}
        @type max_time: C{datetime}
        @type percent_invalid_rows: C{float}
        @type num_invalid_rows: C{int}
        """
        super(DataStatsArtifact, self).__init__(customer, batch_id)
        self.__max_time = max_time
        self.__min_time = min_time
        self.__num_invalid_rows = num_invalid_rows
        self.__batch_id = batch_id
        self.__percent_invalid_rows = percent_invalid_rows
        self.__dataset = dataset

    def _to_dict(self):
        return {
            'batch_id': self.__batch_id,
            'max_time': self.__max_time,
            'min_time': self.__min_time,
            'num_invalid_rows': self.__num_invalid_rows,
            'percent_invalid_rows': self.__percent_invalid_rows
        }


class BatchColumnStatsArtifact(DataGatewayBaseArtifact):
    type = 'batch_column_stats'

    def __init__(self, customer, dataset, batch_id, stat_name, string_value=None,
                 integer_value=None, float_value=None, date_value=None, boolean_value=None):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type batch_id: C{str}
        @type stat_name: C{str}
        @type string_value: C{str}
        @type integer_value: C{int}
        @type float_value: C{float}
        @type date_value: C{datetime} or C{date}
        @type boolean_value: C{bool}
        """
        super(BatchColumnStatsArtifact, self).__init__(customer, batch_id)
        self.__customer = customer
        self.__dataset = dataset
        self.__batch_id = batch_id
        self.__stat_name = stat_name
        self.__string_value = string_value
        self.__integer_value = integer_value
        self.__float_value = float_value
        self.__date_value = date_value
        self.__boolean_value = boolean_value

    def _to_dict(self):
        return {
            'batch_id': self.__batch_id,
            'boolean_value': self.__boolean_value,
            'dataset': self.__dataset,
            'date_value': self.__date_value,
            'float_value': self.__float_value,
            'integer_value': self.__integer_value,
            'stat_name': self.__stat_name,
            'string_value': self.__string_value
        }


class ValuesPerColumnArtifact(DataGatewayBaseArtifact):
    type = 'META_values_per_column'

    def __init__(self, customer, dataset, batch_id, column_name, string_value=None,
                 integer_value=None, float_value=None, date_value=None, boolean_value=None):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type batch_id: C{str}
        @type column_name: C{str}
        @type string_value: C{list} of C{str}
        @type integer_value: C{list} of C{int}
        @type float_value: C{list} of C{float}
        @type date_value: C{list} of (C{datetime} or C{date})
        @type boolean_value: C{list} of C{bool}
        """
        super(ValuesPerColumnArtifact, self).__init__(customer, batch_id)
        self.__customer = customer
        self.__dataset = dataset
        self.__batch_id = batch_id
        self.__column_name = column_name
        self.__string_value = string_value
        self.__integer_value = integer_value
        self.__float_value = float_value
        self.__date_value = date_value
        self.__boolean_value = boolean_value

    def _to_dict(self):
        parent_dict = super(ValuesPerColumnArtifact, self)._to_dict()
        parent_dict.update({
            'batch_id': self.__batch_id,
            'boolean_value': self.__boolean_value,
            'dataset': self.__dataset,
            'date_value': self.__date_value,
            'float_value': self.__float_value,
            'integer_value': self.__integer_value,
            'column_name': self.__column_name,
            'string_value': self.__string_value
        })
        return parent_dict


class NewValuesPerColumnArtifact(ValuesPerColumnArtifact):
    type = 'new_values_per_column'


class OldValuesPerColumnArtifact(ValuesPerColumnArtifact):
    type = 'old_values_per_column'


class BatchFrequentValuesPerColumnArtifact(ValuesPerColumnArtifact):
    type = 'frequent_values_per_column'


class RawDataSummaryArtifact(DataGatewayBaseArtifact):
    type = 'raw_data_summary'

    def __init__(self, customer, dataset, batch_id, num_raw_rows):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type batch_id: C{str}
        @type num_raw_rows: C{int}
        """
        super(RawDataSummaryArtifact, self).__init__(customer, batch_id)
        self.__batch_id = batch_id
        self.__num_raw_rows = num_raw_rows
        self.__dataset = dataset

    def _to_dict(self):
        return {
            'dataset': self.__dataset,
            'batch_id': self.__batch_id,
            'num_raw_rows': self.__num_raw_rows,
        }


class DataGatewayConfigurationArtifact(DataGatewayBaseArtifact):
    type = 'data_gateway_configuration'

    def __init__(self, customer, dataset, batch_id, config):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type batch_id: C{str}
        @type config: C{dict}
        """
        super(DataGatewayConfigurationArtifact, self).__init__(customer, batch_id)
        self.__config = config
        self.__batch_id = batch_id
        self.__dataset = dataset

    def _to_dict(self):
        return {
            'dataset': self.__dataset,
            'batch_id': self.__batch_id,
            'config': self.__config,
        }
