from collections import defaultdict
from datetime import datetime, timedelta
import os
from random import Random
import string
import tempfile
import pandas as pd

__author__ = 'izik'


class DataGenerator(object):
    def __init__(self):
        self.random_value_creator = {
            'int': self.create_rand_int,
            'str': self.create_random_string,
            'datetime': self.create_random_datetime,
            'double': self.create_random_double
        }

    def generate_data_in_temp_folder(self, data_specification, seed=None):
        tmp_folder = tempfile.mkdtemp()
        randomizer = Random(seed)
        with open(os.path.join(tmp_folder, 'data.csv'), 'w') as f:
            f.write(','.join(data_specification.header) + '\n')
            for line_number in xrange(data_specification.item_count):
                f.write(self.create_line(data_specification.schema, randomizer) + '\n')
        return os.path.join(tmp_folder, 'data.csv')

    def generate_data_as_list_of_dicts(self, data_specification, seed=None):
        res = []
        randomizer = Random(seed)
        for i in xrange(data_specification.item_count):
            res.append(self.create_row(data_specification, randomizer, i))
        return res

    def generate_pandas_dataframe(self, data_specification, seed=None):
        list_of_dicts = self.generate_data_as_list_of_dicts(data_specification, seed)
        field_to_values = defaultdict(lambda: [])
        for entry in list_of_dicts:
            for field_name, field_value in entry.iteritems():
                field_to_values[field_name].append(field_value)

        return pd.DataFrame(field_to_values)

    def generate_spark_dataframe(self, sql_context, data_specification, seed=None):
        pandas_df = self.generate_pandas_dataframe(data_specification, seed)
        return sql_context.createDataFrame(pandas_df)


    def generate_data_as_string(self, data_specification):
        s = ','.join(data_specification.header) + '\n'
        for line_number in xrange(data_specification.item_count):
            s = s + self.create_line(data_specification.schema, data_specification.randomizer) + '\n'
        return s

    def create_rand_int(self, rand):
        return rand.randint(0, 1000000)

    def create_random_string(self, rand):
        return ''.join(rand.choice(string.ascii_uppercase + string.digits) for _ in xrange(16))

    def create_random_datetime(self, rand):
        min_datetime = datetime(1950, 1, 1)
        added_minutes = rand.randint(0, 60000000)
        res = min_datetime + timedelta(minutes=added_minutes)
        return res

    def create_random_double(self, rand):
        return rand.random()

    def create_row(self, data_spec, randomizer, index):
        row = {}
        for spec in data_spec.additional_columns:
            column_name = spec[0]
            value_specification = spec[1]
            row[column_name] = value_specification.generate_value(row, randomizer, index)
        return row

    def create_line(self, schema, rand):
        values = [self.random_value_creator[x](rand) for x in schema]
        return ','.join([str(x) for x in values])

