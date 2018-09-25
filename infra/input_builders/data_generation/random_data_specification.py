from datetime import datetime, timedelta
from random import Random
import string

__author__ = 'izik'


class RandomDataSpecifications(object):
    types = ['str', 'int', 'datetime', 'double']

    def __init__(self, seed=None):
        self.__randomizer = Random(seed)
        self.__line_count = 0
        self.__col_count = 0
        self.__header = []
        self.__schema = []
        self.__additional_column_specs = []

    @classmethod
    def Rand(cls, seed=None):
        res = RandomDataSpecifications(seed)
        random = res.__randomizer
        line_count = random.randint(10, 1000)
        col_count = random.randint(3, 7)
        header = ['field' + str(x + 1) for x in xrange(col_count)]
        schema = []
        for col_index in xrange(col_count):
            col_type = random.randint(0, len(RandomDataSpecifications.types) - 1)
            schema.append(RandomDataSpecifications.types[col_type])
        res.set_stuff(line_count=line_count, col_count=col_count, header=header, schema=schema)

    @property
    def item_count(self):
        return self.__line_count

    @property
    def schema(self):
        return self.__schema

    @property
    def header(self):
        return self.__header

    @property
    def additional_columns(self):
        return self.__additional_column_specs

    def set_stuff(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, '_{}__{}'.format(self.__class__.__name__, key), value)
        return self

    def add_column_by_specs(self, column_name, value_spec):
        self.__additional_column_specs.append((column_name, value_spec))
        return self

    def add_columns(self, *columns_specs):
        for column_name, value_specs in columns_specs:
            self.add_column_by_specs(column_name, value_specs)
        return self

    def set_item_count(self, item_count):
        self.__line_count = item_count
        return self


class ValueSpecification(object):
    def generate_value(self, current_row, randomizer, index):
        raise NotImplementedError()


class IndexValueSpec(ValueSpecification):
    def generate_value(self, current_row, randomizer, index):
        return index


class FunctionValueSpec(ValueSpecification):
    def __init__(self, f):
        self.__f = f

    def generate_value(self, current_row, randomizer, index):
        return self.__f(current_row, randomizer, index)


class ConstantValueSpecification(ValueSpecification):
    def __init__(self, value):
        self.__value = value

    def generate_value(self, current_row, randomizer, index):
        return self.__value


class UnifiedIntSpec(ValueSpecification):
    def __init__(self, min_value=0, max_value=1000000):
        self.__max_value = max_value
        self.__min_value = min_value

    def generate_value(self, current_row, randomizer, index):
        return randomizer.randint(self.__min_value, self.__max_value)


class UnifiedRandomValueSpec(ValueSpecification):
    @staticmethod
    def create_rand_int(rand):
        return rand.randint(0, 1000000)

    @staticmethod
    def create_random_string(rand):
        return ''.join(rand.choice(string.ascii_uppercase + string.digits) for _ in xrange(16))

    @staticmethod
    def create_random_datetime(rand):
        min_datetime = datetime(1950, 1, 1)
        added_minutes = rand.randint(0, 60000000)
        res = min_datetime + timedelta(minutes=added_minutes)
        return res

    @staticmethod
    def create_random_double(rand):
        return rand.random()

    def generate_value(self, current_row, randomizer, index):
        return self.__random_value_creator[self.__type](randomizer)

    def __init__(self, type):
        self.__type = type
        self.__random_value_creator = {
            'int': self.create_rand_int,
            'str': self.create_random_string,
            'datetime': self.create_random_datetime,
            'double': self.create_random_double
        }


class BernuliValueSpec(ValueSpecification):
    def generate_value(self, current_row, randomizer, index):
        return self.__positive_value if randomizer.random() < self.__p else self.__negative_value

    def __init__(self, p=0.5, positive_value=1, negative_value=0):
        self.__p = p
        self.__positive_value = positive_value
        self.__negative_value = negative_value


class FilteredValueSpec(ValueSpecification):
    def generate_value(self, current_row, randomizer, index):
        return self.__value_specification.generate_value(current_row, randomizer, index) if self.__predicate(current_row) else None

    def __init__(self, predicate, value_specification):
        self.__value_specification = value_specification
        self.__predicate = predicate


class MultiFilterSpec(ValueSpecification):
    def generate_value(self, current_row, randomizer, index):
        for filters_spec in self.__filtered_value_specifications:
            val = filters_spec.generate_value(current_row, randomizer, index)
            if val is not None:
                return val
        return None

    def __init__(self, *filtered_value_specifications):
        self.__filtered_value_specifications = [FilteredValueSpec(filter_spec, value_spec) for filter_spec, value_spec in
                                                filtered_value_specifications]

        # build present kernel with (ID, white, groumd, suspect, bla bla)
        # build unique identifier column. call it ID.
        # add boolean column with percentage (whites)
        # add boolean column with percentage (ground)
        # add column called universe. IGNORE???????????????????????????
        # set universe value to all column answering a specific condition
        # add column called x.
        # set value to be equal x to something anwering a filter.

        # create scores dataset
        # ID column from the a subset of the kernel column (with percentage of who to take).
        # assign value with probability x for rows answering condition y
