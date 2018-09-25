from datetime import datetime

import dateutil


class QueryMetadata(object):
    def __init__(self, customer, quest_id, query_id, split_kernel_id, sphere_id, query_timestamp):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type split_kernel_id: C{str}
        @type sphere_id: C{str}
        @type query_timestamp: C{datetime}
        """
        self.__customer = customer
        self.__quest_id = quest_id
        self.__query_id = query_id
        self.__split_kernel_id = split_kernel_id
        self.__sphere_id = sphere_id
        self.__query_timestamp = query_timestamp

    @property
    def customer(self):
        return self.__customer

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def query_id(self):
        return self.__query_id

    @property
    def split_kernel_id(self):
        return self.__split_kernel_id

    @property
    def sphere_id(self):
        return self.sphere_id

    @property
    def timestamp(self):
        return self.__query_timestamp

    def to_dict(self):
        """
        @rtype: C{dict}
        """
        return {
            'customer': self.__customer,
            'quest_id': self.__quest_id,
            'query_id': self.__query_id,
            'split_kernel_id': self.__split_kernel_id,
            'sphere_id': self.__sphere_id,
            'query_timestamp': self.__query_timestamp.strftime('%Y-%m-%dT%H:%M:%S'),
        }

    @classmethod
    def from_dict(cls, data):
        """
        @type data: C{dict}
        @rtype: L{QueryMetadata}
        """
        if isinstance(data['query_timestamp'], basestring):
            data['query_timestamp'] = dateutil.parser.parse(data['query_timestamp'])
        return cls(**data)


class QueryExecutionUnit(object):
    def __init__(self, timestamp, batches, sphere_id, query_id, seed=0, stats_sphere_id=None):
        self.__stats_sphere_id = stats_sphere_id if stats_sphere_id is not None else sphere_id
        self.__sphere_id = sphere_id
        self.__batches = batches
        self.__timestamp = timestamp
        self.__seed = seed
        self.__query_id = query_id

    @property
    def timestamp(self):
        return self.__timestamp

    @property
    def seed(self):
        return self.__seed

    @property
    def batches(self):
        return self.__batches

    @property
    def sphere_id(self):
        return self.__sphere_id

    @property
    def stats_sphere_id(self):
        return self.__stats_sphere_id

    @property
    def query_id(self):
        return self.__query_id

    @classmethod
    def from_dict(cls, dictionary):
        my_dict = dictionary.copy()
        my_dict['batches'] = [Batch.from_dict(b) for b in my_dict['batches']]
        if isinstance(my_dict['timestamp'], basestring):
            my_dict['timestamp'] = dateutil.parser.parse(my_dict['timestamp'])

        return cls(**my_dict)

    def to_dict(self):
        return {
            'query_id': self.query_id,
            'timestamp': self.timestamp.strftime('%Y-%m-%dT%H:%M:%S'),
            'batches': [x.to_dict() for x in self.batches],
            'sphere_id': self.sphere_id,
            'stats_sphere_id': self.stats_sphere_id,
            'seed': self.seed
        }

    def __repr__(self):
        return '{}_{}_{}_{}'.format(self.__timestamp, repr(self.__batches), self.__sphere_id, self.__seed)

    def __eq__(self, other):
        return self.__sphere_id == other.__sphere_id and \
            self.__batches == other.__batches and \
            self.__timestamp == other.__timestamp and \
            self.__seed == other.__seed


class Batch(object):
    def __init__(self, customer_id, dataset_id, batch_id):
        self.__batch_id = batch_id
        self.__dataset_id = dataset_id
        self.__customer_id = customer_id

    @property
    def batch_id(self):
        return self.__batch_id

    @property
    def dataset_id(self):
        return self.__dataset_id

    @property
    def customer_id(self):
        return self.__customer_id

    @classmethod
    def from_dict(cls, dictionary):
        return cls(**dictionary)

    def to_dict(self):
        return {
            'batch_id': self.__batch_id,
            'dataset_id': self.__dataset_id,
            'customer_id': self.__customer_id
        }

    def __repr__(self):
        return '{}_{}_{}'.format(self.__customer_id, self.__dataset_id, self.__batch_id)

    def __eq__(self, other):
        return self.__batch_id == other.__batch_id and \
            self.__customer_id == other.__customer_id and \
            self.__dataset_id == other.__dataset_id
