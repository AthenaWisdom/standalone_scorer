from source.storage.stores.general_quest_data_store.types import QueryExecutionUnit


class RuntimeQueryExecutionUnit(object):
    def __init__(self, role, query_id, query_execution_unit, kernel_id):
        """
        @type role: C{str}
        @type query_id: C{str}
        @type query_execution_unit: L{source.storage.stores.general_quest_data_store.types.QueryExecutionUnit}
        @type kernel_id: C{str}
        @return:
        @rtype:
        """
        self.__kernel_id = kernel_id
        self.__query_execution_unit = query_execution_unit
        self.__query_id = query_id
        self.__role = role

    @property
    def query_id(self):
        return self.__query_id

    @property
    def role(self):
        return self.__role

    @property
    def query_execution_unit(self):
        return self.__query_execution_unit

    @property
    def kernel_id(self):
        return self.__kernel_id

    @classmethod
    def from_dict(cls, dictionary):
        my_dict = dictionary.copy()
        my_dict['query_execution_unit'] = QueryExecutionUnit.from_dict(my_dict['query_execution_unit'])
        return cls(**my_dict)

    def to_dict(self):
        return {
            'role': self.__role,
            'query_id': self.__query_id,
            'query_execution_unit': self.__query_execution_unit.to_dict(),
            'kernel_id': self.__kernel_id
        }

    @classmethod
    def build_runtime_data_units(cls, past_data_units):
        runtime_data_units = []
        if len(past_data_units) > 0:
            max_timestamp = max([data_unit.timestamp for data_unit in past_data_units])
            for data_unit in past_data_units:
                kernel_hash = data_unit.query_id
                past_role = "validation_past" if max_timestamp == data_unit.timestamp else "training_past"
                runtime_data_units.append(
                    cls(past_role, data_unit.query_id, data_unit, kernel_hash))
        return runtime_data_units
