from copy import deepcopy


class SphereConfigurationBuilder(object):
    class BasicConfig(object):
        @classmethod
        def default(cls):
            return deepcopy({'datetime_format': '%Y-%m-%d %H:%M:%S',
                             'legit_field_name': 'name',
                             'main_time_field': 'date',
                             'customer_id': 'best_customer_ever_test',
                             'dataset_id': 'some_test_ds',
                             'execution_id': 'some_exec_id',
                             'ignore_fields': []
                             })
