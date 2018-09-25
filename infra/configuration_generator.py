__author__ = 'Shahars'
from dateutil.parser import parse as parse_datetime


class ConfigurationGenerator(object):
    @staticmethod
    def get_sphere_config():
        config = {
            'customer_id': 'best_customer_ever_test',
            'dataset_id': 'some_test_ds',
            'execution_id': 'some_exec_id',
            'clean_config': None,
            'fetcher_config': {
                'delimiter': ',',
                'have_header': True
            },
            'global_config': {
                'reference_date': parse_datetime('2015-12-08 00:00:00.0'),
                'main_id_field': 'name',
                'main_timestamp_column': 'date',

                'timestamp_format': "%Y-%m-%d %H:%M:%S.%f",
                'all_time_cols': ['date']
            },
            'setup_maker_config': {
                'should_hash': False,
                'rounding_minutes': [60],
                'input_csv_days': [1],
                'fields_to_hash': ['name'],
                'connecting_fields_config': {
                    'max_time_connecting_fields': 3,
                    'non_time_conn_fields_unique_thresh': 2,
                    'max_non_time_connecting_fields': 2
                },
                'extra_params_legit_fields': [],
                'basic_params_config': {},

                'non_time_fields_unique_thresh': 4
            },
            'stats_config': {
                'numeric_fields': ['age', 'stuff']
            }
        }
        return config

    @staticmethod
    def get_open_query_with_ts_config(customer_id, dataset_id, dataset_def):
        config = {"reference_date": parse_datetime("2015-12-02 14:12:51"),
                  "kernel_config": {
                      "present_random_threshold": "0",
                      "past_random_threshold": "0.5",
                      "external_columns": [],
                      "whites":
                          {"positive": [{"source": {"type": "internal",
                                                    "customer_id": customer_id,
                                                    "dataset_id": dataset_id,
                                                    "data_schema": dataset_def
                                                    },
                                         "apply_funcs": [{
                                             "time_slice": {"start_days": -30, "end_days": 0},
                                             "calc_only_active_users": False,
                                             "aggregate_by": "random",
                                             "column": "name",
                                             "filter_by": "ge",
                                             "value_to_compare": ""
                                         }
                                         ]}],
                           "negative": []},
                      "universe": {
                          "positive": [{"source": {"type": "internal",
                                                   "customer_id": customer_id,
                                                   "dataset_id": dataset_id,
                                                   "data_schema": dataset_def},
                                        "apply_funcs": [{
                                            "time_slice": {},
                                            "calc_only_active_users": True,
                                            "aggregate_by": "",
                                            "column": "",
                                            "filter_by": "",
                                            "value_to_compare": ""
                                        }
                                        ]}],
                          "negative": []
                      },
                      "ground": {"positive": [{"source": {"type": "internal",
                                                          "customer_id": customer_id,
                                                          "dataset_id": dataset_id,
                                                          "data_schema": dataset_def
                                                          },
                                               "apply_funcs": [{
                                                   "time_slice": {"start_days": 0, "end_days": 30},
                                                   "calc_only_active_users": False,
                                                   "aggregate_by": "random",
                                                   "column": "name",
                                                   "filter_by": "lt",
                                                   "value_to_compare": ""
                                               }
                                               ]}],
                                 "negative": []}
                  }}

        return config

    @staticmethod
    def get_open_query_w_and_u_config(customer_id, whites_dataset_id, universe_dataset_id, dataset_def, new_mode=False):
        config = {"reference_date": parse_datetime("2015-12-02 14:12:51"),
                  "kernel_config": {
                      "validation_type": "cross-validation",
                      "mode": "openQuery",
                      "present_random_threshold": "0",
                      "past_random_threshold": "0.5",
                      "external_columns": [],
                      "whites":
                          {"positive": [{"source": {"type": "internal",
                                                    "customer_id": customer_id,
                                                    "dataset_id": whites_dataset_id,
                                                    "data_schema": dataset_def
                                                    },
                                         "apply_funcs": [{
                                             "time_slice": {},
                                             "calc_only_active_users": False,
                                             "aggregate_by": "random",
                                             "column": "name",
                                             "filter_by": "ge",
                                             "value_to_compare": "",
                                         }
                                         ]}],
                           "negative": []},
                      "universe": {
                          "positive": [{"source": {"type": "internal",
                                                   "customer_id": customer_id,
                                                   "dataset_id": universe_dataset_id,
                                                   "data_schema": dataset_def},
                                        "apply_funcs": [{
                                            "time_slice": {},
                                            "calc_only_active_users": True,
                                            "aggregate_by": "",
                                            "column": "",
                                            "filter_by": "",
                                            "value_to_compare": "",
                                        }
                                        ]}],
                          "negative": []
                      },
                      "ground": {"positive": [{"source": {"type": "internal",
                                                          "customer_id": customer_id,
                                                          "dataset_id": whites_dataset_id,
                                                          "data_schema": dataset_def
                                                          },
                                               "apply_funcs": [{
                                                   "time_slice": {},
                                                   "calc_only_active_users": False,
                                                   "aggregate_by": "random",
                                                   "column": "name",
                                                   "filter_by": "lt",
                                                   "value_to_compare": "",
                                                   "where_clause": ""
                                               }
                                               ]}],
                                 "negative": []}
                  }}

        return config

    @staticmethod
    def get_open_query_only_w_config(customer_id, whites_dataset_id, sphere_id, dataset_def):
        config = {"reference_date": parse_datetime("2015-12-02 14:12:51"),
                  "kernel_config": {
                      "present_random_threshold": 0,
                      "past_random_threshold": 0.5,
                      "external_columns": [],
                      "whites":
                          {"positive": [{"source": {"type": "internal",
                                                    "customer_id": customer_id,
                                                    "dataset_id": whites_dataset_id,
                                                    "data_schema": dataset_def
                                                    },
                                         "apply_funcs": [{
                                             "time_slice": {},
                                             "calc_only_active_users": False,
                                             "aggregate_by": "random",
                                             "column": "name",
                                             "filter_by": "ge",
                                             "value_to_compare": None,
                                             "where_clause":  None
                                         }
                                         ]}],
                           "negative": []},
                      "universe": {
                          "positive": [{"source": {"type": "stats",
                                                   "customer_id": customer_id,
                                                   "sphere_id": sphere_id,
                                                   "data_schema": dataset_def},
                                        "apply_funcs": [{
                                            "time_slice": {},
                                            "calc_only_active_users": True,
                                            "aggregate_by": "",
                                            "column": "",
                                            "filter_by": "",
                                            "value_to_compare": "",
                                            "where_clause":  None
                                        }
                                        ]}],
                          "negative": []
                      },
                      "ground": {"positive": [{"source": {"type": "internal",
                                                          "customer_id": customer_id,
                                                          "dataset_id": whites_dataset_id,
                                                          "data_schema": dataset_def
                                                          },
                                               "apply_funcs": [{
                                                   "time_slice": {},
                                                   "calc_only_active_users": False,
                                                   "aggregate_by": "random",
                                                   "column": "name",
                                                   "filter_by": "lt",
                                                   "value_to_compare": None,
                                                   "where_clause":  None
                                               }
                                               ]}],
                                 "negative": []}
                  }}

        return config

    @staticmethod
    def get_query_transactional_configuration(customer_id, dataset_id, dataset_schema):
        config = {"reference_date": parse_datetime("2015-12-07 14:12:51"),
                  "kernel_config": {
                      "present_random_threshold": "0",
                      "past_random_threshold": "0.5",
                      "external_columns": [],
                      "raise_on_count_error": "0",
                      "whites":
                          {"positive": [{"source": {"type": "transactional",
                                                    "customer_id": customer_id,
                                                    "dataset_id": dataset_id,
                                                    "data_schema": dataset_schema
                                                    },
                                         "apply_funcs": [{
                                             "time_slice": {"start_days": -10, "end_days": 0},
                                             "calc_only_active_users": False,
                                             "aggregate_by": "",
                                             "column": "",
                                             "filter_by": "",
                                             "value_to_compare": "",
                                             "where_clause": None
                                         }
                                         ]}],
                           "negative": []},
                      "universe": {
                          "positive": [{"source": {"type": "transactional",
                                                   "customer_id": customer_id,
                                                   "dataset_id": dataset_id,
                                                   "data_schema": dataset_schema},
                                        "apply_funcs": [{
                                            "time_slice": {"start_days": -10, "end_days": 0},
                                            "calc_only_active_users": True,
                                            "aggregate_by": "",
                                            "column": "",
                                            "filter_by": "",
                                            "value_to_compare": "",
                                            "where_clause": None
                                        }
                                        ]}],
                          "negative": []
                      },
                      "ground": {}

                  }}

        return config
