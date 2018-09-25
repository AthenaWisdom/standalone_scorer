import operator
from random import shuffle

from source.data_ingestion.setup_maker.sphere_setup.connecting_fields.connecting_fields_maker import \
    ConnectingFieldsMaker
from source.utils.es_with_context_sender import send_es_event


class ConnectingFieldsThresholdsMaker(ConnectingFieldsMaker):
    def run(self, input_csvs, uniques_dict, time_cols, non_time_cols, global_sphere_config, setup_maker_config):
        if len(setup_maker_config.connecting_fields_config.fde_connecting_fields):
            return setup_maker_config.connecting_fields_config.fde_connecting_fields.to_builtin()
        time_connecting_fields = self.prepare_time_connecting_fields(setup_maker_config, global_sphere_config,
                                                                     time_cols)
        non_time_connecting_fields = self.prepare_non_time_connecting_fields(uniques_dict, setup_maker_config,
                                                                             global_sphere_config, non_time_cols)
        connecting_fields = time_connecting_fields + non_time_connecting_fields
        self.report_results(connecting_fields, setup_maker_config)
        return connecting_fields

    @staticmethod
    def prepare_time_connecting_fields(setup_maker_config, global_sphere_config, time_cols):
        max_time_connecting_fields = setup_maker_config.connecting_fields_config.max_time_connecting_fields
        rounding_minutes = setup_maker_config.rounding_minutes
        main_time_col = global_sphere_config.main_timestamp_column

        time_connecting_fields = ["%s_rounded_%d" % (main_time_col, round_t)
                                  for round_t in rounding_minutes]
        new_time_columns = list(set(time_cols) - set(time_connecting_fields))
        shuffle(new_time_columns)
        time_connecting_places = max_time_connecting_fields - len(time_connecting_fields)
        if time_connecting_places > 0:
            time_connecting_fields.extend(new_time_columns[:time_connecting_places])
        return time_connecting_fields

    def prepare_non_time_connecting_fields(self, uniques_dict, setup_maker_config, global_sphere_config, non_time_cols):
        extra_params_legit_fields = setup_maker_config.extra_params_legit_fields
        id_field = global_sphere_config.main_id_field

        # if more than 1 legit field, all should appear in connecting fields
        temp = self.find_non_time_above_unique_thresh(uniques_dict, non_time_cols, setup_maker_config)
        if len(extra_params_legit_fields) > 0:
            non_time_connecting = set(temp + [id_field] + extra_params_legit_fields)
        # otherwise, the only legit field shouldn't appear there
        else:
            non_time_connecting = set(temp).difference({id_field})
        return list(non_time_connecting)

    @staticmethod
    def find_non_time_above_unique_thresh(uniques_dict, non_time_cols, setup_maker_config):
        non_time_conn_fields_unique_thresh = setup_maker_config.connecting_fields_config.\
            non_time_conn_fields_unique_thresh
        max_non_time_connecting_fields = setup_maker_config.connecting_fields_config.max_non_time_connecting_fields

        res = {col: uniques_dict["unique_%s" % col] for col in non_time_cols
               if uniques_dict["unique_%s" % col] >= non_time_conn_fields_unique_thresh}

        sorted_res = sorted(res.items(), key=operator.itemgetter(1), reverse=True)[:max_non_time_connecting_fields]
        res = [r[0] for r in sorted_res]

        return res

    @staticmethod
    def report_results(connecting_fields, setup_maker_config):
        max_time_connecting_fields = setup_maker_config.connecting_fields_config.max_time_connecting_fields
        max_non_time_connecting_fields = setup_maker_config.connecting_fields_config.max_non_time_connecting_fields
        non_time_conn_fields_unique_thresh = setup_maker_config.connecting_fields_config.\
            non_time_conn_fields_unique_thresh
        send_es_event("connecting_fields_report", {
            "connecting_fields": connecting_fields,
            "max_time_fields": max_time_connecting_fields,
            "max_non_time_fields": max_non_time_connecting_fields,
            "non_time_connecting_unique_thresh":
                non_time_conn_fields_unique_thresh
        })
