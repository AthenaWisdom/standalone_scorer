from random import shuffle
import numpy as np
import operator
from source.data_ingestion.setup_maker.sphere_setup.connecting_fields.connecting_fields_maker import \
    ConnectingFieldsMaker

__author__ = 'user'


class InformationConnectingFieldsMaker(ConnectingFieldsMaker):
    def __init__(self):
        pass

    def run(self, input_csvs, uniques_dict, time_cols, non_time_cols, global_sphere_config, setup_maker_config):
        largest_input_csv = self.__get_largest_input_csv(input_csvs)
        # time_connecting_fields = self.prepare_time_connecting_fields(setup_maker_config, global_sphere_config,
        #                                                              time_cols)
        connecting_fields = self.prepare_non_time_connecting_fields(largest_input_csv, setup_maker_config,
                                                                      global_sphere_config)
        return connecting_fields

    def prepare_non_time_connecting_fields(self, df, setup_maker_config, global_sphere_config):
        extra_params_legit_fields = setup_maker_config.extra_params_legit_fields
        id_field = global_sphere_config.main_id_field

        connecting_fields = self.find_non_time_connecting_fields(df, setup_maker_config)
        # if more than 1 legit field, all should appear in connecting fields
        if len(extra_params_legit_fields) > 0:
            connecting_fields = set(connecting_fields + [id_field] + extra_params_legit_fields)
        # otherwise, the only legit field shouldn't appear there
        else:
            connecting_fields = set(connecting_fields).difference({id_field})
        return list(connecting_fields)

    def prepare_time_connecting_fields(self, setup_maker_config, global_sphere_config, time_cols):
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

    def calc_some(self, time_to_check, threshold):
        return lambda x: self.only_sublinear_efficient(x, time_to_check, threshold)

    def full_calculation_efficient(self, cc, setup_maker_config):
        time_to_check = setup_maker_config["connecting_fields_config"].get("time_to_check", 1000)
        threshold = setup_maker_config["connecting_fields_config"].get("threshold", 0.2)

        exact_count_rdd = cc.mapPartitions(lambda x: only_sublinear_efficient(x, time_to_check, threshold))
        exact_count = exact_count_rdd.collect()

        # combine
        x = {}
        for i in range(0, len(exact_count)):
            for k, v in exact_count[0].items():  # k - field name
                try:
                    type(x[k])
                except:
                    x[k] = {}
                for kv, vv in v.items():
                    try:
                        x[k][kv] += vv
                    except:
                        x[k][kv] = vv

        # calculate information for each field
        information = {}
        total = {}
        for f, v in x.items():  # field, values
            d = np.array(v.values())
            total[f] = float(np.sum(d))
            p = d / total[f]
            information[f] = -np.sum(np.multiply(p, np.log(p)))

        # calculate score
        score = {}
        for f, v in information.items():
            score[f] = float(self.calc_score_from_I(information[f], total[f])[0])

        # sort according to score
        sorted_name_score = sorted(score.items(), key=operator.itemgetter(1), reverse=True)
        return information, score, sorted_name_score

    # the score from the information
    def calc_score_from_I(self, I, M):
        a = np.log(np.sqrt(M))
        max_score = a * np.exp(-1.0)
        score = I * np.exp(-I / a)
        norm_score = score / max_score
        return score, norm_score

    # remove duplicates based on identical scores
    # label
    def remove_duplicates(self, sorted_list):
        new_sorted_list = [sorted_list[0]]
        irrelevant_list = []
        for k in range(1, len(sorted_list)):
            if sorted_list[k][1] < 0.000000001:
                new_item_type = 'irrelevant'
            else:
                new_item_type = 'new'
                for j in range(0, len(new_sorted_list)):
                    if abs(sorted_list[k][1] - sorted_list[j][1]) < 0.00001:
                        new_item_type = 'duplicate'
            if new_item_type == 'new':
                new_sorted_list.append(sorted_list[k])
            else:
                irrelevant_list.append([sorted_list[k], new_item_type])
        return new_sorted_list, irrelevant_list

    def find_non_time_connecting_fields(self, input_csv, setup_maker_config):
        max_non_time_connecting_fields = setup_maker_config.connecting_fields_config.max_non_time_connecting_fields
        kk = input_csv
        num_partitions = max([2, min([200, int(round(kk.count() / 5000))])])
        cc = kk.rdd.coalesce(num_partitions)

        information, score, sorted_name_score = self.full_calculation_efficient(cc, setup_maker_config)
        return [col_tuple[0] for col_tuple in sorted_name_score[:max_non_time_connecting_fields]]

    def __get_largest_input_csv(self, input_csvs):
        max_input_number = max([int(filter(str.isdigit, input_obj.name)) for input_obj in input_csvs])

        for input_obj in input_csvs:
            if int(filter(str.isdigit, input_obj.name)) == max_input_number:
                return input_obj.df


def only_sublinear_efficient(list_of_lists, time_to_check, threshold):
    # threshold = 0.5                   # below this, cardinality is sublinear in time
    # time_to_check = 4              # after 1000 inputs, check sublinearity
    card_field = {}  # during check-time
    count_field = {}  # after check: to count field or not
    exact_count = {}  # the exact count of only the sublinear fields
    total_values = 0
    t = 0
    for row in list_of_lists:
        t = t + 1  # counting time
        r = row.asDict()  # I like dictionaries :)

        # count how many different values there are in each field for the first time_to_check rows
        if t < time_to_check:  # before check time
            for k, v in r.items():  # go over items (first time_to_check items)
                try:
                    card_field[k].add(v)
                except:
                    card_field[k] = set()
                    card_field[k].add(v)

        # at check time, find the fields that have sublinear cardinality
        if t == time_to_check:
            for k in r.keys():
                count_field[k] = False  # if at check time, cardinality is below linear threshold
                if float(len(card_field[k])) / float(t) < threshold:
                    count_field[k] = True

        # in the rest of the rows, count cardinality only for sublinear fields
        if t > time_to_check:  # after check time
            for k, v in r.items():
                if count_field[k]:  # go over only relevant fields
                    try:
                        type(exact_count[k])
                    except:
                        exact_count[k] = {}
                    try:
                        type(exact_count[k][v])
                    except:
                        exact_count[k][v] = 0
                        total_values += 1
                    exact_count[k][v] += 1  # exact count
    return (exact_count,)
