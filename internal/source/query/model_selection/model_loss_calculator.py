import logging
import numpy as np
import pandas as pd

__author__ = 'Shahars'


class ModelsLossCalculator(object):
    LOW_SELECT_TH = 0.5
    HIGH_SELECT_TH = 0.8
    HIGH_PUNISH_THRESH = 4
    LOW_PUNISH_TH = 1
    HIT_RATE_COUNTS = np.array([10, 20, 50, 100, 250, 500, 1000, 2500, 5000])
    HIT_RATE_PERCENTAGES = np.array([1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])

    def __init__(self):
        self.__logger = logging.getLogger('endor')

    def calc_models_losses(self, hit_rates, kernel_summary, hit_rate_thresholds):

        relevant_sorted_thresholds = self.__get_relevant_thresholds(kernel_summary, hit_rate_thresholds)
        punish_weights = self.__get_punish_weights(relevant_sorted_thresholds)

        hr_only_thresholds = hit_rates.ix[relevant_sorted_thresholds]

        # TOOD: should I add the first round of removing scorers?
        models_losses = self.__get_models_losses(hr_only_thresholds, punish_weights)
        return relevant_sorted_thresholds, models_losses

    def __get_models_losses(self, thresh_hr_df, punish_weights):

        max_thresh = self.__get_threshold_stats(thresh_hr_df)["max"]

        if 0 in max_thresh.values:
            # if 0 is max value for some threshold (but not all)
            # we replace it with a small value to avoid inf in total punish
            if np.prod(max_thresh.values) > 0:
                max_thresh.replace(0, 1e-12, inplace=True)
            # all thresholds values are 0: we cannot choose a good predictor,
            # should stop and output default
            else:
                error_msg = "maximal values of all selected thresholds are 0. Cannot choose best model " \
                            "based on this data, returning default model"
                self.__logger.warning(error_msg)
                return None

        # max_thresh = thresholds_stats.ix["max"]
        thresh_punish = ((max_thresh - thresh_hr_df.T) / max_thresh) ** 2
        total_punish = thresh_punish.dot(punish_weights)
        return total_punish

    @staticmethod
    def __get_threshold_stats(thresh_hr_df):
        return pd.DataFrame({"max": thresh_hr_df.max(axis=1)})

    def __get_relevant_thresholds(self, kernel_summary, hit_rate_thresholds):
        user_thresholds = hit_rate_thresholds
        low_thresh = int(min(user_thresholds))
        high_threshold = int(max(user_thresholds))
        suspects_amount = kernel_summary['summary']['num_candidates']
        header, all_th = self.__build_all_threshes_et_header(suspects_amount)
        selected_th = all_th[(low_thresh <= all_th) & (all_th <= high_threshold)]
        if len(selected_th) == 0:
            selected_th = all_th[-3:]
        elif len(selected_th) < 3:
            init_th_ind = np.nonzero(np.in1d(all_th, selected_th))[0]
            min_thresh_ind = min(init_th_ind)
            max_thresh_ind = max(init_th_ind)
            if min_thresh_ind > 0:
                selected_th = sorted({all_th[min_thresh_ind - 1]} | set(selected_th))
            if max_thresh_ind < len(all_th) - 1:
                selected_th = sorted({all_th[max_thresh_ind + 1]} | set(selected_th))
        sel_indices = np.in1d(all_th, selected_th)
        return header[sel_indices]

    def __get_punish_weights(self, thresholds):

        punish_weights = pd.DataFrame({"w": [
            self.LOW_PUNISH_TH + float(i * (self.HIGH_PUNISH_THRESH - self.LOW_PUNISH_TH)) / (len(thresholds) - 1)
            for i in range(len(thresholds))]}, index=thresholds)
        return punish_weights

    def __get_minimization_weights(self, thresholds_num):
        select_weights = [
            self.LOW_SELECT_TH + float(i * (self.HIGH_SELECT_TH - self.LOW_SELECT_TH)) / (thresholds_num - 1)
            for i in range(thresholds_num)]
        return select_weights

    def __build_all_threshes_et_header(self, suspects_amount):
        trimmed_perc_th = self.HIT_RATE_PERCENTAGES[self.HIT_RATE_PERCENTAGES <= 50]
        trimmed_perc_th_as_count = [(float(perc) / 100) * suspects_amount for perc in trimmed_perc_th]

        trimmed_count_th = self.HIT_RATE_COUNTS[self.HIT_RATE_COUNTS <= 0.5 * suspects_amount]

        all_threshes = np.array(sorted(set(trimmed_count_th) | set(trimmed_perc_th_as_count)))
        perc_indices = np.in1d(all_threshes, trimmed_perc_th_as_count)
        count_indices = np.in1d(all_threshes, trimmed_count_th)

        count_header = np.array(["hr_count%s" % str(i) for i in trimmed_count_th])
        perc_header = np.array(["hr_prcntg%s" % str(i) for i in trimmed_perc_th])
        h = np.array([""] * len(all_threshes), dtype='object')
        h[perc_indices] = perc_header
        h[count_indices] = count_header
        return h, all_threshes
