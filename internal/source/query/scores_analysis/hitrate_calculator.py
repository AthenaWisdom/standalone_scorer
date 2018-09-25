import logging

import pandas as pd
import numpy as np
from source.query.scores_analysis.analysis_helpers \
    import calc_true_pos_of_top_prcntg, get_count_by_percentage, \
    calc_true_pos_of_bottom_percentage, get_count_by_percentage_for_bottom

__author__ = 'Shahars'


class HitRateCalculator(object):
    HIT_RATE_COUNTS = [10, 20, 50, 100, 250, 500, 1000, 2500, 5000]
    HIT_RATE_PERCENTAGES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    def __init__(self):
        self.__logger = logging.getLogger('endor.machine_learning.stats_calculator')

    def calc_sorted_scorer_hitrate(self, sorted_scores_df, scorer_name):
        hrs = {}
        for thresh in self.HIT_RATE_COUNTS:
            top_hr = self.calc_top_hitrate_by_count(sorted_scores_df, thresh)
            hrs["hr_count"+str(thresh)] = top_hr
            bottom_hr = self.__calc_bottom_hr_by_count(sorted_scores_df, thresh)
            hrs["hr_bottom_count"+str(thresh)] = bottom_hr

        for percentage_thresh in self.HIT_RATE_PERCENTAGES:
            top_hr = self.__calc_top_hitrate_by_percentage(sorted_scores_df, percentage_thresh)
            hrs["hr_prcntg"+str(percentage_thresh)] = top_hr
            bottom_hr = self.__calc_bottom_hr_by_percentage(sorted_scores_df, percentage_thresh)
            hrs["hr_bottom_prcntg"+str(percentage_thresh)] = bottom_hr
        hr_df = pd.DataFrame({scorer_name: hrs.values()}, index=hrs.keys())
        return hr_df

    def calc_top_hitrate_by_count(self, sorted_scores_df, top_count):
        try:
            true_pos = calc_true_pos_of_top_prcntg(top_count, sorted_scores_df)
             # if the (bottom/top) is lower than number of suspects, we should look at number of suspects.
            x = min(np.ceil(top_count), sum(sorted_scores_df['is_suspect']))
            hit_rate = float(true_pos)/float(x)
            return hit_rate
        except Exception as e:
            self.__logger.warning("couldn't calculate hit-rate due to error: '%s', returning hit-rate -1" % str(e))
            return -1

    def __calc_top_hitrate_by_percentage(self, sorted_scores_df, percentage):
        try:
            top_count = get_count_by_percentage(sorted_scores_df, percentage)
            return self.calc_top_hitrate_by_count(sorted_scores_df, top_count)
        except Exception as e:
            self.__logger.warning("couldn't calculate hit-rate due to error: '%s', returning hit-rate -1" % str(e))
            return -1

    def __calc_bottom_hr_by_count(self, sorted_scores_df, bottom_count):
        try:
            true_pos = calc_true_pos_of_bottom_percentage(bottom_count, sorted_scores_df)
            # if the (bottom/top) is lower than number of suspects, we should look at number of suspects.
            # For bottom, the suspects are only those in intersection of spheres and candidates.
            x = min(np.ceil(bottom_count), sum(sorted_scores_df['initially_scored']))
            hit_rate = float(true_pos)/float(x)
            return hit_rate
        except Exception as e:
            self.__logger.warning("couldn't calculate hit-rate due to error: '%s', returning hit-rate -1" % str(e))
            return -1

    def __calc_bottom_hr_by_percentage(self, sorted_scores_df, percentage):
        try:
            bottom_count = get_count_by_percentage_for_bottom(sorted_scores_df, percentage)
            return self.__calc_bottom_hr_by_count(sorted_scores_df, bottom_count)
        except Exception as e:
            self.__logger.warning("couldn't calculate hit-rate due to error: '%s', returning hit-rate -1" % str(e))
            return -1

