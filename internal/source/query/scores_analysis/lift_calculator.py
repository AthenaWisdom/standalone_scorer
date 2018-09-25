import logging

import pandas as pd

from source.query.scores_analysis.analysis_helpers import get_count_by_percentage, calc_true_pos_of_top_prcntg

__author__ = 'Shahars'


class LiftCalculator(object):
    LIFT_PERCENTAGES = [1, 5, 10]

    def __init__(self, hitrate_calculator):
        self.__logger = logging.getLogger('endor.machine_learning.stats_calculator')
        self.__hitrate_calculator = hitrate_calculator

    def calc_sorted_scorer_lift(self, sorted_scores_df, scorer_name):
        lifts = {}
        for percentage in self.LIFT_PERCENTAGES:

            lift = self.__calc_lift(sorted_scores_df, percentage)
            lifts["lift_"+str(percentage)] = lift
        lifts_df = pd.DataFrame({scorer_name: lifts.values()}, index=lifts.keys())
        return lifts_df

    def __calc_lift(self, sorted_scores_df, percentage):
        try:
            graded_pop_count = len(sorted_scores_df.index)
            overall_hitrate = self.__hitrate_calculator.calc_top_hitrate_by_count(sorted_scores_df, graded_pop_count)
            top_prcntg_pop_count = get_count_by_percentage(sorted_scores_df, percentage)
            true_pos = calc_true_pos_of_top_prcntg(top_prcntg_pop_count, sorted_scores_df)
            return true_pos/float(overall_hitrate * top_prcntg_pop_count)
        except Exception as e:
            self.__logger.warning("couldn't calculate lift due to error: '%s', returning lift -1" %str(e))
            return -1

