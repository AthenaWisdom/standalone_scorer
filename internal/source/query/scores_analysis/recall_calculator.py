import logging
import pandas as pd
from source.query.scores_analysis.analysis_helpers import calc_true_pos_of_top_prcntg, get_count_by_percentage, \
    calc_true_pos_of_bottom_percentage

__author__ = 'Shahars'


class RecallCalculator(object):
    RECALL_PERCENTAGES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    def __init__(self):
        self.__logger = logging.getLogger('endor.machine_learning.stats_calculator')

    def calc_sorted_scorer_recall(self, sorted_scores_df, scorer_name):
        recalls = {}
        for percentage_thresh in self.RECALL_PERCENTAGES:

            top_recall = self.__calc_top_recall_by_percentage(sorted_scores_df, percentage_thresh)
            recalls["recall_prcntg"+str(percentage_thresh)] = top_recall
            bottom_recall = self.__calc_bottom_recall_by_percentage(sorted_scores_df, percentage_thresh)
            recalls["recall_bottom_prcntg"+str(percentage_thresh)] = bottom_recall
        recall_df = pd.DataFrame({scorer_name: recalls.values()}, index=recalls.keys())
        return recall_df

    def __calc_top_recall_by_percentage(self, sorted_scores_df, percentage):
        try:
            top_count = get_count_by_percentage(sorted_scores_df, percentage)
            return self.__calc_recall_by_count(sorted_scores_df, top_count)
        except Exception as e:
            self.__logger.warning("couldn't calculate hit-rate due to error: '%s', returning hit-rate -1" % str(e))
            return -1

    def __calc_bottom_recall_by_percentage(self, sorted_scores_df, percentage_thresh):
        try:
            bottom_count = get_count_by_percentage(sorted_scores_df, percentage_thresh)
            return self.__calc_bottom_recall_by_count(sorted_scores_df, bottom_count)
        except Exception as e:
            self.__logger.warning("couldn't calculate hit-rate due to error: '%s', returning hit-rate -1" % str(e))
            return -1

    def __calc_recall_by_count(self, sorted_scores_df, top_count):
        ground_size = sorted_scores_df["is_positive"].sum()
        true_pos = calc_true_pos_of_top_prcntg(top_count, sorted_scores_df)
        return float(true_pos)/float(ground_size)

    def __calc_bottom_recall_by_count(self, sorted_scores_df, bottom_count):
        ground_size = sorted_scores_df["is_positive"].sum()
        true_pos = calc_true_pos_of_bottom_percentage(bottom_count, sorted_scores_df)
        return float(true_pos)/float(ground_size)