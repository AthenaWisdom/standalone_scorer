import numpy as np
__author__ = 'Shahars'


def calc_true_pos_of_top_prcntg(top_prcntg_pop_count, sorted_scores_df):
    top_graded_pop_by_prcntg_df = sorted_scores_df.head(top_prcntg_pop_count)
    true_pos = sum(top_graded_pop_by_prcntg_df["is_positive"])
    return true_pos


def calc_true_pos_of_bottom_percentage(bottom_prcntg_pop_count, sorted_scores_df):
    initially_scored = sorted_scores_df[sorted_scores_df["initially_scored"] == 1]
    bottom_graded_pop_by_prcntg_df = initially_scored.tail(bottom_prcntg_pop_count)
    true_pos = sum(bottom_graded_pop_by_prcntg_df["is_positive"])
    return true_pos


def get_count_by_percentage(scores_df, percentage):
    graded_pop_count = len(scores_df.index)
    top_count = int(np.ceil((float(graded_pop_count*percentage)/float(100))))
    return top_count


def get_count_by_percentage_for_bottom(scores_df, percentage):
    initially_scored_df = scores_df[scores_df['initially_scored'] == 1]
    graded_pop_count = len(initially_scored_df.index)
    bottom_count = int(np.ceil((float(graded_pop_count*percentage)/float(100))))
    return bottom_count
