import logging
import pandas as pd
import numpy as np
from sklearn import metrics

__author__ = 'Shahars'


class AucCalculator(object):
    def __init__(self):
        self.__logger = logging.getLogger('endor.machine_learning.stats_calculator')

    def calc_sorted_scorer_auc(self, sorted_scores_df, scorer_name):
        fpr, tpr, thresholds = metrics.roc_curve(sorted_scores_df["is_positive"], sorted_scores_df[scorer_name])
        if np.isfinite(tpr).all() and np.isfinite(fpr).all():
            auc = metrics.auc(fpr, tpr)
        else:
            self.__logger.warning("no two classes among suspects, returns auc -1")
            auc = -1
        auc_df = pd.DataFrame({scorer_name: auc}, index=["auc"])
        return auc_df

'''
A quick sanity test for completely random scores and many enough candidates:
if __name__ == '__main__':
    N = 20000
    df = pd.DataFrame(np.random.randint(0, 1e7 ,size=(N, 1)) / 1.0e7, columns=list('A'))
    df2 = pd.DataFrame(np.random.randint(0, 100, size=(N, 1)) > 90, columns=['is_positive'])
    df = df.join(df2)
    df.sort(columns='A', ascending=False, inplace=True)
    calc = AucCalculator()
    print calc.calc_sorted_scorer_auc(df, 'A')
'''
