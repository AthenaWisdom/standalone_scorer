__author__ = 'Shahars'

import numpy as np
from scipy.stats.kde import gaussian_kde
from source.query.ml_phase.bag_worker.bag_weighters.bag_kl_weighter import BagKLWeighter


class BagKLByKDEWeighter(BagKLWeighter):
    def __init__(self, name, normalize, params):
        super(BagKLByKDEWeighter, self).__init__(name, normalize)
        self.width = params.width
        self.eval_points = np.linspace(params.eval_min, params.eval_max, params.eval_num_points)

    def _evaluate_dist(self):
        w_rand_prcntg = self.bag.props_df["rand_W_prcntg"].values
        w_bag_prcntg = self.bag.props_df["W_prcntg"].values
        bag_pdf = gaussian_kde(w_bag_prcntg, bw_method=self.width)
        rand_pdf = gaussian_kde(w_rand_prcntg, bw_method=self.width)
        self.bag_n = bag_pdf(self.eval_points)
        self.rand_n = rand_pdf(self.eval_points)

