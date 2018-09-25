__author__ = 'Shahars'
from source.query.ml_phase.bag_worker.bag_weighters.bag_kl_weighter import BagKLWeighter
import numpy as np

class BagKLByHistWeighter(BagKLWeighter):
    def __init__(self, name, normalize, params):
        super(BagKLByHistWeighter, self).__init__(name, normalize)
        self.bins_num = params.bins_num

    def _evaluate_dist(self):
        w_rand_prcntg = self.bag.props_df["rand_W_prcntg"].values
        w_bag_prcntg = self.bag.props_df["W_prcntg"].values

        self.bag_n, bag_bins = np.histogram(w_bag_prcntg, self.bins_num)
        self.rand_n, rand_bins = np.histogram(w_rand_prcntg, bag_bins)

