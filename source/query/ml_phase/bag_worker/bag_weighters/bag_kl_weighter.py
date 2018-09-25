__author__ = 'Shahars'
import numpy as np
from source.query.ml_phase.bag_worker.bag_weighters.bag_weighter import BagWeighter


class BagKLWeighter(BagWeighter):
    def __init__(self, name, normalize):
        super(BagKLWeighter, self).__init__(name, normalize)
        self.bag = None
        self.bag_n = None
        self.rand_n = None

    def weight_bag_et_update(self, bag):
        self.bag = bag
        if "rand_W_prcntg" not in self.bag.props_df.columns:
            self.__calc_random_w_bag_dist()
        self._evaluate_dist()
        if self.normalize_weights_bool:
            self.__normalize_weights()
        weight = self.__calc_kl()
        self.bag.add_bag_weight(self.name, weight)

    def _evaluate_dist(self):
        raise NotImplementedError()

    def __calc_kl(self):
        z = self.bag_n*np.log(self.bag_n.astype(float)/self.rand_n.astype(float))
        z[np.isinf(z)] = np.nan
        s = np.nansum(z)
        return s

    def __normalize_weights(self):
        self.bag_n = self.bag_n.astype(float)/(sum(self.bag_n)).astype(float)
        self.rand_n = self.rand_n.astype(float)/(sum(self.rand_n)).astype(float)

    def __calc_random_w_bag_dist(self):
        pop_size = len(self.bag.universe)
        num_whites = len(self.bag.whites)
        cluster_pop_rand_whites = np.random.permutation(np.append(np.zeros(pop_size-num_whites), np.ones(num_whites)))

        # distribute population between clusters, and count num of whites in each cluster
        calc_rand_wn_2 = lambda cluster_size: sum(np.random.choice(cluster_pop_rand_whites, cluster_size))
        self.bag.props_df["rand_WN"] = self.bag.props_df["N"].apply(calc_rand_wn_2)

        self.bag.props_df["rand_W_prcntg"] = \
            self.bag.props_df["rand_WN"].astype(float)/self.bag.props_df["N"].astype(float)

