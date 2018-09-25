__author__ = 'Shahars'
from source.query.ml_phase.bag_worker.bag_weighters.bag_weighter import BagWeighter


class BagPastAucWeighter(BagWeighter):
    def __init__(self, name, normalize, params):
        super(BagPastAucWeighter, self).__init__(name, normalize)

    def weight_bag_et_update(self, bag):
        pass
