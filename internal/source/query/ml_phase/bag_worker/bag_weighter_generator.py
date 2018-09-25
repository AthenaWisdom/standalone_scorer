__author__ = 'Shahars'

# from bag_weighter import BagKLByHistWeighter, BagKLByKDEWeighter
from source.query.ml_phase.bag_worker import bag_weighters


class BagWeighterGenerator(object):

    CONFIG_RULES = {"method": (basestring, True),
                    "normalize": (bool, True),
                    "params": ({}, True)
                    }

    def __init__(self, config, logger):
        self.config = config
        # self.bag_weighters_data = bag_weighters_data
        self.logger = logger


    def create_weighters(self):
        weighters = {}
        for w_id, w_data in enumerate(self.config):
            WeighterClass = getattr(bag_weighters, w_data.method)
                # self.retrieve_weighter_class(w_data.method)
            weighter_name = w_data.method
            normalize = w_data.normalize
            weighter_params = w_data.params
            weighters[w_id] = WeighterClass(weighter_name, normalize, weighter_params)

        return weighters.values()

    # def retrieve_weighter_class(self, class_name):
    #     if class_name == "kde":
    #         return BagKLByKDEWeighter
    #     elif class_name == "hist":
    #         return BagKLByHistWeighter
    #     else:
    #         self.logger.warning("weighting bag method %s isn't supported, only KL hist or kde" % class_name)