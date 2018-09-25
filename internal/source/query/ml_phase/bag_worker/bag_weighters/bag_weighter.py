__author__ = 'Shahars'



class BagWeighter(object):
    def __init__(self, name, should_normalize):
        self.name = name
        self.normalize_weights_bool = should_normalize

    def weight_bag_et_update(self, bag):
        raise NotImplementedError()








