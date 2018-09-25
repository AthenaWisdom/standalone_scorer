import numpy as np
from source.query.merger.base_learning_merger import BaseLearningMerger

__author__ = 'user'


class BalancedMerger(BaseLearningMerger):
    def __init__(self, logger, scorer_name, classifier):
        super(BalancedMerger, self).__init__(logger, scorer_name, classifier)

    def train_merger(self, x_train, y_train):

        balanced_x_train, balanced_y_train = self.__build_balanced_x_y(x_train, y_train)
        self.classifier = self.classifier.fit(balanced_x_train, balanced_y_train)

    def __build_balanced_x_y(self, x_train, y_train):
        # elements_to_add, indices = self.find_type_et_number_to_add(y_train)
        # copied_indices = np.random.choice(indices, elements_to_add)
        # balanced_y = np.append(y_train, y_train[copied_indices])
        #
        # balanced_x = np.append(x_train, x_train[copied_indices], axis=0)
        # return balanced_x, balanced_y
        non_white_ind = np.where(np.in1d(y_train, [0]))[0]
        white_ind = np.where(np.in1d(y_train, [1]))[0]
        number_of_whites = sum(y_train)
        zero_ind = np.random.choice(non_white_ind, number_of_whites,replace=False)
        balanced_x = np.append(x_train[white_ind], x_train[zero_ind], axis=0)
        balanced_y = np.append(y_train[white_ind], y_train[zero_ind])
        return balanced_x, balanced_y

    def find_type_et_number_to_add(self, arr):
        ones_count = arr.sum()
        zero_count = len(arr) - ones_count
        if ones_count > zero_count:
            elements_to_add = ones_count - zero_count
            indices = np.where(np.in1d(arr, [0]))[0]
        else:
            elements_to_add = zero_count - ones_count
            indices = np.where(np.in1d(arr, [1]))[0]
        return elements_to_add, indices