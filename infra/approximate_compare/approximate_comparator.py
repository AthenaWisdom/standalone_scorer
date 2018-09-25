#see https://github.com/snakile/approximate_comparator/blob/master/approximate_comparator.py


from insignificant_digit_cutter import cut_insignificant_digits_recursively

def is_almost_equal(first, second, places=7):
    '''returns False if first and second aren't equal up to a desired precision.
    Given two data structures, returns True if all elements of these structures
    (in any nesting level) are either equal or almost equal.
    floats are almost equal if they're equal when we consider only the
    [places] most significant digits of each.
    '''
    if first == second: return True
    cut_first = cut_insignificant_digits_recursively(first, places)
    cut_second = cut_insignificant_digits_recursively(second, places)
    return cut_first == cut_second



# shallow dict difference calculator
class ShallowDictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        return self.set_current - self.intersect
    def removed(self):
        return self.set_past - self.intersect
    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])