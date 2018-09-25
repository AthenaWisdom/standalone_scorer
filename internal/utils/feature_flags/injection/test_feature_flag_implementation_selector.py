from mock import Mock
import unittest2
from source.utils.feature_flags.injection.feature_flag_implementation_selector import FeatureFlagImplementationSelector

__author__ = 'izik'


class One(object):
    def get_number(self):
        return 1


class Two(object):
    def get_number(self):
        return 2


class FeatureFlagImplementationSelectorTests(unittest2.TestCase):
    def test_implementation_is_chosen_according_to_flags(self):
        feature_flags_engine = Mock()
        selector = FeatureFlagImplementationSelector('use_izik1',
                                                     {
                                                         'one': lambda: One(),
                                                         'two': lambda: Two()
                                                     }, feature_flags_engine)

        feature_flags_engine.get_value = lambda flag: 'one'
        self.assertEqual(1, selector.get_number())
        self.assertTrue('One' in selector.__str__())
        self.assertTrue('One' in str(selector))
        instance_set = set()
        instance_set.add(selector)

        feature_flags_engine.get_value = lambda flag: 'two'
        self.assertEqual(2, selector.get_number())
        self.assertTrue('Two' in selector.__str__())
        self.assertTrue('Two' in str(selector))

        instance_set.add(selector)
        self.assertEqual(len(instance_set), 1)
