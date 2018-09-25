__author__ = 'izik'


class FeatureFlagsResolver(object):
    def __init__(self, resolved_flags):
        self.__resolved_flags = resolved_flags


    def get_value(self, feature_flag_name, *tags):
        if feature_flag_name not in self.__resolved_flags:
            raise ValueError(
                'feature flag "{}" was unresolved, are you using the correct code version?'.format(feature_flag_name))

        return self.__resolved_flags[feature_flag_name]
