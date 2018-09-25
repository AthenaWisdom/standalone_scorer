ALLOWED_MEMBERS = {'__feature_flags_engine', '__value_to_implementation_map', '__flag_name',
                   '__getattr', '__class__'}


class FeatureFlagImplementationSelector(object):
    def __init__(self, flag_name, value_to_implementation_map, feature_flags_provider):
        object.__setattr__(self, '__feature_flags_provider', feature_flags_provider)
        object.__setattr__(self, '__value_to_implementation_map', {})
        object.__setattr__(self, '__value_to_functor_map', value_to_implementation_map)
        object.__setattr__(self, '__flag_name', flag_name)


    def __get_obj(self):
        flag_name = object.__getattribute__(self, "__flag_name")
        value_to_implementation_map = object.__getattribute__(self, "__value_to_implementation_map")
        value_to_functor_map = object.__getattribute__(self, "__value_to_functor_map")
        feature_flags_engine = object.__getattribute__(self, "__feature_flags_provider")

        implementation_name = feature_flags_engine.get_value(flag_name)
        if implementation_name not in value_to_implementation_map:
            value_to_implementation_map[implementation_name] = value_to_functor_map[implementation_name]()

        return value_to_implementation_map[implementation_name]

    def __getattribute__(self, name):
        obj = object.__getattribute__(self, "_FeatureFlagImplementationSelector__get_obj")()
        return getattr(obj, name)

    def __delattr__(self, name):
        obj = object.__getattribute__(self, "_FeatureFlagImplementationSelector__get_obj")()
        delattr(obj, name)

    def __setattr__(self, name, value):
        obj = object.__getattribute__(self, "_FeatureFlagImplementationSelector__get_obj")()
        setattr(obj, name, value)

    def __nonzero__(self):
        obj = object.__getattribute__(self, "_FeatureFlagImplementationSelector__get_obj")()
        return bool(obj)

    def __str__(self):
        obj = object.__getattribute__(self, "_FeatureFlagImplementationSelector__get_obj")()
        return str(obj)

    def __repr__(self):
        obj = object.__getattribute__(self, "_FeatureFlagImplementationSelector__get_obj")()
        return repr(obj)

