__author__ = 'user'


class ScorerKey(object):
    def __init__(self, scorer_name, scorer_params=None, scorer_tags=None):
        self.__name = scorer_name
        self.__params = scorer_params
        self.__tags = scorer_tags
        self.__full_name = self.__build_full_name(scorer_name, scorer_params, scorer_tags)

    def __str__(self):
        return self.__full_name

    def __repr__(self):
        return self.__full_name

    @property
    def scorer_name(self):
        return self.__name

    @property
    def scorer_params(self):
        return self.__params

    def __build_full_name(self, scorer_name, scorer_params, scorer_tags):
        full_name = scorer_name
        if scorer_params:
            params_str = '_'.join("%s=%s" % (key, val) for (key, val) in
                                  scorer_params.iteritems())
            full_name = "_".join([full_name, params_str])
        if scorer_tags:
            full_name = "_".join([full_name, scorer_tags])
        return full_name


class MergerKey(object):
    def __init__(self, merger_name, merger_params, scorer_key):
        self.__name = merger_name
        self.__params = merger_params
        self.__scorer_key = scorer_key
        self.__full_name = self.__build_full_name(merger_name, merger_params, scorer_key)

    def __str__(self):
        return self.__full_name

    def __repr__(self):
        return self.__full_name

    @property
    def model_name(self):
        return self.__name

    @property
    def model_params(self):
        return self.__params

    @property
    def scorer_name(self):
        return str(self.__scorer_key)

    def __build_full_name(self, merger_name, merger_params, scorer_key):

        params_str = '_'.join("%s=%s" % (key, val) for (key, val) in merger_params.iteritems())
        if len(params_str) > 0:
            return "_".join(["merged", str(scorer_key), "by", merger_name, params_str])
        else:
            return "_".join(["merged", str(scorer_key), "by", merger_name])

