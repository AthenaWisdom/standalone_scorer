import hashlib
import itertools


class MergerStrategiesProvider(object):
    def get_merging_tasks(self, ml_conf, merger_conf):
        res = ({'merger_id': w, 'scorer_id': x, 'merger_model': y, 'variant': z}
                for w, x, y, z in self.get_strategies(ml_conf, merger_conf))
        return res

    @staticmethod
    def get_all_scorer_names(ml_conf):
        return MergerStrategiesProvider.__get_single_scorer_names(ml_conf) + \
               MergerStrategiesProvider.__get_all_ensemble_names(ml_conf)

    def get_strategies(self, ml_conf, merger_conf):
        merger_names_et_params = self.__build_merger_name_et_params(merger_conf)
        all_names = self.get_all_scorer_names(ml_conf)
        return [(hash_triplet(elem[0][0], elem[0][1], elem[1]), elem[1], elem[0][0], elem[0][1])
                for elem in list(itertools.product(merger_names_et_params, all_names))]

    @staticmethod
    def __build_params_list(parameters):
        v = list(itertools.product(*parameters.values()))
        k = parameters.keys()
        return [dict(zip(k, v[i])) for i in range(len(v))]

    @staticmethod
    def __get_single_scorer_names(ml_conf):
        scorers = ml_conf["scorers"]
        internal_scorers = []
        external_scorers = []
        for scorer_conf in scorers:
            scorer_tags = scorer_conf.get("tags", ["internal"])
            if "external" in scorer_tags:
                external_scorers.append("_".join([scorer_conf["name"], scorer_conf["unscored_handler"],
                                                  scorer_conf["conf"]["external_file_name"],
                                                  scorer_conf["conf"]["external_col_name"]]))
            else:
                internal_scorers.append("_".join([scorer_conf["name"], scorer_conf["unscored_handler"]]))
        return internal_scorers+external_scorers

    @staticmethod
    def __get_all_ensemble_names(ml_conf):
        ensembles = ml_conf["ensembles"]
        all_ensemble_names = []
        for ensemble in ensembles:
            ensemble_name = ensemble["ensemble_scheme"]
            reg_factors = ensemble["parameters"]
            params_str = '_'.join("%s=%s" % (key, val) for (key, val) in reg_factors.iteritems())
            full_ensemble_name = "_".join([ensemble_name, params_str])
            ensemble_tags = ensemble.get("tags")

            if ensemble_tags:
                tags_for_ensemble_name = '_'.join(ensemble_tags)
                full_ensemble_name += '_' + tags_for_ensemble_name
            all_ensemble_names.append(full_ensemble_name)

        return all_ensemble_names

    def __build_merger_name_et_params(self, merger_conf):
        merging_schemas = merger_conf["merging_schemes"]
        all_merging_schema_couples = []
        for merging_schema in merging_schemas:
            merger_name = merging_schema["merged_scores_function"]
            all_merging_schema_couples.extend([(merger_name, merging_schema["parameters"])])
        return all_merging_schema_couples


def hash_triplet(model, param_dict, scorer_name):
    sha_obj = hashlib.sha512()
    sha_obj.update(model)
    sha_obj.update(repr(param_dict))
    sha_obj.update(scorer_name)
    return sha_obj.hexdigest()
