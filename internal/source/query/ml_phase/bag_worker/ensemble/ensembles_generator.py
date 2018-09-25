# from source.utils.random_seed_provider import ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___
#
# __author__ = 'shahars'
# from source.query.ml_phase.bag_worker.ensemble.ensemble_scores import UserEnsemble, DefaultEnsemble
#
#
# class EnsemblesGenerator(object):
#
#     CONFIG_RULES = [{"name": (basestring, False, "default"),
#                     "elements": ([{"coef": (object, True),
#                                    "base_func": (basestring, True),
#                                    "factor": (object, True),
#                                    "base_func_params": (object, True)
#                                    }], False, [])
#                      }]
#
#     def __init__(self, config, scorers_names, logger):
#         config.validate(self.CONFIG_RULES)
#         self.config = config
#         self.scorers_names = scorers_names
#         self.logger = logger
#
#     def generate_user_ensembles(self, past_bag, universe, whites, ground):
#         all_ensembles = {}
#         for ensemble_data in self.config:
#             ensemble_name = ensemble_data.name
#             random_provider = ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___.get_provider()
#             ensemble = UserEnsemble(ensemble_name, past_bag, universe, whites, ground, ensemble_data.elements,
#                                     self.scorers_names, self.logger)
#             all_ensembles[ensemble_name] = ensemble
#
#         return all_ensembles
#
#     def generate_default_ensembles(self, default_weights_dict):
#         # all_ensembles = {}
#         # for ensemble_data in self.config:
#         #     ensemble_name = ensemble_data.name
#         #
#         # return {self.config.name: DefaultEnsemble(self.config.name, self.scorers_names, default_weights)}
#         all_ensembles_data = self.prepare_default_ensemble_data(default_weights_dict)
#         all_ensembles = {}
#         for ens_name, ens_data in all_ensembles_data.iteritems():
#             weights_dict = dict(zip(ens_data["scorer_names"], ens_data["weights"]))
#             all_ensembles[ens_name] = DefaultEnsemble(ens_name, ens_data["scorer_names"], weights_dict)
#         return all_ensembles
#
#     def prepare_default_ensemble_data(self, default_weights_dict):
#         ensemble_names = default_weights_dict[default_weights_dict.keys()[0]].keys()
#         # print ensemble_names
#         all_ensembles_data = {ens_name: {"scorer_names": [], "weights": []} for ens_name in ensemble_names}
#         # print ensemble_names
#
#         for scorer_name, scorer_weights_dict in default_weights_dict.iteritems():
#             for ens_name, scorer_weight in scorer_weights_dict.iteritems():
#
#                 all_ensembles_data[ens_name]["scorer_names"].append(scorer_name)
#                 all_ensembles_data[ens_name]["weights"].append(scorer_weight)
#         return all_ensembles_data
