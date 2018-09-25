# __author__ = 'shahars'
#
# import pandas as pd
#
# import numpy as np
# from source.query.ml_phase.results_analyze import calc_auc, calc_lift, calc_hitrate
#
#
# class DefaultEnsemble(object):
#     def __init__(self, ensemble_name, scorers_names, scorers_weights):
#         self.scorers_names = scorers_names
#         self.scorers_weights_dict = scorers_weights
#         self.ensemble_name = ensemble_name
#
#     def calc_ensembled_scores(self, scores_df, ensemble_name):
#
#         weighted_scores = scores_df.apply(self.get_person_weighted_score, axis=1)
#         ensembled_scores = pd.DataFrame({ensemble_name: weighted_scores}, index=scores_df.index)
#         return ensembled_scores
#
#     def get_person_weighted_score(self, scores):
#         weighted_scores = [scores[scorer]*self.scorers_weights_dict[scorer] for scorer in self.scorers_names]
#         return np.nansum(np.array(weighted_scores))
#
#
# class UserEnsemble(DefaultEnsemble):
#     def __init__(self, ensemble_name, past_bag, universe, whites, ground, weights_func_data, scorers_names, logger):
#         self.universe = universe
#         self.whites = whites
#         self.ground = ground
#         self.logger = logger
#         self.logger.info("start calculating scorer ensemble weights")
#         weights_dict = self.calc_scorers_weights(weights_func_data, scorers_names, past_bag)
#         super(UserEnsemble, self).__init__(ensemble_name, scorers_names, weights_dict)
#
#     def calc_scorers_weights(self, weights_func_data, scorers_names, past_bag):
#         scorers_weights_dict = {}
#         for scorer in scorers_names:
#             self.logger.info("calculating weight for scorer %s" % scorer)
#             past_scores_df = past_bag.get_scores_by_function(scorer)
#             scorers_weights_dict[scorer] = self.calc_scorer_weight(past_scores_df, weights_func_data)
#         return scorers_weights_dict
#
#     def get_base_func_res(self, func_name, element, scores):
#         if func_name.lower() == "auc":
#             return calc_auc(scores, self.universe, self.whites, self.ground, self.logger)
#         elif func_name.lower() == "hr":
#             params = element.base_func_params
#             func_type = params.type
#             thresh = params.thresh
#             return calc_hitrate(scores, self.universe, self.whites, self.ground, thresh, func_type, self.logger)
#
#         elif func_name.lower() == "lift":
#             params = element.base_func_params
#             thresh = params.thresh
#             return calc_lift(scores, self.ground, thresh, self.logger)
#         else:
#             raise ValueError("base function %s is not supported, only supported: auc, hr, lift" % func_name)
#
#
#     def calc_scorer_weight(self, past_scores_df, weights_func_data):
#         all_elements_res = []
#         for element in weights_func_data:
#             coefficient = element.coef
#             factor = element.factor
#             func_name = element.base_func
#             self.logger.info("calculating %s ensemble weight base function" % func_name)
#             base_func_res = self.get_base_func_res(func_name, element, past_scores_df)
#             # print factor
#             element_res = coefficient*(base_func_res**factor)
#             all_elements_res.append(element_res)
#         return np.nansum(np.array(all_elements_res))
#
#
# class LearningEnsemble(DefaultEnsemble):
#     def __init__(self):
#         pass