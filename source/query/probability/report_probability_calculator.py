from source.query.scores_service.scores_service import ScoresService


class ReportProbabilityCalculator(object):
    def __init__(self, scores_service, probability_calculator):
        """

        @type scores_service: L{ScoresService}
        @type probability_calculator: L{ReportProbabilityInterpolation}
        """
        self.__scores_service = scores_service
        self.__probability_calculator = probability_calculator

    def calculate(self, chosen_merger_key, customer, past_query_id, present_num_scored_candidates, quest_id,
                  feature_flags):

        past_hit_rate_df = self.__scores_service.load_single_merger_performance(customer, quest_id, past_query_id,
                                                                                chosen_merger_key, 'hit_rate',
                                                                                feature_flags)
        chosen_merger_name_as_column = str(chosen_merger_key)
        past_hit_rate_dict = past_hit_rate_df.to_dict()[chosen_merger_name_as_column]

        past_baselines_df = self.__scores_service.load_single_merger_performance(customer, quest_id, past_query_id,
                                                                                 chosen_merger_key, 'baselines',
                                                                                 feature_flags)

        past_num_candidates = past_baselines_df.to_dict()['count_non_whites']['scored'] + \
                              past_baselines_df.to_dict()['count_non_whites']['unscored']
        past_num_scored = past_baselines_df.to_dict()['count_non_whites']['scored']

        probability_vector, compressed_graph = self.__probability_calculator.calculate(past_hit_rate_dict,
                                                                                       present_num_scored_candidates,
                                                                                       past_num_candidates,
                                                                                       past_num_scored)
        return probability_vector, compressed_graph, past_baselines_df.to_dict()



