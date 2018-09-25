import logging
import os
import pandas as pd
import numpy as np
from source.query.ml_phase.bag_worker.unscored_handlers import RandomUnscoredHandler
from source.query.scores_analysis.auc_calculator import AucCalculator
from source.query.scores_analysis.hitrate_calculator import HitRateCalculator
from source.query.scores_analysis.lift_calculator import LiftCalculator
from source.query.scores_analysis.recall_calculator import RecallCalculator
from source.utils.random_seed_provider import ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___

__author__ = 'Shahars'


class ResultsAnalyzer(object):
    def __init__(self, random_pop_scorer, hitrate_calculator, lift_calculator, recall_calculator, auc_calculator):
        self.__logger = logging.getLogger('endor.machine_learning.stats_calculator')
        self.__random_scorer = random_pop_scorer
        self.__hitrate_calculator = hitrate_calculator
        self.__lift_calculator = lift_calculator
        self.__recall_calculator = recall_calculator
        self.__auc_calculator = auc_calculator

    def calc_stats(self, scores_df, ground, whites, universe):
        scored_including_whites = scores_df.shape[0]
        unscored_including_whites = len(universe) - scored_including_whites
        processed_scores = self._preprocess_scores(scores_df, universe, whites)
        # columns=scores_df.columns

        scores_with_ground_df = self.__get_ground_in_scores(processed_scores, ground)

        scored_non_whites = processed_scores['initially_scored'].sum()
        unscored_non_whites_count = processed_scores.shape[0] - scored_non_whites
        ground_in_unscored = scores_with_ground_df[(scores_with_ground_df['initially_scored'] == 0.0) & (scores_with_ground_df['is_positive'] == 1)].shape[0]
        ground_in_scored = scores_with_ground_df[(scores_with_ground_df['initially_scored'] == 1.0) & (scores_with_ground_df['is_positive'] == 1)].shape[0]

        baselines = {
            'scored': {
                'count': scored_including_whites,
                'count_non_whites': scored_non_whites,
                'ground_non_whites': ground_in_scored
            },
            'unscored': {
                'count': unscored_including_whites,
                'count_non_whites': unscored_non_whites_count,
                'ground_non_whites': ground_in_unscored
            }
        }

        validation_success = self.__validate_input(processed_scores, universe, whites, ground)
        if not validation_success:

            return pd.DataFrame(columns=scores_df.columns), pd.DataFrame(columns=scores_df.columns), \
                   pd.DataFrame(columns=scores_df.columns), pd.DataFrame(columns=scores_df.columns), baselines


        scorer_names = scores_df.columns
        lifts_df = pd.DataFrame()
        auc_df = pd.DataFrame()
        hr_df = pd.DataFrame()
        recall_df = pd.DataFrame()
        for scorer_name in scorer_names:
            # sort scorer scores in ascending order
            sorted_scores_df = scores_with_ground_df.sort_values(by=scorer_name, ascending=False)
            sorted_single_scorer_scores = sorted_scores_df[[scorer_name, "is_positive", "initially_scored",
                                                            'is_suspect']]

            scorer_lifts_df, scorer_recall_df, scorer_hr_df, scorer_auc_df = \
                self.__calculate_stats_per_scorer(sorted_single_scorer_scores, scorer_name)

            lifts_df = pd.merge(lifts_df, scorer_lifts_df, left_index=True, right_index=True, how="outer")
            recall_df = pd.merge(recall_df, scorer_recall_df, left_index=True, right_index=True, how="outer")
            hr_df = pd.merge(hr_df, scorer_hr_df, left_index=True, right_index=True, how="outer")
            auc_df = pd.merge(auc_df, scorer_auc_df, left_index=True, right_index=True, how="outer")

        self.__logger.info("Finished computing stats for each merit function.")
        return auc_df, hr_df, lifts_df, recall_df, baselines

    def __score_unscored_pop(self, scores_df, universe):
        res_df = pd.DataFrame()
        for scorer_col in scores_df.columns:
            self.__logger.info("scoring the unscored population, of scorer %s" % scorer_col)
            scores_arr = np.array([scores_df.index.values, scores_df[scorer_col].values]).transpose()

            temp_scores_df = self.__random_scorer.score_the_unscored_pop(scores_arr, universe, scorer_col,
                                                                         self.__logger)
            del scores_arr
            res_df = pd.merge(res_df, temp_scores_df, left_index=True, right_index=True,how="outer")
        del scores_df
        return res_df

    def __remove_non_universe_from_scores(self, scores_df, universe, whites):
        self.__logger.info("removing non universe and whites from scores")
        universe_to_score = list(set(scores_df.index) - set(whites))
        good_scores = scores_df.ix[universe_to_score].dropna()
        self.__logger.info("finished removing non universe and whites from scores")
        return good_scores

    def __cast_scores_to_float(self, scores_df):
        scores_df = scores_df.astype(float)
        return scores_df

    def _preprocess_scores(self, scores_df, universe, whites):

        initially_scored_pop = scores_df.index.values
        with_random_scores_df = self.__score_unscored_pop(scores_df, universe)
        with_random_scores_df["initially_scored"] = with_random_scores_df.index.isin(initially_scored_pop).astype(int)
        suspects = set(universe) - set(whites)
        with_random_scores_df['is_suspect'] = with_random_scores_df.index.isin(suspects).astype(int)
        with_random_scores_df = self.__remove_non_universe_from_scores(with_random_scores_df, universe, whites)
        with_random_scores_df = self.__cast_scores_to_float(with_random_scores_df)
        return with_random_scores_df

    def check_valid_input(self, scores_df, universe, whites, ground):
        if not self.__check_valid_u_w_gt(universe, whites, ground) or not self.__check_valid_scores(scores_df):
            return False
        return True

    def __get_ground_in_scores(self, processed_scores, ground):
        processed_scores["is_positive"] = (processed_scores.index.isin(ground)).astype(int)
        return processed_scores

    def __validate_input(self, processed_scores, universe, whites, ground):
        try:
            valid_input = self.check_valid_input(processed_scores, universe, whites, ground)
            if not valid_input:
                self.__logger.warning("input for calc_stats isn't valid")
                return False
        except Exception as e:
            self.__logger.warning("could not validate input for calc stats due to: %s" % str(e))
            return False
        return True

    def __check_valid_u_w_gt(self, universe, whites, ground):
        u_size = len(universe)
        g_size = len(ground)
        if u_size * g_size == 0:
            self.__logger.warning("Universe size: %d, ground size: %d, one of them is empty" % (u_size, g_size))
            return False
        if len(whites) == 0:
            self.__logger.warning("whites set is empty")
        return True

    def __check_valid_scores(self, scores_df):
        row_num = len(scores_df.index.values)
        if row_num == 0:
            self.__logger.warning("Received an empty scores list, can't continue")
            return False

        if np.isnan(scores_df.values).any():
            self.__logger.warning("NaN scores exist, they're not supported, can't continue analyzing results")
            return False
        return True

    def __calculate_stats_per_scorer(self, sorted_scores_df, scorer_name):

        lifts_df = self.__lift_calculator.calc_sorted_scorer_lift(sorted_scores_df, scorer_name)
        hr_df = self.__hitrate_calculator.calc_sorted_scorer_hitrate(sorted_scores_df, scorer_name)
        recall_df = self.__recall_calculator.calc_sorted_scorer_recall(sorted_scores_df, scorer_name)
        auc_df = self.__auc_calculator.calc_sorted_scorer_auc(sorted_scores_df, scorer_name)
        return lifts_df, recall_df, hr_df, auc_df

    def check_valid_stats(self, stats):
        for stats_type, df in stats.iteritems():
            if stats_type == 'baselines':
                continue
            if len(df) == 0:
                return False
            if -1 in df.values:
                return False
            if np.isnan(df.values).any():
                return False
        return True

    def prettify_results_summary_for_es(self, stats):
        lift_from_df = stats['lift'].to_dict().values()[0]
        lifts_as_found_needs = {key.replace('_', '_percentage_'): value for (key, value) in lift_from_df.iteritems()}

        hit_rates_from_df = stats['hit_rate'].to_dict().values()[0]
        hit_rates_as_found_needs = {key.replace('count', 'count_').replace('prcntg', 'percentage_').
                                        replace('hr', 'hit_rate'): value for (key, value) in
                                    hit_rates_from_df.iteritems()}

        auc_as_found_needs = stats['auc'].values[0][0]

        recall_from_df = stats['recall'].to_dict().values()[0]
        recall_as_found_needs = {key.replace('prcntg', 'percentage_'): value for (key, value) in
                                 recall_from_df.iteritems()}

        return auc_as_found_needs, hit_rates_as_found_needs, recall_as_found_needs, lifts_as_found_needs

    def random_scorer(self):
        return self.__random_scorer

    def hitrate_calculator(self):
        return self.__hitrate_calculator

    def lift_calculator(self):
        return self.__lift_calculator

    def recall_calculator(self):
        return self.__recall_calculator

    def auc_calculator(self):
        return self.__auc_calculator


def local_main():
    in_path = '/home/user/Documents/data/results_analyzer_test/2c134ff5427bf6b69e6fa5a15eb9c683-0/'


    random_provider = ___DO_NOT_USE___RandomSeedProviderHolder___DO_NOT_USE___.get_provider()

    hit_rate_calculator = HitRateCalculator()
    res_analyzer = ResultsAnalyzer(RandomUnscoredHandler(random_provider), hit_rate_calculator,
                        LiftCalculator(hit_rate_calculator), RecallCalculator(), AucCalculator())
    universe = pd.read_csv(os.path.join(in_path, 'universe.csv'), index_col=0, header=None).index.values
    ground = pd.read_csv(os.path.join(in_path, 'ground.csv'), index_col=0, header=None).index.values
    whites = pd.read_csv(os.path.join(in_path, 'whites.csv'), index_col=0, header=None).index.values
    print len(ground)/float(len(set(universe)-set(whites)))

    scores_df = pd.read_csv(os.path.join(in_path, 'all_scores.csv'), index_col=0)

    return res_analyzer.calc_stats(scores_df, ground, whites, universe)

if __name__ == '__main__':
    auc_df, hr_df, lifts_df, recall_df, baselines = local_main()
    auc_df.to_csv('/home/user/Documents/data/results_analyzer_test/2c134ff5427bf6b69e6fa5a15eb9c683-0/auc.csv')

    hr_df.to_csv('/home/user/Documents/data/results_analyzer_test/2c134ff5427bf6b69e6fa5a15eb9c683-0/hr.csv')
    lifts_df.to_csv('/home/user/Documents/data/results_analyzer_test/2c134ff5427bf6b69e6fa5a15eb9c683-0/lifts.csv')
    recall_df.to_csv('/home/user/Documents/data/results_analyzer_test/2c134ff5427bf6b69e6fa5a15eb9c683-0/recall.csv')



