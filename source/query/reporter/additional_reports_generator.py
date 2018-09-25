import os
from cStringIO import StringIO
import pandas as pd
import numpy as np

from source.query.scores_service.scores_service import ScoresService
from source.storage.stores.reports_store.interface import ReportsStoreInterface
from source.storage.stores.reports_store.types import Report


class KernelReader(object):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IoHandlerInterface}
        """
        self.__io_handler = io_handler

    def read_raw_kernel(self, customer, quest_id, query_id):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype L{pd.DataFrame}
        """
        kernel_folder_path = os.path.join('sandbox-' + customer, 'Quests', quest_id, query_id, 'kernelCsv', 'data')
        files = self.__io_handler.list_dir(kernel_folder_path)
        kernel_path = filter(lambda f: 'part' in f, files)[0]
        data = StringIO(self.__io_handler.load_raw_data(kernel_path))
        df = pd.read_csv(data)
        return df


class AdditionalReportsGeneratorSelector(object):
    def __init__(self, past_based_reports_generator, no_past_reports_generator):
        self.__past_based_reports_generator = past_based_reports_generator
        self.__no_past_reports_generator = no_past_reports_generator

    def get_additional_reports_generator(self, has_past):
        if has_past:
            return self.__past_based_reports_generator
        else:
            return self.__no_past_reports_generator




class PastBasedReportsGenerator(object):
    REPORT_COLUMNS = ["ID", "Rank", "Probability", "Lift"]
    def __init__(self, kernel_reader, scores_service, report_store):
        """
        @type kernel_reader: L{KernelReader}
        @type scores_service: L{ScoresService}
        @type report_store: L{ReportsStoreInterface}
        """
        self.__scores_service = scores_service
        self.__kernel_reader = kernel_reader
        self.__report_store = report_store

    def create_and_store_additional_reports(self, report, feature_flags):
        """
        @type report: L{Report}
        @type feature_flags: C{dict}
        @rtype C{dict} of C{str} -> L{Report}
        """
        past_full_kernel = self.__kernel_reader.read_raw_kernel(report.customer, report.quest_id, 'training0')

        scores_past = self.__scores_service.load_merged_scores(report.customer, report.quest_id, 'training0',
                                                               feature_flags,
                                                               report.chosen_merger).dropna(subset=["score"])
        present_full_kernel = self.__kernel_reader.read_raw_kernel(report.customer, report.quest_id, 'prediction')

        scores_present = self.__scores_service.load_merged_scores(report.customer, report.quest_id, 'prediction',
                                                                  feature_flags,
                                                                  report.chosen_merger).dropna(subset=["score"])

        reports_bundle = self.__create_additional_reports(report, past_full_kernel, present_full_kernel,
                                                                      scores_past, scores_present)
        for k, v in reports_bundle.iteritems():
            self.__report_store.store_mutated_report(k, v)

        return reports_bundle



    def __create_additional_reports(self, report, past_kernel, present_kernel, selected_merger_scores_past,
                                            selected_merger_scores_present):

        # kernel: (index, [u'NORMAL_ID', u'raw_white', u'raw_ground', u'raw_universe',
        # u'has_stats', u'ground', u'white', u'candidate', u'ID']

        # report.df: unhashed_id(index), Rank, Probability, Lift
        # selected_merger_scores_past: user_id(hashed, index), score, origin_id

        # score the whites: (ID, handled_score, Probability(=handled_score), Lift)


        whites_unscored_baseline_dict = self.__calc_unscored_whites_baseline(past_kernel, selected_merger_scores_past)
        whites_scored_baseline_dict = self.__calc_scored_whites_baseline(past_kernel, selected_merger_scores_past)
        whites_scored_by_baseline_df, whites_unscored_by_baseline_df= self.__score_whites_by_baseline(present_kernel,
                                                                                                      selected_merger_scores_present, whites_scored_baseline_dict["baseline"],
                                                                                                      whites_unscored_baseline_dict["baseline"])

        unscored_baseline_dict = self.__calc_unscored_baseline(past_kernel, selected_merger_scores_past)
        unscored_by_bottom, unscored_by_baseline_df = self.__score_the_unscored(present_kernel,
                                                                                selected_merger_scores_present,
                                                                                unscored_baseline_dict["baseline"])


        # rest: we don't touch it
        non_white_scored_cands_scores_df = self.__get_non_white_scored_candidates(present_kernel, selected_merger_scores_present, report.df)
        no_white_scored_cands_baseline_dict = self.__calc_scored_baseline(past_kernel, selected_merger_scores_past)

    # add ind column
        for data in [unscored_by_baseline_df, unscored_by_bottom, whites_scored_by_baseline_df, whites_unscored_by_baseline_df]:
            data['ind'] = data['ID']
            data['Rank'] = np.nan
        non_white_scored_cands_scores_df['ind'] = non_white_scored_cands_scores_df['ID']

        # no whites, unscored at bottom:
        nowhites_afterscored_baseline = (unscored_baseline_dict["a"] +
                                         no_white_scored_cands_baseline_dict["a"])/float(unscored_baseline_dict["b"] + no_white_scored_cands_baseline_dict["b"])
        NoWhitesAfterScored_df = self.__build_no_whites_afterscored_report(non_white_scored_cands_scores_df, unscored_by_bottom, nowhites_afterscored_baseline)

        # build different reports:

        #  no whites, baseline for unscored:
        NoWhitesBaselineForUnscored_df = self.__build_no_whites_baseline_unscored_report(non_white_scored_cands_scores_df,
                                                                                         unscored_by_baseline_df,
                                                                                         nowhites_afterscored_baseline)
        # with whites, leave unscored:
        scored_et_whites_baseline = (no_white_scored_cands_baseline_dict["a"] + whites_unscored_baseline_dict["a"] +
                                     whites_scored_baseline_dict["a"])/float(no_white_scored_cands_baseline_dict["b"] +
                                                                             whites_unscored_baseline_dict["b"] +
                                                                             whites_scored_baseline_dict["b"])

        WithWhitesLeaveUnscored_df = self.__build_whites_no_unscored_report(non_white_scored_cands_scores_df,
                                                                            whites_scored_by_baseline_df,
                                                                            whites_unscored_by_baseline_df,
                                                                            scored_et_whites_baseline)
        # with whites, unscored at bottom:
        all_baseline = (unscored_baseline_dict["a"] + no_white_scored_cands_baseline_dict["a"] + whites_unscored_baseline_dict["a"] +
                        whites_scored_baseline_dict["a"])/float(unscored_baseline_dict["b"] + no_white_scored_cands_baseline_dict["b"] +
                                                                whites_unscored_baseline_dict["b"] +
                                                                whites_scored_baseline_dict["b"])

        WithWhitesAfterScored_df = self.__build_whites_afterscored_report(non_white_scored_cands_scores_df,
                                                                          unscored_by_bottom,
                                                                          whites_scored_by_baseline_df,
                                                                          whites_unscored_by_baseline_df, all_baseline)
        # with whites, baseline for unscored:
        WithWhitesBaselineForUnscored_df = self.__build_whites_baseline_for_unscored_report(non_white_scored_cands_scores_df,
                                                                                            unscored_by_baseline_df,
                                                                                            whites_scored_by_baseline_df,
                                                                                            whites_unscored_by_baseline_df,
                                                                                            all_baseline)
        customer = report.customer
        quest_id = report.quest_id
        chosen_merger = report.chosen_merger
        return {
            'NoWhitesLeaveUnscored': Report(customer, quest_id,
                                            self.__unhash_report(non_white_scored_cands_scores_df[self.REPORT_COLUMNS],
                                                                              present_kernel), chosen_merger),
            'NoWhitesAfterScored': Report(customer, quest_id, self.__unhash_report(NoWhitesAfterScored_df, present_kernel),
                                          chosen_merger),
            'NoWhitesBaselineForUnscored': Report(customer, quest_id, self.__unhash_report(NoWhitesBaselineForUnscored_df,
                                                                                    present_kernel), chosen_merger),
            'WithWhitesLeaveUnscored': Report(customer, quest_id, self.__unhash_report(WithWhitesLeaveUnscored_df,
                                                                                present_kernel), chosen_merger),
            'WithWhitesAfterScored': Report(customer, quest_id, self.__unhash_report(WithWhitesAfterScored_df, present_kernel),
                                            chosen_merger),
            'WithWhitesBaselineForUnscored': Report(customer, quest_id, self.__unhash_report(WithWhitesBaselineForUnscored_df,
                                                                                      present_kernel), chosen_merger)
        }


    def __score_the_unscored(self, present_kernel, selected_merger_scores_present, unscored_baseline):
        # handle the unscored (only non white) (ID, handled_score)
        non_white_candidates_unscored_present = self.__get_unscored_non_white_cands(present_kernel, selected_merger_scores_present)

        if len(non_white_candidates_unscored_present)>0:
            # score the unscored by past baseline (ID, handled_score)
            unscored_by_baseline_df = pd.DataFrame({"ID": non_white_candidates_unscored_present, "handled_score": unscored_baseline})

            # score the unscored in bottom of the report: (ID, handled_score)
            unscored_by_bottom = self.__score_unscored_by_bottom(non_white_candidates_unscored_present, selected_merger_scores_present)

        else:
            unscored_by_baseline_df = pd.DataFrame({"ID":[], "handled_score":[]})
            unscored_by_bottom = pd.DataFrame({"ID":[], "handled_score":[]})
        #     TODO: add prob and lift
        return unscored_by_bottom, unscored_by_baseline_df


    def __get_non_white_scored_candidates(self, present_kernel, selected_merger_scores_present, report_df):

        hashed_report = pd.merge(report_df, present_kernel, left_index=True,
                                 right_on="NORMAL_ID", how='left')[self.REPORT_COLUMNS]
        hashed_report_w_scores = pd.merge(hashed_report, selected_merger_scores_present, left_on="ID",
                                          right_index=True, how='left')

        whites_in_universe_present = present_kernel[(present_kernel['raw_white'] == 1) & (present_kernel['raw_universe'] == 1)]['ID'].values
        non_white_scored_cands_scores = hashed_report_w_scores[~hashed_report_w_scores.index.isin(whites_in_universe_present)]
        non_white_scored_cands_scores.index = range(len(non_white_scored_cands_scores))
        return non_white_scored_cands_scores[self.REPORT_COLUMNS+["score"]]

    def __score_whites_by_baseline(self, present_kernel, selected_merger_scores_present, whites_scored_baseline, whites_unscored_baseline):


        #  Calculating the whites that are also in the universe (hashed)
        whites_in_universe_present = present_kernel[(present_kernel['raw_white']==1) & (present_kernel['raw_universe']==1)]['ID'].values

        # Splitting the whites to whites_scored and whites_unscored
        whites_in_universe_scored_present = list(set(selected_merger_scores_present.index.values).intersection(set(whites_in_universe_present)))
        whites_scored_df = pd.DataFrame({"ID": whites_in_universe_scored_present, "handled_score": whites_scored_baseline})

        whites_in_universe_unscored_present = list(set(whites_in_universe_present) - set(whites_in_universe_scored_present))


        whites_unscored_df = pd.DataFrame({"ID": whites_in_universe_unscored_present, "handled_score": whites_unscored_baseline})


        return whites_scored_df, whites_unscored_df


    def __score_unscored_by_bottom(self, non_white_candidates_unscored_present, selected_merger_scores_present):
        minimal_score = min(selected_merger_scores_present['score'].values)

        bottom_scored = pd.DataFrame({"ID": non_white_candidates_unscored_present, "handled_score": minimal_score-1})

        return bottom_scored



    def __get_unscored_non_white_cands(self, kernel, scores):
        whites_in_universe_past = kernel[(kernel['raw_white'] == 1) & (kernel['raw_universe'] == 1)]['ID'].values
        non_whites_candidates_scored_past = list(set(scores.index.values) - set(whites_in_universe_past))
        non_white_candidates_unscored_past = list(set(kernel[kernel['candidate'] == 1]['ID'].values) -
                                                  set(non_whites_candidates_scored_past))
        return non_white_candidates_unscored_past

    def __build_no_whites_afterscored_report(self, non_white_scored_cands_scores_df, unscored_by_bottom, no_whites_yes_unscored_past_baseline):
        # ID, Rank, Probability, Lift
        # unscored are by bottom score, hence we sort by score here and not by probability
        unscored_by_bottom['score'] = unscored_by_bottom['handled_score']

        non_white_scored_cands_scores_df['Lift'] = non_white_scored_cands_scores_df['Probability']/no_whites_yes_unscored_past_baseline
        unscored_by_bottom["Lift"] = np.nan
        unscored_by_bottom["Probability"] = np.nan

        NoWhitesAfterScored_df = pd.concat([non_white_scored_cands_scores_df,
                                            unscored_by_bottom]).sort(columns=['score', "Rank", 'ind'],
                                                                      ascending=[False, True, False])
        NoWhitesAfterScored_df["Rank"] = range(1, len(NoWhitesAfterScored_df.index) + 1)
        return NoWhitesAfterScored_df[self.REPORT_COLUMNS]

    def __build_no_whites_baseline_unscored_report(self, non_white_scored_cands_scores_df, unscored_by_baseline_df, no_whites_yes_unscored_past_baseline):
        unscored_by_baseline_df['Probability'] = unscored_by_baseline_df['handled_score']

        no_whites_unscored_baseline_df = pd.concat([non_white_scored_cands_scores_df,
                                                    unscored_by_baseline_df]).sort(columns=['Probability', "Rank", 'ind'],
                                                                                   ascending=[False, True, False])
        no_whites_unscored_baseline_df['Lift'] = no_whites_unscored_baseline_df['Probability']/no_whites_yes_unscored_past_baseline
        no_whites_unscored_baseline_df["Rank"] = range(1, len(no_whites_unscored_baseline_df.index) + 1)
        return no_whites_unscored_baseline_df[self.REPORT_COLUMNS]

    def __build_whites_no_unscored_report(self, non_white_scored_cands_scores_df, whites_scored_by_baseline_df,
                                          whites_unscored_by_baseline_df, scored_et_whites_baseline):
        whites_scored_by_baseline_df['Probability'] = whites_scored_by_baseline_df['handled_score']
        whites_unscored_by_baseline_df['Probability'] = whites_unscored_by_baseline_df['handled_score']
        whites_no_unscored_df = pd.concat([whites_scored_by_baseline_df, whites_unscored_by_baseline_df,
                          non_white_scored_cands_scores_df]).sort(columns=['Probability', "Rank", 'ind'],
                                                                  ascending=[False, True, False])
        whites_no_unscored_df['Lift'] = whites_no_unscored_df['Probability']/scored_et_whites_baseline
        whites_no_unscored_df["Rank"] = range(1, len(whites_no_unscored_df.index) + 1)
        return whites_no_unscored_df[self.REPORT_COLUMNS]


    def __build_whites_baseline_for_unscored_report(self, non_white_scored_cands_scores_df, unscored_by_baseline_df,
                                                    whites_scored_by_baseline_df, whites_unscored_by_baseline_df, all_baseline):
        whites_scored_by_baseline_df['Probability'] = whites_scored_by_baseline_df['handled_score']
        whites_unscored_by_baseline_df['Probability'] = whites_unscored_by_baseline_df['handled_score']
        unscored_by_baseline_df['Probability']=unscored_by_baseline_df['handled_score']
        whites_baseline_for_unscored_df = pd.concat([whites_scored_by_baseline_df, whites_unscored_by_baseline_df,
                                                     non_white_scored_cands_scores_df, unscored_by_baseline_df])\
            .sort(columns=['Probability', "Rank", 'ind'], ascending=[False, True, False])


        whites_baseline_for_unscored_df['Lift'] = whites_baseline_for_unscored_df['Probability']/all_baseline
        whites_baseline_for_unscored_df["Rank"] = range(1, len(whites_baseline_for_unscored_df.index) + 1)

        return whites_baseline_for_unscored_df[self.REPORT_COLUMNS]

    def __build_whites_afterscored_report(self, non_white_scored_cands_scores_df, unscored_by_bottom,
                                          whites_scored_by_baseline_df, whites_unscored_by_baseline_df, all_baseline):
        whites_scored_by_baseline_df['Probability'] = whites_scored_by_baseline_df['handled_score']
        whites_unscored_by_baseline_df['Probability'] = whites_unscored_by_baseline_df['handled_score']


        with_whites_df = pd.concat([whites_scored_by_baseline_df, whites_unscored_by_baseline_df,
                          non_white_scored_cands_scores_df]).sort(
                            columns=['Probability', "Rank", 'ind'], ascending=[False, True, False])

        with_whites_df["Lift"] = with_whites_df['Probability']/all_baseline
        unscored_by_bottom["Lift"] = np.nan
        # add the unscored at the bottom
        res = pd.concat([with_whites_df, unscored_by_bottom.sort(columns=['ind'], ascending=False)])
        res["Rank"] = range(1, len(res.index) + 1)
        return res[self.REPORT_COLUMNS]

    def __calc_scored_whites_baseline(self, past_kernel, selected_merger_scores_past):
        past_ground = past_kernel[past_kernel['raw_ground']==1]['ID'].values
        whites_in_universe_past = past_kernel[(past_kernel['raw_white']==1) & (past_kernel['raw_universe']==1)]['ID'].values
        # whites who are also scored
        whites_in_universe_scored_past = list(set(selected_merger_scores_past.index.values).intersection(set(whites_in_universe_past)))

        return self.__calc_baseline(past_ground, whites_in_universe_scored_past)

    def __calc_baseline(self, past_ground, population_past):
        b = len(population_past)
        if len(population_past) == 0:
            return {"a":0,"b":0,"baseline":0}
        else:
            a = len(set(past_ground).intersection(set(population_past)))

            past_baseline = a / float(b)
            return {"a":a, "b":b, "baseline":past_baseline}


    def __calc_unscored_whites_baseline(self, past_kernel, selected_merger_scores_past):
        past_ground = past_kernel[past_kernel['raw_ground']==1]['ID'].values
        whites_in_universe_past = past_kernel[(past_kernel['raw_white']==1) & (past_kernel['raw_universe']==1)]['ID'].values
        # unscored whites
        whites_in_universe_unscored_past = list(set(whites_in_universe_past) - set(selected_merger_scores_past.index.values))
        return self.__calc_baseline(past_ground, whites_in_universe_unscored_past)



    def __calc_unscored_baseline(self, past_kernel, selected_merger_scores_past):
        ground_past_no_whites = past_kernel[past_kernel['ground']==1]['ID'].values
        non_white_candidates_unscored_past = self.__get_unscored_non_white_cands(past_kernel, selected_merger_scores_past)
        return self.__calc_baseline(ground_past_no_whites, non_white_candidates_unscored_past)

    def __calc_scored_baseline(self, past_kernel, selected_merger_scores_past):

        ground_past_no_whites = past_kernel[past_kernel['ground']==1]['ID'].values
        whites_past = past_kernel[(past_kernel['raw_white'] == 1)]['ID'].values
        non_white_scored_cands_scores = selected_merger_scores_past[~selected_merger_scores_past.index.isin(whites_past)]
        return self.__calc_baseline(ground_past_no_whites, non_white_scored_cands_scores.index.values)

    def __unhash_report(self, scores_df, present_kernel):
        scores_df.columns = ["hashed_id", "Rank", "Probability", "Lift"]
        hashed_report = pd.merge(scores_df, present_kernel, left_on="hashed_id",
                                 right_on="ID", how='left')[["NORMAL_ID", "Rank", "Probability", "Lift"]]
        hashed_report.columns = [self.REPORT_COLUMNS]
        return hashed_report


class NoPastReportsGenerator(object):
    REPORT_COLUMNS = ["ID", "Rank"]

    def __init__(self, kernel_reader, scores_service, report_store):
        self.__scores_service = scores_service
        self.__kernel_reader = kernel_reader
        self.__report_store = report_store

    def create_and_store_additional_reports(self, report, feature_flags):
        present_full_kernel = self.__kernel_reader.read_raw_kernel(report.customer, report.quest_id, 'prediction')

        scores_present = self.__scores_service.load_merged_scores(report.customer, report.quest_id, 'prediction',
                                                                  feature_flags,
                                                                  report.chosen_merger).dropna(subset=["score"])
        reports_bundle = self.__create_additional_reports(report, present_full_kernel,
                                                                  scores_present)

        for k, v in reports_bundle.iteritems():
            self.__report_store.store_mutated_report(k, v)

        return reports_bundle


    def __create_additional_reports(self, report, present_kernel, selected_merger_scores_present):
        non_white_scored_cands_scores_df = self.__get_non_white_scored_candidates(present_kernel,
                                                                                  selected_merger_scores_present,
                                                                                  report.df)

        non_white_candidates_unscored_present = self.__get_unscored_non_white_cands(present_kernel, selected_merger_scores_present)

        if len(non_white_candidates_unscored_present)>0:

            # score the unscored in bottom of the report: (ID, handled_score)
            unscored_by_bottom = self.__score_unscored_by_bottom(non_white_candidates_unscored_present, selected_merger_scores_present)

        else:

            unscored_by_bottom = pd.DataFrame({"ID":[], "handled_score":[]})

        for data in [non_white_scored_cands_scores_df, unscored_by_bottom]:
            data['ind']=data['ID']

        NoWhitesAfterScored_df = self.__build_no_whites_afterscored_report(non_white_scored_cands_scores_df, unscored_by_bottom)

        customer = report.customer
        quest_id = report.quest_id
        chosen_merger = report.chosen_merger
        return {
            'NoWhitesLeaveUnscored': Report(customer, quest_id,
                                            self.__unhash_report(non_white_scored_cands_scores_df[self.REPORT_COLUMNS],
                                                                 present_kernel), chosen_merger),
            'NoWhitesAfterScored': Report(customer, quest_id, self.__unhash_report(NoWhitesAfterScored_df,
                                                                                   present_kernel), chosen_merger)
        }


    def __get_non_white_scored_candidates(self, present_kernel, selected_merger_scores_present, report_df):

        hashed_report = pd.merge(report_df, present_kernel, left_index=True,
                                 right_on="NORMAL_ID", how='left')[self.REPORT_COLUMNS]
        hashed_report_w_scores = pd.merge(hashed_report, selected_merger_scores_present, left_index=True,
                                          right_index=True, how='left')

        whites_in_universe_present = present_kernel[(present_kernel['raw_white'] == 1) & (present_kernel['raw_universe'] == 1)]['ID'].values
        non_white_scored_cands_scores = hashed_report_w_scores[~hashed_report_w_scores.index.isin(whites_in_universe_present)]
        non_white_scored_cands_scores.index = range(len(non_white_scored_cands_scores))
        return non_white_scored_cands_scores

    def __score_unscored_by_bottom(self, non_white_candidates_unscored_present, selected_merger_scores_present):
        minimal_score = min(selected_merger_scores_present['score'].values)

        bottom_scored = pd.DataFrame({"ID": non_white_candidates_unscored_present, "handled_score": minimal_score-1})

        return bottom_scored



    def __get_unscored_non_white_cands(self, kernel, scores):
        whites_in_universe_past = kernel[(kernel['raw_white'] == 1) & (kernel['raw_universe'] == 1)]['ID'].values
        non_whites_candidates_scored_past = list(set(scores.index.values) - set(whites_in_universe_past))
        non_white_candidates_unscored_past = list(set(kernel[kernel['candidate'] == 1]['ID'].values) -
                                                  set(non_whites_candidates_scored_past))
        return non_white_candidates_unscored_past



    def __build_no_whites_afterscored_report(self, non_white_scored_cands_scores_df, unscored_by_bottom):
        # unscored are by bottom score, hence we sort by score here and not by probability
        unscored_by_bottom['score'] = unscored_by_bottom['handled_score']

        NoWhitesAfterScored_df = pd.concat([non_white_scored_cands_scores_df,
                                            unscored_by_bottom]).sort(columns=['score', "Rank", 'ind'],
                                                                      ascending=[False, True, False])
        return NoWhitesAfterScored_df[self.REPORT_COLUMNS]



    def __unhash_report(self, scores_df, present_kernel):
        scores_df.columns = ["hashed_id", "Rank"]
        hashed_report = pd.merge(scores_df, present_kernel, left_on="hashed_id",
                                 right_on="ID", how='left')[["NORMAL_ID", "Rank"]]
        hashed_report.columns = [self.REPORT_COLUMNS]
        return hashed_report







