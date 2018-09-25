__author__ = 'Shahars'


import numpy as np
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer


class YanivLearnerNoML(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(YanivLearnerNoML, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)
        if not type(params) is dict:
            params = params.to_builtin()

        self.TH1 = (abs(params["ML_TH1"])) % 1
        self.TH2 = params["ML_TH2"]
        self.TH3 = np.floor(abs(params["ML_TH1"]))
        self.ShouldWeANDConditions = params["ML_MISC"] >= 0
        self.categFeatures = params["categorical_features"]
        self.numFeatures = params["numerical_features"]

    def _get_high_prcntg_clusters(self, bag):
        high_prcntg_clusters_idx = bag.props_df[bag.props_df["W_prcntg"] > self.TH1].index.values
        return high_prcntg_clusters_idx

    def _get_larger_than_median_vals(self, bag):

        cat_num_props_df = bag.props_df[self.numFeatures+self.categFeatures]
        median_vec = cat_num_props_df.median(axis=0)
        larger_than_med_df = cat_num_props_df > median_vec
        larger_than_med_count = larger_than_med_df[larger_than_med_df == True].count(axis=1)

        pos_idx_median = larger_than_med_count[larger_than_med_count >= self.TH3].index.values
        return pos_idx_median

    def _build_all_w_clusters(self, bag):
        high_prcntg_clusters_idx = self._get_high_prcntg_clusters(bag)
        clusters_median_idx = self._get_larger_than_median_vals(bag)

        if self.ShouldWeANDConditions:
            clusters_idx_combined = np.intersect1d(clusters_median_idx, high_prcntg_clusters_idx)
        else:
            clusters_idx_combined = np.unique(np.concatenate([clusters_median_idx, high_prcntg_clusters_idx]))
            self.logger.info("using OR between conditions, combining %d Prob clusters and %d std clusters into %d unique "
                             "ones" % (len(high_prcntg_clusters_idx), len(clusters_median_idx), len(clusters_idx_combined)))

        w_clusters_idx = clusters_idx_combined
        self.w_clusters_idx = w_clusters_idx

    def score_bag(self, bag):
        self.logger.info("selecting only positive clusters")
        self._build_all_w_clusters(bag)
        self.logger.info("start scoring population")
        scores_dict = {p_id:  len((set(p_clusters).intersection(self.w_clusters_idx))) for p_id, p_clusters in
                       bag.pop_to_clusters_dict.iteritems()}

        scores_arr = np.array([scores_dict.keys(), scores_dict.values()]).transpose()
        return scores_arr
