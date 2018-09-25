__author__ = 'Shahars'


import pandas as pd

import numpy as np
from sklearn.ensemble import BaggingClassifier
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer


class ClustersMLScorer(BagScorer):

    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_holder, scorer_tags, params):

        super(ClustersMLScorer, self).__init__(
            name, normalize, invert, unscored_handler, random_provider, context_holder, scorer_tags)
        if not type(params) is dict:
            params = params.to_builtin()
        self.TH1 = (abs(params["ML_TH1"])) % 1

        self.TH2 = params["ML_TH2"]
        thresholds_part = "_".join([str(params["ML_TH2"]), str(params["ML_TH1"])])
        # self._name = "_".join([self.name, thresholds_part])

        self.TH3 = np.floor(abs(params["ML_TH1"]))
        self.ShouldWeANDConditions = params["ML_MISC"] >= 0
        self.n_estimators = params["n_estimators"]
        self.base_estimator = params["base_estimator"]

        self.categFeatures = params["categorical_features"]
        self.numFeatures = params["numerical_features"]

        self.cluster_scores_df = pd.DataFrame()

    def _get_high_prcntg_clusters(self, bag):
        self.logger.info("starts building higher than percentage clusters")
        high_prcntg_clusters_idx = bag.props_df[bag.props_df["W_prcntg"] > self.TH1].index.values
        self.logger.info("finished building higher than percentage clusters")

        return high_prcntg_clusters_idx

    def _get_larger_than_median_vals(self, bag):
        self.logger.info("start building larger than median clusters")
        cat_num_props_df = bag.props_df[self.numFeatures+self.categFeatures]
        median_vec = cat_num_props_df.median(axis=0)
        larger_than_med_df = cat_num_props_df > median_vec
        larger_than_med_count = larger_than_med_df[larger_than_med_df == True].count(axis=1)

        pos_idx_median = larger_than_med_count[larger_than_med_count >= self.TH3].index.values
        self.logger.info("finished building larger than median clusters")

        return pos_idx_median

    def _build_all_w_clusters(self, bag):
        self.logger.info("start building positive clusters")
        high_prcntg_clusters_idx = self._get_high_prcntg_clusters(bag)
        clusters_median_idx = self._get_larger_than_median_vals(bag)

        if self.ShouldWeANDConditions:
            clusters_idx_combined = np.intersect1d(clusters_median_idx, high_prcntg_clusters_idx)
        else:
            clusters_idx_combined = np.unique(np.concatenate([clusters_median_idx, high_prcntg_clusters_idx]))
            self.logger.debug("using OR between conditions, combining %d Prob clusters and %d std clusters into %d unique "
                              "ones" % (len(high_prcntg_clusters_idx), len(clusters_median_idx), len(clusters_idx_combined)))
        w_clusters_idx = clusters_idx_combined
        self.w_clusters_idx = w_clusters_idx
        self.logger.info("finished building positive clusters")


    def _build_trimmed_g_clusters(self, bag):

        self.logger.info("start building negative clusters")
        g_clusters_idx = np.setdiff1d(bag.props_df.index.values, self.w_clusters_idx)
        # aspire equal size of gray and white clusters
        min_cluster_size = min(len(g_clusters_idx), len(self.w_clusters_idx))
        np.random.seed(self._random_provider.get_randomizer_seed(self.RANDOM_ISOLATION_KEY))
        min_g_clusters_idx = np.random.permutation(g_clusters_idx)[:min_cluster_size]
        self.g_clusters_idx = min_g_clusters_idx
        self.logger.info("finished building negative clusters")

    def _build_labeled_data(self, bag):
        self.logger.info("start building labeled data")

        self._build_all_w_clusters(bag)
        self._build_trimmed_g_clusters(bag)
        data = bag.props_df.ix[np.concatenate([self.w_clusters_idx, self.g_clusters_idx])]
        self.logger.debug("number of total clusters is %d" % len(bag.props_df.index))
        self.logger.debug("number of positive clusters is %d" % len(self.w_clusters_idx))
        self.logger.debug("number of negative clusters is %d" % len(self.g_clusters_idx))

        data["index"] = data.index

        # data["label"] = data["index"].apply(lambda ind: 1 if ind in self.w_clusters_idx else 0)
        data["label"] = np.in1d(data["index"], self.w_clusters_idx).astype(int)
        data = data.drop(["index"], axis=1)
        self.labeled_data = data
        self.logger.info("finished building labeled data")


    def _build_train_x_y(self):
        self.logger.info("start building training data")
        train_y = self.labeled_data["label"].values
        self.train_y = train_y
        feat_data = self.labeled_data[self.categFeatures+self.numFeatures]
        train_x = feat_data.values
        self.train_x = train_x
        self.logger.info("finished building training data")

    def _get_train_x_y(self):
        return self.train_x, self.train_y

    def _build_test_x(self, bag):
        feat_data = bag.props_df[self.categFeatures+self.numFeatures]
        test_x = feat_data.values
        self.test_x = test_x

    def _get_test_x(self):
        return self.test_x

    def _calc_clusters_scores(self, bag):
        self.logger.info("start calculating clusters' scores")
        self._build_labeled_data(bag)
        self._build_train_x_y()
        self._build_test_x(bag)
        seed = self._random_provider.get_randomizer_seed(self.RANDOM_ISOLATION_KEY)
        bagging = BaggingClassifier(self.base_estimator, n_estimators=self.n_estimators, random_state=seed)
            # GradientBoostingClassifier(learning_rate=0.5, n_estimators=self.n_estimators-40, max_depth=2)
        # bagging = BaggingClassifier(self.base_estimator, n_estimators=self.n_estimators   )
        if (0 in self.train_x.shape) or (0 in self.train_y.shape):
            error_message = "no training clusters! ML not applied, clusters scores list is empty"
            self.logger.warning(error_message)
            self.cluster_scores_df = pd.DataFrame({"score": []})
        else:
            self.logger.debug("started training ML scorer ensemble")
            bagging.fit(self.train_x, self.train_y)
            self.logger.debug("finished training ML scorer ensemble")
            res = bagging.predict_proba(self.test_x)
            self.logger.debug("finished predicting all clusters")
            preds = res[:, 1]
            #IMPORTANT: assumes we give scores to ALL the clusters
            cluster_scores_df = pd.DataFrame({"score": preds}, index=range(len(preds)))
            high_cluster_scores = cluster_scores_df[cluster_scores_df["score"] > self.TH2]
            self.cluster_scores_df = high_cluster_scores
    def score_bag(self, bag):
        raise NotImplementedError()



