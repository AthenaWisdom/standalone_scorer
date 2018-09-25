import numpy as np

import scipy as sp
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer


class WeightedPropagationScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(WeightedPropagationScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)
        
    def score_bag(self, bag):

        self.logger.info("start calculating scores by method: %s" % self.name)
        props_df=bag.props_df
        usersXclusters = bag.pop_to_clusters_map.T
        rawWhites=bag.whites
        pop=bag.universe_in_bag
        rawWhitesClean=rawWhites[np.in1d(rawWhites,pop)]
        whites=np.searchsorted(pop,rawWhitesClean)
        fullUniverse=np.arange(usersXclusters.shape[0])
        isWhite=np.in1d(fullUniverse,whites)

        X=usersXclusters
        clusterSize=np.array(X.sum(axis=0)).reshape(-1)
        w2=1./props_df['N'].values
        w=np.sqrt(w2)
        W=sp.sparse.diags(w,0)
        
        userDeg=X.dot(clusterSize*w2)*1.
        d=userDeg**(-0.5)
        D=sp.sparse.diags(d,0)
        DXW=D.dot(X).dot(W)
        Y=isWhite*1.
        
        V=Y.copy()
        alpha=0.1
        for i in range(10):
            V=alpha*DXW.dot(DXW.T.dot(V)) +(1-alpha)*Y
        pop_scores=V        
        score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
        scores_arr = self.remove_non_universe_from_scores(bag, scores_arr)
        return scores_arr