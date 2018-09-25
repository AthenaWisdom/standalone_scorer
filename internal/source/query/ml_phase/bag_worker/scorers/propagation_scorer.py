__author__ = 'Igor'

import numpy as np

import scipy as sp
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer


class PropagationScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(PropagationScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)
    
    def score_bag(self, bag):

        self.logger.info("start calculating scores by method: %s" % self.name)
        usersXclusters = bag.pop_to_clusters_map.T
        rawWhites=bag.whites
        pop=bag.universe_in_bag
        rawWhitesClean=rawWhites[np.in1d(rawWhites,pop)]
        whites=np.searchsorted(pop,rawWhitesClean)
        fullUniverse=np.arange(usersXclusters.shape[0])
        isWhite=np.in1d(fullUniverse,whites)

        clusterSize=np.array(usersXclusters.sum(axis=0)).reshape(-1)
        # User degree: Sum over adjacency matrix edges
        # (each edge is the number of clusters by which the 2 people are connected)
        userDeg=usersXclusters.dot(clusterSize-1)*1.

        invSqrtUserDeg=userDeg**(-0.5)
        D=sp.sparse.diags(invSqrtUserDeg,0)
        DX=D.dot(usersXclusters)
        Y=isWhite*1.
        
        V=Y.copy()
        alpha=0.1
        for i in range(10):
            V=alpha*DX.dot(DX.T.dot(V)) +(1-alpha)*Y

        pop_scores=V        
        score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
        scores_arr = self.remove_non_universe_from_scores(bag, scores_arr)
        return scores_arr
