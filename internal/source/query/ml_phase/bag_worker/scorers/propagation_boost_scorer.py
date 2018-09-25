__author__ = 'Igor'

import numpy as np

import scipy as sp
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer


# noinspection PyTypeChecker
class PropagationBoostScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(PropagationBoostScorer, self).__init__(name, normalize, invert,  unscored_handler, random_provider, context_provider, scorer_tags)
    
    def score_bag(self, bag):

        self.logger.info("start calculating scores by method: %s" % self.name)
        usersXclusters = bag.pop_to_clusters_map.T
        rawWhites=bag.whites
        pop=bag.universe_in_bag
        rawWhitesClean=rawWhites[np.in1d(rawWhites,pop)]

        whites=np.searchsorted(pop,rawWhitesClean)
        fullUniverse=np.arange(usersXclusters.shape[0])
        rawUniverse=bag.universe
        isWhite=np.in1d(fullUniverse,whites)

        def splitByKeys(df,keys):
            if type(keys) is not list:
                keys=[keys]

            if not (keys):
                yield df
            else:
                key=keys[0]
                if key[:4]=='disc':
                    _,parts,orgKey= key.split('.')[:3]
                    sortedVals=np.unique(df[orgKey].values)
                    bins= sortedVals[np.linspace(0,sortedVals.shape[0]-1,int(parts)+1).astype(int)]
                    bins[-1]+=100
                    df[key]=np.digitize(df[orgKey].values,bins)-1
                vals=np.unique(df[key].values)                
                for val in vals:                
                    for j in splitByKeys(df[df[key]==val].copy(),keys[1:]):
                        yield j



        
        isGrey=(isWhite==False)
        def evalBag8(df):  #ratio
            X=usersXclusters[:,df.index.values]
            userScore=np.array(X.sum(axis=1)).reshape(-1)
            meanWhites=userScore[isWhite].mean()
            meanGreys=userScore[isGrey].mean()
            stdWhites=userScore[isWhite].std()
            stdGreys=userScore[isGrey].std()
            ratio=(meanWhites-meanGreys) *1./np.sqrt(stdWhites**2+stdGreys**2)
            return ratio

        class FiniteQueue(object):
            def __init__(self,maxsize=100):
                self.arr=[]
                self.maxsize=maxsize       
            def put(self,item):
                self.arr+=[item]
                self.arr=sorted(self.arr,key=lambda x:-x[0])
                if len(self.arr)>self.maxsize:
                    self.arr=self.arr[:self.maxsize]
            
        Q=FiniteQueue(maxsize=100)
        pWhite=isWhite.sum()*1./isWhite.shape[0]
        ii=0
        for df in splitByKeys(bag.props_df,['SUB_CLUSTERS_FILE','disc.10.deg','disc.10.thrs_0','disc.10.thrs_1']):
            ii+=1

            score=evalBag8(df)
            Q.put((score,df))
            



        ########################################
        #### RankBoost: My implementation   ####
        ########################################
        score=np.zeros(usersXclusters.shape[0])

        Y=isWhite*1

        v = np.empty(usersXclusters.shape[0], dtype=np.float)
        v[isWhite] = 1. / isWhite.sum()
        v[isGrey] = 1. / isGrey.sum()


        for j,(_,df) in enumerate(Q.arr):

            X=usersXclusters[:,df.index.values]
            clusterSize=np.array(X.sum(axis=0)).reshape(-1)
            w2=1./clusterSize
            w=np.sqrt(w2)
            W=sp.sparse.diags(w,0)
            userDeg=X.dot(clusterSize*w2)*1.
            d=userDeg**(-0.5)
            D=sp.sparse.diags(d,0)
            DXW=D.dot(X).dot(W)
            Y=isWhite*1.
            
            isValid=np.array(X.sum(axis=1)>0).reshape(-1)
            Ymean=Y.mean()
            Y-=Ymean
            #Y-=Y.mean()
            
            
            V=Y.copy()
            alpha=0.1
            for i in range(10):
                V=alpha*DXW.dot(DXW.T.dot(V)) +(1-alpha)*Y
            V=DXW.dot(DXW.T.dot(V))
            h=V+Ymean
   
    
            v1h1 = np.average(h[isWhite]*1., weights=v[isWhite], axis=0)
            v0h0 = np.average(h[isGrey]*1., weights=v[isGrey], axis=0)
            r=v1h1-v0h0
            weight=0.5*np.log((1+r)/(1-r))
            v[isWhite]*=np.exp(-weight*h[isWhite])
            v[isGrey]*=np.exp(weight*h[isGrey])
            v[isWhite]/=v[isWhite].sum()
            v[isGrey]/=v[isGrey].sum()
            score+=weight*h


            
        pop_scores=score
        score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
        scores_arr = self.remove_non_universe_from_scores(bag, scores_arr)
        return scores_arr
