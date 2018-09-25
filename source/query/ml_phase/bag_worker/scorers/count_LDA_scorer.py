__author__ = 'Igor'

import numpy as np
import scipy as sp
from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer

"""
Complexity:
heaviest part evaluation of bags (applying count scorer thousands of times)
All binning are upper bounded by 10, so number of bags is mostly dependent on number of FILEs (10*10*10*NUM_OF_FILES)
"""

def LDA(allScores,Y):
    
    isWhite=(Y==1)
    isGrey=(isWhite==False)
    

    whiteScores=allScores[isWhite,:]
    greyScores=allScores[isGrey,:]
    
    whiteMean=np.array(whiteScores.mean(axis=0)).reshape(-1)#
    greyMean=np.array(greyScores.mean(axis=0)).reshape(-1)

    
    EXY_whites=whiteScores.T.dot(whiteScores) *1./whiteScores.shape[0]
    EXY_greys=greyScores.T.dot(greyScores)*1./greyScores.shape[0]
    
    whiteCov=EXY_whites-whiteMean.reshape(-1,1)*whiteMean.reshape(1,-1)
    greyCov=EXY_greys-greyMean.reshape(-1,1)*greyMean.reshape(1,-1)

    mu=whiteMean-greyMean
    sigma=np.array(whiteCov+greyCov)

    #Optimal w:sp.linalg.pinv(sigma).dot(mu)
    finalScore=allScores.dot(sp.linalg.pinv(sigma).dot(mu))
    finalScore=finalScore-np.mean(finalScore)
    return finalScore


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
        # noinspection PyTypeChecker
        for val in vals:
            for j in splitByKeys(df[df[key]==val].copy(),keys[1:]):
                yield j
                

class FiniteQueue(object):
    def __init__(self,maxsize=100):
        self.arr=[]
        self.maxsize=maxsize       
    def put(self,item):
        self.arr+=[item]
        self.arr=sorted(self.arr,key=lambda x:-x[0])
        if len(self.arr)>self.maxsize:
            self.arr=self.arr[:self.maxsize]
            
            
class CountLDAScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags, params):
        super(CountLDAScorer, self).__init__(
            name, normalize, invert, unscored_handler, random_provider, context_provider, scorer_tags)
    
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

        maxBags= np.unique(bag.props_df['SUB_CLUSTERS_FILE'].values).shape[0] * 10 * 10 *10
        self.logger.debug("starting evaluating bags")


        Q=FiniteQueue(maxsize=100)
        ii=0
        for df in splitByKeys(bag.props_df,['SUB_CLUSTERS_FILE','disc.10.deg','disc.10.thrs_0','disc.10.thrs_1']):
            ii+=1
            score=evalBag8(df)
            Q.put((score,df))
            if ii%200==0:
                self.logger.debug("finished evaluating %d bags out of up to %d" % (ii,maxBags))

            

        Y=isWhite*1
        allScores=np.array([]).reshape([Y.shape[0],0])
        

        for j,(_,df) in enumerate(Q.arr):
            X=usersXclusters[:,df.index.values]
            h=np.array(X.sum(axis=1)).reshape(-1) #this is count score based on bag
            allScores=np.hstack([allScores,h.reshape(-1,1)])

        score=LDA(allScores,Y) #This is the aggregator
        pop_scores=score
        score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
        scores_arr = self.remove_non_universe_from_scores(bag, scores_arr) #Removes whites as well, I think
        return scores_arr