__author__ = 'Igor'

import numpy as np

from source.query.ml_phase.bag_worker.scorers.bag_scorer import BagScorer

"""
Generates X top evaluated Bags.
A. Bag generating:
    1. Recursively splits by Y features.
    2. Feature Binning is done for each intermediate bag, and not initially for all clusters
    3. A bag is the outcome of entire splitting process (on all features)
    4. X=100 constant
    5. Evaluating bags is done by summing number of "good" clusters
    6. "good" clusters: for which number of whites is greater than (expected value + 1.5 * std of value)

B. Scoring each bag - Naive Bayes
C. Aggregating bags - Boost: each bag gets samples weights (higher weight on whites we're mistaken about)

"""


class BoostScorer(BagScorer):
    def __init__(self, name, normalize, invert, unscored_handler, random_provider, context_holder, scorer_tags, params):
        super(BoostScorer, self).__init__(name, normalize, invert, unscored_handler, random_provider, context_holder, scorer_tags)

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


        from sklearn.naive_bayes import MultinomialNB
        Y=isWhite
        X=usersXclusters

        nUsers=usersXclusters.shape[0]
        nWhites=isWhite.sum()


        #Bag generating function by recursive splitting
        def splitByKeys(df,keys):
            if type(keys) is not list:
                keys=[keys]
            if not (keys):
                yield df
            else:
                key=keys[0]
                #Binning:
                if key[:4]=='disc': #disc.numBins.prop
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


        def evalBag(df,p=None):
            WN=df.WN.values
            N=df.N.values    
            if N.shape[0]<=10:
                return 1
            if p is None:
                p=np.sum(WN)*1./np.sum(N)
            if p==0:
                return 0
            diff2=(WN-p*N)**2
            VarOverN=diff2*1./N
            return np.mean(VarOverN)*1./p

        #mesures distance from best linear regression:
        def evalBag2(df):
            A = np.vstack([df['N'].values, np.ones(len(df['N']))]).T
            B= df['WN'].values.T
            x,r,_,_=np.linalg.lstsq(A, B)
            #print x,r
            if r.shape[0]==0: return 0
            return r[0]*1./len(df.index)

        #count the % of "good" clusters in the bag: clusters for which number of whites is greater than (expected value + 1.5 * std of value)
        def evalBag3(df):
            #p=np.sum(df['WN'])*1./np.sum(df['N'])
            p = np.mean(df['W_prcntg'])
            bag_weight = np.sum(df['WN'] > 10+df['N'] * p + 1.5 * np.sqrt(df['N']*p*(1-p)))
            return bag_weight * 1./len(df.index)



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
        #use 'disc.numBins.prop' to create a new column in the df which is a binned version of prop, and split by it


        maxBags= np.unique(bag.props_df['SUB_CLUSTERS_FILE'].values).shape[0] * 20 * 10 *10
        self.logger.debug("starting evaluating bags")
        ii=0
        for df in splitByKeys(bag.props_df,['SUB_CLUSTERS_FILE','disc.20.deg','disc.10.thrs_1','disc.10.thrs_0']):
            #heuristically evaluate the bag for its potential contribution
            ii+=1
            score=evalBag3(df)
            Q.put((score,df))
            if ii%200==0:
                self.logger.debug("finished evaluating %d bags out of up to %d" % (ii,maxBags))



        ########################################
        #### RankBoost: My implementation   ####
        ########################################
        #Based on http://jmlr.csail.mit.edu/papers/volume4/freund03a/freund03a.pdf

        score=np.zeros(usersXclusters.shape[0])
        nUsers=usersXclusters.shape[0]

        Y=isWhite*1
        isGrey=(isWhite==False)

        v = np.empty(usersXclusters.shape[0], dtype=np.float)

        #Set initial weights for all samples:
        v[isWhite] = 1. / isWhite.sum()
        v[isGrey] = 1. / isGrey.sum()

        for j,(_,df) in enumerate(Q.arr):

            estimator=MultinomialNB(alpha=np.min(v),fit_prior=False)
            X_temp=usersXclusters[:,df.index.values]

            #Train using the sample weights:
            estimator.fit(X_temp,Y,sample_weight=v)
            h=estimator.predict_proba(X_temp)[:,1]

            #Update sample weights, calculate the multiplier of these scores:
            v1h1 = np.average(h[isWhite]*1., weights=v[isWhite], axis=0)
            v0h0 = np.average(h[isGrey]*1., weights=v[isGrey], axis=0)
            r=v1h1-v0h0
            weight=0.5*np.log((1+r)/(1-r))
            v[isWhite]*=np.exp(-weight*h[isWhite])
            v[isGrey]*=np.exp(weight*h[isGrey])
            v[isWhite]/=v[isWhite].sum()
            v[isGrey]/=v[isGrey].sum()

            score+=weight*h

            self.logger.info("finished scoring by %d / 100 bags" % j)

            #guess=pd.DataFrame({'score':score},index=pop)
            #print j,testRawGuess(guess)  

        pop_scores = score
        score_dict = dict(zip(bag.universe_in_bag[range(pop_scores.shape[0])], pop_scores))
        scores_arr = np.array([score_dict.keys(), score_dict.values()]).transpose()
        scores_arr = self.remove_non_universe_from_scores(bag, scores_arr) #Removes whites as well, I think
        return scores_arr