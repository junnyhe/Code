
#################### define neural net (ffnet) classifier class ####################
from numpy import *
from ffnet import ffnet, mlgraph, savenet, loadnet, exportnet
import itertools

class ffnetClassifier:
    
    def __init__(self,nNodes,maxfun=''):
        self.nNodes=nNodes
        self.maxfun=maxfun
        self.net=''

    def fit(self,X,y):
        dim=X.shape
        nFeatures=int(dim[1])
        
        #input = [ [0.,0.], [0.,1.], [1.,0.], [1.,1.] ]
        #target  = [ [1.], [0.], [0.], [1.] ]
        input = list(X)
        target = list(y)
        
        conec = mlgraph( (nFeatures,self.nNodes,1) )
        self.net = ffnet(conec)
        if self.maxfun=='':
            self.net.train_tnc(input, target)
        else:
            self.net.train_tnc(input, target, maxfun = self.maxfun)
        #self.net.test(input, target, iprint = 2)
        
    '''
    # don't need, just dump and load ffnetClassifier object, same as scikit-learn object
    def dump(self,netfile):
        savenet(self.net, netfile ) # "xor.net"
        #exportnet(self.net, "xor.f")
        
    def load(self,netfile):
        self.net = loadnet(netfile) # xor.net
    '''   
    def predict_proba(self,X):
        #predict train
        p_pred = self.net( list(X) ) #output score
        p_pred=array(list(itertools.chain.from_iterable(p_pred)))
        return array(zip(1-p_pred,p_pred))
    
    def predict(self,X):
        p_pred = self.predict_proba(X) #output score
        y_pred=p_pred*0
        y_pred[where(p_pred>0.5)]=1 #convert to 0,1 target
        return y_pred
       