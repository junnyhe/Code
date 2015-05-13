from numpy import *
def getAUC(py,y):
    
    score_label=array([py,y]).transpose()
    #print score_label.shape
    score_label=score_label[score_label[:,0].argsort()[::-1],:]# sort by score
    
    n1=sum(y)
    n0=len(y)-n1
    
    n0left=n0

    correctpair=0
    for i in xrange(len(score_label)):
        if score_label[i,1]>0.5:       
            correctpair+=n0left
        else:
            n0left-=1

    auc= correctpair*1.0/(n0*n1)

    return auc
