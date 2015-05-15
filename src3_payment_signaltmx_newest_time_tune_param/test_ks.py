import csv
import gzip
import sys
import numpy as np
import time
import pickle
from numpy import *
import matplotlib.pyplot as pl
import random
from matplotlib.colors import ListedColormap
from operator import itemgetter
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_moons, make_circles, make_classification
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.lda import LDA
from sklearn.qda import QDA

from sklearn.metrics import roc_curve, auc
sys.path.append("/Users/junhe/Box Sync/Documents/workspace/fraud_model/src/model_tools")
sys.path.append("/Users/junhe/Box Sync/Documents/workspace/fraud_model/src/csv_operations")

import csv_ops
from csv_ops import *
from getAUC import *
from ks_roc import *

from ks import *



#################### load data ####################
data_dir='/Users/junhe/Box Sync/Documents/Data/Model_Data/'
model_dir='/Users/junhe/Box Sync/Documents/Model_Results/'


    
pt_pred=pickle.load(open(model_dir+"pv_pred.p",'rb'))
yt=pickle.load(open(model_dir+"yv.p",'rb'))


# Evaluate KS
output_suffix='test_ks'


ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve= ks(yt,pt_pred)

out_dt = open(model_dir + 'KS_validation_' + str(output_suffix)+'.csv', 'wb')
out_csv = csv.writer(out_dt)

out_csv.writerow(["KS_"+str(output_suffix)])
out_csv.writerow([ks])
out_csv.writerow(["KS_pos_for_"+str(output_suffix)])
out_csv.writerow([ks_pos])
# cum prob for target and none target, used for plotting Lorenz curve
out_csv.writerow(["Lorenz_Curve_"+str(output_suffix)])
for row in lorenz_curve:
    out_csv.writerow(row)
    
out_dt.close()



# Evaluate KS
output_suffix='test_ks2'


ks2, ks_pos2, pctl2, tpr2, fpr2, tp_cumcnt2, fp_cumcnt2, threshold2, lorenz_curve, lorenz_curve_capt_rate = ks_roc(yt,pt_pred)

out_dt = open(model_dir + 'KS_validation_' + str(output_suffix)+'.csv', 'wb')
out_csv = csv.writer(out_dt)

out_csv.writerow(["KS_"+str(output_suffix)])
out_csv.writerow([ks2])
out_csv.writerow(["KS_pos_for_"+str(output_suffix)])
out_csv.writerow([ks_pos2])
# cum prob for target and none target, used for plotting Lorenz curve
out_csv.writerow(["Lorenz_Curve_"+str(output_suffix)])
for row in lorenz_curve:
    out_csv.writerow(row)
    
out_dt.close()



# Plot Lorenz
pl.figure(1, figsize=(8,8))
pl.clf()
pl.plot(pctl, tpr, 'x',label='tgt ks' )
pl.plot(pctl, fpr, '*',label='non-tgt ks')
pl.plot(pctl2, tpr2,label='tgt ks2' )
pl.plot(pctl2, fpr2, '-',label='non-tgt ks2')
pl.plot([0, 1], [0, 1], 'k--')
pl.xlim([0.0, 1.0])
pl.ylim([0.0, 1.0])
pl.xlabel('False Positive Rate')
pl.ylabel('True Positive Rate')
pl.title('Compare ks and ks2')
pl.legend(loc="lower right")
pl.show()


# Plot ROC (compare with sci-kit roc)
fpr_sk, tpr_sk, thresholds_sk = roc_curve(yt,pt_pred)

pl.figure(2, figsize=(8,8))
pl.clf()
pl.plot(fpr, tpr, 'bx',label='ROC from ks')
pl.plot(fpr2, tpr2, 'y-',label='ROC from ks2')
pl.plot(fpr_sk, tpr_sk, 'r--',label='ROC from SciKit')
pl.plot([0, 1], [0, 1], 'k--')
pl.xlim([0.0, 1.0])
pl.ylim([0.0, 1.0])
pl.xlabel('False Positive Rate')
pl.ylabel('True Positive Rate')
pl.title('ROC Curve Comp')
pl.legend(loc="lower right")
pl.show()

