import csv
import gzip
import sys
import numpy as np
import time
import pickle
from numpy import *
#import matplotlib.pyplot as pl
import random
from sklearn import tree
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

from sklearn.metrics import roc_curve, auc
sys.path.append("/home/junhe/fraud_model/Code/tools/model_tools")
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")

from load_data import *
import copy as cp
from operator import itemgetter


def predict_proba_rf_fast_one_row(model,X):
    # model is a "RandomForestClassifier"
    # only fast for one row, but not for many rows
    return mean([estimator.predict_proba(X) for estimator in model.estimators_],axis=0)

def load_var_list(var_list_filename):
    fin = csv.reader(open(var_list_filename,'rU'))
    var_list = []
    for row in fin:
        var_list.append(row[0])
    return var_list

class rf_reason_code:
    
    def __init__(self,model,var_list_filename,trivial_input_values_file):
        self.model_var_name_list = array(load_var_list(var_list_filename))
        self.trivial_input_values = pickle.load(open(trivial_input_values_file,'rb'))
        importances_index = zip(range(len(model.feature_importances_)),model.feature_importances_)
        importances_index = array(sorted(importances_index,key=itemgetter(1),reverse=True))
        self.reasoncode_candidate_var_index_list = importances_index[:50,0]
    
    def get_reasoncode(self,model,data):
        score0=predict_proba_rf_fast_one_row(model,data)[0,1]
        results = []
        for i in self.reasoncode_candidate_var_index_list:
            row_new = cp.deepcopy(data)
            row_new[i] = self.trivial_input_values[i]
            score_new=predict_proba_rf_fast_one_row(model,row_new)[0][1]
            results.append([i,score_new-score0])
        
        reasoncode_index = array(sorted(results,key=itemgetter(1)))
        reasoncode_index = reasoncode_index[:5,0]
        reasoncode_index = reasoncode_index.astype(int)
        reasoncode_list = self.model_var_name_list[reasoncode_index]
        return reasoncode_list
    

data_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/'
result_dir='/home/junhe/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_woeSmth=0/'

ins_file=data_dir+'model_data_pmt_ins_ds_rcind_fc_imp_woe.csv.gz'
var_list_filename=result_dir+'model_var_list_signal.csv'
target_name='target'  
X,y = load_data_fast(ins_file, var_list_filename, target_name)
   
model=DecisionTreeClassifier(max_depth=8)
model2=RandomForestClassifier(max_depth=8, n_estimators=200, max_features="auto",random_state=0,n_jobs=-1)

model.fit(X,y)
model2.fit(X,y)

data= X[676,:]

model.predict_proba(data)


t0=time.time()
model2.predict_proba(data)
print time.time() - t0

t0=time.time()
predict_proba_rf_fast_one_row(model2,data)
print time.time() - t0


'''
for i,row in enumerate(X): 
    #score =model2.predict_proba(row)
    score =predict_proba_rf_fast_one_row(model2,row)
    if score[0][1]>0.5:
        print i, score
'''

var_list_filename = var_list_filename
trivial_input_values_file = result_dir+'trivial_input_values.p'
#trivial_input_values = median(X,axis=0)
#pickle.dump(trivial_input_values,open(trivial_input_values_file,'wb'))
model = model2

rf_reason_code_obj = rf_reason_code(model,var_list_filename,trivial_input_values_file)
print rf_reason_code_obj.get_reasoncode(model,X[676,:])
print rf_reason_code_obj.get_reasoncode(model,X[437,:])





'''
427 [[ 0.16186983  0.83813017]]
428 [[ 0.1671281  0.8328719]]
429 [[ 0.23273272  0.76726728]]
437 [[ 0.30865211  0.69134789]]
463 [[ 0.32617523  0.67382477]]
676 [[ 0.44621662  0.55378338]]
715 [[ 0.3162664  0.6837336]]
730 [[ 0.37923969  0.62076031]]
797 [[ 0.18073426  0.81926574]]
815 [[ 0.22507052  0.77492948]]
850 [[ 0.3231456  0.6768544]]
852 [[ 0.12331153  0.87668847]]
855 [[ 0.20088165  0.79911835]]
857 [[ 0.34440863  0.65559137]]
859 [[ 0.17591082  0.82408918]]
'''