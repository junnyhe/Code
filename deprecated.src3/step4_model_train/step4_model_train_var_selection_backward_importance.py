import csv
import gzip
import sys
import copy
import numpy as np
import time
import pickle
#import matplotlib.pyplot as pl
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
sys.path.append("/home/junhe/fraud_model/Code/tools/model_tools")
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")

import csv_ops
from csv_ops import *
from load_data import *
from getAUC import *
#from ks_roc import *
from model_performance_evaluation import performance_eval_train_validation
from model_performance_evaluation import performance_eval_test





def train_evaluate_importance(classifier, X,y,Xv,yv):
    # Train Model
    print '\nModel training starts...'
    t0=time.time()
    model = classifier 
    model.fit(X, y)
    print "Model training done, taking ",time.time()-t0,"secs"
    
    # Predict Train
    y_pred = model.predict(X)
    p_pred = model.predict_proba(X)
    p_pred = p_pred[:,1]
    
    # Predict Validation
    yv_pred = model.predict(Xv)
    pv_pred = model.predict_proba(Xv)
    pv_pred = pv_pred[:,1]
    
    # Performance Evaluation: Train and Validation
    t0=time.time()
    #ks, auc, lorenz_curve_capt_rate = performance_eval_train_validation(y,p_pred,yv,pv_pred,output_dir,output_suffix)
    auc = getAUC(pv_pred,yv)
    var_import = zip(range(X.shape[1]),model.feature_importances_)
    
    return auc, var_import



def backward_selection_by_importance(classifier,train_data_file,test_data_file,var_list_filename,output_file_name,remove_rate = 0.01):
    #remove_rate = 0.01 # fraction of variables with lowest importance will be removed
    
    #===============================================================================
    # routine to perform backward variable selection by feature importance
    #===============================================================================
    
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    var_list=array(var_list)
    
    output_file=open(output_file_name,'w')
    output_csv=csv.writer(output_file)
    output_csv.writerow(['step','auc','var cnt','var list', 'var names'])
    
    #################### Load train and validation data ####################
    print 'Loading data for modeling starts ...'
    t0=time.time()
    target_name='target'
    X0,y = load_data_fast(train_data_file, var_list_filename, target_name)
    Xv0,yv = load_data_fast(test_data_file, var_list_filename, target_name)
    print "Loading data done, taking ",time.time()-t0,"secs"
    
    
    #################### Variable selection ####################
    curr_var_list = arange(X0.shape[1])
    step=0
    while True:
        # train model
        X=X0[:,curr_var_list]
        Xv=Xv0[:,curr_var_list]
        auc, var_import = train_evaluate_importance(classifier,X,y,Xv,yv)
        # output results
        print auc, len(curr_var_list), curr_var_list
        output_csv.writerow([step,auc,len(curr_var_list),list(curr_var_list),list(var_list[curr_var_list])])
        #
        # update var list for next step
        num_var_to_remove = max(int(len(curr_var_list)*remove_rate),1)
        var_import_sorted = sorted(var_import,key=itemgetter(1),reverse=True)
        new_var_index_list = [row[0] for row in var_import_sorted]
        new_var_index_list = new_var_index_list[:-1*num_var_to_remove] # remove the last few variables
        curr_var_list = curr_var_list[new_var_index_list]
        step += 1
        #
        # termination criteria
        if len(curr_var_list)<=30 :
            print "Final var list is:",curr_var_list,"\nauc is:",auc
            break
        
    
    output_file.close()
    
    

data_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/'
result_dir='/home/junhe/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_var_selection/'

train_data_file=data_dir+'model_data_pmt_ins_ds_rcind_fc_imp_woe.csv.gz'
test_data_file=data_dir+'test_data_sept_pmt_ds_rcind_fc_imp_woe.csv.gz'
var_list_filename = result_dir+'model_var_list_signal_rc_tmx_rc_ind.csv'

output_file_name=result_dir+'results_var_selection_backward_importance.csv'

classifier = RandomForestClassifier(max_depth=None, n_estimators=200, max_features="auto",random_state=0,n_jobs=-1)
#classifier = RandomForestClassifier(max_depth=16, n_estimators=100, max_features="auto",random_state=0,n_jobs=-1)


backward_selection_by_importance(classifier,train_data_file,test_data_file,var_list_filename,output_file_name,remove_rate = 0.01)




