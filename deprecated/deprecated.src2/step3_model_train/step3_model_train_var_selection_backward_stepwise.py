import csv
import gzip
import sys
import copy
import numpy as np
import time
import pickle
#import matplotlib.pyplot as pl
import random
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
sys.path.append("/fraud_model/Code/tools/model_tools")
sys.path.append("/fraud_model/Code/tools/csv_operations")

import csv_ops
from csv_ops import *
from load_data import *
from getAUC import *
#from ks_roc import *
from model_performance_evaluation import performance_eval_train_validation
from model_performance_evaluation import performance_eval_test





def train_evaluate_auc(classifier, X,y,Xv,yv):
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
    #print "ks evaluation, taking ",time.time()-t0,"secs"
    
    return auc



def backward_selection(classifier,train_data_file,test_data_file,full_var_list_filename,output_file_name,output_log_file_name):    
    
    #===============================================================================
    # routine to perform backward stepwise variable selection
    #===============================================================================
    
    varlist_file=open(full_var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    full_var_list=[]
    for row in varlist_csv:
        full_var_list.append(row[0])
    full_var_list=array(full_var_list)
           
    
    output_file=open(output_file_name,'w')
    output_csv=csv.writer(output_file)
    output_csv.writerow(['step','auc','n_var','var_index','var_names'])#header
    
    output_log_file=open(output_log_file_name,'w')
    output_log_csv=csv.writer(output_log_file)
    output_log_csv.writerow(['step','var to remove','var list after removal','auc','auc delta'])
    
    #################### Load train and validation data ####################
    print 'Loading data for modeling starts ...'
    t0=time.time()
    target_name='target'
    X0,y = load_data_fast(train_data_file, full_var_list_filename, target_name)
    Xv0,yv = load_data_fast(test_data_file, full_var_list_filename, target_name)
    print "Loading data done, taking ",time.time()-t0,"secs"

    
    #################### Variable selection ####################
    curr_var_list = range(X0.shape[1])
    
    auc0 = train_evaluate_auc(classifier,X0,y,Xv0,yv)
    step = 0
    print "Intial auc is:",auc0
    output_csv.writerow([step,auc0,len(curr_var_list),curr_var_list,list(full_var_list[curr_var_list])])
    
    while len(curr_var_list) >= 20:
        metric_list = []
        for var in curr_var_list:
            temp_var_list = list(copy(curr_var_list))
            temp_var_list.remove(var)
            X=X0[:,temp_var_list]
            Xv=Xv0[:,temp_var_list]
            auc = train_evaluate_auc(classifier,X,y,Xv,yv)
            print "\n",var,"is removed from var list"
            print auc, auc-auc0
            output_log_csv.writerow([step,var,temp_var_list,auc,auc-auc0])
            metric_list.append(auc)
        
        # find auc max in this round, and it's index in the candidate var list
        step+=1
        auc_max = max([metric for metric in metric_list if not isnan(metric)])
        var_index_to_remove = metric_list.index(auc_max)
        var_to_remove = curr_var_list[var_index_to_remove]
        
        # earlier termination if improvement is not significant enough
        if  auc_max-auc0<-0.005:
            print "No more significant improvement, variable selection stopped" 
            break
        
        # update auc previous and var list for next round selection
        auc0 = auc_max
        curr_var_list.remove(var_to_remove)
        output_csv.writerow([step,auc0,len(curr_var_list),curr_var_list,list(full_var_list[curr_var_list])]) # output var selection results
        
        print "\nvar to remove:",var_index_to_remove,var_to_remove ,"removed auc",auc0
        print "curr_var_list is",curr_var_list
    
    output_file.close()
    output_log_file.close()
    




data_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/'
result_dir='/fraud_model/Results/Model_Results_Signal_Tmx_v2pmt_var_selection/'

classifier = RandomForestClassifier(max_depth=16, n_estimators=100, max_features="auto",random_state=0,n_jobs=-1)
train_data_file=data_dir+'model_data_pmt_oos_ds_fc_imp_woe_add_rc_ind.csv.gz'
test_data_file=data_dir+'test_data_sept_pmt_ds_fc_imp_woe_add_rc_ind.csv.gz'
#full_var_list_filename = result_dir+'model_var_list_signal_rc_tmx_rc_ind_400.csv'
full_var_list_filename = result_dir+'model_var_list_signal_rc_tmx_rc_ind_40.csv'
output_file_name=result_dir+'results_var_selection_backward_stepwise_test.csv'
output_log_file_name=result_dir+'results_var_selection_backward_stepwise_log_test.csv'

backward_selection(classifier,train_data_file,test_data_file,full_var_list_filename,output_file_name,output_log_file_name)
