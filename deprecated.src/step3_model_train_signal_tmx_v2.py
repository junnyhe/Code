import csv
import gzip
import sys
import numpy as np
import time
import pickle
from numpy import *
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
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/model_tools")
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")

import csv_ops
from csv_ops import *
from load_data import *
#from getAUC import *
#from ks_roc import *
from model_performance_evaluation import performance_eval_train_validation
from model_performance_evaluation import performance_eval_test


def model_train_validation(ins_file, oos_file, classifier, var_list_filename, result_dir, output_suffix):
    """
    train model
    evaluate on the train and validation data
    evaluate the model performance on the train and validation data
    """
    #################### Load train and validation data ####################
    print 'Loading data for modeling starts ...'
    t0=time.time()
    target_name='target'
    X,y = load_data(ins_file, var_list_filename, target_name)
    Xv,yv = load_data(oos_file, var_list_filename, target_name)
    print "Loading data done, taking ",time.time()-t0,"secs"
    
    # Train Model
    print '\nModel training starts...'
    t0=time.time()
    model = classifier
    model.fit(X, y)
    print "Model training done, taking ",time.time()-t0,"secs"
    pickle.dump(model,open(result_dir+"model.p",'wb')) # save model to disk
    
    # Predict Train
    y_pred = model.predict(X)
    p_pred = model.predict_proba(X)
    p_pred = p_pred[:,1]
    
    # Predict Validation
    yv_pred = model.predict(Xv)
    pv_pred = model.predict_proba(Xv)
    pv_pred = pv_pred[:,1]
    
    # Performance Evaluation: Train and Validation
    performance_eval_train_validation(y,p_pred,yv,pv_pred,result_dir,output_suffix)
    
    
    #################### Random Forest Feature Importance ######################
    try:
        varlist_file=open(var_list_filename,'rU')
        varlist_csv=csv.reader(varlist_file)
        var_list=[]
        for row in varlist_csv:
            var_list.append(row[0])
        out_feat_import = open(result_dir + 'feature_import_' + str(output_suffix)+'.csv', 'wb')
        feat_import_csv = csv.writer(out_feat_import)
        var_import = zip(range(len(var_list)),var_list,model.feature_importances_)
        feat_import_csv.writerow(['var seq num','var name','importance'])
        print "RandomForest classifier, var importance was output"
        for row in var_import:
            feat_import_csv.writerow(row)
    except:
        print "Not RandomForest classifier, var importance not created"
    

    

def model_test_data_evaluation(test_data_file, var_list_filename, model_file, result_dir, output_suffix):
    
    #################### Load Model and Evaluate Performance ##################
    ############################### Test Data #################################
    
    # Load Test Data
    print 'Loading test data starts ...'
    t0=time.time()
    target_name='target'
    key_name='payment_request_id'
    X,y,key = load_data_with_key(test_data_file, var_list_filename, target_name, key_name)
    print "Loading test data done, taking ",time.time()-t0,"secs"
    
    # Load Model
    print 'Loading model ...'
    t0=time.time()
    model = pickle.load(open(model_file,'rb'))
    
    # Predict Test Data
    y_pred = model.predict(X)
    p_pred = model.predict_proba(X)
    p_pred = p_pred[:,1]
    
    # Output test data score results
    result_key_score = zip(key,p_pred)
    score_file=open(result_dir+"score_"+output_suffix,"w")
    score_csv=csv.writer(score_file)
    score_csv.writerow(["payment_request_id","score"])
    for row in result_key_score:
        score_csv.writerow(row)
    score_file.close()

    # Performance Evaluation: Test
    print 'Evalutate model performance ...'
    performance_eval_test(y,p_pred,result_dir,output_suffix)
    


joblist=[
        #('RandomForest_signal','model_var_list_signal.csv'), # suffix and varlist
        #('RandomForest_tmxpayer','model_var_list_tmxpayer.csv'),
        #('RandomForest_tmxpayee','model_var_list_tmxpayee.csv'),
        #('RandomForest_signal_tmxpayer','model_var_list_signal_tmxpayer.csv'),
        #('RandomForest_signal_tmxpayee','model_var_list_signal_tmxpayee.csv'),
        ('RandomForest_signal_tmxboth','model_var_list_signal_tmxboth.csv')
        ]

########################### Instantiate Classifiers ############################


classifiers = {
    "Logistic":LogisticRegression(),
    "NearestNeighbors":KNeighborsClassifier(100),
    "LinearSVM":SVC(kernel="linear", C=0.025),
    "RBFSVM":SVC(gamma=2, C=1),
    "DecisionTree":DecisionTreeClassifier(max_depth=4),
    "RandomForest":RandomForestClassifier(max_depth=None, n_estimators=200, max_features="auto",random_state=0),
    "AdaBoost":AdaBoostClassifier(n_estimators=500,random_state=0),
    "GradientBoost":GradientBoostingClassifier(n_estimators=500, learning_rate=1.0,max_depth=None, random_state=0),
    "NaiveBayes":GaussianNB(),
    "LDA":LDA(),
    "QDA":QDA()
    }


        
############################# Main: Run Different Classifiers ################################


data_dir='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/'
result_dir='/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/'

for job in joblist:
    
    # Train Model and Evaluate Performance on Train and Validation Data
    output_suffix=job[0]   
    var_list_filename=result_dir+job[1] 
    ins_file=data_dir+'model_data_ds_ins_imp_woe.csv.gz'
    oos_file=data_dir+'model_data_ds_oos_imp_woe.csv.gz'
    classifier=classifiers["RandomForest"]
    
    #model_train_validation(ins_file, oos_file, classifier, var_list_filename, result_dir, output_suffix)
    
    
    # Load Model and Evaluate Performance on Test Data
    test_data_file = data_dir+'test_data_sept_ds_imp_woe.csv.gz'
    model_file = result_dir+"model.p"
    output_suffix = job[0]+'_test_sept'
    model_test_data_evaluation(test_data_file, var_list_filename, model_file, result_dir, output_suffix)
    
    test_data_file = data_dir+'test_data_oct_ds_imp_woe.csv.gz'
    model_file = result_dir+"model.p"
    output_suffix = job[0]+'_test_oct'
    model_test_data_evaluation(test_data_file, var_list_filename, model_file, result_dir, output_suffix)
    
    test_data_file = data_dir+'test_data_nov_ds_imp_woe.csv.gz'
    model_file = result_dir+"model.p"
    output_suffix = job[0]+'_test_nov'
    model_test_data_evaluation(test_data_file, var_list_filename, model_file, result_dir, output_suffix)
    
    test_data_file = data_dir+'test_data_dec_ds_imp_woe.csv.gz'
    model_file = result_dir+"model.p"
    output_suffix = job[0]+'_test_dec'
    model_test_data_evaluation(test_data_file, var_list_filename, model_file, result_dir, output_suffix)
    
