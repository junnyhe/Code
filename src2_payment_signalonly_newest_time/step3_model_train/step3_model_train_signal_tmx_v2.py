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
#from getAUC import *
#from ks_roc import *
from model_performance_evaluation import performance_eval_train_validation
from model_performance_evaluation import performance_eval_test
from model_performance_evaluation import performance_eval_test_downsample


def model_train_validation(ins_file, oos_file, classifier, var_list_filename, output_dir, output_suffix):
    """
    train model
    evaluate on the train and validation data
    evaluate the model performance on the train and validation data
    """
    #################### Load train and validation data ####################
    print 'Loading data for modeling starts ...'
    t0=time.time()
    target_name='target'
    X,y = load_data_fast(ins_file, var_list_filename, target_name)
    Xv,yv = load_data_fast(oos_file, var_list_filename, target_name)
    print "Loading data done, taking ",time.time()-t0,"secs"
    
    # prepare trivial input values for generating reason code in production
    trivial_input_values_file = output_dir+'trivial_input_values.p'
    trivial_input_values = median(X,axis=0)
    pickle.dump(trivial_input_values,open(trivial_input_values_file,'wb'))
    
    # Train Model
    print '\nModel training starts...'
    t0=time.time()
    model = classifier
    model.fit(X, y)
    print "Model training done, taking ",time.time()-t0,"secs"
    pickle.dump(model,open(output_dir+"model.p",'wb')) # save model to disk
    
    '''
    #export to tree graph in DOT format, tree only
    tree.export_graphviz(model,out_file=output_dir+'tree.dot')
    os.system("dot -Tpng "+output_dir+"tree.dot -o "+output_dir+"tree.png")
    '''
    
    # Predict Train
    y_pred = model.predict(X)
    p_pred = model.predict_proba(X)
    p_pred = p_pred[:,1]
    
    # Predict Validation
    yv_pred = model.predict(Xv)
    pv_pred = model.predict_proba(Xv)
    pv_pred = pv_pred[:,1]
    
    # Performance Evaluation: Train and Validation
    ks, auc, lorenz_curve_capt_rate = performance_eval_train_validation(y,p_pred,yv,pv_pred,output_dir,output_suffix)
    
    
    #################### Random Forest Feature Importance ######################
    try:
        varlist_file=open(var_list_filename,'rU')
        varlist_csv=csv.reader(varlist_file)
        var_list=[]
        for row in varlist_csv:
            var_list.append(row[0])
        out_feat_import = open(output_dir + 'feature_import_' + str(output_suffix)+'.csv', 'wb')
        feat_import_csv = csv.writer(out_feat_import)
        var_import = zip(range(len(var_list)),var_list,model.feature_importances_)
        feat_import_csv.writerow(['var seq num','var name','importance'])
        print "RandomForest classifier, var importance was output"
        for row in var_import:
            feat_import_csv.writerow(row)
    except:
        print "Not RandomForest classifier, var importance not created"
    
    
    return ks, auc, lorenz_curve_capt_rate

    

def model_test_data_evaluation(test_data_file, var_list_filename, model_file, output_dir, output_suffix):
    
    #################### Load Model and Evaluate Performance ##################
    ############################### Test Data #################################
    
    # Load Test Data
    print 'Loading test data starts ...'
    t0=time.time()
    target_name='target'  
    X,y = load_data_fast(test_data_file, var_list_filename, target_name)
    print "Loading test data done, taking ",time.time()-t0,"secs"
    
    # Load Model
    print 'Loading model ...'
    t0=time.time()
    model = pickle.load(open(model_file,'rb'))
    
    # Predict Test Data
    y_pred = model.predict(X)
    p_pred = model.predict_proba(X)
    p_pred = p_pred[:,1]

    # Performance Evaluation: Test
    print 'Evalutate model performance ...'
    ks, auc, lorenz_curve_capt_rate = performance_eval_test(y,p_pred,output_dir,output_suffix)
    
    return ks, auc, lorenz_curve_capt_rate
    

def model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix, good_downsample_rate):
    
    #################### Load Model and Evaluate Performance ##################
    ############################### Test Data #################################
    # Ad Hoc code
    # compare model results with rules
    
    # Load Test Data
    print 'Loading test data starts ...'
    t0=time.time()
    target_name='target'
    key_name='payment_request_id'
    tag_name='manual_review'
    X,y,key,tag = load_data_with_key_tag_fast(test_data_file, var_list_filename, target_name, key_name, tag_name)
    print "Loading test data done, taking ",time.time()-t0,"secs"
    
    # Load Model
    print 'Loading model ...'
    t0=time.time()
    model = pickle.load(open(model_file,'rb'))
    
    # Predict Test Data
    y_pred = model.predict(X)
    p_pred = model.predict_proba(X)
    p_pred = p_pred[:,1]

    # Performance Evaluation: Test
    print 'Evalutate model performance ...'
    ks, auc, lorenz_curve_capt_rate = performance_eval_test_downsample(y,p_pred,output_dir,output_suffix,good_downsample_rate)
    
    ####################### compare catch_rate, hit_rate, refer_rate between model and rule ######################
    scale_factor = (1-y)*(1/good_downsample_rate)+y
    '''
    rule_cmp_outfile=csv.writer(open(output_dir+"score_ruletag_"+output_suffix+".csv",'w'))
    rule_cmp_outfile.writerow(['payment_request_id','fraud_tag','score','manual_review_tag'])
    for i in range(len(p_pred)):
        rule_cmp_outfile.writerow([key[i],y[i],p_pred[i],tag[i],scale_factor[i]])
    '''
    # find rates of rule
    catch_rate_rule = sum(y*tag*scale_factor)/sum(y*scale_factor) # fraud found by rule tag / total fraud
    hit_rate_rule = sum(y*tag*scale_factor)/sum(tag*scale_factor) # fraud found by rule tag / total referred by rule
    refer_rate_rule = sum(tag*scale_factor)/sum(scale_factor) # fraud found by rule tag / total referred by rule
    
    # get score threshold for the same catch rate, and calculate hit_rate and refer_rate
    score_fraud_pmt=p_pred[y==1]
    score_threshold= percentile(score_fraud_pmt,(1-catch_rate_rule)*100) 
    score_referred= p_pred>=score_threshold
    
    catch_rate_score = sum(y*score_referred*scale_factor)/sum(y*scale_factor) # fraud found by score referred / total fraud
    hit_rate_score = sum(y*score_referred*scale_factor)/sum(score_referred*scale_factor) # fraud found by score_referred / total referred by score_referred
    refer_rate_score = sum(score_referred*scale_factor)/sum(scale_factor) # fraud found by score / total referred byscore
    
    rule_model_rates = [catch_rate_rule, hit_rate_rule, refer_rate_rule,catch_rate_score, hit_rate_score, refer_rate_score,score_threshold]
    print ['catch_rate_rule', 'hit_rate_rule', 'refer_rate_rule','catch_rate_score', 'hit_rate_score', 'refer_rate_score', 'score_threshold']
    print rule_model_rates
    
    return ks, auc, lorenz_curve_capt_rate, rule_model_rates
    

def format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate):
    # organize results for one case: KS and HitRate @ different CatchRate
    results_one_case = []
    results_one_case.append(ks)
    results_one_case.append(auc)
    for nRow in range(1,21): # nRow in capt_rate table
        catch_rate = lorenz_curve_capt_rate[nRow][1]
        hit_rate = lorenz_curve_capt_rate[nRow][3]/(lorenz_curve_capt_rate[nRow][3]+lorenz_curve_capt_rate[nRow][4]/good_downsample_rate)
        results_one_case.append(hit_rate)
    return results_one_case
    
    
    

if len(sys.argv) <=1:
    data_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/'
    result_dir='/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time/'
elif len(sys.argv) ==3:
    data_dir=sys.argv[1]
    result_dir=sys.argv[2]
else:
    print "stdin input should be 0 or 2 vars, 0 using data and result location in code, 2 using input."
    
    
good_downsample_rate = 0.05 #used to scale back hit rate


########################### Instantiate Classifiers ############################


classifiers = {
    "Logistic":LogisticRegression(),
    "NearestNeighbors":KNeighborsClassifier(100),
    "LinearSVM":SVC(kernel="linear", C=0.025),
    "RBFSVM":SVC(gamma=2, C=1),
    "DecisionTree":DecisionTreeClassifier(max_depth=32),
    "RandomForest":RandomForestClassifier(max_depth=None, n_estimators=200, max_features="auto",random_state=0,n_jobs=4),
    "RandomForest2":RandomForestClassifier(max_depth=8, n_estimators=200, max_features="auto",random_state=0,n_jobs=4),
    "AdaBoost":AdaBoostClassifier(n_estimators=500,random_state=0),
    "GradientBoost":GradientBoostingClassifier(n_estimators=500, learning_rate=1.0,max_depth=None, random_state=0),
    "NaiveBayes":GaussianNB(),
    "LDA":LDA(),
    "QDA":QDA()
    }

joblist=[
        (classifiers["RandomForest"],'pmt_signal','model_var_list_signal.csv'), # suffix and varlist
        #(classifiers["RandomForest"],'RandomForest_tmxpayer','model_var_list_tmxpayer.csv'),
        #(classifiers["RandomForest"],'RandomForest_tmxpayee','model_var_list_tmxpayee.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmxpayer','model_var_list_signal_tmxpayer.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmxpayee','model_var_list_signal_tmxpayee.csv'),
        #(classifiers["RandomForest"],'RandomForest_tmxpayer_tmxpayee','model_var_list_tmxpayer_tmxpayee.csv'),
        #(classifiers["RandomForest"],'RandomForest_tmxpayerpayee_comp','model_var_list_tmxpayerpayee_comp.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmxboth','model_var_list_signal_tmxboth.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmxboth_120','model_var_list_signal_tmxboth_120.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmxboth_800','model_var_list_signal_tmxboth_800.csv'),
        #(classifiers["RandomForest2"],'RandomForest_signal_tmxboth_RF2','model_var_list_signal_tmxboth.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_107','model_var_list_signal_107.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_123','model_var_list_signal_123.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_127','model_var_list_signal_127.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_100','model_var_list_signal_100.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_90','model_var_list_signal_90.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_80','model_var_list_signal_80.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmx6','model_var_list_signal_tmx6.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmx_payee_true_ip','model_var_list_signal_tmx_payee_true_ip.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmx_payer_true_ip','model_var_list_signal_tmx_payer_true_ip.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmx_payer_account_address_zip','model_var_list_signal_tmx_payer_account_address_zip.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_ip2_tmx_payer_true_ip','model_var_list_signal_ip2_tmx_payer_true_ip.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmx_payer_true_ip_organization','model_var_list_signal_tmx_payer_true_ip_organization.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_ip2_tmx_payee_true_ip','model_var_list_signal_ip2_tmx_payee_true_ip.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_tmx_rc_ind','model_var_list_signal_tmx_rc_ind.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_rc_tmx_rc_ind','model_var_list_signal_rc_tmx_rc_ind.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_rc_tmx_rc_ind_400','model_var_list_signal_rc_tmx_rc_ind_400.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_rc_tmx_rc_ind_163','model_var_list_signal_rc_tmx_rc_ind_163.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_rc_tmx_rc_ind_292','model_var_list_signal_rc_tmx_rc_ind_292.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_rc_tmx_rc_ind_393','model_var_list_signal_rc_tmx_rc_ind_393.csv'),
        #(classifiers["RandomForest"],'RandomForest_signal_rc_tmx_rc_ind_456','model_var_list_signal_rc_tmx_rc_ind_456.csv'),
        #(classifiers["Logistic"],'Logistic_signal_full','model_var_list_signal.csv'), # suffix and varlist
        #(DecisionTreeClassifier(max_depth=32),'DecisionTree_signal_full_depth=32','model_var_list_signal.csv'), # suffix and varlist
        #(DecisionTreeClassifier(max_depth=16),'DecisionTree_signal_full_depth=16','model_var_list_signal.csv'), # suffix and varlist
        #(DecisionTreeClassifier(max_depth=8),'DecisionTree_signal_full_depth=8','model_var_list_signal.csv'), # suffix and varlist
        #(DecisionTreeClassifier(max_depth=64),'DecisionTree_signal_full_depth=64','model_var_list_signal.csv'), # suffix and varlist
        #(DecisionTreeClassifier(max_depth=None),'DecisionTree_signal_full_depth=none','model_var_list_signal.csv'), # suffix and varlist
        #(DecisionTreeClassifier(max_depth=3),'DecisionTree_signal_full_depth=3','model_var_list_signal.csv'), # suffix and varlist
        ]

    
############################# Main: Run Different Classifiers ################################

for job in joblist:
    
    result_summary = []
    result_summary.append(['Case','KS','AUC']+['HitRate@'+str(i)+'%CatchRate' for i in range(5,105,5)] + ['catch_rate_rule', 'hit_rate_rule', 'refer_rate_rule','catch_rate_score', 'hit_rate_score', 'refer_rate_score', 'score_threshold']) #header for result summary
    
    # Train Model and Evaluate Performance on Train and Validation Data
    classifier=job[0]
    output_suffix=job[1]
    var_list_filename=result_dir+job[2]
    
    
    output_dir=result_dir+output_suffix+"/"
    if os.path.exists(output_dir):
        print "results folder:",output_dir," already exist"
    else:
        print "results folder:",output_dir," not exist; will be created"
        os.system("mkdir "+output_dir.replace(' ','\ '))
    
    ouput_result_summary_file = open(output_dir+'results_summary_'+output_suffix+'.csv','w')
    ouput_result_summary=csv.writer(ouput_result_summary_file)# output file for result summary
    
    ins_file=data_dir+'model_data_pmt_ins_ds_imp_woe.csv.gz'
    oos_file=data_dir+'model_data_pmt_oos_ds_imp_woe.csv.gz'
    
    
    ks, auc, lorenz_curve_capt_rate = model_train_validation(ins_file, oos_file, classifier, var_list_filename, output_dir, output_suffix)
    ####result_summary.append(['Jul, Aug']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate))# append results for one case to summary
    
    
    # Load Model and Evaluate Performance on Test Data
    test_data_file = data_dir+'model_data_pmt_oos_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_Validation'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['Validation']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    
    test_data_file = data_dir+'test_data_1mo_pmt_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_1mo'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['1mo']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    
    test_data_file = data_dir+'test_data_2mo_pmt_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_2mo'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['2mo']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    
    test_data_file = data_dir+'test_data_3mo_pmt_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_3mo'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['3mo']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    
    test_data_file = data_dir+'test_data_4mo_pmt_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_4mo'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['4mo']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    
    test_data_file = data_dir+'test_data_5mo_pmt_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_5mo'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['5mo']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    
    test_data_file = data_dir+'test_data_6mo_pmt_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_6mo'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['6mo']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    
    
    '''
    test_data_file = data_dir+'test_data_-1mo_pmt_ds_imp_woe.csv.gz'
    model_file = output_dir+"model.p"
    output_suffix = job[1]+'_-1mo'
    ks, auc, lorenz_curve_capt_rate, rule_model_rates = model_test_data_evaluation_comp_ruletag(test_data_file, var_list_filename, model_file, output_dir, output_suffix,good_downsample_rate)
    result_summary.append(['-1mo']+format_results_one_case(ks, auc, lorenz_curve_capt_rate, good_downsample_rate) + rule_model_rates)# append results for one case to summary
    '''
    
    
    for row in result_summary:
        ouput_result_summary.writerow(row)
    
ouput_result_summary_file.close()