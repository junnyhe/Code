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


joblist=[
          #('_signal','model_var_list_signal.csv'), # suffix and varlist
          #('_tmxpayer','model_var_list_tmxpayer.csv'),
          #('_tmxpayee','model_var_list_tmxpayee.csv'),
          #('_signal_tmxpayer','model_var_list_signal_tmxpayer.csv'),
          #('_signal_tmxpayee','model_var_list_signal_tmxpayee.csv'),
          #('_signal_tmxboth','model_var_list_signal_tmxboth.csv'),
          ('_signal_tmxboth_200','model_var_list_signal_tmxboth_200.csv')
          ]


################################ Load Data #####################################

def main():

    data_dir='/Users/junhe/Box Sync/Documents/Data/Model_Data_Signal_Tmx/'
    result_dir='/Users/junhe/Box Sync/Documents/Results/Model_Results_Signal_Tmx/'
    
    for job in joblist:
    
        add_output_suffix=job[0]   
        var_list_filename=job[1] 
        
        target_name='target'
        
        varlist_file=open(result_dir+var_list_filename,'rU')
        varlist_csv=csv.reader(varlist_file)
        var_list=[]
        for row in varlist_csv:
            var_list.append(row[0])
        
        
        ins_file='model_data_ds_ins_imp_woe.csv.gz'
        insfile=gzip.open(data_dir+ins_file,'rb')
        inscsv=csv.DictReader(insfile)
        
        oos_file='model_data_ds_oos_imp_woe.csv.gz'
        oosfile=gzip.open(data_dir+oos_file,'rb')
        ooscsv=csv.DictReader(oosfile)
        
        print 'loading data for model ...'
        data_ins=[]
        for row in inscsv:
            try:
                row_float = [float(row[var]) for var in var_list+[target_name]]
                data_ins.append(row_float)
            except:
                print "Warning: Row contains none numeric values, skipping ............"
        
        data_ins=np.array(data_ins)
        
        
        data_oos=[]
        for row in ooscsv:
            try:
                row_float = [float(row[var]) for var in var_list+[target_name]]
                data_oos.append(row_float)
            except:
                print "Warning: Row contains none numeric values, skipping ............"
        
        
        data_oos=np.array(data_oos)
        
        
        X=data_ins[:,:-1]
        y=data_ins[:,-1]
        
        Xt=data_oos[:,:-1]
        yv=data_oos[:,-1]
        
        
        
        ########################### Instantiate Classifiers ############################
        
        namesList = ["Logistic","NearestNeighbors", "LinearSVM", "RBFSVM", "DecisionTree",
                 "RandomForest", "AdaBoost", "GradientBoost", "NaiveBayes", "LDA", "QDA"]
        classifiersList = [
            LogisticRegression(),
            KNeighborsClassifier(100),
            SVC(kernel="linear", C=0.025),
            SVC(gamma=2, C=1),
            DecisionTreeClassifier(max_depth=4),
            RandomForestClassifier(max_depth=None, n_estimators=200, max_features="auto",random_state=0),
            AdaBoostClassifier(n_estimators=500,random_state=0),
            GradientBoostingClassifier(n_estimators=500, learning_rate=1.0,max_depth=None, random_state=0),
            GaussianNB(),
            LDA(),
            QDA()]
        
        classifiers=dict(zip(namesList,classifiersList))
        
        
        ################ Run Different Classifiers (nnet run separately) ###############
        
        #classifierTestList= ["Logistic","NearestNeighbors","DecisionTree","RandomForest","AdaBoost","GradientBoost"]
        #classifierTestList= ["Logistic","DecisionTree","RandomForest", "AdaBoost"]
        classifierTestList= ["RandomForest"]
        #classifierTestList= ["AdaBoost"]
        #classifierTestList= ["GradientBoost"]
        #classifierTestList= ["Logistic"]
        results=[]
        for classifierName in classifierTestList:
            print "\nfitting model:",classifierName,"..."
            t0=time.time()
            
            ####################### Train and Evaluate Model ###########################
            
            # Train Model
            model = classifiers[classifierName]
            model.fit(X, y)
            print "Model training done, taking ",time.time()-t0,"secs"
            
            # Predict Train
            y_pred = model.predict(X)
            p_pred = model.predict_proba(X)
            p_pred = p_pred[:,1]
            
            # Predict Validation
            yv_pred = model.predict(Xt)
            pv_pred = model.predict_proba(Xt)
            pv_pred = pv_pred[:,1]
            
            # Error rate
            score = model.score(Xt,yv) # accuracy (percentage classified correctly)
            errorRateInTest=1-score
            
            
            ########################## ROC and AUC #####################################
            
            # Compute KS, ROC curve and AUC for train
            ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve, lorenz_curve_capt_rate= ks_roc(y,p_pred)
            auc = getAUC(p_pred,y)
            
            # Compute KS, ROC curve and AUC for validation
            ks_v, ks_pos_v, pctl_v, tpr_v, fpr_v, tp_cumcnt_v, fp_cumcnt_v, threshold_v, lorenz_curve_v, lorenz_curve_capt_rate_v = ks_roc(yv,pv_pred)
            auc_v = getAUC(pv_pred,yv)
            
            # Plot ROC
            pl.figure(1, figsize=(8,8))
            pl.clf()
            pl.plot(fpr, tpr, 'b-', label='ROC Train(AUC=%0.3f)' % auc)
            pl.plot(fpr_v, tpr_v, 'r--', label='ROC Validation (AUC=%0.3f)' % auc_v)
            pl.plot([0, 1], [0, 1], 'k--')
            pl.xlim([0.0, 1.0])
            pl.ylim([0.0, 1.0])
            pl.xlabel('False Positive Rate')
            pl.ylabel('True Positive Rate')
            pl.title('ROC Curve: '+ classifierName+add_output_suffix+ ' Model')
            pl.legend(loc="lower right")
            #pl.show()
            pl.savefig(result_dir+'ROC_AUC_'+classifierName+add_output_suffix+'.png', bbox_inches='tight')
            
            # Plot Lorenz
            pl.figure(2, figsize=(8,8))
            pl.clf()
            pl.plot(pctl, tpr, 'b-',label='Fraud Train',linewidth=2.0 )
            pl.plot(pctl, fpr, 'g-',label='Non-Fraud Train',linewidth=2.0)
            pl.plot(pctl_v, tpr_v, 'm--',label='Fraud Validation' )
            pl.plot(pctl_v, fpr_v, 'r--',label='Non-Fraud Validation')
            pl.plot([0, 1], [0, 1], 'k--')
            pl.xlim([0.0, 1.0])
            pl.ylim([0.0, 1.0])
            pl.xlabel('Score Percentile')
            pl.ylabel('Cumulative Rate')
            tilte_lorenz="Lorenz Curve:KS Train={:.3f}({:.3f});KS Vali={:.3f}({:.3f})".format(float(ks),float(ks_pos),float(ks_v),float(ks_pos_v))
            pl.title(tilte_lorenz)
            pl.legend(loc="lower right")
            #pl.show()
            pl.savefig(result_dir+'Lorenz_'+classifierName+add_output_suffix+'.png', bbox_inches='tight')
            
            # save results to disk
            pickle.dump(pv_pred,open(result_dir+"pv_pred.p",'wb'))
            pickle.dump(yv,open(result_dir+"yv.p",'wb'))
            
            
            ############# # Output validation Lorenz and KS results to file ##############
            
            out_dt = open(result_dir + 'KS_validation_' + str(classifierName+add_output_suffix)+'.csv', 'wb')
            out_csv = csv.writer(out_dt)
            
            out_csv.writerow(["KS_"+str(classifierName+add_output_suffix)])
            out_csv.writerow([ks_v])
            out_csv.writerow(["KS_Position_for_"+str(classifierName+add_output_suffix)])
            out_csv.writerow([ks_pos_v])
            out_csv.writerow(["Capture_Rate_"+str(classifierName+add_output_suffix)])
            for row in lorenz_curve_capt_rate_v:
                out_csv.writerow(row)
            out_csv.writerow(["Lorenz_Curve_"+str(classifierName+add_output_suffix)])
            for row in lorenz_curve_v:
                out_csv.writerow(row)
                
            out_dt.close()
        
            
            results.append((classifierName+add_output_suffix,auc_v,ks_v))
            
            
            #################### Random Forest Feature Importance ######################
            if classifierName=="RandomForest":
                out_feat_import = open(result_dir + 'feature_import_' + str(classifierName+add_output_suffix)+'.csv', 'wb')
                feat_import_csv = csv.writer(out_feat_import)
                var_iport = zip(range(len(var_list)),var_list,model.feature_importances_)
                feat_import_csv.writerow(['var seq num','var name','importance'])
                for row in var_iport:
                    feat_import_csv.writerow(row)
        
            
            
        # Print results for all classifier
        print ('ClassifierName','AUC on Validation','KS on Validation')
        for row in results:
            print row


main()