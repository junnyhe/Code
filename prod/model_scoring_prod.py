import csv
import gzip
import sys
import time
import pickle
from numpy import *
import random

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier


sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/model_tools")
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")


class model_scoring:
    def __init__(self,impute_value_filename,risk_table_filename,var_list_filename,model_filename):
        ######################## load all the required variable ############################
        
        # load imputation mapping table from pickle
        self.impute_values = pickle.load(open(impute_value_filename,'rb'))
        
        # load risk table from pickle
        self.table = pickle.load(open(risk_table_filename,'rb'))
        
        # load modeling variables from csv
        varlist_file=open(var_list_filename,'rU')
        varlist_csv=csv.reader(varlist_file)
        self.var_list=[]
        for row in varlist_csv:
            self.var_list.append(row[0])
        
        # load model from pickle  
        self.model = pickle.load(open(model_filename,'rb'))


    def score(self,data):
        ########################### data prep ################################
        
        # impute replace
        imp_var_list=self.impute_values.keys()
        for var in imp_var_list:
            try:
                float(data[var])
            except:
                data[var]=self.impute_values[var]
        
        # woe assign
        woe_var_list=self.table.keys()
        for var_name in woe_var_list:
            try:
                data['lo_'+var_name]= self.table[var_name][data[var_name]]['log_odds_sm']
            except:
                data['lo_'+var_name]= self.table[var_name]['default']['log_odds_sm']
                # print 'Warning: new value of the variable: ',var_name,'=',data[var_name],' is not found in the risk table. Default log_odds of overall population is assigned.'
        
        
        ########################### scoring ####################################
        
        X=array([float(data[var]) for var in self.var_list])
        
        p_pred = self.model.predict_proba(X)
        p_pred = p_pred[:,1][0]
        
        return p_pred



################ test: compare scores with dev code ##################
if __name__=="__main__":
    #initialize scoring instance
    work_dir = '/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/'
    impute_value_filename = work_dir+'impute_values.p'
    risk_table_filename = work_dir+'risk_table.p'
    var_list_filename= '/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/model_var_list_signal_tmxboth.csv'
    model_filename= '/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/model.p'
    
    M=model_scoring(impute_value_filename,risk_table_filename,var_list_filename,model_filename)
    
    # test with oos data
    work_dir='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/'
    input_file='model_data_ds_oos.csv.gz'
    
    infile=gzip.open(work_dir+input_file,'rb')
    incsv=csv.DictReader(infile)
    p=[]
    for data in incsv:
        p.append(M.score(data))
    
    # compare with oos scored with dev code
    pt_pred=pickle.load(open("/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/pv_pred.p",'rb'))
    
    p_comp = zip(p,pt_pred)
    for row in p_comp:
        print row
