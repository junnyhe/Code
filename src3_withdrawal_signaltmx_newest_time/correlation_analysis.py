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
from scipy.stats.stats import pearsonr

sys.path.append("/Users/junhe/Box Sync/Documents/workspace/fraud_model/src/model_tools")
sys.path.append("/Users/junhe/Box Sync/Documents/workspace/fraud_model/src/csv_operations")

import csv_ops
from csv_ops import *
from load_data import *
#from getAUC import *
#from ks_roc import *
from model_performance_evaluation import performance_eval_train_validation
from model_performance_evaluation import performance_eval_test


def correlation_analysis(input_file, output_file, var_list_filename, target_name):
    """
    Calculate Pearson correlation coefficient of target with signal
    """
    
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    infile=gzip.open(input_file,'rb')
    inscsv=csv.DictReader(infile)
    outfile=open(output_file,'w')
    outcsv=csv.writer(outfile)
    

    data={}
    data[target_name]=[]
    for var in var_list:
        data[var]=[]
    for row in inscsv:
        data[target_name].append(float(row[target_name]))
        
        for var in var_list:
            data[var].append(float(row[var]))
    
    
    outcsv.writerow(['signal_name','corr_pearson'])
    for var in var_list:
        corr=pearsonr(data[var],data[target_name])
        print var,corr[0]
        outcsv.writerow([var,corr[0]])
        

    


def correlation_analysis_2d(input_file, output_file, var_list_filename, target_name):
    """
    Calculate Pearson correlation coefficient of target with signal
    """
    
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    infile=gzip.open(input_file,'rb')
    inscsv=csv.DictReader(infile)
    outfile=open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    outfile=open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    
    data={}
    data[target_name]=[]
    for var in var_list:
        data[var]=[]
    for row in inscsv:
        data[target_name].append(float(row[target_name]))
        
        for var in var_list:
            data[var].append(float(row[var]))
    
    
    outcsv.writerow(['var_name']+var_list+[target_name])

    for var1 in var_list+[target_name]:
        row=[]
        row.append(var1)
        for var2 in var_list+[target_name]:
            print "Calculating variable pair:", var1,var2
            corr=pearsonr(data[var1],data[var2])
            row.append(corr[0])
        outcsv.writerow(row)
    


########################### Run Signal Target Correlation ############################


data_dir='/Users/junhe/Box Sync/Documents/Data/Model_Data_Signal_Tmx/'
result_dir='/Users/junhe/Box Sync/Documents/Results/Model_Results_Signal_Tmx/'

input_file=data_dir+'model_data_ds_ins_imp_woe.csv.gz'
var_list_filename=result_dir+'model_var_list_signal_tmxboth.csv'
target_name='target'

print "Analyze correlation for ", input_file

output_file=result_dir+'correlation_signal_target.csv'
correlation_analysis(input_file, output_file, var_list_filename,target_name)

output_file=result_dir+'correlation_signal_target_2d.csv'
correlation_analysis_2d(input_file, output_file, var_list_filename,target_name)
