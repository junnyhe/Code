################################################ def csv_impute ###########################################################
#! /usr/bin/env python
import csv
import gzip
from numpy import *
import cPickle as pickle
import json
################################################################################
# Define imputation functions                                                  #
# 1. Calculate statistics, create missing value mapping                        #
# 2. Replace missing values                                                    #
################################################################################

def impute_create_mapping(work_dir,ins_file,imp_median_var_list_file,imp_zero_var_list_file):
    '''
    (Currently, only handles impute median and zero; more logic will be added)
    Calculate statistics for variables that need imputation if necessary;
    Create missing value mapping for each variable with missing values;
    Mapping will be saved in work_dir as: 'impute_values.csv'
    
    Input:
    4 inputs:
    work_dir='/Users/junhe/Documents/Data/' # everything should/will be in work_dir
    ins_file="model_data_ds.csv.gz" # in sample data used to calculate the stats
    imp_median_var_list_file=dir+"impute_var_list_median.csv" # var list for imputing missing to median 
    imp_zero_var_list_file=dir+"impute_var_list_zero.csv" # var list for imputeing missing to zero
    (var list file format: one row one var name, no header, no coma, no quotes)
    '''
    
    print "\nPrepare missing value mapping file ..."
    
    insfile=gzip.open(work_dir+ins_file,'rb')
    inscsv=csv.DictReader(insfile)
    
    imp_median_file=open(imp_median_var_list_file,'rU')
    imp_median_csv=csv.reader(imp_median_file)
    
    imp_median_var_list=[]
    for row in imp_median_csv:
        imp_median_var_list.append(row[0])
        
    
    imp_zero_file=open(imp_zero_var_list_file,'rU')
    imp_zero_csv=csv.reader(imp_zero_file)
    
    imp_zero_var_list=[]
    for row in imp_zero_csv:
        imp_zero_var_list.append(row[0])
    
    
    full_var_list=imp_median_var_list
    
    
    table={} 
    for var_name in full_var_list:
        table[var_name]={}
        table[var_name]['data']=[]
    
    nRow=0
    for row in inscsv: # load required variables
        for var_name in full_var_list:
            #missing values will be skipped
            try:
                var_value = float(row[var_name])
                table[var_name]['data'].append(var_value)
            except:
                continue
    
    for var_name in full_var_list: #compute stats
        data_col = table[var_name]['data']
        table[var_name]['mean'] = mean(table[var_name]['data'])
        table[var_name]['median'] = median(table[var_name]['data'])
    
    impute_values={}
    for var_name in imp_median_var_list:
        impute_values[var_name]=table[var_name]['median']
    
    for var_name in imp_zero_var_list:
        impute_values[var_name]=0
    
    
    #output imputation replace values as csv file
    header_impute_values = sorted(impute_values.keys())
    outfile=open(work_dir+'impute_values.csv','w')
    outcsv=csv.writer(outfile)
    outcsv.writerow(header_impute_values)
    outcsv.writerow([impute_values[key] for key in header_impute_values])
    
    
    #output imputation replace values as pickle file
    pickle.dump(impute_values,open(work_dir+"impute_values.p",'wb')) 
    
    print "Imputation mapping file, 'impute_values.csv', 'impute_values.p' created in work_dir:", work_dir



def impute_replace(work_dir,input_file,output_file):
    '''
    Using replacement values in csv files,
    Replace missing values in input_file
    Both input and output file should/will be in work_dir
    Imputation values mapping 'impute_values.csv' should be in work_dir, otherwise will fail,
    It is created by function: impute_create_mapping()
    
    3 inputs:
    work_dir='/Users/junhe/Documents/Data/'
    input_file="model_data_ds_ins.csv.gz"
    output_file="model_data_ds_ins_imp.csv.gz"
    '''
    
    print "\nPerforming imputation for file: ",input_file,"..."
    
    # load risk table from csv
    impfile=open(work_dir+'impute_values.csv','rb')
    impcsv=csv.DictReader(impfile)
    impute_values=impcsv.next()
    imp_var_list=impute_values.keys()
    
    # input
    infile=gzip.open(work_dir+input_file,'rb')
    incsv=csv.DictReader(infile)
    header_out=incsv.fieldnames
    
    # ouput
    outfile=gzip.open(work_dir+output_file,'w')
    outcsv=csv.writer(outfile)
    outcsv.writerow(header_out)
    
    # replace
    for row in incsv:
        for var in imp_var_list:
            try:
                float(row[var])
            except:
                row[var]=impute_values[var]
        outcsv.writerow([row[var] for var in header_out])
    
    print "Imputation done for file: ",input_file
    
    
def impute_replace_pickle(work_dir,input_file,output_file):
    '''
    Using replacement values in Pickle files,
    Replace missing values in input_file
    Both input and output file should/will be in work_dir
    Imputation values mapping 'impute_values.csv' should be in work_dir, otherwise will fail,
    It is created by function: impute_create_mapping()
    
    3 inputs:
    work_dir='/Users/junhe/Documents/Data/'
    input_file="model_data_ds_ins.csv.gz"
    output_file="model_data_ds_ins_imp.csv.gz"
    '''
    
    print "\nPerforming imputation for file: ",input_file,"..."
    
    # load risk table from pickle
    impute_values = pickle.load(open(work_dir+'impute_values.p','rb'))
    imp_var_list=impute_values.keys()
    
    # input
    infile=gzip.open(work_dir+input_file,'rb')
    incsv=csv.DictReader(infile)
    header_out=incsv.fieldnames
    
    #output
    outfile=gzip.open(work_dir+output_file,'w')
    outcsv=csv.writer(outfile)
    outcsv.writerow(header_out)
    
    # replace
    for row in incsv:
        for var in imp_var_list:
            try:
                float(row[var])
            except:
                row[var]=impute_values[var]
        outcsv.writerow([row[var] for var in header_out])
    
    print "Imputation done for file: ",input_file
    
''' usage examples:
work_dir='/Users/junhe/Documents/Data/'

input_file="test_data_sept_ds.csv.gz"
output_file="test_data_sept_ds_imp.csv.gz"
impute_replace(work_dir,input_file,output_file)

'''