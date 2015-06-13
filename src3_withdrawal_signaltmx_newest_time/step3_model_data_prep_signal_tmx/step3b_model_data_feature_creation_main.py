import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
import pickle

from math import radians, cos, sin, asin, sqrt
from multiprocessing import Pool

sys.path.append("/fraud_model/Code/src3/step3_model_data_prep_signal_tmx")
from step3a_model_data_rc_tmxrc_ind_creation import *
from step3b_model_data_feature_creation import *
from step3c_model_data_impute_woe_assigin import *


def feature_creation_batch(input_file,output_file):
    
    ##### 1. input output files #####
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)

    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    ##### 2. instantiate scoring object #####
    time_var_filename=work_dir+'var_list_time_diff.csv'
    ppcmp_var_filename=work_dir+'var_list_ppcmp.csv'
    leven_dist_var_filename=work_dir+'var_list_leven_dist.csv'
    
    step3_feature_creation = feature_creation(time_var_filename, ppcmp_var_filename, leven_dist_var_filename)
    
    ###### 3. prepare new header ######
    header=incsv.fieldnames
    
    # additional header for feature engineering
    header_feature_cr=\
    ['df_'+var for var in step3_feature_creation.time_var_list]+\
    ['df_signal159_payee_last_review','df_signal169_payer_last_review']+\
    ['payee_reviewed_ever', # review time signals
    'payee_reviewed_le_7d',
    'payee_reviewed_7_30d',
    'payee_reviewed_30_60d',       
    'payer_reviewed_ever',
    'payer_reviewed_le_7d',
    'payer_reviewed_7_30d',
    'payer_reviewed_30_60d',]+\
    ['ip2_'+var for var in step3_feature_creation.ip_var_list]+\
    ['ip3_'+var for var in step3_feature_creation.ip_var_list]+\
    ['ip_dist_payer_payee_proxy','ip_dist_payer_payee_true','ip_dist_payer_proxy_true','ip_dist_payee_proxy_true']+\
    ['ppcmp_'+var for var in step3_feature_creation.ppcmp_var_list]+\
    ['leven_dist_'+var for var in step3_feature_creation.leven_dist_var_list]+\
    ['zip3_'+var for var in step3_feature_creation.zip_var_list]+\
    ['zip4_'+var for var in step3_feature_creation.zip_var_list]+\
    ['md_df_'+var for var in step3_feature_creation.time_var_list]+\
    ['md_df_signal159_payee_last_review','md_df_signal169_payer_last_review']
    
    header = header + header_feature_cr
    outcsv.writerow([var for var in header])
    
    ##### 4. batch processing data #####
    t0=time.time()
    nRow=0
    for row in incsv:
        
        row = step3_feature_creation.process(row)
        
        outcsv.writerow([row[var] for var in header])
        
        nRow+=1
        if nRow%100000 == 0:
            print nRow,"row has been processed; time lapsed:",time.time()-t0

    
    infile.close()
    outfile.close()


def feature_creation_batch_helper(arg):
    feature_creation_batch(arg[0],arg[1])
    #feature_creation(input_file,output_file)
    

if len(sys.argv) <=1:
    work_dir=''#/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/'
elif len(sys.argv) ==2:
    work_dir=sys.argv[1]
else:
    print "stdin input should be 0 or 1 vars, 0 using data location in code, 1 using input."

if __name__=="__main__":
    
    
    input_list = (
                  [work_dir+"model_data_wd_ins_ds_rcind.csv.gz",work_dir+"model_data_wd_ins_ds_rcind_fc.csv.gz"],
                  [work_dir+"model_data_wd_oos_ds_rcind.csv.gz",work_dir+"model_data_wd_oos_ds_rcind_fc.csv.gz"],
                  [work_dir+"test_data_1mo_wd_ds_rcind.csv.gz",work_dir+"test_data_1mo_wd_ds_rcind_fc.csv.gz"],
                  [work_dir+"test_data_2mo_wd_ds_rcind.csv.gz",work_dir+"test_data_2mo_wd_ds_rcind_fc.csv.gz"],
                  [work_dir+"test_data_3mo_wd_ds_rcind.csv.gz",work_dir+"test_data_3mo_wd_ds_rcind_fc.csv.gz"],
                  [work_dir+"test_data_4mo_wd_ds_rcind.csv.gz",work_dir+"test_data_4mo_wd_ds_rcind_fc.csv.gz"],
                  [work_dir+"test_data_5mo_wd_ds_rcind.csv.gz",work_dir+"test_data_5mo_wd_ds_rcind_fc.csv.gz"],
                  [work_dir+"test_data_6mo_wd_ds_rcind.csv.gz",work_dir+"test_data_6mo_wd_ds_rcind_fc.csv.gz"],
                  )
                # Inputs: feature_creation(input_file,output_file)
    pool = Pool(processes=4)
    pool.map(feature_creation_batch_helper, input_list)
    
    #feature_creation_helper([work_dir+"model_data_wd_oos_ds_rcind.csv.gz",work_dir+"model_data_wd_oos_ds_rcind_fc.csv.gz"])    #csv_EDD(work_dir+"model_data_wd_ins_ds_rcind_fc.csv.gz")


