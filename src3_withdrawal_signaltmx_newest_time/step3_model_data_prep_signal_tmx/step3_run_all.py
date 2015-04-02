import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *

from math import radians, cos, sin, asin, sqrt
from multiprocessing import Pool


from step3a_model_data_rc_tmxrc_ind_creation import *
from step3b_model_data_feature_creation import *
from step3c_model_data_impute_woe_assigin import *
    


def model_data_processing_batch(input_file,output_file):
    
    ##### 1.input output files #####
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    ##### 2.instantiate scoring object #####
    rc_entry_filename=work_dir+'/entry_list_reasoncode.csv'
    step1_rc_ind_creation = rc_ind_creation(rc_entry_filename)
    
    payer_tmxrc_entry_filename=work_dir+'/entry_list_tmxrc_payer_selected.csv'
    payee_tmxrc_entry_filename=work_dir+'/entry_list_tmxrc_payee_selected.csv'
    step2_tmxrc_ind_creation = tmxrc_ind_creation(payer_tmxrc_entry_filename,payee_tmxrc_entry_filename)
    
    time_var_filename=work_dir+'/var_list_time_diff.csv'
    ppcmp_var_filename=work_dir+'/var_list_ppcmp.csv'
    leven_dist_var_filename=work_dir+'/var_list_leven_dist.csv'
    step3_feature_creation = feature_creation(time_var_filename, ppcmp_var_filename, leven_dist_var_filename)
    
    impute_value_filename = work_dir+'/impute_values.p'
    step4_impute_replace = impute_replace(impute_value_filename)
    
    risk_table_filename = work_dir+'/risk_table.p'
    step5_woe_assign = woe_assign(risk_table_filename)
    
    pipeline=[
              step1_rc_ind_creation,
              step2_tmxrc_ind_creation,
              step3_feature_creation,
              step4_impute_replace,
              step5_woe_assign
              ]
    

    ###### 3.prepare new header ######
    header = incsv.fieldnames
    
    # additional header for rc ind #####
    rc_entry_filename=work_dir+'/entry_list_reasoncode.csv'
    rc_entry_file=open(rc_entry_filename,'rU')
    rc_entry_csv=csv.reader(rc_entry_file)
    rc_entry_list=[]
    for row in rc_entry_csv:
        rc_entry_list.append(row[0])    
    header_rc=["tmx_payer_rc_ind_"+rc for rc in rc_entry_list] + ["tmx_payee_rc_ind_"+rc for rc in rc_entry_list]
            
    # additional header for tmxrc ind
    tmxrc_ind_select_var_filename=work_dir+'/var_list_rc_tmxrc_ind_selected.csv'
    tmxrc_ind_select_var_file=open(tmxrc_ind_select_var_filename,'rU')
    tmxrc_ind_select_var_csv=csv.reader(tmxrc_ind_select_var_file)
    tmxrc_ind_select_var_list=[]
    for row in tmxrc_ind_select_var_csv:
        tmxrc_ind_select_var_list.append(row[0])
    header_rctmxrc=tmxrc_ind_select_var_list
    
    
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
    
    # additional header for woe
    header_woe = ['lo_'+var for var in step5_woe_assign.woe_var_list]
    
    header = header + header_rc + header_rctmxrc + header_feature_cr + header_woe
    outcsv.writerow(header)
    
    ##### 4. batch processing data #####
    t0=time.time()
    nRow=0
    for row in incsv:
        
        for runable in pipeline:
            row = runable.process(row)
        outcsv.writerow([row[key] for key in header])
        
        nRow+=1
        if nRow%10000 == 0:
            print input_file,nRow,"row has been processed; time lapsed:",time.time()-t0


def model_data_processing_batch_helper(arg):
    model_data_processing_batch(arg[0],arg[1])


work_dir = "/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/" 

input_list = (
              [work_dir+"model_data_wd_ins_ds.csv.gz",work_dir+"model_data_wd_ins_ds_rcind_fc_imp_woe.csv.gz"],
              [work_dir+"model_data_wd_oos_ds.csv.gz",work_dir+"model_data_wd_oos_ds_rcind_fc_imp_woe.csv.gz"],
              [work_dir+"test_data_dec_wd_ds.csv.gz",work_dir+"test_data_dec_wd_ds_rcind_fc_imp_woe.csv.gz"],
              [work_dir+"test_data_nov_wd_ds.csv.gz",work_dir+"test_data_nov_wd_ds_rcind_fc_imp_woe.csv.gz"],
              [work_dir+"test_data_oct_wd_ds.csv.gz",work_dir+"test_data_oct_wd_ds_rcind_fc_imp_woe.csv.gz"],
              [work_dir+"test_data_sept_wd_ds.csv.gz",work_dir+"test_data_sept_wd_ds_rcind_fc_imp_woe.csv.gz"],
              [work_dir+"test_data_aug_wd_ds.csv.gz",work_dir+"test_data_aug_wd_ds_rcind_fc_imp_woe.csv.gz"],
              [work_dir+"test_data_jul_wd_ds.csv.gz",work_dir+"test_data_jul_wd_ds_rcind_fc_imp_woe.csv.gz"]
              )
            # Inputs: rc_ind_creation(input_file,output_file)
pool = Pool(processes=3)
pool.map(model_data_processing_batch_helper, input_list)

#csv_EDD(work_dir+"model_data_wd_ins_ds_rcind.csv.gz")


# test
#arg=[work_dir+"model_data_wd_oos_ds.csv.gz",work_dir+"model_data_wd_oos_ds_rcind_fc_imp_woe.csv.gz"]
#model_data_processing_batch_helper(arg)
    
