import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
import pickle
from multiprocessing import Pool



sys.path.append("/home/junhe/fraud_model/Code/src3/step3_model_data_prep_signal_tmx")
from step3a_model_data_rc_tmxrc_ind_creation import *
from step3b_model_data_feature_creation import *
from step3c_model_data_impute_woe_assigin import *


def impute_replace_woe_assign_batch(input_file,output_file):
    
    print "\nPerforming imputation for file: ",input_file,"..."
    
    ##### 1.input output files #####
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    ##### 2.instantiate scoring object #####
    impute_value_filename = work_dir+'impute_values.p'
    step4_impute_replace = impute_replace(impute_value_filename)
    
    risk_table_filename = work_dir+'risk_table.p'
    step5_woe_assign = woe_assign(risk_table_filename)
    
    ###### 3.prepare new header ######
    header=incsv.fieldnames
    
    # additional header for woe
    header_woe = ['lo_'+var for var in step5_woe_assign.woe_var_list]
    
    header = header + header_woe
    outcsv.writerow(header)

    ##### 4. batch processing data #####
    t0=time.time()
    nRow=0
    for row in incsv:
        row = step4_impute_replace.process(row)
        row = step5_woe_assign.process(row)
        outcsv.writerow([row[var] for var in header])
        
        nRow+=1
        if nRow%10000 == 0:
            print nRow,"row has been processed; time lapsed:",time.time()-t0


def impute_replace_woe_assign_batch_helper(arg):
    impute_replace_woe_assign_batch(arg[0],arg[1])
    
    
if len(sys.argv) <=1:
    work_dir=''#/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/'
elif len(sys.argv) ==2:
    work_dir=sys.argv[1]
else:
    print "stdin input should be 0 or 1 vars, 0 using data location in code, 1 using input."

if __name__=="__main__":  

    input_list = (
                  [work_dir+"model_data_wd_ins_ds_rcind_fc.csv.gz",work_dir+"model_data_wd_ins_ds_rcind_fc_imp_woe.csv.gz"], 
                  [work_dir+"model_data_wd_oos_ds_rcind_fc.csv.gz",work_dir+"model_data_wd_oos_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_1mo_wd_ds_rcind_fc.csv.gz",work_dir+"test_data_1mo_wd_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_2mo_wd_ds_rcind_fc.csv.gz",work_dir+"test_data_2mo_wd_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_3mo_wd_ds_rcind_fc.csv.gz",work_dir+"test_data_3mo_wd_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_4mo_wd_ds_rcind_fc.csv.gz",work_dir+"test_data_4mo_wd_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_5mo_wd_ds_rcind_fc.csv.gz",work_dir+"test_data_5mo_wd_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_6mo_wd_ds_rcind_fc.csv.gz",work_dir+"test_data_6mo_wd_ds_rcind_fc_imp_woe.csv.gz"],
                  )
    pool = Pool(processes=4)
    pool.map(impute_replace_woe_assign_batch_helper, input_list)
    
    #csv_EDD(work_dir+'model_data_wd_ins_ds_rcind_fc_imp_woe.csv.gz')]
    
    #impute_replace_woe_assign_batch_helper([work_dir+"model_data_wd_oos_ds_rcind_fc.csv.gz",work_dir+"model_data_wd_oos_ds_rcind_fc_imp_woe0.csv.gz"])
    





