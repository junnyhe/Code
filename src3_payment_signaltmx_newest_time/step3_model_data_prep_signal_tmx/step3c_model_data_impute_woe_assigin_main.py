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
    impute_value_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/impute_values.p'
    step4_impute_replace = impute_replace(impute_value_filename)
    
    risk_table_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/risk_table.p'
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
    
    
work_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/' # everything should/will be in work_dir

if __name__=="__main__":  

    input_list = (
                  [work_dir+"model_data_pmt_ins_ds_rcind_fc.csv.gz",work_dir+"model_data_pmt_ins_ds_rcind_fc_imp_woe.csv.gz"], 
                  [work_dir+"model_data_pmt_oos_ds_rcind_fc.csv.gz",work_dir+"model_data_pmt_oos_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_sept_pmt_ds_rcind_fc.csv.gz",work_dir+"test_data_sept_pmt_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_oct_pmt_ds_rcind_fc.csv.gz",work_dir+"test_data_oct_pmt_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_nov_pmt_ds_rcind_fc.csv.gz",work_dir+"test_data_nov_pmt_ds_rcind_fc_imp_woe.csv.gz"],
                  [work_dir+"test_data_dec_pmt_ds_rcind_fc.csv.gz",work_dir+"test_data_dec_pmt_ds_rcind_fc_imp_woe.csv.gz"]
                  )
    pool = Pool(processes=3)
    pool.map(impute_replace_woe_assign_batch_helper, input_list)
    
    #csv_EDD(work_dir+'model_data_pmt_ins_ds_rcind_fc_imp_woe.csv.gz')]
    
    #impute_replace_woe_assign_batch_helper([work_dir+"model_data_pmt_oos_ds_rcind_fc.csv.gz",work_dir+"model_data_pmt_oos_ds_rcind_fc_imp_woe0.csv.gz"])
    





