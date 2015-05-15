import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
import pickle
from operator import itemgetter
from multiprocessing import Pool


sys.path.append("/home/junhe/fraud_model/Code/src3/step3_model_data_prep_signal_tmx")
from step3a_model_data_rc_tmxrc_ind_creation import *
from step3b_model_data_feature_creation import *
from step3c_model_data_impute_woe_assigin import *


def rc_tmxrc_ind_creation_batch(input_file,output_file):
    
    ##### 1.input output files #####
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    ##### 2.instantiate scoring object #####
    rc_entry_filename=work_dir+'entry_list_reasoncode.csv'
    step1_rc_ind_creation = rc_ind_creation(rc_entry_filename)
    
    payer_tmxrc_entry_filename=work_dir+'entry_list_tmxrc_payer_selected.csv'
    payee_tmxrc_entry_filename=work_dir+'entry_list_tmxrc_payee_selected.csv'
    step2_tmxrc_ind_creation = tmxrc_ind_creation(payer_tmxrc_entry_filename,payee_tmxrc_entry_filename)
    
    ###### 3.prepare new header ######
    header = incsv.fieldnames
    
    # additional header for rc ind #####
    rc_entry_filename=work_dir+'entry_list_reasoncode.csv'
    rc_entry_file=open(rc_entry_filename,'rU')
    rc_entry_csv=csv.reader(rc_entry_file)
    rc_entry_list=[]
    for row in rc_entry_csv:
        rc_entry_list.append(row[0])    
    header_rc=["tmx_payer_rc_ind_"+rc for rc in rc_entry_list] + ["tmx_payee_rc_ind_"+rc for rc in rc_entry_list]
            
    # additional header for tmxrc ind
    tmxrc_ind_select_var_filename=work_dir+'var_list_rc_tmxrc_ind_selected.csv'
    tmxrc_ind_select_var_file=open(tmxrc_ind_select_var_filename,'rU')
    tmxrc_ind_select_var_csv=csv.reader(tmxrc_ind_select_var_file)
    tmxrc_ind_select_var_list=[]
    for row in tmxrc_ind_select_var_csv:
        tmxrc_ind_select_var_list.append(row[0])
    header_rctmxrc=tmxrc_ind_select_var_list
    
    header = header + header_rc + header_rctmxrc
    outcsv.writerow(header)
    
    ##### 4. batch processing data #####
    t0=time.time()
    nRow=0
    for row in incsv:
        
        row = step1_rc_ind_creation.process(row)
        row = step2_tmxrc_ind_creation.process(row)
        
        outcsv.writerow([row[key] for key in header])
        
        nRow+=1
        if nRow%10000 == 0:
            print nRow,"row has been processed; time lapsed:",time.time()-t0


def rc_tmxrc_ind_creation_batch_helper(arg):
    rc_tmxrc_ind_creation_batch(arg[0],arg[1])
    


if len(sys.argv) <=1:
    work_dir=''#/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/'
elif len(sys.argv) ==2:
    work_dir=sys.argv[1]
else:
    print "stdin input should be 0 or 1 vars, 0 using data location in code, 1 using input."
    
if __name__=="__main__":
    
    input_list = (
                  [work_dir+"model_data_pmt_ins_ds.csv.gz",work_dir+"model_data_pmt_ins_ds_rcind.csv.gz"],
                  [work_dir+"model_data_pmt_oos_ds.csv.gz",work_dir+"model_data_pmt_oos_ds_rcind.csv.gz"],
                  [work_dir+"test_data_1mo_pmt_ds.csv.gz",work_dir+"test_data_1mo_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_2mo_pmt_ds.csv.gz",work_dir+"test_data_2mo_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_3mo_pmt_ds.csv.gz",work_dir+"test_data_3mo_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_4mo_pmt_ds.csv.gz",work_dir+"test_data_4mo_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_5mo_pmt_ds.csv.gz",work_dir+"test_data_5mo_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_6mo_pmt_ds.csv.gz",work_dir+"test_data_6mo_pmt_ds_rcind.csv.gz"]
                  )
                # Inputs: rc_ind_creation(input_file,output_file)
    pool = Pool(processes=4)
    pool.map(rc_tmxrc_ind_creation_batch_helper, input_list)
    
    #csv_EDD(work_dir+"model_data_pmt_ins_ds_rcind.csv.gz")
    
    #rc_tmxrc_ind_creation_helper([work_dir+"model_data_pmt_oos_ds.csv.gz",work_dir+"model_data_pmt_oos_ds_rcind.csv.gz"])
   

















