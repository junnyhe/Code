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


from sklearn.feature_extraction.text import TfidfVectorizer


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
    rc_entry_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_reasoncode.csv'
    step1_rc_ind_creation = rc_ind_creation(rc_entry_filename)
    
    payer_tmxrc_entry_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_tmxrc_payer_selected.csv'
    payee_tmxrc_entry_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_tmxrc_payee_selected.csv'
    step2_tmxrc_ind_creation = tmxrc_ind_creation(payer_tmxrc_entry_filename,payee_tmxrc_entry_filename)
    
    ###### 3.prepare new header ######
    header = incsv.fieldnames
    
    # additional header for rc ind #####
    rc_entry_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_reasoncode.csv'
    rc_entry_file=open(rc_entry_filename,'rU')
    rc_entry_csv=csv.reader(rc_entry_file)
    rc_entry_list=[]
    for row in rc_entry_csv:
        rc_entry_list.append(row[0])    
    header_rc=["tmx_payer_rc_ind_"+rc for rc in rc_entry_list] + ["tmx_payee_rc_ind_"+rc for rc in rc_entry_list]
            
    # additional header for tmxrc ind
    tmxrc_ind_select_var_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/var_list_rc_tmxrc_ind_selected.csv'
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
    
if __name__=="__main__":
    work_dir = "/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/"
    
    input_list = (
                  [work_dir+"model_data_pmt_ins_ds.csv.gz",work_dir+"model_data_pmt_ins_ds_rcind.csv.gz"],
                  [work_dir+"model_data_pmt_oos_ds.csv.gz",work_dir+"model_data_pmt_oos_ds_rcind.csv.gz"],
                  [work_dir+"test_data_sept_pmt_ds.csv.gz",work_dir+"test_data_sept_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_oct_pmt_ds.csv.gz",work_dir+"test_data_oct_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_nov_pmt_ds.csv.gz",work_dir+"test_data_nov_pmt_ds_rcind.csv.gz"],
                  [work_dir+"test_data_dec_pmt_ds.csv.gz",work_dir+"test_data_dec_pmt_ds_rcind.csv.gz"],
                  )
                # Inputs: rc_ind_creation(input_file,output_file)
    pool = Pool(processes=3)
    pool.map(rc_tmxrc_ind_creation_batch_helper, input_list)
    
    #csv_EDD(work_dir+"model_data_pmt_ins_ds_rcind.csv.gz")
    
    #rc_tmxrc_ind_creation_helper([work_dir+"model_data_pmt_oos_ds.csv.gz",work_dir+"model_data_pmt_oos_ds_rcind.csv.gz"])
   

















