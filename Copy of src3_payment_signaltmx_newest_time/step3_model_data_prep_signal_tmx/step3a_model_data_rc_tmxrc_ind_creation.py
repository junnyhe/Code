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


class rc_ind_creation:
    
    def __init__(self,rc_entry_filename):
        #=======================================================================
        # initialize for rc/tmxrc ind conversion
        #=======================================================================
        # payer rc entry list 
        rc_entry_file=open(rc_entry_filename,'rU')
        rc_entry_csv=csv.reader(rc_entry_file)
        rc_entry_list=[]
        for row in rc_entry_csv:
            rc_entry_list.append(row[0])
        self.rc_entry_set=set(rc_entry_list)
    
    
    def process(self,row):
        
        # initialize with zeros
        for rc in self.rc_entry_set:
            row["tmx_payer_rc_ind_"+rc]=0
            row["tmx_payee_rc_ind_"+rc]=0 
        
        # populate payer if in risk list
        if row['tmx_payer_reason_code'] != '':
            exec "payer_rc_list_in_row=" + str(row['tmx_payer_reason_code']).replace(';',',')
            for rc in payer_rc_list_in_row:
                if rc in self.rc_entry_set:
                    row["tmx_payer_rc_ind_"+rc]=1
                            
    
        # populate payee if in risk list
        if row['tmx_payee_reason_code'] != '':
            exec "payee_rc_list_in_row=" + str(row['tmx_payee_reason_code']).replace(';',',')
            for rc in payee_rc_list_in_row:
                if rc in self.rc_entry_set:
                    row["tmx_payee_rc_ind_"+rc]=1
    
        return row
    
class tmxrc_ind_creation:
    
    def __init__(self,payer_tmxrc_entry_filename,payee_tmxrc_entry_filename):
        #=======================================================================
        # initialize for rc/tmxrc ind conversion
        #=======================================================================

        # tmx detect string
        self.str_detect_set = set(["_T_TOR","VPN","_PossibleVM","_Malware"])
        
        # payer rc entry list 
        payer_tmxrc_entry_file=open(payer_tmxrc_entry_filename,'rU')
        payer_tmxrc_entry_csv=csv.reader(payer_tmxrc_entry_file)
        payer_tmxrc_entry_list=[]
        for row in payer_tmxrc_entry_csv:
            payer_tmxrc_entry_list.append(row[0])
        self.payer_tmxrc_entry_set=set(payer_tmxrc_entry_list)
        
        # payee tmxrc entry list 
        payee_tmxrc_entry_file=open(payee_tmxrc_entry_filename,'rU')
        payee_tmxrc_entry_csv=csv.reader(payee_tmxrc_entry_file)
        payee_tmxrc_entry_list=[]
        for row in payee_tmxrc_entry_csv:
            payee_tmxrc_entry_list.append(row[0])
        self.payee_tmxrc_entry_set=set(payee_tmxrc_entry_list)


    def process(self,row):
        
        # convert tmxrc for payer
        for tmxrc in self.payer_tmxrc_entry_set:
            row["payer_tmxrc_ind"+tmxrc]=0
        
        for tmxrc in self.str_detect_set:
            row["payer_tmxrc_ind_"+tmxrc]=0
        
        if row['tmx_payer_tmx_reason_code'] != '':
            exec "payer_tmxrc_list_in_row=" + str(row['tmx_payer_tmx_reason_code']).replace(';',',')
            
            for tmxrc in payer_tmxrc_list_in_row:
                if tmxrc in self.payer_tmxrc_entry_set:
                    row["payer_tmxrc_ind"+tmxrc]=1
                for entry in self.str_detect_set:
                    if entry in tmxrc:
                        row["payer_tmxrc_ind_"+entry]=1
                    else:
                        row["payer_tmxrc_ind_"+entry]=0
            
    
        # convert tmxrc for payee
        for tmxrc in self.payee_tmxrc_entry_set:
            row["payee_tmxrc_ind"+tmxrc]=0
        
        for tmxrc in self.str_detect_set:
            row["payee_tmxrc_ind_"+tmxrc]=0
            
        if row['tmx_payee_tmx_reason_code'] != '':
            exec "payee_tmxrc_list_in_row="+str(row['tmx_payer_tmx_reason_code']).replace(';',',')
            
            for tmxrc in payee_tmxrc_list_in_row:
                if tmxrc in self.payee_tmxrc_entry_set:
                    row["payee_tmxrc_ind"+tmxrc]=1
                for entry in self.str_detect_set:
                    if entry in tmxrc:
                        row["payee_tmxrc_ind_"+entry]=1
                    else:
                        row["payee_tmxrc_ind_"+entry]=0
        
        return row


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
    

work_dir = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/'

if __name__=="__main__":
    
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
   

















