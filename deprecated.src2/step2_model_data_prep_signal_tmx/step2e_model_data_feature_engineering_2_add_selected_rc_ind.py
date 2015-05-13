import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
from operator import itemgetter
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *
from multiprocessing import Pool

import nltk
import string
import os

from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem.porter import PorterStemmer



def rc_ind_addition_one_row(row,rc_ind_select_var_set,payer_rc_var_set,payee_rc_var_set,str_detect_list ):
    
    
    # initialize with zeros
    for var in rc_ind_select_var_set:
        row[var]=0 
    

    # populate payer if in risk list
    if row['tmx_payer_tmx_reason_code'] != '':
        
        payer_rc_list_in_row = row['tmx_payer_tmx_reason_code'].replace('{','').replace('}','').split(',')
        for rc in payer_rc_list_in_row:
            if rc in payer_rc_var_set:
                if "payer_rc_ind"+rc in rc_ind_select_var_set:
                    row["payer_rc_ind"+rc]=1
            for str in str_detect_list:
                if str in rc:
                    if "payer_rc_ind"+str in rc_ind_select_var_set:
                        row["payer_rc_ind_"+str]=1


    
    # populate payee if in risk list
    if row['tmx_payee_tmx_reason_code'] != '':
        payee_rc_list_in_row = row['tmx_payee_tmx_reason_code'].replace('{','').replace('}','').split(',')
        for rc in payee_rc_list_in_row:
            if rc in payee_rc_var_set:
                if "payee_rc_ind"+rc in rc_ind_select_var_set:
                    row["payee_rc_ind"+rc]=1
            for str in str_detect_list:
                if str in rc:
                    if "payee_rc_ind"+str in rc_ind_select_var_set:
                        row["payee_rc_ind_"+str]=1
    
    return row


def rc_ind_addition(input_file,output_file):
    
    str_detect_list = ["_T_TOR","VPN","_PossibleVM","_Malware"]
    
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    header = incsv.fieldnames
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    # payer rc var list 
    payer_rc_var_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_rc_ind_payer.csv'
    payer_rc_var_file=open(payer_rc_var_filename,'rU')
    payer_rc_var_csv=csv.reader(payer_rc_var_file)
    payer_rc_var_list=[]
    for row in payer_rc_var_csv:
        payer_rc_var_list.append(row[0])
    payer_rc_var_set=set(payer_rc_var_list)
    
    # payee rc var list 
    payee_rc_var_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_rc_ind_payee.csv'
    payee_rc_var_file=open(payee_rc_var_filename,'rU')
    payee_rc_var_csv=csv.reader(payee_rc_var_file)
    payee_rc_var_list=[]
    for row in payee_rc_var_csv:
        payee_rc_var_list.append(row[0])
    payee_rc_var_set=set(payee_rc_var_list)
    
    # rc ind select var list 
    rc_ind_select_var_filename='/home/junhe/fraud_model/Results/Model_Results_Signal_Tmx_v2pmt_rc_ind/model_var_list_rc_ind_400.csv'
    rc_ind_select_var_file=open(rc_ind_select_var_filename,'rU')
    rc_ind_select_var_csv=csv.reader(rc_ind_select_var_file)
    rc_ind_select_var_list=[]
    for row in rc_ind_select_var_csv:
        rc_ind_select_var_list.append(row[0])
    rc_ind_select_var_set=set(rc_ind_select_var_list)
    
    
    header=header+rc_ind_select_var_list
    outcsv.writerow(header)
    
    nRow=0
    for row in incsv:
        
        row_rc_ind=rc_ind_addition_one_row(row,rc_ind_select_var_list,payer_rc_var_set,payee_rc_var_set,str_detect_list)
        
        outcsv.writerow([row_rc_ind[key] for key in header])
        
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows are processed for file:",input_file


def rc_ind_addition_helper(arg):
    rc_ind_addition(arg[0],arg[1])
    

work_dir = "/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/"

input_list = (
              [work_dir+"model_data_pmt_ins_ds_fc_imp_woe.csv.gz",work_dir+"model_data_pmt_ins_ds_fc_imp_woe_add_rc_ind.csv.gz"],
              [work_dir+"model_data_pmt_oos_ds_fc_imp_woe.csv.gz",work_dir+"model_data_pmt_oos_ds_fc_imp_woe_add_rc_ind.csv.gz"],
              [work_dir+"test_data_sept_pmt_ds_fc_imp_woe.csv.gz",work_dir+"test_data_sept_pmt_ds_fc_imp_woe_add_rc_ind.csv.gz"],
              [work_dir+"test_data_oct_pmt_ds_fc_imp_woe.csv.gz",work_dir+"test_data_oct_pmt_ds_fc_imp_woe_add_rc_ind.csv.gz"],
              [work_dir+"test_data_nov_pmt_ds_fc_imp_woe.csv.gz",work_dir+"test_data_nov_pmt_ds_fc_imp_woe_add_rc_ind.csv.gz"],
              [work_dir+"test_data_dec_pmt_ds_fc_imp_woe.csv.gz",work_dir+"test_data_dec_pmt_ds_fc_imp_woe_add_rc_ind.csv.gz"],
              )
            # Inputs: rc_ind_creation(input_file,output_file)
pool = Pool(processes=3)
pool.map(rc_ind_addition_helper, input_list)


csv_EDD(work_dir+"model_data_pmt_ins_ds_fc_imp_woe_add_rc_ind.csv.gz")

'''
#===============================================================================
# Old non-parallel
#===============================================================================
input_file=work_dir+"model_data_pmt_ins_ds_fc_imp_woe.csv.gz"
output_file=work_dir+"model_data_pmt_ins_ds_fc_imp_woe_add_rc_ind.csv.gz"
rc_ind_addition(input_file,output_file)
'''
















